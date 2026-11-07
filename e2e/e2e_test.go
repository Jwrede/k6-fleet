// Package e2e runs k6-fleet against real k6 and a real HTTP target. It needs k6
// in PATH and K6FLEET_E2E=1. Docker and ssh cases are switched on by
// K6FLEET_E2E_DOCKER_IMAGE and K6FLEET_E2E_SSH.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var fleetBin string

func TestMain(m *testing.M) {
	if os.Getenv("K6FLEET_E2E") == "" {
		fmt.Println("skipping: set K6FLEET_E2E=1 to run against real k6")
		os.Exit(0)
	}
	dir, err := os.MkdirTemp("", "k6fleet-e2e-")
	if err != nil {
		panic(err)
	}
	fleetBin = filepath.Join(dir, "k6-fleet")
	cmd := exec.Command("go", "build", "-o", fleetBin, "../cmd/k6-fleet")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		panic(string(out))
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// target records what the fleet actually did, independent of what it reports.
type target struct {
	mu       sync.Mutex
	hits     map[int]int
	tokens   map[string]int
	setups   int
	teardown int
	requests int
	srv      *httptest.Server
	url      string
}

func newTarget(t *testing.T) *target {
	t.Helper()
	tg := &target{hits: map[int]int{}, tokens: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/setup", func(w http.ResponseWriter, _ *http.Request) {
		tg.mu.Lock()
		tg.setups++
		tg.mu.Unlock()
		fmt.Fprint(w, "tok-123")
	})
	mux.HandleFunc("/teardown", func(_ http.ResponseWriter, r *http.Request) {
		tg.mu.Lock()
		tg.teardown++
		tg.tokens["teardown:"+r.URL.Query().Get("token")]++
		tg.mu.Unlock()
	})
	mux.HandleFunc("/hit", func(w http.ResponseWriter, r *http.Request) {
		i, _ := strconv.Atoi(r.URL.Query().Get("i"))
		tg.mu.Lock()
		tg.hits[i]++
		tg.tokens[r.URL.Query().Get("token")]++
		tg.requests++
		tg.mu.Unlock()
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/ok", func(w http.ResponseWriter, _ *http.Request) {
		tg.mu.Lock()
		tg.requests++
		tg.mu.Unlock()
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/fail", func(w http.ResponseWriter, _ *http.Request) {
		tg.mu.Lock()
		tg.requests++
		tg.mu.Unlock()
		w.WriteHeader(500)
	})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tg.srv = httptest.NewUnstartedServer(mux)
	tg.srv.Listener.Close()
	tg.srv.Listener = l
	tg.srv.Start()
	tg.url = tg.srv.URL
	t.Cleanup(tg.srv.Close)
	return tg
}

type summary struct {
	ExitCode int    `json:"exit_code"`
	Aborted  string `json:"aborted"`
	Metrics  map[string]struct {
		Values map[string]float64 `json:"values"`
	} `json:"metrics"`
	Thresholds []struct {
		Series, Source string
		OK             bool
		Actual         float64
	} `json:"thresholds"`
	Checks []struct {
		Group, Name string
		Passes      uint64 `json:"passes"`
		Fails       uint64 `json:"fails"`
	} `json:"checks"`
	Agents []struct {
		Name       string
		Iterations float64
		ExitCode   int `json:"exit_code"`
	} `json:"agents"`
	Warnings []string `json:"warnings"`
}

func fleet(t *testing.T, tg *target, script string, flags ...string) (int, summary, string) {
	t.Helper()
	dir := t.TempDir()
	sum := filepath.Join(dir, "summary.json")
	args := append([]string{"run", "--summary-export", sum}, flags...)
	args = append(args, filepath.Join("scripts", script), "--", "-e", "BASE="+tg.url)
	cmd := exec.Command(fleetBin, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	code := 0
	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
	case <-time.After(150 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("k6-fleet hung:\n%s", out.String())
	}
	var s summary
	if b, err := os.ReadFile(sum); err == nil {
		if err := json.Unmarshal(b, &s); err != nil {
			t.Fatal(err)
		}
	}
	t.Log(out.String())
	return code, s, out.String()
}

func TestSharedIterationsAcrossThreeAgents(t *testing.T) {
	tg := newTarget(t)
	code, s, out := fleet(t, tg, "shared.js", "--local", "3")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if len(tg.hits) != 1000 {
		t.Errorf("target saw %d distinct iterations, want 1000", len(tg.hits))
	}
	for i, n := range tg.hits {
		if n != 1 {
			t.Errorf("iteration %d ran %d times", i, n)
		}
	}
	if tg.setups != 1 || tg.teardown != 1 {
		t.Errorf("setup %d teardown %d, want 1 and 1", tg.setups, tg.teardown)
	}
	if tg.tokens["tok-123"] != 1000 || tg.tokens["teardown:tok-123"] != 1 {
		t.Errorf("setup data did not reach every VU and teardown: %v", tg.tokens)
	}
	if got := s.Metrics["iterations"].Values["count"]; got != 1000 {
		t.Errorf("reported iterations %v", got)
	}
	if got := s.Metrics["http_reqs"].Values["count"]; got != 1002 {
		t.Errorf("reported requests %v, want 1000 + setup + teardown", got)
	}
	d := s.Metrics["deterministic"].Values
	if d["min"] != 0 || d["max"] != 999 || d["avg"] != 499.5 {
		t.Errorf("deterministic trend: %v", d)
	}
	if p := d["p(95)"]; math.Abs(p-949.05)/949.05 > 0.005 {
		t.Errorf("p(95) %v outside 0.5%% of 949.05", p)
	}
	var total uint64
	for _, c := range s.Checks {
		if c.Fails != 0 {
			t.Errorf("%+v", c)
		}
		total += c.Passes
	}
	if total != 2000 {
		t.Errorf("checks %d", total)
	}
	var sum float64
	for _, a := range s.Agents {
		if a.Iterations == 0 {
			t.Errorf("agent %s did nothing", a.Name)
		}
		sum += a.Iterations
	}
	if sum != 1000 {
		t.Errorf("per-agent iterations add to %v", sum)
	}
}

// A single k6 process and the fleet must agree on everything that is exact.
func TestAgreesWithSingleK6(t *testing.T) {
	tg := newTarget(t)
	dir := t.TempDir()
	exp := filepath.Join(dir, "k6.json")
	cmd := exec.Command("k6", "run", "--quiet", "--no-usage-report", "--summary-export", exp, "-e", "BASE="+tg.url, "scripts/shared.js")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("plain k6: %v\n%s", err, out)
	}
	var single struct {
		Metrics map[string]map[string]any `json:"metrics"`
	}
	b, err := os.ReadFile(exp)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &single); err != nil {
		t.Fatal(err)
	}
	num := func(m, k string) float64 {
		v := single.Metrics[m][k]
		if vals, ok := single.Metrics[m]["values"].(map[string]any); ok {
			v = vals[k]
		}
		f, _ := v.(float64)
		return f
	}
	_, s, _ := fleet(t, newTarget(t), "shared.js", "--local", "3")
	for _, c := range [][2]string{{"iterations", "count"}, {"http_reqs", "count"}} {
		if a, b := num(c[0], c[1]), s.Metrics[c[0]].Values[c[1]]; a != b {
			t.Errorf("%s %s: k6 %v fleet %v", c[0], c[1], a, b)
		}
	}
	for _, k := range []string{"avg", "min", "max"} {
		if a, b := num("deterministic", k), s.Metrics["deterministic"].Values[k]; a != b {
			t.Errorf("deterministic %s: k6 %v fleet %v", k, a, b)
		}
	}
	for _, k := range []string{"p(90)", "p(95)"} {
		a, b := num("deterministic", k), s.Metrics["deterministic"].Values[k]
		if a == 0 || math.Abs(a-b)/a > 0.005 {
			t.Errorf("deterministic %s: k6 %v fleet %v", k, a, b)
		}
	}
}

func TestArrivalRateIsTotalNotPerAgent(t *testing.T) {
	tg := newTarget(t)
	code, s, out := fleet(t, tg, "arrival.js", "--local", "3")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if got := s.Metrics["iterations"].Values["count"]; got < 350 || got > 362 {
		t.Errorf("iterations %v, want about 360", got)
	}
	if tg.requests < 350 || tg.requests > 362 {
		t.Errorf("target saw %d", tg.requests)
	}
}

func TestRampingVUsAddUp(t *testing.T) {
	tg := newTarget(t)
	code, s, out := fleet(t, tg, "ramp.js", "--local", "3")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if got := s.Metrics["vus"].Values["max"]; got < 8 || got > 9 {
		t.Errorf("max vus %v, want 9 across agents", got)
	}
}

func TestThresholdFailureExits99(t *testing.T) {
	tg := newTarget(t)
	code, s, out := fleet(t, tg, "slow.js", "--local", "3")
	if code != 99 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if len(s.Thresholds) != 1 || s.Thresholds[0].OK || s.Thresholds[0].Actual < 290 {
		t.Errorf("%+v", s.Thresholds)
	}
	cmd := exec.Command("k6", "run", "--quiet", "--no-usage-report", "-e", "BASE="+tg.url, "scripts/slow.js")
	if err := cmd.Run(); err == nil || cmd.ProcessState.ExitCode() != 99 {
		t.Errorf("plain k6 disagrees: %v", err)
	}
}

func TestAbortOnFailStopsAllAgents(t *testing.T) {
	tg := newTarget(t)
	start := time.Now()
	code, s, out := fleet(t, tg, "abort.js", "--local", "3")
	if code != 99 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if d := time.Since(start); d > 30*time.Second {
		t.Errorf("took %v for a 60 s test that should abort at once", d)
	}
	if !strings.Contains(s.Aborted, "rate<0.1") {
		t.Errorf("aborted %q", s.Aborted)
	}
}

func TestScriptAbortEndsTheFleet(t *testing.T) {
	tg := newTarget(t)
	start := time.Now()
	code, _, out := fleet(t, tg, "testabort.js", "--local", "3")
	if code != 108 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if d := time.Since(start); d > 30*time.Second {
		t.Errorf("took %v", d)
	}
}

func TestBrokenScript(t *testing.T) {
	tg := newTarget(t)
	code, _, out := fleet(t, tg, "broken.js", "--local", "2")
	if code == 0 || !strings.Contains(out, "fails at init") {
		t.Fatalf("exit %d\n%s", code, out)
	}
}

func TestCLIOptionsReachTheAgents(t *testing.T) {
	tg := newTarget(t)
	dir := t.TempDir()
	script := filepath.Join(dir, "plain.js")
	src := "import http from 'k6/http';\nexport default function () { http.get(__ENV.BASE + '/ok'); }\n"
	if err := os.WriteFile(script, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	sum := filepath.Join(dir, "s.json")
	cmd := exec.Command(fleetBin, "run", "--local", "3", "--summary-export", sum, script,
		"--", "--vus", "6", "--iterations", "300", "-e", "BASE="+tg.url)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if tg.requests != 300 {
		t.Errorf("target saw %d, want 300", tg.requests)
	}
}

func TestReportAndLiveMetrics(t *testing.T) {
	tg := newTarget(t)
	dir := t.TempDir()
	html := filepath.Join(dir, "report.html")
	if keep := os.Getenv("K6FLEET_E2E_REPORT"); keep != "" {
		html = keep
	}
	port := freePort(t)
	var body string
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(4 * time.Second)
		resp, err := http.Get("http://127.0.0.1:" + port + "/metrics")
		if err != nil {
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		body = string(b)
	}()
	code, s, out := fleet(t, tg, "ramp.js", "--local", "3", "--report", html, "--listen", "127.0.0.1:"+port)
	<-done
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if !strings.Contains(body, `k6_fleet_metric{name="http_reqs",stat="count"}`) {
		t.Errorf("no live metrics during the run:\n%s", body)
	}
	page, err := os.ReadFile(html)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(page), "<polyline") {
		t.Error("report has no chart")
	}
	if s.Metrics["http_reqs"].Values["count"] == 0 {
		t.Error("empty summary")
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

// A killed agent must end the run with exit 3, and its k6 must not keep sending
// load. --on-agent-loss=abort is the default.
func TestKilledAgentAbortsAndTakesK6Along(t *testing.T) {
	tg := newTarget(t)
	dir := t.TempDir()
	sum := filepath.Join(dir, "s.json")
	cmd := exec.Command(fleetBin, "run", "--local", "3", "--summary-export", sum, "scripts/ramp.js", "--", "-e", "BASE="+tg.url, "--stage", "60s:3")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second)
	pids, err := exec.Command("pgrep", "-P", strconv.Itoa(cmd.Process.Pid)).Output()
	if err != nil {
		t.Fatalf("no agents: %v\n%s", err, out.String())
	}
	agents := strings.Fields(string(pids))
	if len(agents) != 3 {
		t.Fatalf("agents %v", agents)
	}
	k6pids, _ := exec.Command("pgrep", "-P", agents[1]).Output()
	k6pid := strings.TrimSpace(string(k6pids))
	if k6pid == "" {
		t.Fatal("agent has no k6 child")
	}
	if err := exec.Command("kill", "-9", agents[1]).Run(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	select {
	case <-waited:
	case <-time.After(40 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("run did not stop:\n%s", out.String())
	}
	if c := cmd.ProcessState.ExitCode(); c != 3 {
		t.Errorf("exit %d\n%s", c, out.String())
	}
	if _, err := os.Stat("/proc/" + k6pid); err == nil {
		t.Errorf("k6 %s outlived its agent", k6pid)
	}
}

type webAPI struct {
	Phase    string  `json:"phase"`
	Run      int     `json:"run"`
	VUs      float64 `json:"vus"`
	ExitCode *int    `json:"exit_code"`
	Requests []struct {
		Name     string `json:"name"`
		Requests uint64 `json:"requests"`
		Fails    uint64 `json:"fails"`
	} `json:"requests"`
	Failures []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	} `json:"failures"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
	Timeline []struct {
		VUs float64 `json:"vus"`
	} `json:"timeline"`
}

func webState(t *testing.T, port string) (s webAPI) {
	t.Helper()
	resp, err := http.Get("http://127.0.0.1:" + port + "/api/state")
	if err != nil {
		return s
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Errorf("state: %v", err)
	}
	return s
}

func webUntil(t *testing.T, port, what string, out *bytes.Buffer, ok func(webAPI) bool) webAPI {
	t.Helper()
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if s := webState(t, port); ok(s) {
			return s
		}
	}
	t.Fatalf("%s not reached: %+v\n%s", what, webState(t, port), out.String())
	return webAPI{}
}

func webPost(t *testing.T, port, action, body string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:"+port+"/api/"+action, strings.NewReader(body))
	req.Header.Set("X-K6-Fleet", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s: %d %s", action, resp.StatusCode, b)
	}
}

func startWeb(t *testing.T, port string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.Command(fleetBin, "run", "--local", "2", "--web", "127.0.0.1:"+port, "--summary-export", filepath.Join(dir, "s.json"), "scripts/web.js")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	webUntil(t, port, "the page", &out, func(s webAPI) bool { return s.Phase == "idle" })
	return cmd, &out
}

func stopWeb(t *testing.T, cmd *exec.Cmd, out *bytes.Buffer) int {
	t.Helper()
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	select {
	case <-waited:
	case <-time.After(20 * time.Second):
		t.Fatalf("page did not close on interrupt:\n%s", out.String())
	}
	return cmd.ProcessState.ExitCode()
}

// Everything the page does, checked at the target where possible: nothing
// arrives before Start, VUs follow the form and the edit, requests end with
// Stop, and a second test runs in the same process.
func TestWebControlsRealK6(t *testing.T) {
	tg := newTarget(t)
	port := freePort(t)
	cmd, out := startWeb(t, port)
	counts := func() (int, int, int) {
		tg.mu.Lock()
		defer tg.mu.Unlock()
		return tg.setups, tg.requests, tg.teardown
	}
	time.Sleep(2 * time.Second)
	if setups, requests, _ := counts(); setups != 0 || requests != 0 {
		t.Fatalf("before Start: %d setups, %d requests", setups, requests)
	}

	webPost(t, port, "start", fmt.Sprintf(`{"vus":4,"env":{"BASE":%q,"FAIL":"1"}}`, tg.url))
	vus := func(n float64) func(webAPI) bool { return func(s webAPI) bool { return s.Phase == "running" && s.VUs == n } }
	webUntil(t, port, "4 VUs", out, vus(4))
	s := webUntil(t, port, "request rows", out, func(s webAPI) bool { return len(s.Requests) == 3 && len(s.Failures) > 0 && len(s.Errors) > 0 })
	rows := map[string]uint64{}
	for _, r := range s.Requests {
		rows[r.Name] = r.Fails
	}
	if _, ok := rows["hit"]; !ok || rows["hit"] != 0 || rows["broken"] == 0 {
		t.Errorf("rows: %+v", s.Requests)
	}
	if s.Failures[0].Name != "broken" || s.Failures[0].Status != "500" {
		t.Errorf("failures: %+v", s.Failures)
	}
	if !strings.Contains(s.Errors[0].Message, "deliberate failure") {
		t.Errorf("errors: %+v", s.Errors)
	}
	resp, err := http.Get("http://127.0.0.1:" + port + "/api/logs")
	if err != nil {
		t.Fatal(err)
	}
	logs, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(logs), "first iteration done") {
		t.Errorf("logs: %s", logs)
	}

	webPost(t, port, "vus", `{"vus":9}`)
	webUntil(t, port, "9 VUs", out, vus(9))
	webPost(t, port, "vus", `{"vus":3}`)
	webUntil(t, port, "3 VUs", out, vus(3))

	webPost(t, port, "stop", ``)
	end := webUntil(t, port, "the end of run 1", out, func(s webAPI) bool { return s.Phase == "finished" })
	_, stopped, teardowns := counts()
	time.Sleep(2 * time.Second)
	if _, later, _ := counts(); later != stopped {
		t.Errorf("requests went on after the run ended: %d then %d", stopped, later)
	}
	if teardowns != 1 {
		t.Errorf("teardown ran %d times", teardowns)
	}
	if end.ExitCode == nil || *end.ExitCode != 105 {
		t.Errorf("state after Stop: %+v", end)
	}

	webPost(t, port, "start", fmt.Sprintf(`{"duration":"3s","env":{"BASE":%q}}`, tg.url))
	second := webUntil(t, port, "the end of run 2", out, func(s webAPI) bool { return s.Run == 2 && s.Phase == "finished" })
	if second.ExitCode == nil || *second.ExitCode != 0 || len(second.Requests) != 2 || second.Requests[0].Fails != 0 {
		t.Errorf("second run: %+v", second)
	}
	if setups, _, teardowns := counts(); setups != 2 || teardowns != 2 {
		t.Errorf("%d setups, %d teardowns over two runs", setups, teardowns)
	}
	if c := stopWeb(t, cmd, out); c != 0 {
		t.Errorf("exit %d, want that of the last run\n%s", c, out.String())
	}
}

// Drives the page itself in Chrome against real k6.
func TestWebPageInBrowser(t *testing.T) {
	chrome := os.Getenv("K6FLEET_E2E_CHROME")
	if chrome == "" {
		t.Skip("set K6FLEET_E2E_CHROME to the Chrome binary")
	}
	tg := newTarget(t)
	port := freePort(t)
	cmd, out := startWeb(t, port)
	ui := exec.Command("node", "ui/ui.mjs", "http://127.0.0.1:"+port+"/", chrome, tg.url)
	ui.Env = append(os.Environ(), "UI_SHOTS="+os.Getenv("K6FLEET_E2E_SHOTS"))
	b, err := ui.CombinedOutput()
	t.Log(string(b))
	if err != nil {
		t.Errorf("browser test: %v\n%s", err, out.String())
	}
	stopWeb(t, cmd, out)
}

// Logs what the sample pipeline costs and where. There is no assertion, because
// shared CI runners are too noisy for one; the README quotes the logged numbers.
func TestOverheadAgainstPlainK6(t *testing.T) {
	tg := newTarget(t)
	dir := t.TempDir()
	script := filepath.Join(dir, "load.js")
	src := "import http from 'k6/http';\nexport const options = { vus: 20, duration: '10s' };\nexport default function () { http.get(__ENV.BASE + '/ok'); }\n"
	if err := os.WriteFile(script, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	plain := func(label string, extra ...string) {
		exp := filepath.Join(dir, "plain.json")
		args := append([]string{"run", "--quiet", "--no-usage-report", "--summary-export", exp, "-e", "BASE=" + tg.url}, extra...)
		if out, err := exec.Command("k6", append(args, script)...).CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", label, err, out)
		}
		var r struct {
			Metrics map[string]map[string]any `json:"metrics"`
		}
		b, err := os.ReadFile(exp)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &r); err != nil {
			t.Fatal(err)
		}
		t.Logf("OVERHEAD %-34s reqs/s=%.0f", label, r.Metrics["http_reqs"]["rate"])
	}
	fleetRun := func(label string, extra ...string) {
		sum := filepath.Join(dir, "fleet.json")
		args := append([]string{"run", "--local", "1", "--quiet", "--summary-export", sum, script, "--", "-e", "BASE=" + tg.url}, extra...)
		if out, err := exec.Command(fleetBin, args...).CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", label, err, out)
		}
		var s summary
		b, err := os.ReadFile(sum)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &s); err != nil {
			t.Fatal(err)
		}
		t.Logf("OVERHEAD %-34s reqs/s=%.0f", label, s.Metrics["http_reqs"].Values["rate"])
	}
	tags := "--system-tags=status,name,check,group,scenario"
	plain("plain k6")
	plain("k6 --out json=/dev/null", "--out", "json=/dev/null")
	plain("k6 --out json + few system tags", "--out", "json=/dev/null", tags)
	plain("k6 --out csv=/dev/null", "--out", "csv=/dev/null")
	fleetRun("k6-fleet, one agent")
	fleetRun("k6-fleet, one agent, few tags", tags)
}

func inventory(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{"agents": v})
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "fleet.json")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDockerTransport(t *testing.T) {
	image := os.Getenv("K6FLEET_E2E_DOCKER_IMAGE")
	if image == "" {
		t.Skip("K6FLEET_E2E_DOCKER_IMAGE not set")
	}
	tg := newTarget(t)
	inv := inventory(t, []map[string]any{
		{"name": "d0", "transport": "docker", "image": image, "network": "host"},
		{"name": "d1", "transport": "docker", "image": image, "network": "host", "weight": 2},
	})
	code, s, out := fleet(t, tg, "shared.js", "--inventory", inv)
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if len(tg.hits) != 1000 || s.Metrics["iterations"].Values["count"] != 1000 {
		t.Errorf("hits %d summary %v", len(tg.hits), s.Metrics["iterations"].Values)
	}
	if a, b := s.Agents[0].Iterations, s.Agents[1].Iterations; a < 300 || a > 370 || b < 630 || b > 700 {
		t.Errorf("weights not applied: %v %v", a, b)
	}
}

func TestSSHTransport(t *testing.T) {
	hosts := os.Getenv("K6FLEET_E2E_SSH")
	if hosts == "" {
		t.Skip("K6FLEET_E2E_SSH not set")
	}
	tg := newTarget(t)
	code, s, out := fleet(t, tg, "shared.js", "--ssh", hosts)
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if len(tg.hits) != 1000 || s.Metrics["iterations"].Values["count"] != 1000 {
		t.Errorf("hits %d summary %v", len(tg.hits), s.Metrics["iterations"].Values)
	}
	if len(s.Agents) != len(strings.Split(hosts, ",")) {
		t.Errorf("%d agents", len(s.Agents))
	}
}
