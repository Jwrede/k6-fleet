package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jwrede/k6-fleet/internal/report"
	"github.com/Jwrede/k6-fleet/internal/transport"
)

var binDir string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "k6fleet-test-")
	if err != nil {
		panic(err)
	}
	binDir = dir
	for out, pkg := range map[string]string{"k6-fleet": "../../cmd/k6-fleet", "fakek6": "../fakek6"} {
		if b, err := exec.Command("go", "build", "-o", filepath.Join(dir, out), pkg).CombinedOutput(); err != nil {
			panic(string(b))
		}
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type result struct {
	code   int
	err    error
	out    string
	stderr string
	sum    report.Data
	events []string
	hasSum bool
}

func (r result) count(substr string) int {
	n := 0
	for _, e := range r.events {
		if strings.Contains(e, substr) {
			n++
		}
	}
	return n
}

func (r result) countSuffix(suffix string) int {
	n := 0
	for _, e := range r.events {
		if strings.HasSuffix(e, suffix) {
			n++
		}
	}
	return n
}

type cfg map[string]any

func runFake(t *testing.T, c cfg, n int, tweak func(*Options)) result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	return runFakeCtx(t, ctx, c, n, tweak)
}

func runFakeCtx(t *testing.T, ctx context.Context, c cfg, n int, tweak func(*Options)) result {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("TMPDIR", t.TempDir()) // agents killed on purpose leave their work dir
	log := filepath.Join(dir, "events.log")
	c["eventLog"] = log
	if _, ok := c["totalDuration"]; !ok {
		c["totalDuration"] = "30s"
	}
	script := filepath.Join(dir, "script.json")
	b, _ := json.Marshal(c)
	if err := os.WriteFile(script, b, 0o644); err != nil {
		t.Fatal(err)
	}
	k6 := filepath.Join(binDir, "fakek6")
	var agents []transport.Spec
	for i := 0; i < n; i++ {
		agents = append(agents, transport.Spec{Name: fmt.Sprintf("a%d", i), Transport: "local", K6: k6})
	}
	sum := filepath.Join(dir, "summary.json")
	var out, errb bytes.Buffer
	o := Options{
		Script: script, K6: k6, Agents: agents, Self: filepath.Join(binDir, "k6-fleet"),
		Summary: sum, Stdout: &out, Stderr: &syncWriter{w: &errb}, StartDelay: 300 * time.Millisecond,
	}
	if tweak != nil {
		tweak(&o)
	}
	code, err := Run(ctx, o)
	r := result{code: code, err: err, out: out.String(), stderr: errb.String()}
	if b, e := os.ReadFile(sum); e == nil {
		r.hasSum = true
		if e := json.Unmarshal(b, &r.sum); e != nil {
			t.Fatal(e)
		}
	}
	if b, e := os.ReadFile(log); e == nil {
		r.events = strings.Split(strings.TrimSpace(string(b)), "\n")
	}
	return r
}

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func TestThreeAgentsActLikeOneTest(t *testing.T) {
	r := runFake(t, cfg{
		"iterations": 301, "setupData": map[string]any{"token": "abc"},
		"thresholds": map[string]any{"http_req_duration": []string{"p(95)<100", "avg<50"}, "checks": []string{"rate>0.99"}},
	}, 3, nil)
	if r.code != 0 || r.err != nil {
		t.Fatalf("code %d err %v\n%s\n%s", r.code, r.err, r.out, r.stderr)
	}
	if got := r.sum.Metrics["iterations"].Values["count"]; got != 301 {
		t.Errorf("iterations %v want 301: segments must add up exactly", got)
	}
	if r.count(" setup") != 1 || r.count(" teardown") != 1 {
		t.Errorf("setup and teardown must run once:\n%s", strings.Join(r.events, "\n"))
	}
	if r.count(`setdata {"token":"abc"}`) != 2 {
		t.Errorf("setup data must reach the other two agents:\n%s", strings.Join(r.events, "\n"))
	}
	for _, flag := range []string{"--no-thresholds", "--no-setup", "--linger"} {
		if r.count(flag) != 3 {
			t.Errorf("%s missing on some agent", flag)
		}
	}
	if r.sum.Metrics["vus"].Values["max"] != 6 {
		t.Errorf("vus must be summed: %v", r.sum.Metrics["vus"].Values)
	}
	if len(r.sum.Agents) != 3 || r.sum.Agents[1].Segment != "1/3:2/3" {
		t.Errorf("%+v", r.sum.Agents)
	}
	if !strings.Contains(r.out, "THRESHOLDS") {
		t.Error("summary missing")
	}
}

func TestResumeIsSynchronised(t *testing.T) {
	r := runFake(t, cfg{"iterations": 30}, 4, nil)
	re := regexp.MustCompile(`^instance=\d+ (\d+) resume$`)
	var times []int64
	for _, e := range r.events {
		if m := re.FindStringSubmatch(e); m != nil {
			v, _ := strconv.ParseInt(m[1], 10, 64)
			times = append(times, v)
		}
	}
	if len(times) != 4 {
		t.Fatalf("%d resumes\n%v", len(times), r.events)
	}
	lo, hi := times[0], times[0]
	for _, v := range times {
		lo, hi = min(lo, v), max(hi, v)
	}
	if hi-lo > 100 {
		t.Errorf("start skew %d ms", hi-lo)
	}
}

// The case that makes per-instance thresholds wrong. Two of three instances are
// fast, so each of them alone passes; merged, p(95) sits on the slow instance.
func TestThresholdSeesMergedData(t *testing.T) {
	r := runFake(t, cfg{
		"iterations": 300, "latency": map[string]int{"0": 10, "1": 10, "2": 500},
		"thresholds": map[string]any{"http_req_duration": []string{"p(95)<100"}},
	}, 3, nil)
	if r.code != ExitThresholds {
		t.Fatalf("code %d, want 99\n%s", r.code, r.out)
	}
	if a := r.sum.Thresholds[0].Actual; a < 490 || a > 520 {
		t.Errorf("merged p95 %v", a)
	}
}

func TestSubmetricThreshold(t *testing.T) {
	one := 1
	r := runFake(t, cfg{
		"iterations": 90, "failInstance": &one,
		"thresholds": map[string]any{"http_reqs{status:500}": []string{"count<10"}, "http_req_failed": []string{"rate<0.5"}},
	}, 3, nil)
	if r.code != ExitThresholds {
		t.Fatalf("code %d\n%s", r.code, r.out)
	}
	byseries := map[string]report.ThresholdResult{}
	for _, x := range r.sum.Thresholds {
		byseries[x.Series] = x
	}
	if x := byseries["http_reqs{status:500}"]; x.OK || x.Actual != 30 {
		t.Errorf("%+v", x)
	}
	if x := byseries["http_req_failed"]; !x.OK {
		t.Errorf("%+v", x)
	}
}

func TestAbortOnFailStopsEveryAgent(t *testing.T) {
	r := runFake(t, cfg{
		"iterations": 3000, "paceMs": 10, "latency": map[string]int{"2": 900},
		"thresholds": map[string]any{"http_req_duration": []any{map[string]any{"threshold": "max<100", "abortOnFail": true}}},
	}, 3, nil)
	if r.code != ExitThresholds {
		t.Fatalf("code %d\n%s\n%s", r.code, r.out, r.stderr)
	}
	if r.countSuffix(" stop") != 3 {
		t.Errorf("all agents must get the stop:\n%s", strings.Join(r.events, "\n"))
	}
	if got := r.sum.Metrics["iterations"].Values["count"]; got >= 3000 {
		t.Errorf("test ran to the end: %v", got)
	}
	if !strings.Contains(r.sum.Aborted, "max<100") {
		t.Errorf("aborted %q", r.sum.Aborted)
	}
	if r.count(" teardown") != 1 {
		t.Error("teardown must still run after an abort, as in k6")
	}
}

func TestAbortDelayIsRespected(t *testing.T) {
	r := runFake(t, cfg{
		"iterations": 90, "paceMs": 30, "latency": map[string]int{"0": 900},
		"thresholds": map[string]any{"http_req_duration": []any{map[string]any{"threshold": "max<100", "abortOnFail": true, "delayAbortEval": "1h"}}},
	}, 3, nil)
	if r.sum.Aborted != "" || r.sum.Metrics["iterations"].Values["count"] != 90 {
		t.Fatalf("%q %v", r.sum.Aborted, r.sum.Metrics["iterations"].Values)
	}
	if r.code != ExitThresholds {
		t.Fatalf("final evaluation must still fail: %d", r.code)
	}
}

func TestAgentLossAborts(t *testing.T) {
	one := 1
	r := runFake(t, cfg{"iterations": 900, "paceMs": 5, "killAgent": &one}, 3, nil)
	if r.code != ExitAgentLost {
		t.Fatalf("code %d\n%s\n%s", r.code, r.out, r.stderr)
	}
	if !r.sum.Agents[1].Lost {
		t.Errorf("%+v", r.sum.Agents)
	}
	time.Sleep(300 * time.Millisecond)
	if out, _ := exec.Command("pgrep", "-f", binDir+"/fakek6 run").Output(); len(out) > 0 {
		t.Errorf("k6 outlived its agent: %s", out)
	}
	if r.countSuffix(" stop") != 2 {
		t.Errorf("survivors must stop:\n%s", strings.Join(r.events, "\n"))
	}
}

func TestAgentLossContinue(t *testing.T) {
	one := 1
	r := runFake(t, cfg{"iterations": 300, "paceMs": 5, "killAgent": &one}, 3, func(o *Options) { o.OnLoss = "continue" })
	if r.code != 0 {
		t.Fatalf("code %d\n%s", r.code, r.stderr)
	}
	if len(r.sum.Warnings) == 0 || !strings.Contains(r.sum.Warnings[0], "totals are too low") {
		t.Errorf("%v", r.sum.Warnings)
	}
}

func TestScriptErrorBeforeStart(t *testing.T) {
	one := 1
	r := runFake(t, cfg{"iterations": 30, "crashEarly": &one}, 3, nil)
	if r.code != 107 {
		t.Fatalf("code %d err %v", r.code, r.err)
	}
	if r.err == nil || !strings.Contains(r.err.Error(), "fake script exception") {
		t.Errorf("k6 output must reach the user: %v", r.err)
	}
	if r.count("resume") != 0 {
		t.Error("nothing may start")
	}
}

func TestAgentExitCodePassesThrough(t *testing.T) {
	// Instance 2 has a third of the work paced like the others, but aborts when it
	// is done. The others have to be stopped, as exec.test.abort() would in one k6.
	r := runFake(t, cfg{"iterations": 30, "exitCode": map[string]int{"2": 108}}, 3, nil)
	if r.code != 108 {
		t.Fatalf("code %d", r.code)
	}
	if !strings.Contains(r.sum.Aborted, "did not end cleanly") {
		t.Errorf("aborted %q", r.sum.Aborted)
	}
}

func TestLegacyAbortWithoutExecutionResult(t *testing.T) {
	two := 2
	r := runFake(t, cfg{"iterations": 3000, "paceMs": 10, "exitCode": map[string]int{}, "legacyAbort": &two, "shortInstance": 2}, 3, nil)
	if r.code != 108 {
		t.Fatalf("code %d\n%s\n%s", r.code, r.out, r.stderr)
	}
	if got := r.sum.Metrics["iterations"].Values["count"]; got >= 3000 {
		t.Errorf("the other agents must be stopped, got %v iterations", got)
	}
}

func TestVersionMismatchIsRefused(t *testing.T) {
	dir := t.TempDir()
	wrap := filepath.Join(dir, "k6-old")
	script := fmt.Sprintf("#!/bin/sh\nFAKEK6_VERSION=v1.0.0 exec %s \"$@\"\n", filepath.Join(binDir, "fakek6"))
	if err := os.WriteFile(wrap, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	r := runFake(t, cfg{"iterations": 30}, 2, func(o *Options) { o.Agents[1].K6 = wrap })
	if r.err == nil || !strings.Contains(r.err.Error(), "different k6 versions") {
		t.Fatalf("code %d err %v", r.code, r.err)
	}
}

func TestWeights(t *testing.T) {
	r := runFake(t, cfg{"iterations": 400}, 3, func(o *Options) {
		o.Agents[0].Weight, o.Agents[1].Weight, o.Agents[2].Weight = 1, 2, 1
	})
	want := []float64{100, 200, 100}
	for i, a := range r.sum.Agents {
		if a.Iterations != want[i] {
			t.Errorf("agent %d: %v, want %v", i, a.Iterations, want[i])
		}
	}
}

func TestInterruptStopsAgents(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "e.log")
	script := filepath.Join(dir, "s.json")
	b, _ := json.Marshal(cfg{"iterations": 30000, "paceMs": 10, "eventLog": log, "totalDuration": "5m"})
	_ = os.WriteFile(script, b, 0o644)
	k6 := filepath.Join(binDir, "fakek6")
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(1500*time.Millisecond, cancel)
	code, _ := Run(ctx, Options{
		Script: script, K6: k6, Self: filepath.Join(binDir, "k6-fleet"), Stdout: io.Discard, Stderr: io.Discard,
		Agents: []transport.Spec{{Name: "a", K6: k6}, {Name: "b", K6: k6}}, StartDelay: 100 * time.Millisecond,
	})
	if code != ExitInterrupt {
		t.Fatalf("code %d", code)
	}
	ev, _ := os.ReadFile(log)
	if n := strings.Count(string(ev), " stop\n"); n != 2 {
		t.Fatalf("stops %d\n%s", n, ev)
	}
}

func TestMaxDurationStopsAHungTest(t *testing.T) {
	start := time.Now()
	r := runFake(t, cfg{"iterations": 30000, "paceMs": 10}, 2, func(o *Options) { o.MaxRun = 1500 * time.Millisecond })
	if time.Since(start) > 20*time.Second {
		t.Fatal("not stopped")
	}
	if r.code != ExitError || !strings.Contains(r.sum.Aborted, "timed out") {
		t.Fatalf("code %d aborted %q", r.code, r.sum.Aborted)
	}
}

func TestPrometheusEndpointDuringRun(t *testing.T) {
	var body string
	var wg sync.WaitGroup
	wg.Add(1)
	r := runFake(t, cfg{"iterations": 240, "paceMs": 20}, 2, func(o *Options) {
		o.Listen = "127.0.0.1:0"
		o.OnListen = func(addr string) {
			go func() {
				defer wg.Done()
				time.Sleep(2500 * time.Millisecond)
				resp, err := http.Get("http://" + addr + "/metrics")
				if err != nil {
					return
				}
				defer resp.Body.Close()
				b, _ := io.ReadAll(resp.Body)
				body = string(b)
			}()
		}
	})
	wg.Wait()
	if r.code != 0 {
		t.Fatalf("code %d", r.code)
	}
	if !strings.Contains(body, `k6_fleet_metric{name="http_reqs",stat="count"}`) {
		t.Fatalf("no live metrics:\n%s", body)
	}
}

func TestParseInventory(t *testing.T) {
	a, err := ParseInventory([]byte(`{"agents":[{"transport":"ssh","host":"lg1","weight":2},{"name":"d","transport":"docker","image":"grafana/k6:2.3.0"}]}`))
	if err != nil || len(a) != 2 || a[0].Name != "agent-0" || a[0].Weight != 2 {
		t.Fatalf("%+v %v", a, err)
	}
	for _, bad := range []string{`{"agents":[]}`, `{"agents":[{"nope":1}]}`, `{"agents":[{"name":"a"},{"name":"a"}]}`, `{"agents":[{"weight":-1}]}`} {
		if _, err := ParseInventory([]byte(bad)); err == nil {
			t.Errorf("%s must fail", bad)
		}
	}
}
