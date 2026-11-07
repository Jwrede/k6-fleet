package controller

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// page drives the HTTP API like the browser page does.
type page struct {
	t     *testing.T
	addr  string
	token string
}

func (p page) post(path, body string) (int, string) {
	p.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, "http://"+p.addr+path, strings.NewReader(body))
	req.Header.Set(webHeader, "1")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		p.t.Error(err)
		return 0, ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (p page) mustPost(path, body string) {
	p.t.Helper()
	if code, text := p.post(path, body); code != http.StatusAccepted {
		p.t.Errorf("%s: %d %s", path, code, text)
	}
}

func (p page) get(path string) (int, string, http.Header) {
	p.t.Helper()
	resp, err := http.Get("http://" + p.addr + path)
	if err != nil {
		p.t.Error(err)
		return 0, "", nil
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func (p page) state() apiState {
	p.t.Helper()
	var s apiState
	resp, err := http.Get("http://" + p.addr + "/api/state")
	if err != nil {
		return s
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		p.t.Errorf("state is not JSON: %v", err)
	}
	return s
}

func (p page) waitUntil(what string, ok func(apiState) bool) apiState {
	p.t.Helper()
	var s apiState
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if s = p.state(); ok(s) {
			return s
		}
	}
	p.t.Errorf("never reached %s, last state: %+v", what, s)
	return s
}

func (p page) waitFor(phases ...string) apiState {
	p.t.Helper()
	return p.waitUntil(strings.Join(phases, ","), func(s apiState) bool {
		for _, want := range phases {
			if s.Phase == want {
				return true
			}
		}
		return false
	})
}

// webRun runs the fake fleet with the page on and hands the page to drive.
// drive runs in the background; the run's context is cancelled when it returns,
// which ends the process like Ctrl-C.
func webRun(t *testing.T, c cfg, n int, tweak func(*Options), drive func(page, string)) result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	finished := make(chan struct{})
	r := runFakeCtx(t, ctx, c, n, func(o *Options) {
		o.Web = "127.0.0.1:0"
		if tweak != nil {
			tweak(o)
		}
		events := filepath.Join(filepath.Dir(o.Script), "events.log")
		o.OnWeb = func(addr string) {
			go func() {
				defer close(finished)
				defer cancel()
				drive(page{t: t, addr: addr, token: o.WebToken}, events)
			}()
		}
	})
	<-finished
	return r
}

func TestWebStartsFromTheFormAndRunsAgain(t *testing.T) {
	var idle, first, second apiState
	var logBefore string
	r := webRun(t, cfg{
		"iterations": 200, "paceMs": 10, "setupData": "x", "names": []string{"/a", "/b"},
		"thresholds": map[string]any{"http_req_duration": []string{"p(95)<100"}},
	}, 2, nil, func(p page, events string) {
		idle = p.state()
		time.Sleep(1200 * time.Millisecond) // a test that starts by itself would show now
		b, _ := os.ReadFile(events)
		logBefore = string(b)
		p.mustPost("/api/start", `{}`)
		first = p.waitFor(phaseFinished, phaseFailed)
		p.mustPost("/api/start", `{"env":{"RUN":"two"}}`)
		p.waitUntil("run 2", func(s apiState) bool { return s.Run == 2 })
		second = p.waitFor(phaseFinished, phaseFailed)
	})
	if r.code != 0 || r.err != nil {
		t.Fatalf("code %d err %v\n%s", r.code, r.err, r.stderr)
	}
	if logBefore != "" {
		t.Errorf("k6 ran before Start:\n%s", logBefore)
	}
	if idle.Phase != phaseIdle || len(idle.Agents) != 2 || idle.Agents[0].State != "idle" || idle.Defaults.Duration != "30s" {
		t.Errorf("idle state: %+v", idle)
	}
	if first.Phase != phaseFinished || first.ExitCode == nil || *first.ExitCode != 0 || first.Run != 1 {
		t.Errorf("end state: %+v", first)
	}
	if len(first.Thresholds) != 1 || !first.Thresholds[0].OK || len(first.Timeline) == 0 {
		t.Errorf("thresholds %+v, %d points", first.Thresholds, len(first.Timeline))
	}
	rows := first.Requests
	if len(rows) != 3 || rows[0].Name != "/a" || rows[1].Name != "/b" || rows[2].Name != "Aggregated" {
		t.Fatalf("request rows: %+v", rows)
	}
	if rows[0].Requests != 100 || rows[1].Requests != 100 || rows[2].Requests != 200 || rows[0].Med == 0 {
		t.Errorf("request counts: %+v", rows)
	}
	if second.Run != 2 || second.Requests[2].Requests != 200 {
		t.Errorf("the second run must start from zero: %+v", second.Requests)
	}
	if r.count(`env {"RUN":"two"}`) != 2 {
		t.Errorf("form variables must reach both agents:\n%s", strings.Join(r.events, "\n"))
	}
	if r.count(" setup") != 2 || r.count(" teardown") != 2 {
		t.Errorf("each run has one setup and one teardown:\n%s", strings.Join(r.events, "\n"))
	}
}

func TestWebStopEndsTheRunLikeAnInterrupt(t *testing.T) {
	var mid, end apiState
	r := webRun(t, cfg{"iterations": 2000, "paceMs": 20}, 2, nil, func(p page, _ string) {
		p.mustPost("/api/start", ``)
		p.waitFor(phaseRunning)
		time.Sleep(2500 * time.Millisecond)
		mid = p.state()
		p.mustPost("/api/stop", ``)
		end = p.waitFor(phaseFinished, phaseFailed)
		if code, _ := p.post("/api/stop", ``); code != http.StatusConflict {
			t.Errorf("stop after the end: %d", code)
		}
	})
	if r.code != ExitInterrupt {
		t.Fatalf("code %d, want %d\n%s", r.code, ExitInterrupt, r.stderr)
	}
	if !r.hasSum || r.sum.Metrics["iterations"].Values["count"] >= 2000 {
		t.Errorf("summary must exist and show a shortened run: %v", r.sum.Metrics["iterations"])
	}
	if r.count(" teardown") != 1 {
		t.Errorf("teardown must still run once:\n%s", strings.Join(r.events, "\n"))
	}
	if mid.Phase != phaseRunning || mid.Agents[0].State != "running" || mid.Agents[0].VUs != 2 || mid.VUs != 4 || mid.RPS == 0 {
		t.Errorf("live state: %+v", mid)
	}
	if mid.Agents[0].MemBytes == 0 {
		t.Error("k6 memory must be reported")
	}
	if end.Aborted == "" || end.ExitCode == nil || *end.ExitCode != ExitInterrupt {
		t.Errorf("end state: %+v", end)
	}
}

func TestWebChangesVUsDuringARun(t *testing.T) {
	t.Setenv("FAKEK6_VERSION", "v1.8.2")
	var before, after apiState
	r := webRun(t, cfg{"iterations": 10, "paceMs": 20}, 2, nil, func(p page, _ string) {
		p.mustPost("/api/start", `{"vus":4}`)
		p.waitFor(phaseRunning)
		before = p.waitUntil("4 VUs", func(s apiState) bool { return s.VUs == 4 })
		p.mustPost("/api/vus", `{"vus":10}`)
		after = p.waitUntil("10 VUs", func(s apiState) bool { return s.VUs == 10 })
		p.mustPost("/api/vus", `{"vus":6}`)
		p.waitUntil("6 VUs", func(s apiState) bool { return s.VUs == 6 })
		p.mustPost("/api/stop", ``)
		p.waitFor(phaseFinished, phaseFailed)
	})
	if r.code != ExitInterrupt {
		t.Fatalf("code %d\n%s", r.code, r.stderr)
	}
	if !before.Adjustable || before.Planned != "0s" {
		t.Errorf("state: %+v", before)
	}
	if after.TargetVUs != 10 || after.Agents[0].VUs != 5 {
		t.Errorf("state after the change: %+v", after)
	}
	if r.count("scenario externally-controlled vus=4 max=4 duration=0s") != 2 {
		t.Errorf("scenario must reach both agents:\n%s", strings.Join(r.events, "\n"))
	}
	if r.count("vus=10 max=10") != 2 || r.count("vus=6 max=10") != 2 {
		t.Errorf("every agent gets the test total, and max never shrinks:\n%s", strings.Join(r.events, "\n"))
	}
}

func TestWebRefusesVUsForScriptScenarios(t *testing.T) {
	var code int
	var text string
	webRun(t, cfg{"iterations": 400, "paceMs": 20}, 1, nil, func(p page, _ string) {
		p.mustPost("/api/start", ``)
		p.waitFor(phaseRunning)
		code, text = p.post("/api/vus", `{"vus":3}`)
		p.mustPost("/api/stop", ``)
		p.waitFor(phaseFinished, phaseFailed)
	})
	if code != http.StatusConflict || !strings.Contains(text, "can change only") {
		t.Errorf("%d %s", code, text)
	}
}

func TestWebFixesVUsOnK6Two(t *testing.T) {
	var noDuration int
	var live bool
	var state apiState
	r := webRun(t, cfg{"iterations": 200, "paceMs": 20}, 2, nil, func(p page, _ string) {
		live = p.state().Defaults.LiveVUs
		noDuration, _ = p.post("/api/start", `{"vus":4}`)
		p.mustPost("/api/start", `{"vus":4,"duration":"2s"}`)
		state = p.waitFor(phaseRunning)
		if code, _ := p.post("/api/vus", `{"vus":8}`); code != http.StatusConflict {
			t.Errorf("edit on k6 2.x: %d", code)
		}
		p.waitFor(phaseFinished, phaseFailed)
	})
	if live || state.Adjustable {
		t.Errorf("k6 2.x has no live VUs: %v %+v", live, state)
	}
	if noDuration != http.StatusBadRequest {
		t.Errorf("VUs without a duration: %d", noDuration)
	}
	if r.count(`env {"duration":"2s","vus":"4"}`) != 2 {
		t.Errorf("flags must reach k6 archive:\n%s", strings.Join(r.events, "\n"))
	}
}

func TestWebDurationFromTheFormEndsTheRun(t *testing.T) {
	t.Setenv("FAKEK6_VERSION", "v1.8.2")
	var end apiState
	r := webRun(t, cfg{"iterations": 10, "paceMs": 20}, 2, nil, func(p page, _ string) {
		p.mustPost("/api/start", `{"vus":2,"duration":"2s"}`)
		end = p.waitFor(phaseFinished, phaseFailed)
	})
	if r.code != 0 || end.Phase != phaseFinished || end.Planned != "2s" {
		t.Errorf("code %d state %+v\n%s", r.code, end, r.stderr)
	}
}

func TestWebResetClearsOnlyTheRequestStatistics(t *testing.T) {
	var after apiState
	r := webRun(t, cfg{"iterations": 400, "paceMs": 10}, 1, nil, func(p page, _ string) {
		p.mustPost("/api/start", ``)
		p.waitUntil("requests", func(s apiState) bool { return len(s.Requests) > 0 && s.Requests[0].Requests > 50 })
		p.mustPost("/api/reset", ``)
		after = p.state()
		p.waitFor(phaseFinished, phaseFailed)
	})
	if r.code != 0 {
		t.Fatalf("code %d", r.code)
	}
	if len(after.Requests) > 0 && after.Requests[0].Requests > 50 {
		t.Errorf("not reset: %+v", after.Requests)
	}
	if got := r.sum.Metrics["http_reqs"].Values["count"]; got != 400 {
		t.Errorf("the summary must keep all requests, has %v", got)
	}
}

func TestWebFailuresErrorsAndLogs(t *testing.T) {
	var end apiState
	var logs struct {
		Lines []logLine `json:"lines"`
		Seq   int       `json:"seq"`
	}
	line := `time="2026-11-07T08:00:00Z" level=error msg="GoError: boom\n\tat default (file:///s.js:5:9)\n" executor=shared-iterations scenario=default source=stacktrace`
	webRun(t, cfg{
		"iterations": 60, "paceMs": 5, "failInstance": 1, "logs": []string{line, line, `time="2026-11-07T08:00:01Z" level=info msg="hello" source=console`},
	}, 2, nil, func(p page, _ string) {
		p.mustPost("/api/start", ``)
		end = p.waitFor(phaseFinished, phaseFailed)
		_, body, _ := p.get("/api/logs?since=0")
		if err := json.Unmarshal([]byte(body), &logs); err != nil {
			t.Error(err)
		}
		_, tail, _ := p.get(fmt.Sprintf("/api/logs?since=%d", logs.Seq))
		if !strings.Contains(tail, `"lines":[]`) {
			t.Errorf("since must skip known lines: %s", tail)
		}
	})
	if len(end.Failures) != 1 {
		t.Fatalf("failures: %+v", end.Failures)
	}
	f := end.Failures[0]
	if f.Count != 30 || f.Status != "500" || f.Method != "GET" || f.Name != "/hit" || f.Error != "Internal Server Error" {
		t.Errorf("failure row: %+v", f)
	}
	if rows := end.Requests; rows[len(rows)-1].Fails != 30 || end.FailRatio != 0.5 {
		t.Errorf("requests %+v, ratio %v", rows, end.FailRatio)
	}
	if len(end.Errors) != 1 || end.Errors[0].Count != 4 || len(end.Errors[0].Agents) != 2 || !strings.HasPrefix(end.Errors[0].Message, "GoError: boom\n\tat default") {
		t.Errorf("errors must group by message across agents: %+v", end.Errors)
	}
	levels := map[string]int{}
	for _, l := range logs.Lines {
		levels[l.Level]++
	}
	if levels["error"] != 4 || levels["info"] < 2 {
		t.Errorf("levels: %v\n%+v", levels, logs.Lines)
	}
}

func TestWebDownloads(t *testing.T) {
	files := map[string]string{}
	var before int
	webRun(t, cfg{
		"iterations": 200, "paceMs": 10, "names": []string{`/x,"y"`}, "failInstance": 0,
	}, 1, nil, func(p page, _ string) {
		before, _, _ = p.get("/download/requests.csv")
		p.mustPost("/api/start", ``)
		p.waitFor(phaseFinished, phaseFailed)
		for _, f := range []string{"requests.csv", "failures.csv", "timeline.csv", "errors.csv", "report.html", "summary.json", "logs.txt"} {
			code, body, h := p.get("/download/" + f)
			if code != http.StatusOK || !strings.Contains(h.Get("Content-Disposition"), "k6-fleet-"+f) {
				t.Errorf("%s: %d %v", f, code, h)
			}
			files[f] = body
		}
		if code, _, _ := p.get("/download/nope"); code != http.StatusNotFound {
			t.Errorf("unknown file: %d", code)
		}
	})
	if before != http.StatusNotFound {
		t.Errorf("before the first run: %d", before)
	}
	if !strings.Contains(files["requests.csv"], `GET,"/x,""y""",200,200,`) {
		t.Errorf("requests.csv:\n%s", files["requests.csv"])
	}
	if !strings.Contains(files["failures.csv"], "200,GET") || !strings.HasPrefix(files["timeline.csv"], "second,vus") {
		t.Errorf("failures:\n%s\ntimeline:\n%s", files["failures.csv"], files["timeline.csv"])
	}
	if !strings.Contains(files["report.html"], "<h2>Requests</h2>") || !strings.Contains(files["report.html"], "<h2>Failures</h2>") {
		t.Error("report lacks the request tables")
	}
	var sum struct {
		Requests []struct {
			Name string `json:"name"`
		} `json:"requests"`
	}
	if err := json.Unmarshal([]byte(files["summary.json"]), &sum); err != nil || len(sum.Requests) != 2 {
		t.Errorf("%v %+v", err, sum)
	}
	if !strings.Contains(files["logs.txt"], "controller info") {
		t.Errorf("logs.txt:\n%s", files["logs.txt"])
	}
}

func TestWebAutostart(t *testing.T) {
	var addr string
	r := runFake(t, cfg{"iterations": 20}, 2, func(o *Options) {
		o.Web, o.Autostart = "127.0.0.1:0", true
		o.OnWeb = func(a string) { addr = a }
	})
	if r.code != 0 {
		t.Fatalf("code %d %s", r.code, r.stderr)
	}
	if _, err := http.Get("http://" + addr + "/api/state"); err == nil {
		t.Error("the server must close when the run ends")
	}
}

func TestWebFailedRunIsShownAndTheNextOneWorks(t *testing.T) {
	var end apiState
	r := webRun(t, cfg{"iterations": 20, "crashEarly": 1}, 2, nil, func(p page, _ string) {
		p.mustPost("/api/start", ``)
		end = p.waitFor(phaseFailed)
		p.mustPost("/api/start", ``)
		p.waitUntil("run 2", func(s apiState) bool { return s.Run == 2 })
		p.waitFor(phaseFailed)
	})
	if r.code == 0 {
		t.Fatal("run must fail")
	}
	if !strings.Contains(end.Error, "before the test started") {
		t.Errorf("end state: %+v", end)
	}
}

func TestWebShowsABrokenScriptBeforeStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	script := filepath.Join(dir, "s.json")
	if err := os.WriteFile(script, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	var s apiState
	o := Options{
		Script: script, K6: filepath.Join(binDir, "fakek6"), Web: "127.0.0.1:0", Stdout: io.Discard, Stderr: io.Discard,
		OnWeb: func(addr string) {
			go func() {
				defer cancel()
				s = page{t: t, addr: addr}.state()
			}()
		},
	}
	if _, err := Run(ctx, o); err != nil {
		t.Fatal(err)
	}
	if s.Phase != phaseIdle || s.Error == "" {
		t.Errorf("state: %+v", s)
	}
}

func TestWebValidatesTheForm(t *testing.T) {
	webRun(t, cfg{"iterations": 10}, 1, nil, func(p page, _ string) {
		for _, body := range []string{`{"vus":-1}`, `{"duration":"soon"}`, `{"duration":"-5s"}`, `{"env":{"a b":"c"}}`, `{"env":{"--out":"x"}}`, `nonsense`} {
			if code, _ := p.post("/api/start", body); code != http.StatusBadRequest {
				t.Errorf("%s: %d", body, code)
			}
		}
		if code, _ := p.post("/api/stop", ``); code != http.StatusConflict {
			t.Errorf("stop with nothing running: %d", code)
		}
	})
}

func TestWebControlNeedsATokenOffLoopback(t *testing.T) {
	var noToken, wrong, right int
	var pageCode, stateCode int
	webRun(t, cfg{"iterations": 2000, "paceMs": 20}, 1, func(o *Options) {
		o.Web, o.WebToken = "0.0.0.0:0", "secret"
	}, func(p page, _ string) {
		if !p.state().TokenRequired {
			t.Error("token_required must be set")
		}
		p.token = ""
		noToken, _ = p.post("/api/start", ``)
		p.token = "nope"
		wrong, _ = p.post("/api/start", ``)
		pageCode, _, _ = p.get("/")
		stateCode, _, _ = p.get("/api/state")
		p.token = "secret"
		right, _ = p.post("/api/start", ``)
		p.waitFor(phaseRunning)
		p.mustPost("/api/stop", ``)
		p.waitFor(phaseFinished, phaseFailed)
	})
	if noToken != http.StatusUnauthorized || wrong != http.StatusUnauthorized || right != http.StatusAccepted {
		t.Errorf("no token %d, wrong %d, right %d", noToken, wrong, right)
	}
	if pageCode != http.StatusOK || stateCode != http.StatusOK {
		t.Errorf("page %d, state %d: viewing needs no token", pageCode, stateCode)
	}
}

func TestWebGeneratesATokenOffLoopback(t *testing.T) {
	var code int
	r := webRun(t, cfg{"iterations": 10}, 1, func(o *Options) { o.Web = "0.0.0.0:0" }, func(p page, _ string) {
		code, _ = p.post("/api/start", ``)
	})
	if code != http.StatusUnauthorized {
		t.Errorf("start without the generated token: %d", code)
	}
	if !regexp.MustCompile(`control token for the web page: [0-9a-f]{32}`).MatchString(r.stderr) {
		t.Errorf("token not printed:\n%s", r.stderr)
	}
}

func TestWebRejectsCrossSiteRequests(t *testing.T) {
	var noHeader, foreignHost, localName int
	webRun(t, cfg{"iterations": 10}, 1, nil, func(p page, _ string) {
		resp, err := http.Post("http://"+p.addr+"/api/start", "text/plain", nil)
		if err == nil {
			noHeader = resp.StatusCode
			resp.Body.Close()
		}
		req, _ := http.NewRequest(http.MethodPost, "http://"+p.addr+"/api/start", nil)
		req.Host = "rebound.example.com"
		req.Header.Set(webHeader, "1")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			foreignHost = resp.StatusCode
			resp.Body.Close()
		}
		req, _ = http.NewRequest(http.MethodGet, "http://"+p.addr+"/api/state", nil)
		req.Host = "localhost:8089"
		if resp, err := http.DefaultClient.Do(req); err == nil {
			localName = resp.StatusCode
			resp.Body.Close()
		}
	})
	if noHeader != http.StatusForbidden || foreignHost != http.StatusForbidden || localName != http.StatusOK {
		t.Errorf("no header %d, foreign host %d, localhost %d", noHeader, foreignHost, localName)
	}
}

func TestWebAssetsAreLocal(t *testing.T) {
	re := regexp.MustCompile(`https?://[^"\s]*|@import|url\(`)
	for _, f := range []string{"index.html", "app.js", "chart.js", "style.css"} {
		b, err := webFiles.ReadFile("web/" + f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range re.FindAllString(string(b), -1) {
			if m != "http://www.w3.org/2000/svg" {
				t.Errorf("%s refers to %q", f, m)
			}
		}
	}
	var csp string
	var pages = map[string]int{}
	webRun(t, cfg{"iterations": 10}, 1, nil, func(p page, _ string) {
		for _, f := range []string{"/", "/app.js", "/chart.js", "/style.css", "/metrics"} {
			code, _, h := p.get(f)
			pages[f] = code
			if f == "/" {
				csp = h.Get("Content-Security-Policy")
			}
		}
	})
	for f, code := range pages {
		if code != http.StatusOK {
			t.Errorf("%s: %d", f, code)
		}
	}
	if !strings.Contains(csp, "default-src 'none'") {
		t.Errorf("CSP %q", csp)
	}
}

func TestWebAddr(t *testing.T) {
	for in, want := range map[string]struct {
		addr string
		loop bool
	}{
		":8089":          {"127.0.0.1:8089", true},
		"127.0.0.1:8089": {"127.0.0.1:8089", true},
		"localhost:8089": {"localhost:8089", true},
		"[::1]:8089":     {"[::1]:8089", true},
		"0.0.0.0:8089":   {"0.0.0.0:8089", false},
		"10.0.0.5:8089":  {"10.0.0.5:8089", false},
	} {
		addr, loop, err := webAddr(in)
		if err != nil || addr != want.addr || loop != want.loop {
			t.Errorf("%s: %s %v %v", in, addr, loop, err)
		}
	}
	if _, _, err := webAddr("8089"); err == nil {
		t.Error("a bare number must fail")
	}
}

func TestScenarioRewritesTheArchive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.tar")
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	add := func(name, body string) {
		_ = w.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))})
		_, _ = w.Write([]byte(body))
	}
	add("metadata.json", `{"type":"js","options":{"vus":3,"duration":"5m","iterations":9,"stages":[],"thresholds":{"checks":["rate>0.9"]},"scenarios":{"old":{"executor":"constant-vus"}}}}`)
	add("data", "script body")
	_ = w.Close()
	if err := os.WriteFile(path, b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	s := externalScenario{VUs: 7, Duration: "1m0s"}
	if err := s.apply(path); err != nil {
		t.Fatal(err)
	}
	out, _ := os.ReadFile(path)
	r := tar.NewReader(bytes.NewReader(out))
	files := map[string]string{}
	for {
		h, err := r.Next()
		if err != nil {
			break
		}
		body, _ := io.ReadAll(r)
		files[h.Name] = string(body)
	}
	if files["data"] != "script body" {
		t.Errorf("other entries must survive: %v", files)
	}
	var meta struct {
		Type    string
		Options map[string]json.RawMessage
	}
	if err := json.Unmarshal([]byte(files["metadata.json"]), &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Type != "js" || meta.Options["thresholds"] == nil {
		t.Errorf("unrelated metadata must survive: %s", files["metadata.json"])
	}
	for _, k := range []string{"vus", "duration", "iterations", "stages"} {
		if meta.Options[k] != nil {
			t.Errorf("%s must go, it would conflict with scenarios", k)
		}
	}
	if got := string(meta.Options["scenarios"]); got != `{"fleet":{"duration":"1m0s","executor":"externally-controlled","maxVUs":7,"vus":7}}` {
		t.Errorf("scenarios: %s", got)
	}
}
