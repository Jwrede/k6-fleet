package controller

import (
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

func (p page) post(path string) (int, string) {
	p.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, "http://"+p.addr+path, nil)
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

func (p page) state(since int) apiState {
	p.t.Helper()
	var s apiState
	resp, err := http.Get(fmt.Sprintf("http://%s/api/state?since=%d", p.addr, since))
	if err != nil {
		return s
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
			p.t.Errorf("state is not JSON: %v", err)
		}
	}
	return s
}

func (p page) waitFor(phases ...string) apiState {
	p.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		s := p.state(0)
		for _, want := range phases {
			if s.Phase == want {
				return s
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	p.t.Errorf("phase never became %v, last state: %+v", phases, p.state(0))
	return apiState{}
}

// webRun runs the fake fleet with the page on and hands the page to drive.
// drive runs in the background; the run's context is cancelled when it returns,
// which ends the wait after a finished run.
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

func TestWebWaitsForStartAndKeepsTheResult(t *testing.T) {
	var ready, end apiState
	var logBefore string
	r := webRun(t, cfg{
		"iterations": 300, "paceMs": 20, "setupData": "x",
		"thresholds": map[string]any{"http_req_duration": []string{"p(95)<100"}},
	}, 2, nil, func(p page, events string) {
		ready = p.waitFor(phaseReady)
		time.Sleep(1200 * time.Millisecond) // a test that starts by itself would show now
		b, _ := os.ReadFile(events)
		logBefore = string(b)
		if code, body := p.post("/api/start"); code != http.StatusAccepted {
			t.Errorf("start: %d %s", code, body)
		}
		end = p.waitFor(phaseFinished, phaseFailed)
		if code, _ := p.post("/api/start"); code != http.StatusConflict && code != http.StatusServiceUnavailable {
			t.Errorf("second start: %d", code)
		}
		if tail := p.state(len(end.Timeline)); len(tail.Timeline) != 0 {
			t.Errorf("since must skip the points the page has: %d left", len(tail.Timeline))
		}
	})
	if r.code != 0 || r.err != nil {
		t.Fatalf("code %d err %v\n%s", r.code, r.err, r.stderr)
	}
	for _, s := range []string{" setup", " resume"} {
		if strings.Contains(logBefore, s) {
			t.Errorf("%q happened before Start:\n%s", s, logBefore)
		}
	}
	if len(ready.Agents) != 2 || ready.Agents[0].State != "ready" || ready.Agents[1].Segment != "1/2:1" {
		t.Errorf("agents while waiting: %+v", ready.Agents)
	}
	if end.Phase != phaseFinished || end.ExitCode == nil || *end.ExitCode != 0 {
		t.Errorf("end state: %+v", end)
	}
	if len(end.Thresholds) != 1 || !end.Thresholds[0].OK || end.Thresholds[0].Actual == 0 {
		t.Errorf("thresholds: %+v", end.Thresholds)
	}
	if len(end.Timeline) == 0 || end.Agents[0].Iterations+end.Agents[1].Iterations != 300 {
		t.Errorf("timeline %d points, agents %+v", len(end.Timeline), end.Agents)
	}
}

func TestWebStopEndsTheRunLikeAnInterrupt(t *testing.T) {
	var mid, end apiState
	r := webRun(t, cfg{"iterations": 2000, "paceMs": 20}, 2, nil, func(p page, _ string) {
		p.waitFor(phaseReady)
		p.post("/api/start")
		p.waitFor(phaseRunning)
		time.Sleep(2500 * time.Millisecond)
		mid = p.state(0)
		if code, body := p.post("/api/stop"); code != http.StatusAccepted {
			t.Errorf("stop: %d %s", code, body)
		}
		end = p.waitFor(phaseFinished, phaseFailed)
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
	if mid.Phase != phaseRunning || mid.Agents[0].State != "running" || mid.Agents[0].CPU < 0 || len(mid.Timeline) == 0 {
		t.Errorf("live state: %+v", mid)
	}
	if end.Aborted == "" || end.ExitCode == nil || *end.ExitCode != ExitInterrupt {
		t.Errorf("end state: %+v", end)
	}
}

func TestWebStopBeforeStartRunsNothing(t *testing.T) {
	var end apiState
	r := webRun(t, cfg{"iterations": 30}, 2, nil, func(p page, _ string) {
		p.waitFor(phaseReady)
		if code, body := p.post("/api/stop"); code != http.StatusAccepted {
			t.Errorf("stop: %d %s", code, body)
		}
		end = p.waitFor(phaseFinished, phaseFailed)
	})
	if r.code != ExitInterrupt {
		t.Fatalf("code %d", r.code)
	}
	if r.count(" setup") != 0 || r.count(" resume") != 0 {
		t.Errorf("nothing may run:\n%s", strings.Join(r.events, "\n"))
	}
	if end.Phase != phaseFailed || !strings.Contains(end.Error, "stopped from the web page") {
		t.Errorf("end state: %+v", end)
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

func TestWebFailedRunIsShown(t *testing.T) {
	var end apiState
	r := webRun(t, cfg{"iterations": 20, "crashEarly": 1}, 2, nil, func(p page, _ string) {
		end = p.waitFor(phaseFailed)
	})
	if r.code == 0 {
		t.Fatal("run must fail")
	}
	if !strings.Contains(end.Error, "before the test started") {
		t.Errorf("end state: %+v", end)
	}
}

func TestWebControlNeedsATokenOffLoopback(t *testing.T) {
	var noToken, wrong, right int
	var pageCode, stateCode int
	webRun(t, cfg{"iterations": 2000, "paceMs": 20}, 1, func(o *Options) {
		o.Web, o.WebToken = "0.0.0.0:0", "secret"
	}, func(p page, _ string) {
		p.waitFor(phaseReady)
		s := p.state(0)
		if !s.TokenRequired {
			t.Error("token_required must be set")
		}
		p.token = ""
		noToken, _ = p.post("/api/start")
		p.token = "nope"
		wrong, _ = p.post("/api/start")
		if resp, err := http.Get("http://" + p.addr + "/"); err == nil {
			pageCode = resp.StatusCode
			resp.Body.Close()
		}
		if resp, err := http.Get("http://" + p.addr + "/api/state"); err == nil {
			stateCode = resp.StatusCode
			resp.Body.Close()
		}
		p.token = "secret"
		right, _ = p.post("/api/stop")
		p.waitFor(phaseFailed)
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
		p.waitFor(phaseReady)
		code, _ = p.post("/api/stop")
		p.token = ""
	})
	if code != http.StatusUnauthorized {
		t.Errorf("stop without the generated token: %d", code)
	}
	if !regexp.MustCompile(`control token for the web page: [0-9a-f]{32}`).MatchString(r.stderr) {
		t.Errorf("token not printed:\n%s", r.stderr)
	}
}

func TestWebRejectsCrossSiteRequests(t *testing.T) {
	var noHeader, foreignHost, localName int
	webRun(t, cfg{"iterations": 10}, 1, nil, func(p page, _ string) {
		p.waitFor(phaseReady)
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
		p.post("/api/stop")
		p.waitFor(phaseFailed)
	})
	if noHeader != http.StatusForbidden || foreignHost != http.StatusForbidden || localName != http.StatusOK {
		t.Errorf("no header %d, foreign host %d, localhost %d", noHeader, foreignHost, localName)
	}
}

func TestWebPageHasNoExternalAssetsAndMetricsWork(t *testing.T) {
	if m := regexp.MustCompile(`(src|href)=|url\(|@import|https?://[^"]*`).FindAllString(string(webPage), -1); len(m) != 2 {
		// The only URL in the page is the SVG namespace, used twice.
		t.Errorf("external references: %q", m)
	}
	var body, csp string
	var metrics int
	webRun(t, cfg{"iterations": 10}, 1, nil, func(p page, _ string) {
		p.waitFor(phaseReady)
		resp, err := http.Get("http://" + p.addr + "/")
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			body, csp = string(b), resp.Header.Get("Content-Security-Policy")
		}
		if resp, err := http.Get("http://" + p.addr + "/metrics"); err == nil {
			metrics = resp.StatusCode
			resp.Body.Close()
		}
		p.post("/api/stop")
		p.waitFor(phaseFailed)
	})
	if !strings.Contains(body, `id="start"`) || !strings.Contains(csp, "default-src 'none'") {
		t.Errorf("page: %d bytes, CSP %q", len(body), csp)
	}
	if metrics != http.StatusOK {
		t.Errorf("/metrics: %d", metrics)
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
