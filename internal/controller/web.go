package controller

import (
	"crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Jwrede/k6-fleet/internal/report"
)

// Phases of a run, as shown on the page.
const (
	phaseConnecting   = "connecting"
	phaseInitialising = "initialising"
	phaseReady        = "ready"
	phaseSetup        = "setup"
	phaseRunning      = "running"
	phaseStopping     = "stopping"
	phaseTeardown     = "teardown"
	phaseFinished     = "finished"
	phaseFailed       = "failed"
)

// webHeader must be present on control requests. A cross-origin page cannot set
// it without a CORS preflight, and the server answers none, so a website open
// in the operator's browser cannot start a test on localhost.
const webHeader = "X-K6-Fleet"

//go:embed web.html
var webPage []byte

const webCSP = "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'"

type command struct {
	name string
	done chan error
}

type liveAgent struct {
	report.AgentResult
	State string  `json:"state"`
	CPU   float64 `json:"cpu"`
	LagMs int64   `json:"lag_ms"`
}

// apiState is the answer of /api/state.
type apiState struct {
	Phase         string                   `json:"phase"`
	Error         string                   `json:"error,omitempty"`
	Script        string                   `json:"script"`
	K6Version     string                   `json:"k6_version"`
	Planned       string                   `json:"planned,omitempty"`
	ElapsedMs     int64                    `json:"elapsed_ms"`
	ExitCode      *int                     `json:"exit_code,omitempty"`
	Aborted       string                   `json:"aborted,omitempty"`
	TokenRequired bool                     `json:"token_required"`
	Agents        []liveAgent              `json:"agents"`
	Thresholds    []report.ThresholdResult `json:"thresholds"`
	Checks        []report.CheckResult     `json:"checks"`
	Warnings      []string                 `json:"warnings"`
	Timeline      []report.Point           `json:"timeline"` // from the point after ?since=
}

// liveState is an immutable snapshot, built on the event loop once per second
// and on every phase change. HTTP handlers read only this, so they share no
// state with the loop and many viewers cost no more than one.
type liveState struct {
	api  apiState
	data *report.Data
}

func (c *ctl) setPhase(p string) {
	c.phase = p
	c.publish()
}

func (a *agentState) state(started bool) string {
	switch {
	case a.lost:
		return "lost"
	case a.ended:
		return "ended"
	case a.hello == nil:
		return "connecting"
	case !a.ready:
		return "initialising"
	case started:
		return "running"
	}
	return "ready"
}

func (c *ctl) publish() {
	if c.web == nil && c.metrics == nil {
		return
	}
	d := c.snapshot(c.elapsed())
	d.Aborted = c.aborted
	d.Warnings = append([]string(nil), c.warnings...)
	c.store(d, nil, "")
}

func (c *ctl) store(d *report.Data, code *int, errText string) {
	s := &liveState{data: d, api: apiState{
		Phase: c.phase, Error: errText, Script: c.script, K6Version: c.k6, Planned: c.total,
		ElapsedMs: d.DurationMs, ExitCode: code, Aborted: d.Aborted, TokenRequired: c.token != "",
		Thresholds: d.Thresholds, Checks: d.Checks, Warnings: d.Warnings, Timeline: d.Timeline,
	}}
	for i, r := range d.Agents {
		a := c.agents[i]
		s.api.Agents = append(s.api.Agents, liveAgent{AgentResult: r, State: a.state(!c.started.IsZero()), CPU: a.lastCPU, LagMs: a.lastLag})
	}
	c.live.Store(s)
}

// publishEnd records the outcome after the agents are gone.
func (c *ctl) publishEnd(code int, err error) {
	if c.web == nil && c.metrics == nil {
		return
	}
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	c.phase = phaseFailed
	d := c.final
	if d != nil {
		c.phase = phaseFinished
	} else {
		d = c.snapshot(c.elapsed())
		d.Aborted = c.aborted
		d.Warnings = append([]string(nil), c.warnings...)
	}
	c.store(d, &code, errText)
}

// control runs on the event loop, like everything that changes a run.
func (c *ctl) control(name string) error {
	switch name {
	case "start":
		if c.phase != phaseReady {
			return fmt.Errorf("cannot start in phase %s", c.phase)
		}
		c.startNow = true
	case "stop":
		switch c.phase {
		case phaseConnecting, phaseInitialising, phaseReady, phaseSetup:
			// No VU has run yet, so there is nothing to finish: closing the agents
			// removes the paused k6 instances.
			c.fatalCode = ExitInterrupt
			c.fatal = errors.New("stopped from the web page")
		case phaseRunning:
			c.abort("stopped from the web page", ExitInterrupt)
		default:
			return fmt.Errorf("cannot stop in phase %s", c.phase)
		}
	}
	return nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// webAddr defaults to localhost: the page can start load against every target
// in the script, so exposing it has to be a written-out decision.
func webAddr(addr string) (string, bool, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", false, fmt.Errorf("--web: %w", err)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port), isLoopback(host), nil
}

func (c *ctl) serveWeb() error {
	addr, loopback, err := webAddr(c.o.Web)
	if err != nil {
		return err
	}
	c.token = c.o.WebToken
	if !loopback && c.token == "" {
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			return err
		}
		c.token = hex.EncodeToString(b)
		fmt.Fprintf(c.o.Stderr, "control token for the web page: %s\n", c.token)
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Security-Policy", webCSP)
		_, _ = w.Write(webPage)
	})
	mux.HandleFunc("GET /api/state", c.handleState)
	mux.HandleFunc("POST /api/start", c.handleControl("start"))
	mux.HandleFunc("POST /api/stop", c.handleControl("stop"))
	mux.HandleFunc("GET /metrics", c.handleMetrics)
	var h http.Handler = mux
	if loopback {
		h = localOnly(mux)
	}
	c.web = &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = c.web.Serve(l) }()
	fmt.Fprintf(c.o.Stderr, "web page at http://%s/\n", l.Addr())
	if c.o.OnWeb != nil {
		c.o.OnWeb(l.Addr().String())
	}
	return nil
}

// localOnly rejects requests whose Host is not a loopback name. Without it a
// website could point its own domain at 127.0.0.1 (DNS rebinding) and reach the
// page as a same-origin client.
func localOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.Host)
		if err != nil {
			host = strings.Trim(r.Host, "[]")
		}
		if !isLoopback(host) {
			http.Error(w, "this page answers only on a loopback name", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (c *ctl) handleState(w http.ResponseWriter, r *http.Request) {
	s := c.live.Load()
	if s == nil {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "starting", http.StatusServiceUnavailable)
		return
	}
	out := s.api
	since, _ := strconv.Atoi(r.URL.Query().Get("since"))
	if since < 0 || since > len(out.Timeline) {
		since = len(out.Timeline)
	}
	out.Timeline = out.Timeline[since:]
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(out)
}

func (c *ctl) handleControl(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(webHeader) == "" {
			http.Error(w, "missing header "+webHeader, http.StatusForbidden)
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if c.token != "" && subtle.ConstantTimeCompare([]byte(got), []byte(c.token)) != 1 {
			http.Error(w, "wrong or missing token", http.StatusUnauthorized)
			return
		}
		cmd := &command{name: name, done: make(chan error, 1)}
		wait := time.NewTimer(5 * time.Second)
		defer wait.Stop()
		select {
		case c.events <- event{cmd: cmd}:
		case <-wait.C:
			http.Error(w, "controller busy", http.StatusServiceUnavailable)
			return
		}
		select {
		case err := <-cmd.done:
			if err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		case <-wait.C:
			// Nothing reads events before the agents connect or after the run.
			http.Error(w, "controller busy", http.StatusServiceUnavailable)
		}
	}
}
