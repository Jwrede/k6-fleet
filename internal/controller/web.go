package controller

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jwrede/k6-fleet/internal/agg"
	"github.com/Jwrede/k6-fleet/internal/proto"
	"github.com/Jwrede/k6-fleet/internal/report"
)

// Phases of a run, as shown on the page.
const (
	phaseIdle         = "idle"
	phaseConnecting   = "connecting"
	phaseInitialising = "initialising"
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

//go:embed web
var webFiles embed.FS

const webCSP = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; frame-ancestors 'none'"

// rateWindow is the number of seconds that current rates are averaged over.
const rateWindow = 5

type command struct {
	name string
	vus  int64
	done chan error
}

type tickCounts struct {
	reqs, fails map[string]uint64
}

type liveAgent struct {
	report.AgentResult
	State    string  `json:"state"`
	CPU      float64 `json:"cpu"`
	LagMs    int64   `json:"lag_ms"`
	VUs      float64 `json:"vus"`
	MemBytes int64   `json:"mem_bytes"`
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
	Defaults      formDefaults             `json:"defaults"`
	Adjustable    bool                     `json:"adjustable"`
	TargetVUs     int64                    `json:"target_vus,omitempty"`
	VUs           float64                  `json:"vus"`
	RPS           float64                  `json:"rps"`
	FailRatio     float64                  `json:"fail_ratio"`
	Agents        []liveAgent              `json:"agents"`
	Requests      []report.RequestRow      `json:"requests"`
	Failures      []report.FailureRow      `json:"failures"`
	Errors        []errorRow               `json:"errors"`
	Thresholds    []report.ThresholdResult `json:"thresholds"`
	Checks        []report.CheckResult     `json:"checks"`
	Warnings      []string                 `json:"warnings"`
	Run           int                      `json:"run"`
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
	case a.conn == nil || a.hello == nil:
		return "connecting"
	case !a.ready:
		return "initialising"
	case started:
		return "running"
	}
	return "ready"
}

func (c *ctl) publish() {
	if c.o.Web == "" && c.metrics == nil {
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
		ElapsedMs: d.DurationMs, ExitCode: code, Aborted: d.Aborted,
		Adjustable: c.o.Scenario != nil, TargetVUs: c.targetVUs,
		Requests: d.Requests, Failures: d.Failures,
		Thresholds: d.Thresholds, Checks: d.Checks, Warnings: d.Warnings, Timeline: d.Timeline,
	}}
	for i, r := range d.Agents {
		a := c.agents[i]
		s.api.VUs += a.vus
		s.api.Agents = append(s.api.Agents, liveAgent{
			AgentResult: r, State: a.state(!c.started.IsZero()), CPU: a.lastCPU, LagMs: a.lastLag, VUs: a.vus, MemBytes: a.memBytes,
		})
	}
	if n := len(d.Requests); n > 0 {
		total := d.Requests[n-1]
		s.api.RPS = total.RPS
		if total.Requests > 0 {
			s.api.FailRatio = float64(total.Fails) / float64(total.Requests)
		}
	}
	c.live.Store(s)
}

// publishEnd records the outcome after the agents are gone.
func (c *ctl) publishEnd(code int, err error) {
	if c.o.Web == "" && c.metrics == nil {
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
	for i := range c.agents {
		c.agents[i].lastCPU, c.agents[i].vus = 0, 0
	}
	c.store(d, &code, errText)
}

// noteTick keeps the last seconds of request counts for the current rates.
func (c *ctl) noteTick(t *agg.Fleet) {
	if c.o.Web == "" {
		return
	}
	n := tickCounts{reqs: map[string]uint64{}, fails: map[string]uint64{}}
	for k, s := range t.Requests {
		n.reqs[k], n.fails[k] = uint64(s.Sketch.Count), s.NonZero
	}
	c.window = append(c.window, n)
	if len(c.window) > rateWindow {
		c.window = c.window[1:]
	}
}

func (c *ctl) requestRate(key string) (rps, fps float64) {
	if len(c.window) == 0 || c.phase != phaseRunning {
		return 0, 0
	}
	var r, f uint64
	for _, w := range c.window {
		r += w.reqs[key]
		f += w.fails[key]
	}
	n := float64(len(c.window))
	return float64(r) / n, float64(f) / n
}

// control runs on the event loop, like everything that changes a run.
func (c *ctl) control(cmd *command) error {
	switch cmd.name {
	case "stop":
		switch c.phase {
		case phaseConnecting, phaseInitialising, phaseSetup:
			// No VU has run yet, so there is nothing to finish: closing the agents
			// removes the paused k6 instances.
			c.fatalCode = ExitInterrupt
			c.fatal = errors.New("stopped from the web page")
		case phaseRunning:
			c.abort("stopped from the web page", ExitInterrupt)
		default:
			return fmt.Errorf("cannot stop in phase %s", c.phase)
		}
	case "vus":
		if c.phase != phaseRunning {
			return fmt.Errorf("cannot change VUs in phase %s", c.phase)
		}
		if c.o.Scenario == nil {
			return errors.New("VUs can change only if the test was started with a number of VUs on the page")
		}
		if cmd.vus > c.maxVUs {
			c.maxVUs = cmd.vus
		}
		c.targetVUs = cmd.vus
		c.logf("VUs set to %d", cmd.vus)
		for _, a := range c.agents {
			c.send(a, proto.Msg{Type: proto.SetVUs, VUs: cmd.vus, MaxVUs: c.maxVUs})
		}
	case "reset":
		if c.phase != phaseRunning {
			return fmt.Errorf("cannot reset in phase %s", c.phase)
		}
		c.fleet.ResetRequests()
		c.window = nil
		c.logf("request statistics reset")
	}
	c.publish()
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

// runParams is the start form. Empty fields keep what the script says.
type runParams struct {
	VUs      int64             `json:"vus"`
	Duration string            `json:"duration"`
	Env      map[string]string `json:"env"`
}

type formDefaults struct {
	MaxVUs   uint64 `json:"max_vus"`
	Duration string `json:"duration"`
}

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// apply turns the form into options of one run.
func (p runParams) apply(o Options) (Options, error) {
	o.K6Args = append([]string(nil), o.K6Args...)
	if p.VUs < 0 {
		return o, errors.New("VUs must not be negative")
	}
	d := time.Duration(0)
	if p.Duration != "" {
		var err error
		if d, err = time.ParseDuration(p.Duration); err != nil {
			return o, fmt.Errorf("duration: %w", err)
		}
		if d < 0 {
			return o, errors.New("duration must not be negative")
		}
	}
	switch {
	case p.VUs > 0:
		o.Scenario = &externalScenario{VUs: p.VUs, Duration: d.String()}
	case d > 0:
		o.K6Args = append(o.K6Args, "--duration", d.String())
	}
	for k, v := range p.Env {
		if !envName.MatchString(k) {
			return o, fmt.Errorf("bad variable name %q", k)
		}
		o.K6Args = append(o.K6Args, "-e", k+"="+v)
	}
	return o, nil
}

// server is the web page and its API. It outlives the runs it starts.
type server struct {
	o        Options
	token    string
	logs     *logBook
	current  atomic.Pointer[ctl]
	start    chan runParams
	runs     atomic.Int32
	defaults formDefaults
	scriptEr string
}

// serve runs the page until interrupted and returns the result of the last run.
func serve(ctx context.Context, o Options) (int, error) {
	s := &server{o: o, logs: newLogBook(), start: make(chan runParams)}
	addr, loopback, err := webAddr(o.Web)
	if err != nil {
		return ExitError, err
	}
	s.token = o.WebToken
	if !loopback && s.token == "" {
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			return ExitError, err
		}
		s.token = hex.EncodeToString(b)
		fmt.Fprintf(o.Stderr, "control token for the web page: %s\n", s.token)
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return ExitError, err
	}
	var h http.Handler = s.routes()
	if loopback {
		h = localOnly(h)
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	defer srv.Close()
	go func() { _ = srv.Serve(l) }()
	fmt.Fprintf(o.Stderr, "web page at http://%s/\n", l.Addr())
	s.inspect(ctx)
	if o.OnWeb != nil {
		o.OnWeb(l.Addr().String())
	}

	code, runErr := ExitOK, error(nil)
	for {
		var p runParams
		if !o.Autostart {
			select {
			case p = <-s.start:
			case <-ctx.Done():
				return code, runErr
			}
		}
		oo, err := p.apply(o)
		if err != nil {
			return ExitError, err // validated by the handler
		}
		s.logs.clear()
		c := newCtl(ctx, oo)
		c.book = s.logs
		c.phase = phaseConnecting
		c.publish()
		s.runs.Add(1)
		s.current.Store(c)
		code, runErr = c.execute()
		if o.Autostart || ctx.Err() != nil {
			return code, runErr
		}
		c.logf("run ended with exit code %d", code)
	}
}

// inspect reads the script's own VUs and duration for the form. A broken script
// shows up on the page before anybody presses Start.
func (s *server) inspect(ctx context.Context) {
	tmp, err := os.MkdirTemp("", "k6-fleet-ctl-")
	if err != nil {
		return
	}
	defer os.RemoveAll(tmp)
	c := newCtl(ctx, s.o)
	_, info, err := c.buildArchive(tmp)
	if err != nil {
		s.scriptEr = err.Error()
		return
	}
	s.defaults = formDefaults{MaxVUs: info.MaxVUs, Duration: info.TotalDuration}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	assets, err := fs.Sub(webFiles, "web")
	if err != nil {
		panic(err)
	}
	files := http.FileServerFS(assets)
	mux.HandleFunc("GET /{$}", s.static(files))
	mux.HandleFunc("GET /app.js", s.static(files))
	mux.HandleFunc("GET /chart.js", s.static(files))
	mux.HandleFunc("GET /style.css", s.static(files))
	mux.HandleFunc("GET /favicon.ico", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("GET /api/logs", s.handleLogs)
	mux.HandleFunc("POST /api/start", s.control(s.handleStart))
	mux.HandleFunc("POST /api/stop", s.control(s.forward("stop")))
	mux.HandleFunc("POST /api/vus", s.control(s.handleVUs))
	mux.HandleFunc("POST /api/reset", s.control(s.forward("reset")))
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		if c := s.current.Load(); c != nil {
			c.handleMetrics(w, r)
		}
	})
	mux.HandleFunc("GET /download/{file}", s.handleDownload)
	return mux
}

func (s *server) static(files http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", webCSP)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		files.ServeHTTP(w, r)
	}
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

func (s *server) state() (apiState, *report.Data) {
	var out apiState
	var d *report.Data
	c := s.current.Load()
	var live *liveState
	if c != nil {
		live = c.live.Load()
	}
	if live != nil {
		out, d = live.api, live.data
	} else {
		out = apiState{Phase: phaseIdle, Script: filepath.Base(s.o.Script), Error: s.scriptEr}
		for _, a := range s.o.Agents {
			out.Agents = append(out.Agents, liveAgent{AgentResult: report.AgentResult{Name: a.Name}, State: "idle"})
		}
	}
	out.TokenRequired = s.token != ""
	out.Defaults = s.defaults
	out.Errors = s.logs.errorRows()
	out.Run = int(s.runs.Load())
	return out, d
}

func (s *server) handleState(w http.ResponseWriter, r *http.Request) {
	out, _ := s.state()
	since, _ := strconv.Atoi(r.URL.Query().Get("since"))
	if since < 0 || since > len(out.Timeline) {
		since = len(out.Timeline)
	}
	out.Timeline = out.Timeline[since:]
	writeJSON(w, out)
}

func (s *server) handleLogs(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.Atoi(r.URL.Query().Get("since"))
	lines, seq := s.logs.since(since)
	writeJSON(w, map[string]any{"lines": lines, "seq": seq})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

// control wraps the actions that change something: a cross-site request cannot
// carry the header, and off loopback the token is required.
func (s *server) control(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(webHeader) == "" {
			http.Error(w, "missing header "+webHeader, http.StatusForbidden)
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if s.token != "" && subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) != 1 {
			http.Error(w, "wrong or missing token", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	if s.o.Autostart {
		http.Error(w, "this process runs one test with --autostart", http.StatusConflict)
		return
	}
	var p runParams
	if r.ContentLength != 0 {
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	if _, err := p.apply(s.o); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	select {
	case s.start <- p:
		w.WriteHeader(http.StatusAccepted)
	case <-time.After(time.Second):
		http.Error(w, "a test is running", http.StatusConflict)
	}
}

func (s *server) handleVUs(w http.ResponseWriter, r *http.Request) {
	var p struct {
		VUs int64 `json:"vus"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&p); err != nil || p.VUs < 0 {
		http.Error(w, "need a number of VUs", http.StatusBadRequest)
		return
	}
	s.send(w, &command{name: "vus", vus: p.VUs, done: make(chan error, 1)})
}

func (s *server) forward(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		s.send(w, &command{name: name, done: make(chan error, 1)})
	}
}

func (s *server) send(w http.ResponseWriter, cmd *command) {
	c := s.current.Load()
	if c == nil {
		http.Error(w, "no test is running", http.StatusConflict)
		return
	}
	if l := c.live.Load(); l != nil && (l.api.Phase == phaseFinished || l.api.Phase == phaseFailed) {
		http.Error(w, "no test is running", http.StatusConflict)
		return
	}
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
		// Nothing reads events while archiving, connecting or finishing.
		http.Error(w, "controller busy", http.StatusServiceUnavailable)
	}
}

func (s *server) handleDownload(w http.ResponseWriter, r *http.Request) {
	st, d := s.state()
	if d == nil {
		http.Error(w, "no test has run yet", http.StatusNotFound)
		return
	}
	file := r.PathValue("file")
	var write func(io.Writer) error
	ctype := "text/csv; charset=utf-8"
	switch file {
	case "requests.csv":
		write = d.WriteRequestsCSV
	case "failures.csv":
		write = d.WriteFailuresCSV
	case "timeline.csv":
		write = d.WriteTimelineCSV
	case "errors.csv":
		write = func(o io.Writer) error { return writeErrorsCSV(o, st.Errors) }
	case "report.html":
		write, ctype = d.RenderHTML, "text/html; charset=utf-8"
	case "summary.json":
		ctype = "application/json"
		write = func(o io.Writer) error {
			e := json.NewEncoder(o)
			e.SetIndent("", "  ")
			return e.Encode(d)
		}
	case "logs.txt":
		ctype = "text/plain; charset=utf-8"
		write = func(o io.Writer) error {
			lines, _ := s.logs.since(0)
			for _, l := range lines {
				fmt.Fprintf(o, "%s %s %s %s\n", l.Time.Format(time.RFC3339), l.Source, l.Level, l.Text)
			}
			return nil
		}
	default:
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", "k6-fleet-"+file))
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'")
	if err := write(w); err != nil {
		fmt.Fprintln(s.o.Stderr, "download:", err)
	}
}

func writeErrorsCSV(w io.Writer, rows []errorRow) error {
	c := csv.NewWriter(w)
	_ = c.Write([]string{"count", "agents", "message"})
	for _, r := range rows {
		_ = c.Write([]string{strconv.Itoa(r.Count), strings.Join(r.Agents, " "), r.Message})
	}
	c.Flush()
	return c.Error()
}
