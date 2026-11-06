// Package controller drives one distributed run: it archives the script, starts
// the agents, gives each its execution segment, runs setup once, releases all
// k6 instances together, evaluates thresholds on merged data and writes the
// summary.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Jwrede/k6-fleet/internal/agg"
	"github.com/Jwrede/k6-fleet/internal/proto"
	"github.com/Jwrede/k6-fleet/internal/report"
	"github.com/Jwrede/k6-fleet/internal/segment"
	"github.com/Jwrede/k6-fleet/internal/threshold"
	"github.com/Jwrede/k6-fleet/internal/transport"
)

// Exit codes. Those of k6 are passed through; ExitAgentLost and ExitError are
// outside the range k6 uses.
const (
	ExitOK         = 0
	ExitError      = 1
	ExitAgentLost  = 3
	ExitThresholds = 99
	ExitInterrupt  = 105
)

// Options configures a run.
type Options struct {
	Script     string
	K6         string   // k6 on the controller, for archive and inspect
	K6Args     []string // passed to `k6 archive`, so they land in the archive
	Agents     []transport.Spec
	AgentOut   []string // extra --out for every agent
	Summary    string
	HTML       string
	Listen     string // Prometheus endpoint address
	OnLoss     string // abort or continue
	Ready      time.Duration
	MaxRun     time.Duration // 0: derive from the script
	Quiet      bool
	Self       string
	Stdout     io.Writer
	Stderr     io.Writer
	OnListen   func(addr string)
	StartDelay time.Duration
}

type agentState struct {
	spec    transport.Spec
	idx     int
	segment string
	conn    *transport.Conn
	w       *proto.Writer

	hello    *proto.HelloBody
	pongs    int
	bestRTT  int64
	offsetMs int64
	ready    bool
	setup    bool
	dataSet  bool
	tornDown bool
	ended    bool
	endedAt  time.Time
	exited   bool
	lost     bool
	code     int

	cpuSum, cpuMax float64
	cpuN           int
	lastCPU        float64
	hotSeconds     int
	maxLag         int64
	iterations     float64
	requests       float64
}

type event struct {
	a   *agentState
	m   proto.Msg
	err error
}

type ctl struct {
	o       Options
	ctx     context.Context
	agents  []*agentState
	events  chan event
	ts      []threshold.Threshold
	started time.Time

	mu     sync.Mutex // guards fleet and data for the /metrics handler
	fleet  *agg.Fleet
	tick   *agg.Fleet
	second int
	points []report.Point
	k6     string
	script string

	fatal      error
	fatalCode  int
	stopping   bool
	aborted    string
	abortCode  int
	warnings   []string
	warned     map[string]bool
	setupData  json.RawMessage
	setupReady bool
}

// Run executes the test and returns the process exit code.
func Run(ctx context.Context, o Options) (int, error) {
	if o.OnLoss == "" {
		o.OnLoss = "abort"
	}
	if o.Ready == 0 {
		o.Ready = 2 * time.Minute
	}
	if o.StartDelay == 0 {
		o.StartDelay = 1500 * time.Millisecond
	}
	if o.Stdout == nil {
		o.Stdout = os.Stdout
	}
	if o.Stderr == nil {
		o.Stderr = os.Stderr
	}
	c := &ctl{
		o: o, ctx: ctx, events: make(chan event, 1024), warned: map[string]bool{},
		fleet: agg.NewFleet(), tick: agg.NewFleet(), script: filepath.Base(o.Script),
	}
	code, err := c.run()
	c.closeAll()
	return code, err
}

func (c *ctl) logf(format string, args ...any) {
	if !c.o.Quiet {
		fmt.Fprintf(c.o.Stderr, format+"\n", args...)
	}
}

func (c *ctl) warn(key, format string, args ...any) {
	if c.warned[key] {
		return
	}
	c.warned[key] = true
	s := fmt.Sprintf(format, args...)
	c.warnings = append(c.warnings, s)
	fmt.Fprintln(c.o.Stderr, "WARNING: "+s)
}

func (c *ctl) run() (int, error) {
	tmp, err := os.MkdirTemp("", "k6-fleet-ctl-")
	if err != nil {
		return ExitError, err
	}
	defer os.RemoveAll(tmp)
	archive, info, err := c.buildArchive(tmp)
	if err != nil {
		return ExitError, err
	}
	if c.ts, err = threshold.FromOptions(info.Thresholds); err != nil {
		return ExitError, err
	}
	specs, err := agg.SpecsFor(c.ts)
	if err != nil {
		return ExitError, err
	}
	weights := make([]int, len(c.o.Agents))
	for i, s := range c.o.Agents {
		if weights[i] = s.Weight; weights[i] == 0 {
			weights[i] = 1
		}
	}
	plan, err := segment.Split(weights)
	if err != nil {
		return ExitError, err
	}
	if err := c.connect(plan); err != nil {
		return ExitError, err
	}
	if err := c.handshake(); err != nil {
		return c.failure(err)
	}
	if c.o.Listen != "" {
		if err := c.serveMetrics(); err != nil {
			return ExitError, err
		}
	}
	c.logf("%d agents ready to initialise, k6 %s, %d thresholds", len(c.agents), c.k6, len(c.ts))
	for _, a := range c.agents {
		k6 := a.spec.K6
		if k6 == "" {
			k6 = "k6"
		}
		c.send(a, proto.Msg{Type: proto.Prepare, Prepare: &proto.PrepareBody{
			Archive: archive, Segment: a.segment, Sequence: plan.Sequence, Instance: a.idx,
			K6: k6, Out: c.o.AgentOut, Specs: specs,
		}})
	}
	if err := c.pump("all k6 instances to initialise", c.o.Ready, func() bool { return c.all(func(a *agentState) bool { return a.ready }) }); err != nil {
		return c.failure(err)
	}
	if err := c.runSetup(); err != nil {
		return c.failure(err)
	}

	c.release()
	if err := c.waitForEnd(info.TotalDuration); err != nil {
		return c.failure(err)
	}
	c.teardownAndFinish()
	return c.finalise()
}

func (c *ctl) failure(err error) (int, error) {
	if errors.Is(err, context.Canceled) {
		return ExitInterrupt, errors.New("interrupted")
	}
	if c.fatalCode != 0 {
		return c.fatalCode, err
	}
	return ExitError, err
}

type inspectInfo struct {
	Thresholds    json.RawMessage `json:"thresholds"`
	TotalDuration string          `json:"totalDuration"`
	MaxVUs        uint64          `json:"maxVUs"`
}

// buildArchive packs the script and everything it imports into one file, which
// is what makes the run reproducible across agents, and reads the options back.
func (c *ctl) buildArchive(dir string) ([]byte, *inspectInfo, error) {
	path := filepath.Join(dir, "archive.tar")
	args := append([]string{"archive", "-O", path}, c.o.K6Args...)
	args = append(args, c.o.Script)
	if out, err := exec.CommandContext(c.ctx, c.o.K6, args...).CombinedOutput(); err != nil {
		return nil, nil, fmt.Errorf("k6 archive: %w\n%s", err, out)
	}
	out, err := exec.CommandContext(c.ctx, c.o.K6, "inspect", "--execution-requirements", path).Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return nil, nil, fmt.Errorf("k6 inspect: %w\n%s", err, ee.Stderr)
		}
		return nil, nil, fmt.Errorf("k6 inspect: %w", err)
	}
	var info inspectInfo
	if err := json.Unmarshal(out, &info); err != nil {
		return nil, nil, fmt.Errorf("k6 inspect output: %w", err)
	}
	data, err := os.ReadFile(path)
	return data, &info, err
}

func (c *ctl) connect(plan segment.Plan) error {
	var wg sync.WaitGroup
	errs := make([]error, len(c.o.Agents))
	c.agents = make([]*agentState, len(c.o.Agents))
	for i, s := range c.o.Agents {
		a := &agentState{spec: s, idx: i, segment: plan.Segments[i]}
		if a.spec.Name == "" {
			a.spec.Name = fmt.Sprintf("agent-%d", i)
		}
		c.agents[i] = a
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := transport.Start(c.ctx, a.spec, c.o.Self, c.o.Stderr)
			if err != nil {
				errs[i] = err
				return
			}
			a.conn = conn
			a.w = proto.NewWriter(conn.Stdin)
			go c.read(a)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (c *ctl) read(a *agentState) {
	r := proto.NewReader(a.conn.Stdout)
	for {
		m, err := r.Next()
		if err != nil {
			c.events <- event{a: a, err: err}
			return
		}
		c.events <- event{a: a, m: m}
	}
}

func (c *ctl) send(a *agentState, m proto.Msg) {
	if a.w != nil && !a.lost {
		_ = a.w.Send(m)
	}
}

func (c *ctl) all(pred func(*agentState) bool) bool {
	for _, a := range c.agents {
		if !a.lost && !pred(a) {
			return false
		}
	}
	return true
}

// pump processes agent events until done reports true.
func (c *ctl) pump(what string, timeout time.Duration, done func() bool) error {
	var deadline <-chan time.Time
	if timeout > 0 {
		deadline = time.After(timeout)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for !done() {
		if c.fatal != nil {
			return c.fatal
		}
		select {
		case ev := <-c.events:
			c.apply(ev)
		case <-ticker.C:
			c.onTick()
		case <-deadline:
			return fmt.Errorf("timed out waiting for %s", what)
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	}
	return c.fatal
}

func (c *ctl) handshake() error {
	if err := c.pump("agents to connect", c.o.Ready, func() bool { return c.all(func(a *agentState) bool { return a.hello != nil }) }); err != nil {
		return err
	}
	versions := map[string][]string{}
	for _, a := range c.agents {
		versions[a.hello.K6Version] = append(versions[a.hello.K6Version], a.spec.Name)
		c.send(a, proto.Msg{Type: proto.Ping, T: time.Now().UnixMilli()})
	}
	if len(versions) > 1 {
		return fmt.Errorf("agents run different k6 versions: %v", versions)
	}
	for v := range versions {
		if strings.HasPrefix(v, "unavailable") {
			return fmt.Errorf("k6 not usable on an agent: %s", v)
		}
		c.k6 = v
	}
	if err := c.pump("clock measurement", 30*time.Second, func() bool { return c.all(func(a *agentState) bool { return a.pongs >= 3 }) }); err != nil {
		return err
	}
	for _, a := range c.agents {
		if a.offsetMs > 500 || a.offsetMs < -500 {
			c.warn("clock:"+a.spec.Name, "clock on %s differs by %d ms; sample timestamps are not comparable across agents, the synchronised start is corrected for it", a.spec.Name, a.offsetMs)
		}
	}
	return nil
}

// runSetup runs setup() once and hands its result to the other instances, which
// is what k6 does on a single machine with all VUs.
func (c *ctl) runSetup() error {
	first := c.agents[0]
	c.send(first, proto.Msg{Type: proto.Setup})
	if err := c.pump("setup()", 0, func() bool { return first.setup }); err != nil {
		return err
	}
	data := c.setupData
	if len(data) == 0 {
		data = json.RawMessage("null")
	}
	for _, a := range c.agents[1:] {
		c.send(a, proto.Msg{Type: proto.SetData, Data: data})
	}
	return c.pump("setup data to reach the agents", 60*time.Second, func() bool {
		for _, a := range c.agents[1:] {
			if !a.lost && !a.dataSet {
				return false
			}
		}
		return true
	})
}

func (c *ctl) release() {
	at := time.Now().Add(c.o.StartDelay).UnixMilli()
	for _, a := range c.agents {
		c.send(a, proto.Msg{Type: proto.Resume, T: at + a.offsetMs})
	}
	c.started = time.UnixMilli(at)
	c.logf("test starts on all agents at %s", c.started.Format("15:04:05.000"))
}

func (c *ctl) waitForEnd(total string) error {
	limit := c.o.MaxRun
	if d, err := time.ParseDuration(total); err == nil && limit == 0 {
		limit = d + 2*time.Minute + c.o.StartDelay
	}
	err := c.pump("the test to end", limit, func() bool { return c.all(func(a *agentState) bool { return a.ended }) })
	switch {
	case errors.Is(err, context.Canceled):
		c.abort("interrupted", ExitInterrupt)
		// A second interrupt is the shell's business; agents stop within seconds.
		c.ctx = context.Background()
		return c.pump("agents to stop", 30*time.Second, func() bool { return c.all(func(a *agentState) bool { return a.ended }) })
	case err != nil && c.fatal == nil:
		c.abort(err.Error(), ExitError)
		c.ctx = context.Background()
		return c.pump("agents to stop", 30*time.Second, func() bool { return c.all(func(a *agentState) bool { return a.ended }) })
	}
	return err
}

func (c *ctl) abort(reason string, code int) {
	if c.stopping {
		return
	}
	c.stopping = true
	c.aborted, c.abortCode = reason, code
	c.logf("stopping all agents: %s", reason)
	for _, a := range c.agents {
		if !a.ended {
			c.send(a, proto.Msg{Type: proto.Stop})
		}
	}
}

func (c *ctl) teardownAndFinish() {
	first := c.agents[0]
	if !first.lost {
		c.send(first, proto.Msg{Type: proto.Teardown})
		if err := c.pump("teardown()", 0, func() bool { return first.tornDown || first.lost }); err != nil {
			c.warn("teardown", "teardown: %v", err)
		}
	} else {
		c.warn("teardown", "teardown() did not run: agent %s is gone", first.spec.Name)
	}
	for _, a := range c.agents {
		c.send(a, proto.Msg{Type: proto.Finish})
	}
	if err := c.pump("agents to exit", 90*time.Second, func() bool { return c.all(func(a *agentState) bool { return a.exited }) }); err != nil {
		c.warn("exit", "%v", err)
	}
	c.fatal = nil
}

func (c *ctl) apply(ev event) {
	a := ev.a
	if ev.err != nil {
		if a.exited {
			return
		}
		c.onLost(a, ev.err)
		return
	}
	m := ev.m
	switch m.Type {
	case proto.Hello:
		a.hello = m.Hello
	case proto.Pong:
		rtt := time.Now().UnixMilli() - m.Echo
		if a.pongs == 0 || rtt < a.bestRTT {
			a.bestRTT = rtt
			a.offsetMs = m.T - (m.Echo + rtt/2)
		}
		if a.pongs++; a.pongs < 3 {
			c.send(a, proto.Msg{Type: proto.Ping, T: time.Now().UnixMilli()})
		}
	case proto.Ready:
		a.ready = true
	case proto.SetupDone:
		a.setup, c.setupData = true, m.Data
	case proto.DataSet:
		a.dataSet = true
	case proto.TeardownDone:
		a.tornDown = true
	case proto.FrameMsg:
		c.onFrame(a, m.Frame)
	case proto.Log:
		c.logf("[%s] %s", a.spec.Name, m.Text)
	case proto.Error:
		if c.started.IsZero() {
			c.fatal = fmt.Errorf("agent %s: %s", a.spec.Name, m.Text)
		} else {
			c.warn("err:"+a.spec.Name+m.Text, "agent %s: %s", a.spec.Name, m.Text)
		}
	case proto.Ended:
		a.ended, a.endedAt, a.code = true, time.Now(), m.Code
		switch {
		case c.started.IsZero():
			c.fatalCode = m.Code
			c.fatal = fmt.Errorf("k6 on %s exited with code %d before the test started:\n%s", a.spec.Name, m.Code, m.Text)
		case m.Code != 0:
			// exec.test.abort() or a crash ends one instance. A single k6 would stop
			// altogether, so the rest of the fleet stops too.
			c.abort(fmt.Sprintf("k6 on %s ended with code %d", a.spec.Name, m.Code), m.Code)
		}
	case proto.Exit:
		a.exited = true
		if m.Code != 0 && a.code == 0 {
			a.code = m.Code
		}
	}
}

func (c *ctl) onLost(a *agentState, err error) {
	if a.lost {
		return
	}
	a.lost = true
	if !a.ended {
		a.endedAt = time.Now()
	}
	reason := fmt.Sprintf("lost agent %s: %v", a.spec.Name, err)
	switch {
	case c.started.IsZero():
		c.fatal = errors.New(reason)
	case a.ended:
		c.warn("lost:"+a.spec.Name, "%s after its test ended; its last samples may be missing", reason)
	case c.o.OnLoss == "continue":
		c.warn("lost:"+a.spec.Name, "%s; the run continues with the load of the remaining agents and the totals are too low", reason)
	default:
		c.abort(reason, ExitAgentLost)
	}
}

func (c *ctl) onFrame(a *agentState, f *agg.Frame) {
	if f == nil {
		return
	}
	c.mu.Lock()
	c.fleet.Add(a.spec.Name, f)
	c.tick.Add(a.spec.Name, f)
	c.mu.Unlock()
	if s := f.Series["iterations"]; s != nil {
		a.iterations += s.Sum
	}
	if s := f.Series["http_reqs"]; s != nil {
		a.requests += s.Sum
	}
	if f.LagMs > a.maxLag {
		a.maxLag = f.LagMs
	}
	if f.LagMs > 5000 {
		c.warn("lag:"+a.spec.Name, "agent %s processes samples %d ms late; lower the sample volume (--system-tags) or add agents", a.spec.Name, f.LagMs)
	}
	if f.CPU > 0 {
		a.lastCPU = f.CPU
		a.cpuSum += f.CPU
		a.cpuN++
		if f.CPU > a.cpuMax {
			a.cpuMax = f.CPU
		}
		if f.CPU >= 90 {
			if a.hotSeconds++; a.hotSeconds == 5 {
				c.warn("cpu:"+a.spec.Name, "agent %s was above 90%% CPU for 5 s; a saturated generator inflates latencies, so the results may describe the agent rather than the target", a.spec.Name)
			}
		} else {
			a.hotSeconds = 0
		}
	}
}

func (c *ctl) elapsed() time.Duration {
	if c.started.IsZero() {
		return 0
	}
	if d := time.Since(c.started); d > 0 {
		return d
	}
	return 0
}

func (c *ctl) onTick() {
	if c.started.IsZero() || time.Now().Before(c.started) {
		return
	}
	c.mu.Lock()
	t := c.tick
	c.tick = agg.NewFleet()
	c.second++
	p := report.Point{Second: c.second}
	if s := c.fleet.Series["vus"]; s != nil {
		p.VUs = s.Last
	}
	if s := t.Series["http_reqs"]; s != nil {
		p.Reqs = s.Sum
	}
	if s := t.Series["iterations"]; s != nil {
		p.Iters = s.Sum
	}
	if s := t.Series["http_req_duration"]; s != nil && s.Sketch != nil {
		p.P95 = s.Sketch.Quantile(0.95)
	}
	if s := t.Series["http_req_failed"]; s != nil && s.Count > 0 {
		p.Failed = float64(s.NonZero) / float64(s.Count)
	}
	for _, a := range c.agents {
		if a.lastCPU > p.CPUMax {
			p.CPUMax = a.lastCPU
		}
	}
	c.points = append(c.points, p)
	c.mu.Unlock()
	if c.second%5 == 0 {
		c.logf("%4ds vus=%.0f reqs/s=%.0f p95=%.1fms failed=%.2f%% cpu_max=%.0f%%", c.second, p.VUs, p.Reqs, p.P95, 100*p.Failed, p.CPUMax)
	}
	if c.second%2 == 0 {
		c.checkAbort()
	}
}

// checkAbort applies abortOnFail on merged data, every two seconds like k6.
func (c *ctl) checkAbort() {
	if c.stopping {
		return
	}
	el := c.elapsed()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.ts {
		if !t.AbortOnFail || el < t.AbortDelay {
			continue
		}
		s := c.fleet.Series[t.Series]
		if s == nil {
			continue
		}
		if ok, err := t.Check(agg.View{S: s, Elapsed: el}); err == nil && !ok {
			c.abort(fmt.Sprintf("threshold %s on %s crossed", t.Source, t.Series), ExitThresholds)
			return
		}
	}
}

func (c *ctl) snapshot(el time.Duration) *report.Data {
	d := report.New()
	d.Script, d.K6Version, d.Started = c.script, c.k6, c.started
	d.DurationMs = el.Milliseconds()
	d.Metrics = report.Metrics(c.fleet, el)
	d.Thresholds = report.Evaluate(c.ts, c.fleet, el)
	d.AttachThresholds()
	d.Checks = report.Checks(c.fleet)
	d.Timeline = append([]report.Point(nil), c.points...)
	for _, a := range c.agents {
		r := report.AgentResult{
			Name: a.spec.Name, Segment: a.segment, Iterations: a.iterations, Requests: a.requests,
			CPUMax: a.cpuMax, MaxLagMs: a.maxLag, OffsetMs: a.offsetMs, ExitCode: a.code, Lost: a.lost,
		}
		if a.hello != nil {
			r.Host = a.hello.Hostname
		}
		if a.cpuN > 0 {
			r.CPUAvg = a.cpuSum / float64(a.cpuN)
		}
		d.Agents = append(d.Agents, r)
	}
	return d
}

func (c *ctl) finalise() (int, error) {
	end := c.started
	for _, a := range c.agents {
		if a.endedAt.After(end) {
			end = a.endedAt
		}
	}
	el := end.Sub(c.started)
	c.mu.Lock()
	d := c.snapshot(el)
	c.mu.Unlock()
	d.Aborted = c.aborted
	d.Warnings = c.warnings

	code := ExitOK
	failed := false
	for _, r := range d.Thresholds {
		if !r.OK {
			failed = true
		}
	}
	switch {
	case c.abortCode != 0:
		code = c.abortCode
	default:
		for _, a := range c.agents {
			if a.code != 0 {
				code = a.code
				break
			}
		}
		if code == ExitOK && failed {
			code = ExitThresholds
		}
	}
	d.ExitCode = code
	d.WriteText(c.o.Stdout)
	var errs []error
	if c.o.Summary != "" {
		errs = append(errs, d.WriteJSON(c.o.Summary))
	}
	if c.o.HTML != "" {
		errs = append(errs, d.WriteHTML(c.o.HTML))
	}
	return code, errors.Join(errs...)
}

func (c *ctl) serveMetrics() error {
	l, err := net.Listen("tcp", c.o.Listen)
	if err != nil {
		return err
	}
	if c.o.OnListen != nil {
		c.o.OnListen(l.Addr().String())
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		c.mu.Lock()
		d := c.snapshot(c.elapsed())
		c.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		d.WriteProm(w)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(l) }()
	return nil
}

func (c *ctl) closeAll() {
	var wg sync.WaitGroup
	for _, a := range c.agents {
		if a == nil || a.conn == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = a.conn.Close()
		}()
	}
	wg.Wait()
}

// ParseInventory reads the JSON agent list.
func ParseInventory(data []byte) ([]transport.Spec, error) {
	var inv struct {
		Agents []transport.Spec `json:"agents"`
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&inv); err != nil {
		return nil, fmt.Errorf("inventory: %w", err)
	}
	if len(inv.Agents) == 0 {
		return nil, errors.New("inventory: no agents")
	}
	names := map[string]bool{}
	for i := range inv.Agents {
		a := &inv.Agents[i]
		if a.Name == "" {
			a.Name = fmt.Sprintf("agent-%d", i)
		}
		if names[a.Name] {
			return nil, fmt.Errorf("inventory: duplicate agent name %q", a.Name)
		}
		names[a.Name] = true
		if a.Weight < 0 {
			return nil, fmt.Errorf("inventory: agent %s has a negative weight", a.Name)
		}
	}
	return inv.Agents, nil
}
