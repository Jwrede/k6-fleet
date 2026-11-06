// Package agent runs on each load generator. It starts one paused k6 instance,
// reduces its sample stream to frames, and drives k6's REST API on behalf of the
// controller. All communication goes over stdin and stdout.
package agent

import (
	"bufio"
	"bytes"
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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Jwrede/k6-fleet/internal/agg"
	"github.com/Jwrede/k6-fleet/internal/proto"
)

// k6 ExecutionStatus values (lib/execution.go).
const (
	statusPausedBeforeRun = 4
	statusEnded           = 9
)

// fSetPipeSize is F_SETPIPE_SZ, which package syscall does not define.
const fSetPipeSize = 1031

// CodeUnknown in an Ended message means that k6 did not end cleanly and its exit
// code follows in the Exit message.
const CodeUnknown = -1

// logLimit caps forwarded k6 output, so a script logging per iteration cannot
// flood the control channel.
const logLimit = 200

type agent struct {
	w       *proto.Writer
	version string

	mu      sync.Mutex
	aggr    *agg.Aggregator
	cmd     *exec.Cmd
	api     string
	dir     string
	fifo    string
	exited  chan struct{} // closed when k6 has exited
	drained chan struct{} // closed when the sample stream hit EOF
	code    int           // from the REST API, else the process
	ended   bool
	logs    int
}

// Run serves the protocol until stdin closes or the run finishes.
func Run(in io.Reader, out io.Writer, version, k6 string) error {
	a := &agent{w: proto.NewWriter(out), version: version}
	defer a.cleanup()
	r := proto.NewReader(in)
	host, _ := os.Hostname()
	if err := a.w.Send(proto.Msg{Type: proto.Hello, T: nowMs(), Hello: &proto.HelloBody{
		Version: version, K6Version: k6Version(k6), Hostname: host, CPUs: runtime.NumCPU(),
	}}); err != nil {
		return err
	}
	for {
		m, err := r.Next()
		if errors.Is(err, io.EOF) {
			return nil // controller gone: cleanup stops k6, so no orphan load
		}
		if err != nil {
			return err
		}
		if done := a.handle(m); done {
			return nil
		}
	}
}

func (a *agent) handle(m proto.Msg) (done bool) {
	switch m.Type {
	case proto.Ping:
		a.send(proto.Msg{Type: proto.Pong, Echo: m.T, T: nowMs()})
	case proto.Prepare:
		if err := a.prepare(m.Prepare); err != nil {
			a.fail(err)
		}
	case proto.Setup:
		go func() {
			body, err := a.call(http.MethodPost, "/v1/setup", nil, 0)
			if err != nil {
				a.fail(fmt.Errorf("setup: %w", err))
				return
			}
			a.send(proto.Msg{Type: proto.SetupDone, Data: setupData(body)})
		}()
	case proto.SetData:
		if _, err := a.call(http.MethodPut, "/v1/setup", m.Data, 30*time.Second); err != nil {
			a.fail(fmt.Errorf("set setup data: %w", err))
			return false
		}
		a.send(proto.Msg{Type: proto.DataSet})
	case proto.Resume:
		go a.resume(m.T)
	case proto.Teardown:
		go func() {
			if _, err := a.call(http.MethodPost, "/v1/teardown", nil, 0); err != nil {
				a.send(proto.Msg{Type: proto.Log, Text: "teardown failed: " + err.Error()})
			}
			a.send(proto.Msg{Type: proto.TeardownDone})
		}()
	case proto.Stop:
		a.patchStatus(`{"stopped":true}`)
	case proto.Finish:
		a.finish()
		return true
	}
	return false
}

func (a *agent) send(m proto.Msg) { _ = a.w.Send(m) }

func (a *agent) fail(err error) { a.send(proto.Msg{Type: proto.Error, Text: err.Error()}) }

func (a *agent) prepare(p *proto.PrepareBody) error {
	if p == nil {
		return errors.New("prepare without body")
	}
	dir, err := os.MkdirTemp("", "k6-fleet-")
	if err != nil {
		return err
	}
	a.dir = dir
	archive := filepath.Join(dir, "archive.tar")
	if err := os.WriteFile(archive, p.Archive, 0o600); err != nil {
		return err
	}
	a.fifo = filepath.Join(dir, "samples.json")
	if err := syscall.Mkfifo(a.fifo, 0o600); err != nil {
		return err
	}
	port, err := freePort()
	if err != nil {
		return err
	}
	a.api = "http://127.0.0.1:" + strconv.Itoa(port)
	a.aggr = agg.New(p.Specs)
	a.exited = make(chan struct{})
	a.drained = make(chan struct{})

	args := runArgs(p, port, a.fifo, archive)
	cmd := exec.Command(p.K6, args...)
	cmd.Dir = dir
	// k6 must not outlive the agent: with --linger it would otherwise stay, and
	// during a test keep producing load that nobody observes.
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start k6: %w", err)
	}
	a.cmd = cmd

	var tail tailBuffer
	logsDone := make(chan struct{})
	go func() {
		defer close(logsDone)
		a.forwardLogs(pipe, &tail)
	}()
	go a.readSamples()
	go a.frames()
	go func() {
		<-logsDone // cmd.Wait closes the pipe, so read it to the end first
		err := cmd.Wait()
		a.mu.Lock()
		if !a.ended || a.code == CodeUnknown {
			a.code = exitCode(err)
		}
		a.mu.Unlock()
		close(a.exited)
		a.wakeReader()
		a.markEnded(tail.String())
	}()
	go a.pollStatus()
	return nil
}

// runArgs builds the k6 command line. --no-thresholds and disabling the summary
// leave both jobs to the controller, the only place with all samples. k6 2.x
// removed --no-summary; --summary-mode works on 1.x and 2.x.
func runArgs(p *proto.PrepareBody, port int, fifo, archive string) []string {
	args := []string{"run",
		"--paused", "--linger", "--no-setup", "--no-teardown",
		"--summary-mode=disabled", "--no-thresholds", "--no-usage-report", "--quiet",
		"--address", "127.0.0.1:" + strconv.Itoa(port),
		"--execution-segment", p.Segment,
		"--execution-segment-sequence", p.Sequence,
		"--tag", "instance_id=" + strconv.Itoa(p.Instance),
		"--out", "json=" + fifo,
	}
	for _, o := range p.Out {
		args = append(args, "--out", o)
	}
	return append(args, archive)
}

func (a *agent) forwardLogs(r io.Reader, tail *tailBuffer) {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 64<<10), 1<<20)
	for s.Scan() {
		line := s.Text()
		tail.add(line)
		a.mu.Lock()
		a.logs++
		n := a.logs
		a.mu.Unlock()
		switch {
		case n <= logLimit:
			a.send(proto.Msg{Type: proto.Log, Text: line})
		case n == logLimit+1:
			a.send(proto.Msg{Type: proto.Log, Text: "further k6 output suppressed on this agent"})
		}
	}
}

func (a *agent) readSamples() {
	defer close(a.drained)
	f, err := os.OpenFile(a.fifo, os.O_RDONLY, 0)
	if err != nil {
		a.fail(fmt.Errorf("open sample stream: %w", err))
		return
	}
	defer f.Close()
	// The default 64 KiB pipe makes k6 and the agent take turns every few hundred
	// samples. Failure only costs throughput, so the result is ignored.
	_, _, _ = syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), fSetPipeSize, 1<<20)
	drain(f, func(lines [][]byte) {
		a.mu.Lock()
		defer a.mu.Unlock()
		for _, l := range lines {
			a.aggr.Feed(l)
		}
	})
}

// drain reads r in large blocks and hands over complete lines. Taking the lock
// per block and not copying each line keeps the agent's share of the CPU small,
// and that CPU is taken from k6 on the same machine.
func drain(r io.Reader, feed func([][]byte)) {
	buf := make([]byte, 1<<20)
	have := 0
	var lines [][]byte
	for {
		if have == len(buf) { // one line longer than the buffer
			buf = append(buf, make([]byte, len(buf))...)
		}
		n, err := r.Read(buf[have:])
		data := buf[:have+n]
		lines = lines[:0]
		rest := data
		for {
			i := bytes.IndexByte(rest, '\n')
			if i < 0 {
				break
			}
			if i > 0 {
				lines = append(lines, rest[:i])
			}
			rest = rest[i+1:]
		}
		if err != nil && len(rest) > 0 {
			lines = append(lines, rest)
			rest = nil
		}
		if len(lines) > 0 {
			feed(lines)
		}
		have = copy(buf, rest)
		if err != nil {
			return
		}
	}
}

// wakeReader unblocks readSamples if k6 died before opening the FIFO.
func (a *agent) wakeReader() {
	f, err := os.OpenFile(a.fifo, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err == nil {
		f.Close()
	}
}

func (a *agent) frames() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	cpu := newCPUMeter()
	for {
		select {
		case <-t.C:
			a.sendFrame(cpu.percent())
		case <-a.drained:
			return
		}
	}
}

func (a *agent) sendFrame(cpu float64) {
	a.mu.Lock()
	f := a.aggr.Flush()
	a.mu.Unlock()
	f.CPU = cpu
	a.send(proto.Msg{Type: proto.FrameMsg, Frame: f})
}

// pollStatus tells the controller when k6 is initialised and when it is done.
func (a *agent) pollStatus() {
	ready := false
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-a.exited:
			return
		case <-t.C:
		}
		body, err := a.call(http.MethodGet, "/v1/status", nil, 2*time.Second)
		if err != nil {
			continue // the API starts shortly after the process
		}
		var st struct {
			Data struct {
				Attributes struct {
					Status          int  `json:"status"`
					Running         bool `json:"running"`
					ExecutionResult *struct {
						ExitCode int `json:"exit_code"`
					} `json:"execution_result"`
				} `json:"attributes"`
			} `json:"data"`
		}
		if json.Unmarshal(body, &st) != nil {
			continue
		}
		at := st.Data.Attributes
		if !ready && at.Status == statusPausedBeforeRun {
			ready = true
			a.send(proto.Msg{Type: proto.Ready})
		}
		if at.Status >= statusEnded {
			a.mu.Lock()
			switch {
			case at.ExecutionResult != nil:
				a.code = at.ExecutionResult.ExitCode
			case at.Status != statusEnded:
				// k6 1.x has no execution_result. It was interrupted, and the
				// code only becomes known when the lingering process exits.
				a.code = CodeUnknown
			}
			a.mu.Unlock()
			a.markEnded("")
			return
		}
	}
}

func (a *agent) markEnded(text string) {
	a.mu.Lock()
	first := !a.ended
	a.ended = true
	code := a.code
	a.mu.Unlock()
	if first {
		a.send(proto.Msg{Type: proto.Ended, Code: code, Text: text})
	}
}

// resume starts the test at agent-local time at, which the controller computes
// from the measured clock offset so that a slow link does not skew the start.
func (a *agent) resume(at int64) {
	if d := time.Until(time.UnixMilli(at)); d > 0 {
		time.Sleep(d)
	}
	a.patchStatus(`{"paused":false}`)
}

func (a *agent) patchStatus(attrs string) {
	body := fmt.Sprintf(`{"data":{"type":"status","id":"default","attributes":%s}}`, attrs)
	if _, err := a.call(http.MethodPatch, "/v1/status", []byte(body), 30*time.Second); err != nil {
		a.fail(fmt.Errorf("patch status: %w", err))
	}
}

// finish ends the lingering k6 and delivers the last frame.
func (a *agent) finish() {
	if a.cmd == nil {
		a.send(proto.Msg{Type: proto.Exit})
		return
	}
	_ = a.cmd.Process.Signal(os.Interrupt)
	select {
	case <-a.exited:
	case <-time.After(30 * time.Second):
		_ = a.cmd.Process.Kill()
		<-a.exited
	}
	<-a.drained
	a.sendFrame(0)
	a.mu.Lock()
	code := a.code
	a.mu.Unlock()
	a.send(proto.Msg{Type: proto.Exit, Code: code})
}

func (a *agent) cleanup() {
	if a.cmd != nil && a.exited != nil {
		select {
		case <-a.exited:
		default:
			_ = a.cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-a.exited:
			case <-time.After(5 * time.Second):
				_ = a.cmd.Process.Kill()
			}
		}
	}
	if a.dir != "" {
		os.RemoveAll(a.dir)
	}
}

// call sends one request to the local k6 REST API. A zero timeout means none:
// setup and teardown take as long as the script needs, and k6 enforces
// setupTimeout and teardownTimeout itself.
func (a *agent) call(method, path string, body []byte, timeout time.Duration) ([]byte, error) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, method, a.api+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(out)))
	}
	return out, nil
}

// setupData extracts attributes.data from k6's JSON:API envelope. It stays raw,
// because the other agents must receive exactly the same JSON.
func setupData(body []byte) json.RawMessage {
	var env struct {
		Data struct {
			Attributes struct {
				Data json.RawMessage `json:"data"`
			} `json:"attributes"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &env) != nil {
		return nil
	}
	return env.Data.Attributes.Data
}

func k6Version(k6 string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, k6, "version").Output()
	if err != nil {
		return "unavailable: " + err.Error()
	}
	return ParseK6Version(string(out))
}

// ParseK6Version reduces `k6 version` output to its version token. The rest of
// the output names the build and differs between packagings of one release.
func ParseK6Version(s string) string {
	f := strings.Fields(s)
	if len(f) >= 2 && f[0] == "k6" {
		return f[1]
	}
	return strings.TrimSpace(s)
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return 1
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func nowMs() int64 { return time.Now().UnixMilli() }

type tailBuffer struct {
	mu    sync.Mutex
	lines []string
}

func (t *tailBuffer) add(s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lines = append(t.lines, s)
	if len(t.lines) > 15 {
		t.lines = t.lines[1:]
	}
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.Join(t.lines, "\n")
}
