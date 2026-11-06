// fakek6 stands in for k6 in tests that must run without it. It implements the
// parts of the CLI, the REST API and the JSON output that k6-fleet uses, as read
// from the k6 sources. It proves the orchestration logic; the e2e directory runs
// the same scenarios against real k6.
//
// The "script" is a JSON file with the Config below, so that everything reaches
// the agents through the archive, as a real script does.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Config is the content of a fake script.
type Config struct {
	Thresholds    map[string]any `json:"thresholds"`
	TotalDuration string         `json:"totalDuration"`
	Iterations    int            `json:"iterations"`
	PaceMs        int            `json:"paceMs"`
	Latency       map[string]int `json:"latency"` // instance -> ms, default 10
	FailInstance  *int           `json:"failInstance"`
	CrashEarly    *int           `json:"crashEarly"` // exits during init
	KillAgent     *int           `json:"killAgent"`  // kills the agent mid-run
	ExitCode      map[string]int `json:"exitCode"`   // instance -> k6 exit code
	// LegacyAbort makes the instance behave like k6 1.x after exec.test.abort():
	// status Interrupted, no execution_result, real code at process exit.
	LegacyAbort   *int   `json:"legacyAbort"`
	ShortInstance *int   `json:"shortInstance"` // ends after 10 iterations
	EventLog      string `json:"eventLog"`
	SetupData     any    `json:"setupData"`
}

type state struct {
	mu       sync.Mutex
	status   int
	paused   bool
	code     *int
	cfg      Config
	instance int
	start    chan struct{}
	stop     chan struct{}
	once     sync.Once
	stopOnce sync.Once
}

func (s *state) event(format string, a ...any) {
	if s.cfg.EventLog == "" {
		return
	}
	f, err := os.OpenFile(s.cfg.EventLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "instance=%d %d %s\n", s.instance, time.Now().UnixMilli(), fmt.Sprintf(format, a...))
}

func main() {
	if len(os.Args) < 2 {
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version":
		v := os.Getenv("FAKEK6_VERSION")
		if v == "" {
			v = "v2.3.0"
		}
		fmt.Printf("k6 %s (commit/fake, go1.25, linux/amd64)\n", v)
	case "archive":
		archive(os.Args[2:])
	case "inspect":
		cfg := load(os.Args[len(os.Args)-1])
		out, _ := json.Marshal(map[string]any{"thresholds": cfg.Thresholds, "totalDuration": cfg.TotalDuration, "maxVUs": 1})
		fmt.Println(string(out))
	case "run":
		run(os.Args[2:])
	default:
		os.Exit(2)
	}
}

func archive(args []string) {
	var dst, src string
	for i := 0; i < len(args); i++ {
		if args[i] == "-O" {
			dst = args[i+1]
			i++
			continue
		}
		src = args[i]
	}
	data, err := os.ReadFile(src)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(107)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		os.Exit(1)
	}
}

func load(path string) Config {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(107)
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(107)
	}
	return c
}

func share(total int, segment string) int {
	from, to, _ := strings.Cut(segment, ":")
	f, _ := new(big.Rat).SetString(from)
	t, _ := new(big.Rat).SetString(to)
	floor := func(r *big.Rat) int {
		x := new(big.Rat).Mul(r, big.NewRat(int64(total), 1))
		return int(new(big.Int).Div(x.Num(), x.Denom()).Int64())
	}
	return floor(t) - floor(f)
}

func run(args []string) {
	var addr, segment, out string
	instance := 0
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--address":
			addr = args[i+1]
		case "--execution-segment":
			segment = args[i+1]
		case "--out":
			if strings.HasPrefix(args[i+1], "json=") {
				out = strings.TrimPrefix(args[i+1], "json=")
			}
		case "--tag":
			if v, ok := strings.CutPrefix(args[i+1], "instance_id="); ok {
				instance, _ = strconv.Atoi(v)
			}
		}
	}
	cfg := load(args[len(args)-1])
	s := &state{status: 1, paused: true, cfg: cfg, instance: instance, start: make(chan struct{}), stop: make(chan struct{})}
	s.event("args %s", strings.Join(args, " "))
	if cfg.CrashEarly != nil && *cfg.CrashEarly == instance {
		fmt.Fprintln(os.Stderr, "SyntaxError: fake script exception")
		os.Exit(107)
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		os.Exit(106)
	}
	go func() { _ = http.Serve(l, s.routes()) }()

	// As in k6, output files are created with O_RDWR, which never blocks on a FIFO.
	sink, err := os.OpenFile(out, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		os.Exit(1)
	}
	w := bufio.NewWriter(sink)
	for _, m := range [][3]string{
		{"http_req_duration", "trend", "time"}, {"http_reqs", "counter", "default"}, {"iterations", "counter", "default"},
		{"http_req_failed", "rate", "default"}, {"checks", "rate", "default"}, {"vus", "gauge", "default"},
	} {
		fmt.Fprintf(w, `{"type":"Metric","data":{"name":%q,"type":%q,"contains":%q,"thresholds":[],"submetrics":null},"metric":%q}`+"\n", m[0], m[1], m[2], m[0])
	}
	s.mu.Lock()
	s.status = 4
	s.mu.Unlock()

	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt, syscall.SIGTERM)
	select {
	case <-s.start:
	case <-interrupted:
		os.Exit(105)
	}
	s.mu.Lock()
	s.status = 7
	s.mu.Unlock()
	n := share(cfg.Iterations, segment)
	if cfg.ShortInstance != nil && *cfg.ShortInstance == instance {
		n = 10
	}
	s.iterate(w, n)
	code := cfg.ExitCode[strconv.Itoa(instance)]
	s.mu.Lock()
	s.status = 9
	s.code = &code
	if cfg.LegacyAbort != nil && *cfg.LegacyAbort == instance {
		s.status, s.code, code = 10, nil, 108
	}
	s.mu.Unlock()
	s.event("ended code=%d", code)
	<-interrupted // --linger
	w.Flush()
	sink.Close()
	s.event("exit")
	os.Exit(code)
}

func (s *state) iterate(w *bufio.Writer, n int) {
	lat := s.cfg.Latency[strconv.Itoa(s.instance)]
	if lat == 0 {
		lat = 10
	}
	failed := s.cfg.FailInstance != nil && *s.cfg.FailInstance == s.instance
	ts := func() string { return time.Now().UTC().Format(time.RFC3339Nano) }
	point := func(metric string, v float64, tags string) {
		fmt.Fprintf(w, `{"metric":%q,"type":"Point","data":{"time":%q,"value":%v,"tags":{%s}}}`+"\n", metric, ts(), v, tags)
	}
	for i := 0; i < n; i++ {
		select {
		case <-s.stop:
			s.event("iterations=%d stopped", i)
			w.Flush()
			return
		default:
		}
		if s.cfg.KillAgent != nil && *s.cfg.KillAgent == s.instance && i == n/2 {
			w.Flush()
			s.event("killing agent")
			_ = syscall.Kill(os.Getppid(), syscall.SIGKILL)
			time.Sleep(time.Hour)
		}
		status, bad := "200", 0.0
		if failed {
			status, bad = "500", 1
		}
		tags := fmt.Sprintf(`"status":%q,"method":"GET","scenario":"default"`, status)
		point("http_req_duration", float64(lat)+float64(i%5), tags)
		point("http_reqs", 1, tags)
		point("http_req_failed", bad, tags)
		point("checks", 1-bad, `"check":"status is 200","group":""`)
		point("iterations", 1, `"scenario":"default"`)
		point("vus", 2, "")
		if i%20 == 0 {
			w.Flush()
		}
		if s.cfg.PaceMs > 0 {
			time.Sleep(time.Duration(s.cfg.PaceMs) * time.Millisecond)
		}
	}
	s.event("iterations=%d", n)
	w.Flush()
}

func (s *state) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			var env struct {
				Data struct {
					Attributes struct {
						Paused  *bool `json:"paused"`
						Stopped bool  `json:"stopped"`
					} `json:"attributes"`
				} `json:"data"`
			}
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &env)
			if p := env.Data.Attributes.Paused; p != nil && !*p {
				s.event("resume")
				s.once.Do(func() { close(s.start) })
			}
			if env.Data.Attributes.Stopped {
				s.event("stop")
				s.stopOnce.Do(func() { close(s.stop) })
			}
		}
		s.mu.Lock()
		attrs := map[string]any{"status": s.status, "paused": s.status == 4, "running": s.status == 7}
		if s.code != nil {
			attrs["execution_result"] = map[string]int{"exit_code": *s.code}
		}
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"type": "status", "id": "default", "attributes": attrs}})
	})
	mux.HandleFunc("/v1/setup", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			s.event("setup")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"type": "setupData", "id": "default", "attributes": map[string]any{"data": s.cfg.SetupData}}})
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			s.event("setdata %s", body)
			_, _ = w.Write([]byte(`{}`))
		}
	})
	mux.HandleFunc("/v1/teardown", func(w http.ResponseWriter, _ *http.Request) {
		s.event("teardown")
		_, _ = w.Write([]byte(`{}`))
	})
	return mux
}
