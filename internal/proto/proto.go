// Package proto defines the line-delimited JSON protocol between controller and
// agent. It runs over the agent's stdin and stdout, so any transport that can
// start a process and carry its stdio (ssh, docker, a local fork) works and no
// port has to be opened on a load generator.
package proto

import (
	"bufio"
	"encoding/json"
	"io"
	"sync"

	"github.com/Jwrede/k6-fleet/internal/agg"
)

// Message types.
const (
	// Controller to agent.
	Ping     = "ping"
	Prepare  = "prepare"
	Setup    = "setup"
	SetData  = "setdata"
	Resume   = "resume"
	Teardown = "teardown"
	Stop     = "stop"
	Finish   = "finish"

	// Agent to controller.
	Hello        = "hello"
	Pong         = "pong"
	Ready        = "ready"
	SetupDone    = "setup_done"
	DataSet      = "data_set"
	TeardownDone = "teardown_done"
	Ended        = "ended"
	FrameMsg     = "frame"
	Log          = "log"
	Error        = "error"
	Exit         = "exit"
)

// MaxLine bounds one message. The largest one is Prepare with the k6 archive.
const MaxLine = 512 << 20

// HelloBody identifies an agent.
type HelloBody struct {
	Version   string `json:"version"`
	K6Version string `json:"k6_version"`
	Hostname  string `json:"hostname"`
	CPUs      int    `json:"cpus"`
}

// PrepareBody tells an agent how to start its k6 instance.
type PrepareBody struct {
	Archive  []byte     `json:"archive"`
	Segment  string     `json:"segment"`
	Sequence string     `json:"sequence"`
	Instance int        `json:"instance"`
	K6       string     `json:"k6"`
	Out      []string   `json:"out,omitempty"`
	Specs    []agg.Spec `json:"specs,omitempty"`
}

// Msg is one protocol message. Unused fields stay empty.
type Msg struct {
	Type    string          `json:"type"`
	Hello   *HelloBody      `json:"hello,omitempty"`
	Prepare *PrepareBody    `json:"prepare,omitempty"`
	Frame   *agg.Frame      `json:"frame,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	T       int64           `json:"t,omitempty"`    // sender clock, unix ms
	Echo    int64           `json:"echo,omitempty"` // pong: the ping's T
	Code    int             `json:"code,omitempty"` // k6 exit code
	Text    string          `json:"text,omitempty"`
}

// Writer serialises messages from several goroutines.
type Writer struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// NewWriter returns a Writer on w.
func NewWriter(w io.Writer) *Writer { return &Writer{enc: json.NewEncoder(w)} }

// Send writes one message.
func (w *Writer) Send(m Msg) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(m)
}

// Reader reads messages.
type Reader struct{ s *bufio.Scanner }

// NewReader returns a Reader on r.
func NewReader(r io.Reader) *Reader {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 1<<20), MaxLine)
	return &Reader{s: s}
}

// Next returns the next message, or io.EOF when the stream ends.
func (r *Reader) Next() (Msg, error) {
	var m Msg
	if !r.s.Scan() {
		if err := r.s.Err(); err != nil {
			return m, err
		}
		return m, io.EOF
	}
	return m, json.Unmarshal(r.s.Bytes(), &m)
}
