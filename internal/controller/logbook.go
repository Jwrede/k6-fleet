package controller

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const logKeep = 2000

type logLine struct {
	Seq    int       `json:"seq"`
	Time   time.Time `json:"time"`
	Source string    `json:"source"`
	Level  string    `json:"level"`
	Text   string    `json:"text"`
}

// errorRow groups identical errors, like the Exceptions tab of Locust.
type errorRow struct {
	Message string   `json:"message"`
	Count   int      `json:"count"`
	Agents  []string `json:"agents"`
}

// logBook keeps the newest log lines of a run for the page. It is written by the
// event loop and read by HTTP handlers. Its methods accept a nil receiver, which
// is the state without the web page.
type logBook struct {
	mu     sync.Mutex
	seq    int
	lines  []logLine
	errors map[string]*errorRow
}

func newLogBook() *logBook { return &logBook{errors: map[string]*errorRow{}} }

var k6LogLine = regexp.MustCompile(`level=(\w+) msg=("(?:[^"\\]|\\.)*")`)

// add records a line and reads level and message from k6's logfmt output.
func (b *logBook) add(source, text string) {
	level := "info"
	if m := k6LogLine.FindStringSubmatch(text); m != nil {
		level = m[1]
		if s, err := strconv.Unquote(m[2]); err == nil {
			text = s
		}
	}
	b.addLevel(source, level, text)
}

func (b *logBook) addLevel(source, level, text string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	b.lines = append(b.lines, logLine{Seq: b.seq, Time: time.Now(), Source: source, Level: level, Text: text})
	if len(b.lines) > logKeep {
		b.lines = append(b.lines[:0:0], b.lines[len(b.lines)-logKeep:]...)
	}
	if level != "error" && level != "fatal" {
		return
	}
	key := strings.TrimSpace(text)
	row := b.errors[key]
	if row == nil {
		row = &errorRow{Message: key}
		b.errors[key] = row
	}
	row.Count++
	for _, a := range row.Agents {
		if a == source {
			return
		}
	}
	row.Agents = append(row.Agents, source)
}

// since returns the lines after seq, and the highest seq.
func (b *logBook) since(seq int) ([]logLine, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	i := sort.Search(len(b.lines), func(i int) bool { return b.lines[i].Seq > seq })
	return append([]logLine{}, b.lines[i:]...), b.seq
}

func (b *logBook) errorRows() []errorRow {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]errorRow, 0, len(b.errors))
	for _, r := range b.errors {
		row := *r
		row.Agents = append([]string(nil), r.Agents...)
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Message < out[j].Message
	})
	return out
}

// clear starts the book of a new run. Seq keeps growing, so a page that polls
// with an old position does not miss the new lines.
func (b *logBook) clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lines, b.errors = nil, map[string]*errorRow{}
}
