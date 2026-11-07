// Package agg reduces the k6 JSON sample stream to mergeable series. The agent
// side turns each second of samples into a Frame; the controller merges Frames
// from all agents into a Fleet.
package agg

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/Jwrede/k6-fleet/internal/sketch"
	"github.com/Jwrede/k6-fleet/internal/threshold"
)

// Metric types as k6 names them.
const (
	Counter = "counter"
	Gauge   = "gauge"
	Rate    = "rate"
	Trend   = "trend"
)

// Series is the state of one metric or submetric. It is both the per-frame
// delta and, once merged, the running total.
type Series struct {
	Type     string         `json:"t"`
	Contains string         `json:"c,omitempty"`
	Count    uint64         `json:"n"`
	Sum      float64        `json:"s,omitempty"`
	NonZero  uint64         `json:"nz,omitempty"`
	Last     float64        `json:"l,omitempty"`
	LastTime int64          `json:"lt,omitempty"`
	Min      float64        `json:"lo,omitempty"`
	Max      float64        `json:"hi,omitempty"`
	Sketch   *sketch.Sketch `json:"k,omitempty"`
}

func newSeries(typ, contains string) *Series {
	s := &Series{Type: typ, Contains: contains}
	if typ == Trend {
		s.Sketch = sketch.New()
	}
	return s
}

func (s *Series) add(v float64, t int64) {
	if s.Type != Trend && s.Type != Rate && (s.Count == 0 || v < s.Min) {
		s.Min = v
	}
	if s.Type != Trend && s.Type != Rate && (s.Count == 0 || v > s.Max) {
		s.Max = v
	}
	s.Count++
	switch s.Type {
	case Counter:
		s.Sum += v
	case Gauge:
		if t >= s.LastTime {
			s.Last, s.LastTime = v, t
		}
	case Rate:
		if v != 0 {
			s.NonZero++
		}
	case Trend:
		s.Sketch.Add(v)
	}
}

// merge folds a delta into a running total. Gauges are handled by Fleet.
func (s *Series) merge(o *Series) {
	if s.Type != Trend && s.Type != Rate {
		if s.Count == 0 || o.Min < s.Min {
			s.Min = o.Min
		}
		if s.Count == 0 || o.Max > s.Max {
			s.Max = o.Max
		}
	}
	s.Count += o.Count
	s.Sum += o.Sum
	s.NonZero += o.NonZero
	if o.LastTime >= s.LastTime {
		s.Last, s.LastTime = o.Last, o.LastTime
	}
	if s.Sketch != nil {
		s.Sketch.Merge(o.Sketch)
	}
}

// Check is the pass and fail count of one k6 check.
type Check struct {
	Pass uint64 `json:"p"`
	Fail uint64 `json:"f"`
}

// Frame is what an agent sends once per interval.
type Frame struct {
	Series   map[string]*Series `json:"series,omitempty"`
	Checks   map[string]*Check  `json:"checks,omitempty"`
	Lines    uint64             `json:"lines"`
	BadLines uint64             `json:"bad,omitempty"`
	LagMs    int64              `json:"lag,omitempty"`
	CPU      float64            `json:"cpu,omitempty"`
	MemBytes int64              `json:"mem,omitempty"` // resident size of k6

	// Requests and Failures are only filled when per-request statistics were
	// asked for. A Requests series is a Trend of http_req_duration whose NonZero
	// counts failed requests.
	Requests map[string]*Series `json:"rq,omitempty"`
	Failures map[string]uint64  `json:"fl,omitempty"`
}

// Spec asks for a submetric, with Key being the k6 notation.
type Spec struct {
	Key    string            `json:"key"`
	Metric string            `json:"metric"`
	Tags   map[string]string `json:"tags"`
}

// SpecsFor derives the submetrics that the thresholds need.
func SpecsFor(ts []threshold.Threshold) ([]Spec, error) {
	seen := map[string]bool{}
	var out []Spec
	for _, t := range ts {
		if seen[t.Series] {
			continue
		}
		seen[t.Series] = true
		name, tags, err := threshold.SplitSeries(t.Series)
		if err != nil {
			return nil, err
		}
		if len(tags) > 0 {
			out = append(out, Spec{Key: t.Series, Metric: name, Tags: tags})
		}
	}
	return out, nil
}

type metaInfo struct{ typ, contains string }

// Aggregator consumes k6 JSON lines. It is not safe for concurrent use.
type Aggregator struct {
	specs map[string][]Spec
	types map[string]metaInfo
	frame *Frame
	now   func() time.Time
	seen  uint64

	perRequest bool
	names      map[string]bool // request keys sent so far, bounded by MaxRequestNames
}

// MaxRequestNames bounds the request keys one agent reports. k6 uses the URL as
// the name unless the script sets one, so a URL with an ID in it would otherwise
// grow without limit. Further names count as OtherRequests.
const MaxRequestNames = 500

// OtherRequests is the name that collects requests beyond MaxRequestNames.
const OtherRequests = "(other names)"

const durationMetric = "http_req_duration"

// PerRequest turns on statistics per method and name. The tags of every
// http_req_duration sample then have to be decoded, which costs the fast path
// for that metric.
func (a *Aggregator) PerRequest() {
	a.perRequest = true
	a.names = map[string]bool{}
}

// New returns an Aggregator that also tracks the given submetrics.
func New(specs []Spec) *Aggregator {
	a := &Aggregator{specs: map[string][]Spec{}, types: map[string]metaInfo{}, now: time.Now}
	for _, s := range specs {
		a.specs[s.Metric] = append(a.specs[s.Metric], s)
	}
	a.reset()
	return a
}

func (a *Aggregator) reset() {
	a.frame = &Frame{Series: map[string]*Series{}, Checks: map[string]*Check{}}
}

type wireLine struct {
	Type   string `json:"type"`
	Metric string `json:"metric"`
	Data   struct {
		Name     string            `json:"name"`
		Type     string            `json:"type"`
		Contains string            `json:"contains"`
		Time     string            `json:"time"`
		Value    float64           `json:"value"`
		Tags     map[string]string `json:"tags"`
	} `json:"data"`
}

var (
	pointPrefix = []byte(`{"metric":"`)
	pointMiddle = []byte(`","type":"Point","data":{"time":"`)
	pointValue  = []byte(`","value":`)
)

// fastPoint handles the common Point line without a JSON decoder. encoding/json
// costs about 20us per line, which at 60k samples/s takes a whole core from the
// load generator. It reports false for anything it does not fully understand,
// which then takes the generic path. Tags are not decoded here, so metrics with
// submetrics and the checks metric always use the generic path.
func (a *Aggregator) fastPoint(b []byte) bool {
	if !bytes.HasPrefix(b, pointPrefix) {
		return false
	}
	rest := b[len(pointPrefix):]
	i := bytes.IndexByte(rest, '"')
	if i < 0 || !bytes.HasPrefix(rest[i:], pointMiddle[:1]) {
		return false
	}
	name := rest[:i]
	info, ok := a.types[string(name)]
	if !ok || info.typ == Gauge || string(name) == "checks" || len(a.specs[string(name)]) > 0 {
		return false
	}
	if a.perRequest && string(name) == durationMetric {
		return false
	}
	rest = rest[i:]
	if !bytes.HasPrefix(rest, pointMiddle) {
		return false
	}
	rest = rest[len(pointMiddle):]
	i = bytes.IndexByte(rest, '"')
	if i < 0 || !bytes.HasPrefix(rest[i:], pointValue) {
		return false
	}
	ts := rest[:i]
	rest = rest[i+len(pointValue):]
	i = bytes.IndexByte(rest, ',')
	if i < 0 {
		return false
	}
	v, err := strconv.ParseFloat(string(rest[:i]), 64)
	if err != nil {
		return false
	}
	a.frame.Lines++
	s := a.frame.Series[string(name)]
	if s == nil {
		s = a.series(string(name), info)
	}
	s.add(v, 0)
	if a.seen++; a.seen%512 == 1 {
		if t, err := time.Parse(time.RFC3339Nano, string(ts)); err == nil {
			a.noteLag(t)
		}
	}
	return true
}

func (a *Aggregator) noteLag(t time.Time) {
	if lag := a.now().Sub(t).Milliseconds(); lag > a.frame.LagMs {
		a.frame.LagMs = lag
	}
}

// Feed processes one line of `k6 run --out json`.
func (a *Aggregator) Feed(b []byte) {
	if a.fastPoint(b) {
		return
	}
	var l wireLine
	if err := json.Unmarshal(b, &l); err != nil {
		a.frame.BadLines++
		return
	}
	a.frame.Lines++
	switch l.Type {
	case "Metric":
		a.types[l.Data.Name] = metaInfo{l.Data.Type, l.Data.Contains}
	case "Point":
		a.point(&l)
	}
}

func (a *Aggregator) point(l *wireLine) {
	info, ok := a.types[l.Metric]
	if !ok {
		a.frame.BadLines++
		return
	}
	t, _ := time.Parse(time.RFC3339Nano, l.Data.Time)
	ms := t.UnixMilli()
	a.series(l.Metric, info).add(l.Data.Value, ms)
	for _, s := range a.specs[l.Metric] {
		if matches(s.Tags, l.Data.Tags) {
			a.series(s.Key, info).add(l.Data.Value, ms)
		}
	}
	if a.perRequest && l.Metric == durationMetric {
		a.request(l.Data.Tags, l.Data.Value)
	}
	if l.Metric == "checks" {
		key := CheckKey(l.Data.Tags["group"], l.Data.Tags["check"])
		c := a.frame.Checks[key]
		if c == nil {
			c = &Check{}
			a.frame.Checks[key] = c
		}
		if l.Data.Value != 0 {
			c.Pass++
		} else {
			c.Fail++
		}
	}
	if a.seen++; a.seen%512 == 1 && !t.IsZero() {
		a.noteLag(t)
	}
}

func (a *Aggregator) request(tags map[string]string, v float64) {
	name := tags["name"]
	if name == "" {
		name = tags["url"]
	}
	key := RequestKey(tags["method"], name)
	if !a.names[key] {
		if len(a.names) >= MaxRequestNames {
			name = OtherRequests
			key = RequestKey(tags["method"], name)
		} else {
			a.names[key] = true
		}
	}
	if a.frame.Requests == nil {
		a.frame.Requests, a.frame.Failures = map[string]*Series{}, map[string]uint64{}
	}
	s := a.frame.Requests[key]
	if s == nil {
		s = newSeries(Trend, "time")
		a.frame.Requests[key] = s
	}
	s.add(v, 0)
	if tags["expected_response"] == "false" {
		s.NonZero++
		a.frame.Failures[FailureKey(tags["method"], name, tags["status"], tags["error"])]++
	}
}

func (a *Aggregator) series(key string, info metaInfo) *Series {
	s := a.frame.Series[key]
	if s == nil {
		s = newSeries(info.typ, info.contains)
		a.frame.Series[key] = s
	}
	return s
}

func matches(want, have map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// Flush returns the samples since the last Flush.
func (a *Aggregator) Flush() *Frame {
	f := a.frame
	a.reset()
	return f
}

// RequestKey joins a method and a request name.
func RequestKey(method, name string) string { return method + "\x1f" + name }

// FailureKey joins the fields that identify a kind of failure.
func FailureKey(method, name, status, errText string) string {
	return method + "\x1f" + name + "\x1f" + status + "\x1f" + errText
}

// CheckKey joins a k6 group path and check name.
func CheckKey(group, name string) string { return group + "\x1f" + name }

// SplitCheckKey reverses CheckKey.
func SplitCheckKey(key string) (group, name string) {
	group, name, _ = strings.Cut(key, "\x1f")
	return group, name
}

// Fleet is the merged view over all agents.
type Fleet struct {
	Series   map[string]*Series
	Checks   map[string]*Check
	Requests map[string]*Series
	Failures map[string]uint64
	Lines    uint64
	Bad      uint64
	gauges   map[string]map[string]float64
}

// NewFleet returns an empty Fleet.
func NewFleet() *Fleet {
	return &Fleet{
		Series: map[string]*Series{}, Checks: map[string]*Check{}, gauges: map[string]map[string]float64{},
		Requests: map[string]*Series{}, Failures: map[string]uint64{},
	}
}

// summedGauges add up across instances. Any other gauge is a single reading of
// something the script measures, so the newest sample wins, as in one k6.
var summedGauges = map[string]bool{"vus": true, "vus_max": true}

// Add merges a frame from the named agent.
func (f *Fleet) Add(agent string, fr *Frame) {
	f.Lines += fr.Lines
	f.Bad += fr.BadLines
	for k, d := range fr.Series {
		s := f.Series[k]
		if s == nil {
			s = newSeries(d.Type, d.Contains)
			f.Series[k] = s
		}
		if d.Type == Gauge && summedGauges[k] {
			f.addSummedGauge(k, agent, s, d)
			continue
		}
		s.merge(d)
	}
	for k, c := range fr.Checks {
		t := f.Checks[k]
		if t == nil {
			t = &Check{}
			f.Checks[k] = t
		}
		t.Pass += c.Pass
		t.Fail += c.Fail
	}
	for k, d := range fr.Requests {
		s := f.Requests[k]
		if s == nil {
			s = newSeries(Trend, "time")
			f.Requests[k] = s
		}
		s.merge(d)
	}
	for k, n := range fr.Failures {
		f.Failures[k] += n
	}
}

// ResetRequests clears the per-request statistics and leaves everything that
// thresholds and the summary are computed from.
func (f *Fleet) ResetRequests() {
	f.Requests, f.Failures = map[string]*Series{}, map[string]uint64{}
}

func (f *Fleet) addSummedGauge(key, agent string, s, d *Series) {
	last := f.gauges[key]
	if last == nil {
		last = map[string]float64{}
		f.gauges[key] = last
	}
	last[agent] = d.Last
	var total float64
	for _, v := range last {
		total += v
	}
	if s.Count == 0 || total < s.Min {
		s.Min = total
	}
	if s.Count == 0 || total > s.Max {
		s.Max = total
	}
	s.Count += d.Count
	s.Last = total
	if d.LastTime > s.LastTime {
		s.LastTime = d.LastTime
	}
}

// View binds a series to the test duration, which Counter rates need.
type View struct {
	S       *Series
	Elapsed time.Duration
}

// Aggregate implements threshold.Stats.
func (v View) Aggregate(method string, p float64) (float64, bool) {
	s := v.S
	switch s.Type {
	case Counter:
		switch method {
		case "count":
			return s.Sum, true
		case "rate":
			if v.Elapsed <= 0 {
				return 0, true
			}
			return s.Sum / v.Elapsed.Seconds(), true
		}
	case Gauge:
		if method == "value" {
			return s.Last, true
		}
	case Rate:
		if method == "rate" {
			if s.Count == 0 {
				return 0, true
			}
			return float64(s.NonZero) / float64(s.Count), true
		}
	case Trend:
		switch method {
		case "avg":
			return s.Sketch.Avg(), true
		case "min":
			return s.Sketch.Min, true
		case "max":
			return s.Sketch.Max, true
		case "med":
			return s.Sketch.Quantile(0.5), true
		case "p":
			return s.Sketch.Quantile(p), true
		}
	}
	return 0, false
}
