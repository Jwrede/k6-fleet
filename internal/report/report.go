// Package report turns the merged fleet state into a summary: text for the
// terminal, JSON for pipelines, a self-contained HTML page, and the Prometheus
// text format for live scraping.
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Jwrede/k6-fleet/internal/agg"
	"github.com/Jwrede/k6-fleet/internal/sketch"
	"github.com/Jwrede/k6-fleet/internal/threshold"
)

// ThresholdResult is the verdict on one threshold.
type ThresholdResult struct {
	Series string  `json:"series"`
	Source string  `json:"source"`
	OK     bool    `json:"ok"`
	Actual float64 `json:"actual"`
	Error  string  `json:"error,omitempty"`
}

// Metric holds the final values of one metric or submetric.
type Metric struct {
	Type     string             `json:"type"`
	Contains string             `json:"contains"`
	Values   map[string]float64 `json:"values"`
	// Thresholds uses the shape of k6's handleSummary data.
	Thresholds map[string]struct {
		OK bool `json:"ok"`
	} `json:"thresholds,omitempty"`
}

// CheckResult is one k6 check.
type CheckResult struct {
	Group string `json:"group"`
	Name  string `json:"name"`
	Pass  uint64 `json:"passes"`
	Fail  uint64 `json:"fails"`
}

// AgentResult is the per-agent part of the summary.
type AgentResult struct {
	Name       string  `json:"name"`
	Host       string  `json:"host"`
	Segment    string  `json:"segment"`
	Iterations float64 `json:"iterations"`
	Requests   float64 `json:"requests"`
	CPUAvg     float64 `json:"cpu_avg"`
	CPUMax     float64 `json:"cpu_max"`
	MaxLagMs   int64   `json:"max_lag_ms"`
	OffsetMs   int64   `json:"clock_offset_ms"`
	ExitCode   int     `json:"exit_code"`
	Lost       bool    `json:"lost,omitempty"`
}

// Point is one second of the run.
type Point struct {
	Second int     `json:"second"`
	VUs    float64 `json:"vus"`
	Reqs   float64 `json:"reqs"`
	Iters  float64 `json:"iterations"`
	P50    float64 `json:"p50_ms"`
	P95    float64 `json:"p95_ms"`
	P99    float64 `json:"p99_ms"`
	Fails  float64 `json:"fails"`
	Failed float64 `json:"failed_rate"`
	CPUMax float64 `json:"cpu_max"`
}

// RequestRow is the statistics of one method and name.
type RequestRow struct {
	Method      string  `json:"method"`
	Name        string  `json:"name"`
	Requests    uint64  `json:"requests"`
	Fails       uint64  `json:"fails"`
	Avg         float64 `json:"avg_ms"`
	Min         float64 `json:"min_ms"`
	Med         float64 `json:"med_ms"`
	P90         float64 `json:"p90_ms"`
	P95         float64 `json:"p95_ms"`
	P99         float64 `json:"p99_ms"`
	Max         float64 `json:"max_ms"`
	RPS         float64 `json:"rps"`
	FailsPerSec float64 `json:"fails_per_s"`
}

// FailureRow counts one kind of failed request.
type FailureRow struct {
	Method string `json:"method"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error"`
	Count  uint64 `json:"count"`
}

// Total is the name of the row that sums all others.
const Total = "Aggregated"

func requestRow(method, name string, k *sketch.Sketch, fails uint64, rps, fps float64) RequestRow {
	return RequestRow{
		Method: method, Name: name, Requests: uint64(k.Count), Fails: fails, Avg: k.Avg(), Min: k.Min, Max: k.Max,
		Med: k.Quantile(0.5), P90: k.Quantile(0.9), P95: k.Quantile(0.95), P99: k.Quantile(0.99),
		RPS: rps, FailsPerSec: fps,
	}
}

// Requests lists the statistics per request, sorted, with a total as last row.
// rate gives the current requests and failures per second of a key.
func Requests(f *agg.Fleet, rate func(key string) (float64, float64)) []RequestRow {
	if len(f.Requests) == 0 {
		return nil
	}
	out := make([]RequestRow, 0, len(f.Requests)+1)
	total := sketch.New()
	var fails uint64
	var rps, fps float64
	for k, s := range f.Requests {
		method, name, _ := strings.Cut(k, "\x1f")
		r, e := rate(k)
		out = append(out, requestRow(method, name, s.Sketch, s.NonZero, r, e))
		total.Merge(s.Sketch)
		fails += s.NonZero
		rps += r
		fps += e
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Method < out[j].Method
	})
	return append(out, requestRow("", Total, total, fails, rps, fps))
}

// Failures lists the kinds of failed requests, the most frequent first.
func Failures(f *agg.Fleet) []FailureRow {
	out := make([]FailureRow, 0, len(f.Failures))
	for k, n := range f.Failures {
		p := strings.SplitN(k, "\x1f", 4)
		out = append(out, FailureRow{Method: p[0], Name: p[1], Status: p[2], Error: p[3], Count: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Name+out[i].Status < out[j].Name+out[j].Status
	})
	return out
}

// Data is the whole summary.
type Data struct {
	Script     string            `json:"script"`
	K6Version  string            `json:"k6_version"`
	Started    time.Time         `json:"started"`
	DurationMs int64             `json:"duration_ms"`
	ExitCode   int               `json:"exit_code"`
	Aborted    string            `json:"aborted,omitempty"`
	Metrics    map[string]Metric `json:"metrics"`
	Thresholds []ThresholdResult `json:"thresholds"`
	Checks     []CheckResult     `json:"checks"`
	Agents     []AgentResult     `json:"agents"`
	Timeline   []Point           `json:"timeline"`
	// Requests and Failures exist with --web. The last Requests row is the total.
	Requests []RequestRow `json:"requests,omitempty"`
	Failures []FailureRow `json:"failures,omitempty"`
	Warnings []string     `json:"warnings,omitempty"`
	// PercentileRelativeError bounds the percentile values. Everything else is
	// exact.
	PercentileRelativeError float64 `json:"percentile_relative_error"`
}

// Evaluate checks the thresholds against the fleet.
func Evaluate(ts []threshold.Threshold, f *agg.Fleet, elapsed time.Duration) []ThresholdResult {
	out := make([]ThresholdResult, 0, len(ts))
	for _, t := range ts {
		r := ThresholdResult{Series: t.Series, Source: t.Source}
		s := f.Series[t.Series]
		if s == nil {
			r.OK = true
		} else {
			v := agg.View{S: s, Elapsed: elapsed}
			ok, err := t.Check(v)
			r.OK = ok
			if err != nil {
				r.Error = err.Error()
			}
			r.Actual, _ = v.Aggregate(t.Method, t.Percentile)
		}
		out = append(out, r)
	}
	return out
}

// Metrics converts the fleet series to final values.
func Metrics(f *agg.Fleet, elapsed time.Duration) map[string]Metric {
	out := map[string]Metric{}
	for k, s := range f.Series {
		m := Metric{Type: s.Type, Contains: s.Contains, Values: map[string]float64{}}
		switch s.Type {
		case agg.Counter:
			m.Values["count"] = s.Sum
			if elapsed > 0 {
				m.Values["rate"] = s.Sum / elapsed.Seconds()
			}
		case agg.Gauge:
			m.Values["value"], m.Values["min"], m.Values["max"] = s.Last, s.Min, s.Max
		case agg.Rate:
			m.Values["passes"] = float64(s.NonZero)
			m.Values["fails"] = float64(s.Count - s.NonZero)
			if s.Count > 0 {
				m.Values["rate"] = float64(s.NonZero) / float64(s.Count)
			}
		case agg.Trend:
			k := s.Sketch
			m.Values["avg"], m.Values["min"], m.Values["max"] = k.Avg(), k.Min, k.Max
			m.Values["med"] = k.Quantile(0.5)
			m.Values["p(90)"], m.Values["p(95)"], m.Values["p(99)"] = k.Quantile(0.9), k.Quantile(0.95), k.Quantile(0.99)
			m.Values["count"] = float64(k.Count)
		}
		out[k] = m
	}
	return out
}

// Checks lists the checks in a stable order.
func Checks(f *agg.Fleet) []CheckResult {
	out := make([]CheckResult, 0, len(f.Checks))
	for k, c := range f.Checks {
		g, n := agg.SplitCheckKey(k)
		out = append(out, CheckResult{Group: g, Name: n, Pass: c.Pass, Fail: c.Fail})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Group != out[j].Group {
			return out[i].Group < out[j].Group
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// New assembles the fixed part of a summary.
func New() *Data {
	return &Data{PercentileRelativeError: sketch.RelativeError}
}

// AttachThresholds copies verdicts into the per-metric view.
func (d *Data) AttachThresholds() {
	for _, r := range d.Thresholds {
		m, ok := d.Metrics[r.Series]
		if !ok {
			continue
		}
		if m.Thresholds == nil {
			m.Thresholds = map[string]struct {
				OK bool `json:"ok"`
			}{}
		}
		m.Thresholds[r.Source] = struct {
			OK bool `json:"ok"`
		}{r.OK}
		d.Metrics[r.Series] = m
	}
}

// WriteJSON stores the summary at path.
func (d *Data) WriteJSON(path string) error {
	b, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func fmtMs(v float64) string {
	switch {
	case v >= 60000:
		return fmt.Sprintf("%.2fm", v/60000)
	case v >= 1000:
		return fmt.Sprintf("%.2fs", v/1000)
	case v >= 1:
		return fmt.Sprintf("%.2fms", v)
	case v == 0:
		return "0s"
	}
	return fmt.Sprintf("%.0fus", v*1000)
}

func fmtBytes(v float64) string {
	for _, u := range []string{"B", "kB", "MB", "GB"} {
		if math.Abs(v) < 1000 {
			return fmt.Sprintf("%.1f %s", v, u)
		}
		v /= 1000
	}
	return fmt.Sprintf("%.1f TB", v)
}

func fmtValue(m Metric, v float64) string {
	switch m.Contains {
	case "time":
		return fmtMs(v)
	case "data":
		return fmtBytes(v)
	}
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.4g", v)
}

func (d *Data) metricLine(name string, m Metric) string {
	v := m.Values
	switch m.Type {
	case agg.Counter:
		return fmt.Sprintf("%s  %.2f/s", fmtValue(m, v["count"]), v["rate"])
	case agg.Gauge:
		return fmt.Sprintf("%s  min=%s max=%s", fmtValue(m, v["value"]), fmtValue(m, v["min"]), fmtValue(m, v["max"]))
	case agg.Rate:
		return fmt.Sprintf("%.2f%%  %.0f out of %.0f", 100*v["rate"], v["passes"], v["passes"]+v["fails"])
	}
	return fmt.Sprintf("avg=%s min=%s med=%s max=%s p(90)=%s p(95)=%s p(99)=%s",
		fmtValue(m, v["avg"]), fmtValue(m, v["min"]), fmtValue(m, v["med"]), fmtValue(m, v["max"]),
		fmtValue(m, v["p(90)"]), fmtValue(m, v["p(95)"]), fmtValue(m, v["p(99)"]))
}

// WriteText prints the terminal summary.
func (d *Data) WriteText(w io.Writer) {
	fmt.Fprintf(w, "\n  k6-fleet summary: %s, %d agents, k6 %s, %s\n", d.Script, len(d.Agents), d.K6Version, (time.Duration(d.DurationMs) * time.Millisecond).Round(100*time.Millisecond))
	if d.Aborted != "" {
		fmt.Fprintf(w, "  aborted: %s\n", d.Aborted)
	}
	if len(d.Thresholds) > 0 {
		fmt.Fprintf(w, "\n  THRESHOLDS (evaluated on merged data)\n")
		for _, r := range d.Thresholds {
			mark := "ok  "
			if !r.OK {
				mark = "FAIL"
			}
			fmt.Fprintf(w, "    %s %s %q  actual=%s\n", mark, r.Series, r.Source, fmtValue(d.Metrics[r.Series], r.Actual))
			if r.Error != "" {
				fmt.Fprintf(w, "         %s\n", r.Error)
			}
		}
	}
	if len(d.Checks) > 0 {
		fmt.Fprintf(w, "\n  CHECKS\n")
		for _, c := range d.Checks {
			name := c.Name
			if c.Group != "" {
				name = c.Group + " " + name
			}
			fmt.Fprintf(w, "    %-40s %d passed, %d failed\n", name, c.Pass, c.Fail)
		}
	}
	names := make([]string, 0, len(d.Metrics))
	width := 0
	for k := range d.Metrics {
		names = append(names, k)
		if len(k) > width {
			width = len(k)
		}
	}
	sort.Strings(names)
	fmt.Fprintf(w, "\n  METRICS\n")
	for _, k := range names {
		fmt.Fprintf(w, "    %-*s  %s\n", width, k, d.metricLine(k, d.Metrics[k]))
	}
	fmt.Fprintf(w, "\n  AGENTS\n")
	fmt.Fprintf(w, "    %-16s %-10s %10s %10s %8s %8s %8s %6s\n", "name", "segment", "iterations", "requests", "cpu avg", "cpu max", "lag max", "exit")
	for _, a := range d.Agents {
		exit := fmt.Sprint(a.ExitCode)
		if a.Lost {
			exit = "lost"
		}
		fmt.Fprintf(w, "    %-16s %-10s %10.0f %10.0f %7.0f%% %7.0f%% %6dms %6s\n", a.Name, a.Segment, a.Iterations, a.Requests, a.CPUAvg, a.CPUMax, a.MaxLagMs, exit)
	}
	for _, s := range d.Warnings {
		fmt.Fprintf(w, "\n  WARNING: %s", s)
	}
	if len(d.Warnings) > 0 {
		fmt.Fprintln(w)
	}
	fmt.Fprintln(w)
}

func promEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

// WriteProm writes the current state in the Prometheus text format.
func (d *Data) WriteProm(w io.Writer) {
	fmt.Fprintln(w, "# TYPE k6_fleet_metric gauge")
	names := make([]string, 0, len(d.Metrics))
	for k := range d.Metrics {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		m := d.Metrics[k]
		stats := make([]string, 0, len(m.Values))
		for s := range m.Values {
			stats = append(stats, s)
		}
		sort.Strings(stats)
		for _, s := range stats {
			fmt.Fprintf(w, "k6_fleet_metric{name=\"%s\",stat=\"%s\"} %v\n", promEscape(k), promEscape(s), m.Values[s])
		}
	}
	fmt.Fprintln(w, "# TYPE k6_fleet_threshold_ok gauge")
	for _, t := range d.Thresholds {
		v := 0
		if t.OK {
			v = 1
		}
		fmt.Fprintf(w, "k6_fleet_threshold_ok{series=\"%s\",threshold=\"%s\"} %d\n", promEscape(t.Series), promEscape(t.Source), v)
	}
	fmt.Fprintln(w, "# TYPE k6_fleet_agent_cpu_percent gauge")
	for _, a := range d.Agents {
		fmt.Fprintf(w, "k6_fleet_agent_cpu_percent{agent=\"%s\"} %v\n", promEscape(a.Name), a.CPUMax)
	}
}
