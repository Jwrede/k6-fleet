package report

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Jwrede/k6-fleet/internal/agg"
	"github.com/Jwrede/k6-fleet/internal/threshold"
)

func fleet(t *testing.T) *agg.Fleet {
	t.Helper()
	a := agg.New([]agg.Spec{{Key: "http_req_duration{status:200}", Metric: "http_req_duration", Tags: map[string]string{"status": "200"}}})
	for _, l := range []string{
		`{"type":"Metric","data":{"name":"http_req_duration","type":"trend","contains":"time"},"metric":"http_req_duration"}`,
		`{"type":"Metric","data":{"name":"http_reqs","type":"counter","contains":"default"},"metric":"http_reqs"}`,
		`{"type":"Metric","data":{"name":"checks","type":"rate","contains":"default"},"metric":"checks"}`,
		`{"metric":"http_req_duration","type":"Point","data":{"time":"2026-11-06T05:00:00Z","value":120,"tags":{"status":"200"}}}`,
		`{"metric":"http_reqs","type":"Point","data":{"time":"2026-11-06T05:00:00Z","value":1,"tags":{}}}`,
		`{"metric":"checks","type":"Point","data":{"time":"2026-11-06T05:00:00Z","value":1,"tags":{"check":"ok","group":""}}}`,
	} {
		a.Feed([]byte(l))
	}
	f := agg.NewFleet()
	f.Add("a", a.Flush())
	return f
}

func sample(t *testing.T) *Data {
	f := fleet(t)
	pass, _ := threshold.ParseExpr("p(95)<500")
	pass.Series = "http_req_duration"
	fail, _ := threshold.ParseExpr("max<100")
	fail.Series = "http_req_duration{status:200}"
	missing, _ := threshold.ParseExpr("rate<0.1")
	missing.Series = "never_emitted"
	d := New()
	d.Script = "<script>.js"
	d.K6Version = "v2.3.0"
	d.Started = time.Date(2026, 11, 6, 5, 0, 0, 0, time.UTC)
	d.DurationMs = 2000
	d.Metrics = Metrics(f, 2*time.Second)
	d.Thresholds = Evaluate([]threshold.Threshold{pass, fail, missing}, f, 2*time.Second)
	d.AttachThresholds()
	d.Checks = Checks(f)
	d.Agents = []AgentResult{{Name: "a", Segment: "0:1", Iterations: 3, Requests: 1, CPUAvg: 20, CPUMax: 50}}
	d.Timeline = []Point{{Second: 1, Reqs: 1, P95: 120}, {Second: 2, Reqs: 3, P95: 80}}
	d.Warnings = []string{"example warning"}
	return d
}

func TestEvaluate(t *testing.T) {
	d := sample(t)
	if !d.Thresholds[0].OK || d.Thresholds[1].OK || !d.Thresholds[2].OK {
		t.Fatalf("%+v", d.Thresholds)
	}
	if d.Thresholds[1].Actual != 120 {
		t.Fatalf("actual %v", d.Thresholds[1].Actual)
	}
}

func TestMetricValues(t *testing.T) {
	d := sample(t)
	if v := d.Metrics["http_reqs"].Values; v["count"] != 1 || v["rate"] != 0.5 {
		t.Fatalf("%v", v)
	}
	if v := d.Metrics["checks"].Values; v["rate"] != 1 || v["passes"] != 1 {
		t.Fatalf("%v", v)
	}
	if !d.Metrics["http_req_duration"].Thresholds["p(95)<500"].OK {
		t.Fatal("threshold verdict missing from metric view")
	}
}

func TestWriteText(t *testing.T) {
	var b bytes.Buffer
	sample(t).WriteText(&b)
	s := b.String()
	for _, want := range []string{"THRESHOLDS", `FAIL http_req_duration{status:200} "max<100"  actual=120.00ms`, "CHECKS", "http_reqs", "AGENTS", "WARNING: example warning"} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in\n%s", want, s)
		}
	}
}

func TestWriteJSONAndHTML(t *testing.T) {
	dir := t.TempDir()
	d := sample(t)
	js := filepath.Join(dir, "s.json")
	if err := d.WriteJSON(js); err != nil {
		t.Fatal(err)
	}
	var back Data
	raw, _ := os.ReadFile(js)
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Metrics["http_reqs"].Values["count"] != 1 || len(back.Agents) != 1 {
		t.Fatalf("%+v", back)
	}
	h := filepath.Join(dir, "r.html")
	if err := d.WriteHTML(h); err != nil {
		t.Fatal(err)
	}
	page, _ := os.ReadFile(h)
	s := string(page)
	if !strings.Contains(s, "<polyline") || !strings.Contains(s, "&lt;script&gt;.js") || strings.Contains(s, "<script>.js") {
		t.Fatalf("report must chart the timeline and escape the script name:\n%.400s", s)
	}
}

func TestWriteProm(t *testing.T) {
	var b bytes.Buffer
	sample(t).WriteProm(&b)
	s := b.String()
	for _, want := range []string{`k6_fleet_metric{name="http_reqs",stat="count"} 1`, `k6_fleet_threshold_ok{series="http_req_duration{status:200}",threshold="max<100"} 0`, `k6_fleet_agent_cpu_percent{agent="a"} 50`} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in\n%s", want, s)
		}
	}
}

func TestFormatting(t *testing.T) {
	for v, want := range map[float64]string{0: "0s", 0.5: "500us", 12.345: "12.35ms", 2500: "2.50s", 120000: "2.00m"} {
		if got := fmtMs(v); got != want {
			t.Errorf("%v: %q", v, got)
		}
	}
	if got := fmtBytes(1500000); got != "1.5 MB" {
		t.Error(got)
	}
}
