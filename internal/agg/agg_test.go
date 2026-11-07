package agg

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/Jwrede/k6-fleet/internal/threshold"
)

const header = `{"type":"Metric","data":{"name":"http_req_duration","type":"trend","contains":"time","thresholds":[],"submetrics":null},"metric":"http_req_duration"}
{"type":"Metric","data":{"name":"http_reqs","type":"counter","contains":"default","thresholds":[],"submetrics":null},"metric":"http_reqs"}
{"type":"Metric","data":{"name":"http_req_failed","type":"rate","contains":"default","thresholds":[],"submetrics":null},"metric":"http_req_failed"}
{"type":"Metric","data":{"name":"checks","type":"rate","contains":"default","thresholds":[],"submetrics":null},"metric":"checks"}
{"type":"Metric","data":{"name":"vus","type":"gauge","contains":"default","thresholds":[],"submetrics":null},"metric":"vus"}
{"type":"Metric","data":{"name":"queue_depth","type":"gauge","contains":"default","thresholds":[],"submetrics":null},"metric":"queue_depth"}`

func feed(a *Aggregator, s string) {
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '\n' {
			if i > start {
				a.Feed([]byte(s[start:i]))
			}
			start = i + 1
		}
	}
}

func point(metric string, v float64, tags string) string {
	return fmt.Sprintf(`{"metric":%q,"type":"Point","data":{"time":"2026-11-06T05:00:00.5Z","value":%v,"tags":{%s}}}`, metric, v, tags)
}

func TestFeedAndSubmetrics(t *testing.T) {
	a := New([]Spec{{Key: "http_req_duration{status:200}", Metric: "http_req_duration", Tags: map[string]string{"status": "200"}}})
	feed(a, header)
	for i := 0; i < 10; i++ {
		st := "200"
		if i == 9 {
			st = "500"
		}
		a.Feed([]byte(point("http_req_duration", float64(100+i), fmt.Sprintf(`"status":%q`, st))))
		a.Feed([]byte(point("http_reqs", 1, `"status":"200"`)))
	}
	f := a.Flush()
	if got := f.Series["http_req_duration"].Count; got != 10 {
		t.Fatalf("trend count %d", got)
	}
	if got := f.Series["http_req_duration{status:200}"].Count; got != 9 {
		t.Fatalf("submetric count %d", got)
	}
	if f.Series["http_reqs"].Sum != 10 {
		t.Fatalf("counter sum %v", f.Series["http_reqs"].Sum)
	}
	if n := a.Flush(); len(n.Series) != 0 {
		t.Fatal("flush must reset")
	}
}

func TestChecksAndBadLines(t *testing.T) {
	a := New(nil)
	feed(a, header)
	a.Feed([]byte(point("checks", 1, `"check":"status is 200","group":"::login"`)))
	a.Feed([]byte(point("checks", 0, `"check":"status is 200","group":"::login"`)))
	a.Feed([]byte("not json"))
	a.Feed([]byte(point("unknown_metric", 1, "")))
	f := a.Flush()
	c := f.Checks[CheckKey("::login", "status is 200")]
	if c == nil || c.Pass != 1 || c.Fail != 1 {
		t.Fatalf("%+v", c)
	}
	if f.BadLines != 2 {
		t.Fatalf("bad lines %d", f.BadLines)
	}
	if g, n := SplitCheckKey(CheckKey("::a", "b")); g != "::a" || n != "b" {
		t.Fatal(g, n)
	}
}

func frameFor(t *testing.T, lines ...string) *Frame {
	t.Helper()
	a := New(nil)
	feed(a, header)
	for _, l := range lines {
		a.Feed([]byte(l))
	}
	b, err := json.Marshal(a.Flush())
	if err != nil {
		t.Fatal(err)
	}
	var f Frame
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	return &f
}

func TestFleetMerge(t *testing.T) {
	fl := NewFleet()
	fl.Add("a", frameFor(t, point("http_req_duration", 10, ""), point("http_reqs", 1, ""), point("http_req_failed", 0, ""), point("vus", 5, "")))
	fl.Add("b", frameFor(t, point("http_req_duration", 30, ""), point("http_reqs", 1, ""), point("http_req_failed", 1, ""), point("vus", 7, "")))
	v := View{fl.Series["http_req_duration"], 2 * time.Second}
	if avg, _ := v.Aggregate("avg", 0); avg != 20 {
		t.Fatalf("avg %v", avg)
	}
	if mx, _ := v.Aggregate("max", 0); mx != 30 {
		t.Fatalf("max %v", mx)
	}
	if r, _ := (View{fl.Series["http_reqs"], 2 * time.Second}).Aggregate("rate", 0); r != 1 {
		t.Fatalf("counter rate %v", r)
	}
	if r, _ := (View{fl.Series["http_req_failed"], 0}).Aggregate("rate", 0); r != 0.5 {
		t.Fatalf("rate %v", r)
	}
	vus := fl.Series["vus"]
	if vus.Last != 12 || vus.Max != 12 {
		t.Fatalf("vus must be summed: %+v", vus)
	}
	fl.Add("a", frameFor(t, point("vus", 1, "")))
	if vus.Last != 8 || vus.Max != 12 || vus.Min != 5 {
		t.Fatalf("vus after drop: %+v", vus)
	}
}

func TestCustomGaugeTakesNewestSample(t *testing.T) {
	fl := NewFleet()
	mk := func(ts string, v float64) string {
		return fmt.Sprintf(`{"metric":"queue_depth","type":"Point","data":{"time":%q,"value":%v,"tags":{}}}`, ts, v)
	}
	fl.Add("a", frameFor(t, mk("2026-11-06T05:00:02Z", 4)))
	fl.Add("b", frameFor(t, mk("2026-11-06T05:00:01Z", 9)))
	s := fl.Series["queue_depth"]
	if got, _ := (View{s, 0}).Aggregate("value", 0); got != 4 {
		t.Fatalf("newest sample must win, got %v", got)
	}
	if s.Max != 9 {
		t.Fatalf("max %v", s.Max)
	}
}

func TestViewRejectsWrongAggregation(t *testing.T) {
	s := newSeries(Trend, "time")
	if _, ok := (View{s, 0}).Aggregate("rate", 0); ok {
		t.Fatal("trend has no rate")
	}
	if _, ok := (View{newSeries(Gauge, ""), 0}).Aggregate("count", 0); ok {
		t.Fatal("gauge has no count")
	}
}

func TestSpecsFor(t *testing.T) {
	ts := []threshold.Threshold{{Series: "a"}, {Series: "b{x:1}"}, {Series: "b{x:1}"}}
	specs, err := SpecsFor(ts)
	if err != nil || len(specs) != 1 || specs[0].Metric != "b" || specs[0].Tags["x"] != "1" {
		t.Fatalf("%+v %v", specs, err)
	}
}

func TestLagIsRecorded(t *testing.T) {
	a := New(nil)
	a.now = func() time.Time { return time.Date(2026, 11, 6, 5, 0, 3, 0, time.UTC) }
	feed(a, header)
	a.Feed([]byte(point("http_reqs", 1, "")))
	if lag := a.Flush().LagMs; math.Abs(float64(lag-2500)) > 1 {
		t.Fatalf("lag %d", lag)
	}
}

func TestFastPathAgreesWithDecoder(t *testing.T) {
	lines := []string{
		point("http_reqs", 1, `"status":"200"`),
		point("http_req_duration", 12.5, `"a":"b,c"`),
		point("http_req_duration", 1e-3, ""),
		`{"metric":"http_req_duration","type":"Point","data":{"time":"2026-11-06T05:00:00Z","value":7,"metadata":{"x":"y"},"tags":null}}`,
		`{"type":"Point","metric":"http_req_duration","data":{"value":8,"time":"2026-11-06T05:00:00Z","tags":null}}`,
	}
	fast := New(nil)
	feed(fast, header)
	// A spec on the metric forces the decoder for that metric.
	slow := New([]Spec{
		{Key: "x", Metric: "http_req_duration", Tags: map[string]string{"never": "set"}},
		{Key: "y", Metric: "http_reqs", Tags: map[string]string{"never": "set"}},
	})
	feed(slow, header)
	for _, l := range lines {
		fast.Feed([]byte(l))
		slow.Feed([]byte(l))
	}
	f, s := fast.Flush(), slow.Flush()
	if f.Lines != s.Lines || f.BadLines != 0 || s.BadLines != 0 {
		t.Fatalf("lines %d/%d bad %d/%d", f.Lines, s.Lines, f.BadLines, s.BadLines)
	}
	for _, k := range []string{"http_reqs", "http_req_duration"} {
		a, b := f.Series[k], s.Series[k]
		if a.Count != b.Count || a.Sum != b.Sum {
			t.Errorf("%s: %+v vs %+v", k, a, b)
		}
		if a.Sketch != nil && (a.Sketch.Sum != b.Sketch.Sum || a.Sketch.Max != b.Sketch.Max) {
			t.Errorf("%s sketch differs", k)
		}
	}
}

func BenchmarkFeed(b *testing.B) {
	a := New(nil)
	feed(a, header)
	line := []byte(`{"metric":"http_req_duration","type":"Point","data":{"time":"2026-11-06T05:00:00.123456789Z","value":12.345,"tags":{"expected_response":"true","group":"","method":"GET","name":"http://localhost:8080/","proto":"HTTP/1.1","scenario":"default","status":"200","tls_version":"","url":"http://localhost:8080/"}}}`)
	b.SetBytes(int64(len(line)))
	for i := 0; i < b.N; i++ {
		a.Feed(line)
		if i%100000 == 0 {
			a.Flush()
		}
	}
}

func feedRequest(a *Aggregator, name, status, expected string, ms float64) {
	a.Feed([]byte(`{"metric":"http_req_duration","type":"Point","data":{"time":"2026-11-07T08:00:00Z","value":` +
		strconv.FormatFloat(ms, 'f', -1, 64) + `,"tags":{"method":"GET","name":"` + name + `","status":"` + status + `","expected_response":"` + expected + `"}}}`))
}

func TestPerRequestStatistics(t *testing.T) {
	a := New(nil)
	a.Feed([]byte(`{"type":"Metric","data":{"name":"http_req_duration","type":"trend","contains":"time"},"metric":"http_req_duration"}`))
	a.PerRequest()
	feedRequest(a, "/a", "200", "true", 10)
	feedRequest(a, "/a", "500", "false", 30)
	feedRequest(a, "/b", "200", "true", 5)
	f := a.Flush()
	s := f.Requests[RequestKey("GET", "/a")]
	if s == nil || s.Count != 2 || s.NonZero != 1 || f.Failures[FailureKey("GET", "/a", "500", "")] != 1 {
		t.Fatalf("%+v %+v", s, f.Failures)
	}
	if f.Series["http_req_duration"].Count != 3 {
		t.Error("the metric itself must still count every sample")
	}
	fl := NewFleet()
	fl.Add("x", f)
	fl.Add("y", &Frame{Requests: map[string]*Series{RequestKey("GET", "/a"): s}, Failures: f.Failures})
	if got := fl.Requests[RequestKey("GET", "/a")].Count; got != 4 {
		t.Errorf("merged count %d", got)
	}
	if got := fl.Failures[FailureKey("GET", "/a", "500", "")]; got != 2 {
		t.Errorf("merged failures %d", got)
	}
	fl.ResetRequests()
	if len(fl.Requests) != 0 || fl.Series["http_req_duration"].Count != 3 {
		t.Errorf("reset must leave the series: %+v", fl.Series)
	}
}

func TestPerRequestOffKeepsTheFastPath(t *testing.T) {
	a := New(nil)
	a.Feed([]byte(`{"type":"Metric","data":{"name":"http_req_duration","type":"trend","contains":"time"},"metric":"http_req_duration"}`))
	feedRequest(a, "/a", "200", "true", 10)
	if f := a.Flush(); f.Requests != nil {
		t.Errorf("%+v", f.Requests)
	}
}

func TestRequestNamesAreBounded(t *testing.T) {
	a := New(nil)
	a.Feed([]byte(`{"type":"Metric","data":{"name":"http_req_duration","type":"trend","contains":"time"},"metric":"http_req_duration"}`))
	a.PerRequest()
	for i := 0; i < MaxRequestNames+50; i++ {
		feedRequest(a, "/user/"+strconv.Itoa(i), "200", "true", 1)
	}
	feedRequest(a, "/user/0", "200", "true", 1)
	f := a.Flush()
	if len(f.Requests) != MaxRequestNames+1 {
		t.Fatalf("%d keys", len(f.Requests))
	}
	if got := f.Requests[RequestKey("GET", OtherRequests)].Count; got != 50 {
		t.Errorf("overflow row has %d", got)
	}
	if got := f.Requests[RequestKey("GET", "/user/0")].Count; got != 2 {
		t.Errorf("known names keep counting: %d", got)
	}
}
