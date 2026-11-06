package threshold

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

type fake map[string]float64

func (f fake) Aggregate(m string, p float64) (float64, bool) {
	if m == "p" {
		v, ok := f["p"]
		return v * (1 + 0*p), ok
	}
	v, ok := f[m]
	return v, ok
}

func TestParseExpr(t *testing.T) {
	for _, s := range []string{"p(95)<200", "p(99.9) <= 1e3", "rate<0.01", "count>=100", "avg != 3", "value === 1", " med > -1 "} {
		if _, err := ParseExpr(s); err != nil {
			t.Errorf("%q: %v", s, err)
		}
	}
	for _, s := range []string{"", "p95<200", "p(101)<1", "avg ~ 2", "rate<", "bogus<1"} {
		if _, err := ParseExpr(s); err == nil {
			t.Errorf("%q must fail", s)
		}
	}
	th, _ := ParseExpr("p(99.9)<1")
	if th.Method != "p" || th.Percentile < 0.9989 || th.Percentile > 0.9991 {
		t.Fatalf("%+v", th)
	}
}

func TestFromOptions(t *testing.T) {
	raw := json.RawMessage(`{
	  "http_req_duration":["p(95)<200","avg<100"],
	  "http_req_failed":[{"threshold":"rate<0.01","abortOnFail":true,"delayAbortEval":"10s"}],
	  "http_req_duration{status:200}":["max<1000"]
	}`)
	ts, err := FromOptions(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 4 {
		t.Fatalf("%d thresholds", len(ts))
	}
	var abort Threshold
	for _, x := range ts {
		if x.Series == "http_req_failed" {
			abort = x
		}
	}
	if !abort.AbortOnFail || abort.AbortDelay != 10*time.Second {
		t.Fatalf("%+v", abort)
	}
	if ts, err := FromOptions(json.RawMessage("null")); err != nil || ts != nil {
		t.Fatal("null must be empty")
	}
}

func TestCheck(t *testing.T) {
	th, _ := ParseExpr("p(95)<200")
	if ok, _ := th.Check(fake{"p": 150}); !ok {
		t.Error("150<200")
	}
	if ok, _ := th.Check(fake{"p": 200}); ok {
		t.Error("200<200 must fail")
	}
	if ok, _ := th.Check(nil); !ok {
		t.Error("no data must pass")
	}
	r, _ := ParseExpr("rate<0.01")
	if _, err := r.Check(fake{"p": 1}); err == nil {
		t.Error("rate on a trend must report an error")
	}
}

func TestSplitSeries(t *testing.T) {
	n, tags, err := SplitSeries(`http_req_duration{url:"http://a/b,c", status:200}`)
	if err != nil || n != "http_req_duration" {
		t.Fatal(n, err)
	}
	if !reflect.DeepEqual(tags, map[string]string{"url": "http://a/b,c", "status": "200"}) {
		t.Fatalf("%v", tags)
	}
	if n, tags, _ := SplitSeries("iterations"); n != "iterations" || tags != nil {
		t.Fatal(n, tags)
	}
	if _, _, err := SplitSeries("x{a}"); err == nil {
		t.Error("tag without value must fail")
	}
	if _, _, err := SplitSeries("x{a:1"); err == nil {
		t.Error("unterminated filter must fail")
	}
}
