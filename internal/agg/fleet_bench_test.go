package agg

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

// realisticFrame builds one second of an agent's output for an HTTP test: the
// built-in k6 metrics plus submetrics, with latencies spread over several orders
// of magnitude so that the sketches have many buckets.
func realisticFrame(b *testing.B, samples int) []byte {
	b.Helper()
	var specs []Spec
	for i := 0; i < 5; i++ {
		specs = append(specs, Spec{Key: fmt.Sprintf("http_req_duration{name:n%d}", i), Metric: "http_req_duration", Tags: map[string]string{"name": fmt.Sprintf("n%d", i)}})
	}
	a := New(specs)
	trends := []string{"http_req_duration", "http_req_blocked", "http_req_connecting", "http_req_tls_handshaking", "http_req_sending", "http_req_waiting", "http_req_receiving", "iteration_duration"}
	for _, m := range trends {
		a.Feed([]byte(fmt.Sprintf(`{"type":"Metric","data":{"name":%q,"type":"trend","contains":"time"},"metric":%q}`, m, m)))
	}
	for _, m := range []string{"http_reqs", "iterations", "data_sent", "data_received"} {
		a.Feed([]byte(fmt.Sprintf(`{"type":"Metric","data":{"name":%q,"type":"counter","contains":"default"},"metric":%q}`, m, m)))
	}
	a.Feed([]byte(`{"type":"Metric","data":{"name":"http_req_failed","type":"rate","contains":"default"},"metric":"http_req_failed"}`))
	a.Feed([]byte(`{"type":"Metric","data":{"name":"vus","type":"gauge","contains":"default"},"metric":"vus"}`))
	r := rand.New(rand.NewSource(1))
	for i := 0; i < samples; i++ {
		v := math.Exp(r.NormFloat64()*2 + 3)
		tags := fmt.Sprintf(`"name":"n%d"`, i%5)
		for _, m := range trends {
			a.Feed([]byte(point(m, v, tags)))
		}
		for _, m := range []string{"http_reqs", "iterations", "data_sent", "data_received"} {
			a.Feed([]byte(point(m, 1, tags)))
		}
		a.Feed([]byte(point("http_req_failed", 0, tags)))
	}
	a.Feed([]byte(point("vus", 10, "")))
	out, err := json.Marshal(a.Flush())
	if err != nil {
		b.Fatal(err)
	}
	return out
}

// BenchmarkControllerFrame measures what the controller pays per frame: decoding
// and merging. The cost depends on the number of series and sketch buckets, and
// so saturates with the request rate of an agent.
func BenchmarkControllerFrame(b *testing.B) {
	for _, rps := range []int{1000, 20000} {
		b.Run(fmt.Sprintf("reqs_per_s=%d", rps), func(b *testing.B) {
			raw := realisticFrame(b, rps)
			f := NewFleet()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var fr Frame
				if err := json.Unmarshal(raw, &fr); err != nil {
					b.Fatal(err)
				}
				f.Add(fmt.Sprintf("agent-%d", i%100), &fr)
			}
		})
	}
}
