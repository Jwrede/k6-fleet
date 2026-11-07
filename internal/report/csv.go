package report

import (
	"encoding/csv"
	"io"
	"strconv"
)

func num(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }

func writeCSV(w io.Writer, rows [][]string) error {
	c := csv.NewWriter(w)
	if err := c.WriteAll(rows); err != nil {
		return err
	}
	return c.Error()
}

// WriteRequestsCSV writes the statistics per request.
func (d *Data) WriteRequestsCSV(w io.Writer) error {
	rows := [][]string{{"method", "name", "requests", "fails", "avg_ms", "min_ms", "med_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms", "rps", "fails_per_s"}}
	for _, r := range d.Requests {
		rows = append(rows, []string{r.Method, r.Name, strconv.FormatUint(r.Requests, 10), strconv.FormatUint(r.Fails, 10),
			num(r.Avg), num(r.Min), num(r.Med), num(r.P90), num(r.P95), num(r.P99), num(r.Max), num(r.RPS), num(r.FailsPerSec)})
	}
	return writeCSV(w, rows)
}

// WriteFailuresCSV writes the kinds of failed requests.
func (d *Data) WriteFailuresCSV(w io.Writer) error {
	rows := [][]string{{"count", "method", "name", "status", "error"}}
	for _, r := range d.Failures {
		rows = append(rows, []string{strconv.FormatUint(r.Count, 10), r.Method, r.Name, r.Status, r.Error})
	}
	return writeCSV(w, rows)
}

// WriteTimelineCSV writes one row per second of the run.
func (d *Data) WriteTimelineCSV(w io.Writer) error {
	rows := [][]string{{"second", "vus", "reqs", "fails", "iterations", "p50_ms", "p95_ms", "p99_ms", "failed_rate", "cpu_max"}}
	for _, p := range d.Timeline {
		rows = append(rows, []string{strconv.Itoa(p.Second), num(p.VUs), num(p.Reqs), num(p.Fails), num(p.Iters),
			num(p.P50), num(p.P95), num(p.P99), num(p.Failed), num(p.CPUMax)})
	}
	return writeCSV(w, rows)
}
