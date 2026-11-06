package report

import (
	"fmt"
	"html/template"
	"os"
	"sort"
	"strings"
)

const chartW, chartH = 720.0, 120.0

// chart draws one line as inline SVG, so that the report stays a single file.
func chart(vals []float64) template.HTML {
	if len(vals) < 2 {
		return ""
	}
	maxV := 0.0
	for _, v := range vals {
		if v > maxV {
			maxV = v
		}
	}
	if maxV == 0 {
		maxV = 1
	}
	var b strings.Builder
	for i, v := range vals {
		fmt.Fprintf(&b, "%.1f,%.1f ", float64(i)/float64(len(vals)-1)*chartW, chartH-v/maxV*(chartH-6)-3)
	}
	return template.HTML(fmt.Sprintf(`<svg viewBox="0 0 %.0f %.0f" role="img"><polyline fill="none" stroke="currentColor" stroke-width="1.5" points="%s"/></svg>`, chartW, chartH, b.String()))
}

func (d *Data) series(pick func(Point) float64) []float64 {
	out := make([]float64, len(d.Timeline))
	for i, p := range d.Timeline {
		out[i] = pick(p)
	}
	return out
}

func peak(v []float64) float64 {
	m := 0.0
	for _, x := range v {
		if x > m {
			m = x
		}
	}
	return m
}

const page = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>k6-fleet report</title>
<style>
:root{--fg:#1c2024;--bg:#fff;--line:#d9dde1;--muted:#60666c;--ok:#1a7f37;--bad:#c62828}
@media(prefers-color-scheme:dark){:root{--fg:#e6e8ea;--bg:#16181a;--line:#33373b;--muted:#9aa1a7;--ok:#4cc26e;--bad:#ef6a6a}}
body{font:15px/1.5 system-ui,sans-serif;color:var(--fg);background:var(--bg);max-width:860px;margin:0 auto;padding:24px 16px}
h1{font-size:22px;margin:0 0 4px}h2{font-size:16px;margin:28px 0 8px}
.m{color:var(--muted)}table{border-collapse:collapse;width:100%;font-size:14px}
th,td{text-align:left;padding:4px 10px 4px 0;border-bottom:1px solid var(--line);vertical-align:top}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
.ok{color:var(--ok)}.bad{color:var(--bad)}svg{width:100%;height:auto;display:block}
figure{margin:12px 0}figcaption{font-size:13px;color:var(--muted)}.w{overflow-x:auto}
</style></head><body>
<h1>{{.D.Script}}</h1>
<p class="m">{{len .D.Agents}} agents, k6 {{.D.K6Version}}, started {{.D.Started.Format "2006-01-02 15:04:05 MST"}}, {{.Seconds}} s, exit code {{.D.ExitCode}}</p>
{{if .D.Aborted}}<p class="bad">Aborted: {{.D.Aborted}}</p>{{end}}
{{range .D.Warnings}}<p class="bad">{{.}}</p>{{end}}
{{if .D.Thresholds}}<h2>Thresholds</h2><div class="w"><table><tr><th></th><th>Series</th><th>Threshold</th><th class="n">Actual</th></tr>
{{range .D.Thresholds}}<tr><td class="{{if .OK}}ok{{else}}bad{{end}}">{{if .OK}}ok{{else}}FAIL{{end}}</td><td>{{.Series}}</td><td>{{.Source}}</td><td class="n">{{printf "%.4g" .Actual}}</td></tr>{{end}}</table></div>{{end}}
{{range .Charts}}<figure>{{.SVG}}<figcaption>{{.Title}}, peak {{printf "%.4g" .Peak}}</figcaption></figure>{{end}}
{{if .D.Checks}}<h2>Checks</h2><div class="w"><table><tr><th>Check</th><th class="n">Passed</th><th class="n">Failed</th></tr>
{{range .D.Checks}}<tr><td>{{.Group}} {{.Name}}</td><td class="n">{{.Pass}}</td><td class="n">{{.Fail}}</td></tr>{{end}}</table></div>{{end}}
<h2>Metrics</h2><div class="w"><table><tr><th>Metric</th><th>Value</th></tr>
{{range .Rows}}<tr><td>{{.Name}}</td><td>{{.Text}}</td></tr>{{end}}</table></div>
<h2>Agents</h2><div class="w"><table><tr><th>Name</th><th>Segment</th><th class="n">Iterations</th><th class="n">Requests</th><th class="n">CPU avg</th><th class="n">CPU max</th><th class="n">Exit</th></tr>
{{range .D.Agents}}<tr><td>{{.Name}}</td><td>{{.Segment}}</td><td class="n">{{printf "%.0f" .Iterations}}</td><td class="n">{{printf "%.0f" .Requests}}</td><td class="n">{{printf "%.0f" .CPUAvg}}%</td><td class="n">{{printf "%.0f" .CPUMax}}%</td><td class="n">{{if .Lost}}lost{{else}}{{.ExitCode}}{{end}}</td></tr>{{end}}</table></div>
<p class="m">Percentiles have a relative error of at most {{printf "%.1f" .ErrPct}} percent. All other values are exact.</p>
</body></html>
`

// WriteHTML stores a self-contained report at path.
func (d *Data) WriteHTML(path string) error {
	t, err := template.New("report").Parse(page)
	if err != nil {
		return err
	}
	type chartRow struct {
		Title string
		SVG   template.HTML
		Peak  float64
	}
	type row struct{ Name, Text string }
	var charts []chartRow
	for _, c := range []struct {
		title string
		pick  func(Point) float64
	}{
		{"Requests per second", func(p Point) float64 { return p.Reqs }},
		{"http_req_duration p(95) in ms", func(p Point) float64 { return p.P95 }},
		{"Active VUs", func(p Point) float64 { return p.VUs }},
		{"Failed request rate", func(p Point) float64 { return p.Failed }},
		{"Highest agent CPU in percent", func(p Point) float64 { return p.CPUMax }},
	} {
		v := d.series(c.pick)
		charts = append(charts, chartRow{c.title, chart(v), peak(v)})
	}
	names := make([]string, 0, len(d.Metrics))
	for k := range d.Metrics {
		names = append(names, k)
	}
	sort.Strings(names)
	rows := make([]row, 0, len(names))
	for _, k := range names {
		rows = append(rows, row{k, d.metricLine(k, d.Metrics[k])})
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return t.Execute(f, map[string]any{
		"D": d, "Charts": charts, "Rows": rows,
		"Seconds": fmt.Sprintf("%.1f", float64(d.DurationMs)/1000),
		"ErrPct":  d.PercentileRelativeError * 100,
	})
}
