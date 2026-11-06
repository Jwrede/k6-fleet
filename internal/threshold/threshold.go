// Package threshold parses k6 thresholds and evaluates them over fleet totals.
package threshold

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Stats is the merged state of one series, as far as thresholds need it.
type Stats interface {
	// Aggregate returns the value for avg, min, max, med, count, rate, value or
	// p(N). ok is false if this metric type does not have that aggregation.
	Aggregate(method string, percentile float64) (v float64, ok bool)
}

// Threshold is one expression on one series. Series is the text k6 uses as the
// key in options.thresholds, for example http_req_duration{status:200}.
type Threshold struct {
	Series      string
	Source      string
	Method      string
	Percentile  float64
	Op          string
	Value       float64
	AbortOnFail bool
	AbortDelay  time.Duration
}

var exprRe = regexp.MustCompile(`^\s*(avg|min|max|med|count|rate|value|p\(([0-9.]+)\))\s*(>=|<=|===|==|!=|>|<)\s*(-?[0-9.]+(?:[eE][-+]?[0-9]+)?)\s*$`)

// ParseExpr parses "<aggregation> <operator> <number>".
func ParseExpr(src string) (Threshold, error) {
	m := exprRe.FindStringSubmatch(src)
	if m == nil {
		return Threshold{}, fmt.Errorf("malformed threshold expression %q", src)
	}
	t := Threshold{Source: src, Method: m[1], Op: m[3]}
	if m[2] != "" {
		p, err := strconv.ParseFloat(m[2], 64)
		if err != nil || p < 0 || p > 100 {
			return Threshold{}, fmt.Errorf("bad percentile in %q", src)
		}
		t.Method, t.Percentile = "p", p/100
	}
	v, err := strconv.ParseFloat(m[4], 64)
	if err != nil {
		return Threshold{}, fmt.Errorf("bad value in %q", src)
	}
	t.Value = v
	return t, nil
}

// FromOptions reads the thresholds object printed by `k6 inspect`. Each entry
// is a list of strings or of {threshold, abortOnFail, delayAbortEval} objects.
func FromOptions(raw json.RawMessage) ([]Threshold, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var bySeries map[string][]json.RawMessage
	if err := json.Unmarshal(raw, &bySeries); err != nil {
		return nil, fmt.Errorf("thresholds: %w", err)
	}
	var out []Threshold
	for series, list := range bySeries {
		for _, item := range list {
			var src string
			var cfg struct {
				Threshold      string `json:"threshold"`
				AbortOnFail    bool   `json:"abortOnFail"`
				DelayAbortEval any    `json:"delayAbortEval"`
			}
			if err := json.Unmarshal(item, &src); err != nil {
				if err := json.Unmarshal(item, &cfg); err != nil {
					return nil, fmt.Errorf("thresholds on %s: %w", series, err)
				}
				src = cfg.Threshold
			}
			t, err := ParseExpr(src)
			if err != nil {
				return nil, fmt.Errorf("thresholds on %s: %w", series, err)
			}
			t.Series = series
			t.AbortOnFail = cfg.AbortOnFail
			if t.AbortDelay, err = parseDelay(cfg.DelayAbortEval); err != nil {
				return nil, fmt.Errorf("thresholds on %s: %w", series, err)
			}
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Series != out[j].Series {
			return out[i].Series < out[j].Series
		}
		return out[i].Source < out[j].Source
	})
	return out, nil
}

// k6 marshals a duration as a string such as "10s"; older output was numeric
// milliseconds.
func parseDelay(v any) (time.Duration, error) {
	switch d := v.(type) {
	case nil:
		return 0, nil
	case string:
		if d == "" {
			return 0, nil
		}
		return time.ParseDuration(d)
	case float64:
		return time.Duration(d * float64(time.Millisecond)), nil
	}
	return 0, fmt.Errorf("bad delayAbortEval %v", v)
}

// Check evaluates the threshold. An absent series passes, as in k6: a Trend
// without samples has nothing to compare, and a threshold on it is not crossed.
func (t Threshold) Check(s Stats) (ok bool, err error) {
	if s == nil {
		return true, nil
	}
	v, has := s.Aggregate(t.Method, t.Percentile)
	if !has {
		return false, fmt.Errorf("%s: aggregation %q does not apply to this metric type", t.Series, t.Method)
	}
	switch t.Op {
	case ">":
		return v > t.Value, nil
	case ">=":
		return v >= t.Value, nil
	case "<":
		return v < t.Value, nil
	case "<=":
		return v <= t.Value, nil
	case "==", "===":
		return v == t.Value, nil
	case "!=":
		return v != t.Value, nil
	}
	return false, fmt.Errorf("unknown operator %q", t.Op)
}

// SplitSeries separates "name{a:b,c:d}" into the metric name and tag filter.
func SplitSeries(series string) (name string, tags map[string]string, err error) {
	i := strings.IndexByte(series, '{')
	if i < 0 {
		return strings.TrimSpace(series), nil, nil
	}
	if !strings.HasSuffix(strings.TrimSpace(series), "}") {
		return "", nil, fmt.Errorf("malformed series %q", series)
	}
	name = strings.TrimSpace(series[:i])
	body := strings.TrimSpace(series)
	body = body[i+1 : len(body)-1]
	tags = map[string]string{}
	for _, kv := range splitTags(body) {
		k, v, found := strings.Cut(kv, ":")
		if !found {
			return "", nil, fmt.Errorf("malformed tag %q in %q", kv, series)
		}
		tags[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
	}
	return name, tags, nil
}

// splitTags splits on commas outside quotes, so URL tags may contain commas.
func splitTags(s string) []string {
	var out []string
	var quote rune
	start := 0
	for i, r := range s {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case r == '"' || r == '\'':
			quote = r
		case r == ',':
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if strings.TrimSpace(s[start:]) != "" {
		out = append(out, s[start:])
	}
	return out
}
