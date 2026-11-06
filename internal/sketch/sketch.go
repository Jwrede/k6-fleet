// Package sketch implements a mergeable quantile sketch with a fixed relative
// error. Agents build one per Trend series and the controller merges them, so
// percentiles over the whole fleet need no raw samples on the wire.
package sketch

import (
	"math"
	"sort"
)

// RelativeError bounds |estimate-true|/true for any quantile of positive or
// negative values. Sign and zero are kept apart, so values near zero stay exact
// in relative terms.
const RelativeError = 0.005

var (
	gamma    = (1 + RelativeError) / (1 - RelativeError)
	logGamma = math.Log(gamma)
)

// Sketch is a log-bucketed histogram. The exported fields are its wire format.
type Sketch struct {
	Count uint64           `json:"n"`
	Sum   float64          `json:"s"`
	Min   float64          `json:"lo"`
	Max   float64          `json:"hi"`
	Zero  uint64           `json:"z,omitempty"`
	Pos   map[int32]uint64 `json:"p,omitempty"`
	Neg   map[int32]uint64 `json:"m,omitempty"`
}

// New returns an empty sketch.
func New() *Sketch {
	return &Sketch{Pos: map[int32]uint64{}, Neg: map[int32]uint64{}}
}

func key(v float64) int32 {
	return int32(math.Ceil(math.Log(v) / logGamma))
}

// value is the midpoint of bucket k, which is what keeps the error relative.
func value(k int32) float64 {
	return 2 * math.Pow(gamma, float64(k)) / (gamma + 1)
}

// Add records one value. NaN and infinities are ignored.
func (s *Sketch) Add(v float64) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}
	if s.Count == 0 || v < s.Min {
		s.Min = v
	}
	if s.Count == 0 || v > s.Max {
		s.Max = v
	}
	s.Count++
	s.Sum += v
	switch {
	case v > 0:
		s.Pos[key(v)]++
	case v < 0:
		s.Neg[key(-v)]++
	default:
		s.Zero++
	}
}

// Merge adds all of o to s.
func (s *Sketch) Merge(o *Sketch) {
	if o == nil || o.Count == 0 {
		return
	}
	if s.Count == 0 || o.Min < s.Min {
		s.Min = o.Min
	}
	if s.Count == 0 || o.Max > s.Max {
		s.Max = o.Max
	}
	s.Count += o.Count
	s.Sum += o.Sum
	s.Zero += o.Zero
	if s.Pos == nil {
		s.Pos = map[int32]uint64{}
	}
	if s.Neg == nil {
		s.Neg = map[int32]uint64{}
	}
	for k, n := range o.Pos {
		s.Pos[k] += n
	}
	for k, n := range o.Neg {
		s.Neg[k] += n
	}
}

// Avg returns the exact arithmetic mean.
func (s *Sketch) Avg() float64 {
	if s.Count == 0 {
		return 0
	}
	return s.Sum / float64(s.Count)
}

// Quantile estimates quantile q in [0,1] using the rank k6's Trend sink uses,
// q*(n-1). Results are clamped to the exact minimum and maximum.
func (s *Sketch) Quantile(q float64) float64 {
	if s.Count == 0 {
		return 0
	}
	if q <= 0 {
		return s.Min
	}
	if q >= 1 {
		return s.Max
	}
	rank := uint64(q * float64(s.Count-1))
	var seen uint64
	est := s.Max

	found := false
	neg := sortedKeys(s.Neg)
	for i := len(neg) - 1; i >= 0 && !found; i-- { // most negative first
		seen += s.Neg[neg[i]]
		if seen > rank {
			est, found = -value(neg[i]), true
		}
	}
	if !found {
		seen += s.Zero
		if seen > rank {
			est, found = 0, true
		}
	}
	if !found {
		for _, k := range sortedKeys(s.Pos) {
			seen += s.Pos[k]
			if seen > rank {
				est = value(k)
				break
			}
		}
	}
	return math.Min(math.Max(est, s.Min), s.Max)
}

func sortedKeys(m map[int32]uint64) []int32 {
	ks := make([]int32, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
	return ks
}
