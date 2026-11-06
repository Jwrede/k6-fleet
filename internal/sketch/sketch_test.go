package sketch

import (
	"encoding/json"
	"math"
	"math/rand"
	"sort"
	"testing"
)

func exact(vals []float64, q float64) float64 {
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	return s[int(q*float64(len(s)-1))]
}

func TestQuantileWithinRelativeError(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	dists := map[string]func() float64{
		"lognormal": func() float64 { return math.Exp(r.NormFloat64()*1.5 + 4) },
		"uniform":   func() float64 { return r.Float64() * 1000 },
		"signed":    func() float64 { return r.NormFloat64() * 50 },
		"bimodal": func() float64 {
			if r.Intn(10) == 0 {
				return 2000 + r.Float64()*100
			}
			return 5 + r.Float64()
		},
	}
	for name, gen := range dists {
		t.Run(name, func(t *testing.T) {
			s := New()
			vals := make([]float64, 200000)
			for i := range vals {
				vals[i] = gen()
				s.Add(vals[i])
			}
			for _, q := range []float64{0.01, 0.25, 0.5, 0.9, 0.95, 0.99, 0.999} {
				want, got := exact(vals, q), s.Quantile(q)
				if rel := math.Abs(got-want) / math.Abs(want); rel > RelativeError*1.001 {
					t.Errorf("q=%v want %v got %v rel %v", q, want, got, rel)
				}
			}
		})
	}
}

func TestMergeEqualsSingleSketch(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	all, parts := New(), []*Sketch{New(), New(), New()}
	for i := 0; i < 90000; i++ {
		v := math.Exp(r.NormFloat64() + 3)
		all.Add(v)
		parts[i%3].Add(v)
	}
	m := New()
	for _, p := range parts {
		// Through JSON, because that is how sketches reach the controller.
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		var w Sketch
		if err := json.Unmarshal(b, &w); err != nil {
			t.Fatal(err)
		}
		m.Merge(&w)
	}
	if m.Count != all.Count || m.Min != all.Min || m.Max != all.Max {
		t.Fatalf("merged %+v vs %+v", m.Count, all.Count)
	}
	if math.Abs(m.Sum-all.Sum) > 1e-6*all.Sum {
		t.Fatalf("sum %v vs %v", m.Sum, all.Sum)
	}
	for _, q := range []float64{0.5, 0.95, 0.99} {
		if a, b := m.Quantile(q), all.Quantile(q); a != b {
			t.Errorf("q=%v merged %v single %v", q, a, b)
		}
	}
}

func TestEdges(t *testing.T) {
	s := New()
	if s.Quantile(0.5) != 0 || s.Avg() != 0 {
		t.Fatal("empty sketch must report 0")
	}
	s.Add(math.NaN())
	s.Add(math.Inf(1))
	if s.Count != 0 {
		t.Fatal("NaN and Inf must be ignored")
	}
	s.Add(0)
	s.Add(0)
	s.Add(7)
	if s.Quantile(0.5) != 0 || s.Quantile(1) != 7 || s.Quantile(0) != 0 {
		t.Fatalf("zero handling: %v %v", s.Quantile(0.5), s.Quantile(1))
	}
	one := New()
	one.Add(42)
	if one.Quantile(0.99) != 42 {
		t.Fatalf("single value must be exact, got %v", one.Quantile(0.99))
	}
}
