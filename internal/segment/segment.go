// Package segment turns agent weights into k6 execution segments.
package segment

import (
	"fmt"
	"math/big"
	"strings"
)

// Plan holds one k6 --execution-segment value per agent and the shared
// --execution-segment-sequence.
type Plan struct {
	Segments []string
	Sequence string
}

// Split divides [0,1] proportionally to the positive integer weights. Bounds
// are exact rationals, because k6 rejects a sequence that does not end at 1.
func Split(weights []int) (Plan, error) {
	if len(weights) == 0 {
		return Plan{}, fmt.Errorf("no agents")
	}
	total := 0
	for _, w := range weights {
		if w <= 0 {
			return Plan{}, fmt.Errorf("weight must be positive, got %d", w)
		}
		total += w
	}
	bounds := []*big.Rat{new(big.Rat)}
	acc := 0
	for _, w := range weights {
		acc += w
		bounds = append(bounds, big.NewRat(int64(acc), int64(total)))
	}
	p := Plan{}
	seq := make([]string, len(bounds))
	for i, b := range bounds {
		seq[i] = format(b)
	}
	p.Sequence = strings.Join(seq, ",")
	for i := 0; i < len(weights); i++ {
		p.Segments = append(p.Segments, seq[i]+":"+seq[i+1])
	}
	return p, nil
}

func format(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}
	return r.RatString()
}
