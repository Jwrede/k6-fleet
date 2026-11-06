package segment

import (
	"reflect"
	"testing"
)

func TestSplit(t *testing.T) {
	for _, c := range []struct {
		w    []int
		seg  []string
		seqs string
	}{
		{[]int{1}, []string{"0:1"}, "0,1"},
		{[]int{1, 1}, []string{"0:1/2", "1/2:1"}, "0,1/2,1"},
		{[]int{1, 1, 1}, []string{"0:1/3", "1/3:2/3", "2/3:1"}, "0,1/3,2/3,1"},
		{[]int{1, 2, 1}, []string{"0:1/4", "1/4:3/4", "3/4:1"}, "0,1/4,3/4,1"},
	} {
		p, err := Split(c.w)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(p.Segments, c.seg) || p.Sequence != c.seqs {
			t.Errorf("%v: got %v %q", c.w, p.Segments, p.Sequence)
		}
	}
	if _, err := Split(nil); err == nil {
		t.Error("empty fleet must fail")
	}
	if _, err := Split([]int{1, 0}); err == nil {
		t.Error("zero weight must fail")
	}
}
