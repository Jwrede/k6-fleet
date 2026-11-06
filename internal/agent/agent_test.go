package agent

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Jwrede/k6-fleet/internal/proto"
)

func TestRunArgsLeaveThresholdsToController(t *testing.T) {
	args := runArgs(&proto.PrepareBody{Segment: "1/3:2/3", Sequence: "0,1/3,2/3,1", Instance: 1, Out: []string{"influxdb=http://i"}}, 6565, "/f", "/a.tar")
	s := strings.Join(args, " ")
	for _, want := range []string{"--paused", "--linger", "--no-setup", "--no-teardown", "--no-thresholds", "--no-summary",
		"--execution-segment 1/3:2/3", "--execution-segment-sequence 0,1/3,2/3,1", "--out json=/f", "--out influxdb=http://i", "--tag instance_id=1"} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in %s", want, s)
		}
	}
	if args[len(args)-1] != "/a.tar" {
		t.Error("archive must be last")
	}
}

func TestSetupData(t *testing.T) {
	got := setupData([]byte(`{"data":{"type":"setupData","id":"default","attributes":{"data":{"token":"x","n":[1,2]}}}}`))
	if string(got) != `{"token":"x","n":[1,2]}` {
		t.Fatalf("%s", got)
	}
	if setupData([]byte("oops")) != nil {
		t.Fatal("bad body must give nil")
	}
}

func TestParseK6Version(t *testing.T) {
	if got := ParseK6Version("k6 v2.3.0 (commit/abc, go1.25, linux/amd64)\n"); got != "v2.3.0" {
		t.Fatal(got)
	}
}

func TestParseCPULine(t *testing.T) {
	b, tot := parseCPULine("cpu  100 0 50 800 50 0 0 0 0 0")
	if b != 150 || tot != 1000 {
		t.Fatal(b, tot)
	}
	if b, tot := parseCPULine("nope"); b != 0 || tot != 0 {
		t.Fatal(b, tot)
	}
}

func TestTailBufferKeepsLastLines(t *testing.T) {
	var tb tailBuffer
	for i := 0; i < 20; i++ {
		tb.add(strings.Repeat("x", i))
	}
	if got := strings.Split(tb.String(), "\n"); len(got) != 15 || !reflect.DeepEqual(got[14], strings.Repeat("x", 19)) {
		t.Fatalf("%d", len(got))
	}
}
