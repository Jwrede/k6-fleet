package agent

import (
	"os"
	"strconv"
	"strings"
)

// cpuMeter reports host CPU use between calls from /proc/stat. A saturated load
// generator measures itself rather than the target, so the controller warns on
// it. Hosts without /proc report 0.
type cpuMeter struct{ busy, total uint64 }

func newCPUMeter() *cpuMeter {
	m := &cpuMeter{}
	m.busy, m.total = readCPU()
	return m
}

func (m *cpuMeter) percent() float64 {
	b, t := readCPU()
	db, dt := b-m.busy, t-m.total
	m.busy, m.total = b, t
	if t == 0 || dt == 0 {
		return 0
	}
	return 100 * float64(db) / float64(dt)
}

func readCPU() (busy, total uint64) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0
	}
	line, _, _ := strings.Cut(string(data), "\n")
	return parseCPULine(line)
}

func parseCPULine(line string) (busy, total uint64) {
	f := strings.Fields(line)
	if len(f) < 5 || f[0] != "cpu" {
		return 0, 0
	}
	for i, s := range f[1:] {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, 0
		}
		total += v
		if i != 3 && i != 4 { // idle and iowait
			busy += v
		}
	}
	return busy, total
}
