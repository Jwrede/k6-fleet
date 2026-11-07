package agent

import (
	"bytes"
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

// fSetPipeSize is F_SETPIPE_SZ, which package syscall does not define.
const fSetPipeSize = 1031

// bindToAgent stops k6 from outliving the agent. With --linger it would stay
// forever, and during a test it would keep producing load nobody observes.
func bindToAgent(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}

// growPipe avoids k6 and the agent taking turns every few hundred samples on the
// default 64 KiB pipe. Failure only costs throughput, so it is ignored.
func growPipe(f *os.File) {
	_, _, _ = syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), fSetPipeSize, 1<<20)
}

// residentBytes reads the resident set of a process, 0 if it is gone.
func residentBytes(pid int) int64 {
	if pid == 0 {
		return 0
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/statm")
	if err != nil {
		return 0
	}
	f := bytes.Fields(b)
	if len(f) < 2 {
		return 0
	}
	pages, _ := strconv.ParseInt(string(f[1]), 10, 64)
	return pages * int64(os.Getpagesize())
}
