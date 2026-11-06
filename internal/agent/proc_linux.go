package agent

import (
	"os"
	"os/exec"
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
