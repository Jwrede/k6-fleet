//go:build !linux

package agent

import (
	"os"
	"os/exec"
)

func bindToAgent(*exec.Cmd) {}

func growPipe(*os.File) {}

func residentBytes(int) int64 { return 0 }
