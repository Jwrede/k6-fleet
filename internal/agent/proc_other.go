//go:build !linux

package agent

import (
	"os"
	"os/exec"
)

func bindToAgent(*exec.Cmd) {}

func growPipe(*os.File) {}
