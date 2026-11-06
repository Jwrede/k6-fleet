// Package transport starts an agent somewhere and connects to its stdio.
package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// Spec describes one agent in the inventory.
type Spec struct {
	Name      string   `json:"name"`
	Transport string   `json:"transport"` // local, ssh or docker
	Host      string   `json:"host,omitempty"`
	SSHArgs   []string `json:"ssh_args,omitempty"`
	Image     string   `json:"image,omitempty"`
	Network   string   `json:"network,omitempty"`
	Weight    int      `json:"weight,omitempty"`
	K6        string   `json:"k6,omitempty"`
	// Binary overrides the file uploaded to the agent, for a fleet whose
	// platform differs from the controller's.
	Binary string `json:"binary,omitempty"`
}

// Conn is a running agent.
type Conn struct {
	Stdin  io.WriteCloser
	Stdout io.Reader
	cmd    *exec.Cmd
	after  func()
}

// Close ends the session. Closing stdin makes the agent stop k6 and exit, which
// lets it clean up; the kill only covers a stuck connection.
func (c *Conn) Close() error {
	c.Stdin.Close()
	done := make(chan error, 1)
	go func() { done <- c.cmd.Wait() }()
	var err error
	select {
	case err = <-done:
	case <-time.After(15 * time.Second):
		_ = c.cmd.Process.Kill()
		err = <-done
	}
	if c.after != nil {
		c.after()
	}
	return err
}

// Start launches the agent described by s. self is the running k6-fleet binary.
func Start(ctx context.Context, s Spec, self string, stderr io.Writer) (*Conn, error) {
	bin := self
	if s.Binary != "" {
		bin = s.Binary
	}
	switch s.Transport {
	case "local", "":
		return launch(exec.Command(bin, agentArgs(s)...), stderr, nil)
	case "ssh":
		return startSSH(ctx, s, bin, stderr)
	case "docker":
		return startDocker(ctx, s, bin, stderr)
	}
	return nil, fmt.Errorf("agent %s: unknown transport %q", s.Name, s.Transport)
}

func agentArgs(s Spec) []string {
	if s.K6 != "" {
		return []string{"agent", "--k6", s.K6}
	}
	return []string{"agent"}
}

func launch(cmd *exec.Cmd, stderr io.Writer, after func()) (*Conn, error) {
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &Conn{Stdin: in, Stdout: out, cmd: cmd, after: after}, nil
}

// sshBase forces the behaviour a non-interactive run needs. BatchMode makes a
// missing key fail instead of hanging on a password prompt.
func sshBase(s Spec) []string {
	return append([]string{"-o", "BatchMode=yes", "-o", "ConnectTimeout=15"}, s.SSHArgs...)
}

func sshRun(ctx context.Context, s Spec, stdin io.Reader, remote string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	args := append(sshBase(s), s.Host, remote)
	cmd := exec.CommandContext(ctx, "ssh", args...)
	cmd.Stdin = stdin
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("ssh %s %q: %w: %s", s.Host, remote, err, strings.TrimSpace(errb.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

// goArch maps `uname -sm` to GOOS/GOARCH.
func goArch(uname string) string {
	f := strings.Fields(strings.ToLower(uname))
	if len(f) != 2 {
		return uname
	}
	arch := map[string]string{"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}[f[1]]
	if arch == "" {
		arch = f[1]
	}
	return f[0] + "/" + arch
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil))[:16], nil
}

func startSSH(ctx context.Context, s Spec, bin string, stderr io.Writer) (*Conn, error) {
	if s.Host == "" {
		return nil, fmt.Errorf("agent %s: ssh transport needs host", s.Name)
	}
	uname, err := sshRun(ctx, s, nil, "uname -sm")
	if err != nil {
		return nil, err
	}
	if got, want := goArch(uname), runtime.GOOS+"/"+runtime.GOARCH; got != want && s.Binary == "" {
		return nil, fmt.Errorf("agent %s is %s and this binary is %s: build k6-fleet for it and set \"binary\"", s.Name, got, want)
	}
	hash, err := fileHash(bin)
	if err != nil {
		return nil, err
	}
	// The hash in the name makes reruns skip the upload and keeps two controller
	// versions from replacing each other's agent mid-run.
	remote := ".cache/k6-fleet/agent-" + hash
	if _, err := sshRun(ctx, s, nil, "test -x "+remote); err != nil {
		f, err := os.Open(bin)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		script := fmt.Sprintf("mkdir -p .cache/k6-fleet && cat > %[1]s.$$ && chmod +x %[1]s.$$ && mv %[1]s.$$ %[1]s", remote)
		if _, err := sshRun(ctx, s, f, script); err != nil {
			return nil, err
		}
	}
	args := append(sshBase(s), "-T", s.Host, "./"+remote+" "+strings.Join(agentArgs(s), " "))
	return launch(exec.Command("ssh", args...), stderr, nil)
}

// startDocker uses create, cp and start rather than a bind mount, so that it
// also works against a remote daemon through DOCKER_HOST or a docker context.
func startDocker(ctx context.Context, s Spec, bin string, stderr io.Writer) (*Conn, error) {
	if s.Image == "" {
		return nil, fmt.Errorf("agent %s: docker transport needs image", s.Name)
	}
	args := []string{"create", "-i", "--entrypoint", "/tmp/k6-fleet", "--user", "0"}
	if s.Network != "" {
		args = append(args, "--network", s.Network)
	}
	args = append(append(args, s.Image), agentArgs(s)...)
	id, err := dockerOut(ctx, args...)
	if err != nil {
		return nil, err
	}
	remove := func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = exec.CommandContext(c, "docker", "rm", "-f", id).Run()
	}
	if _, err := dockerOut(ctx, "cp", bin, id+":/tmp/k6-fleet"); err != nil {
		remove()
		return nil, err
	}
	conn, err := launch(exec.Command("docker", "start", "-ai", id), stderr, remove)
	if err != nil {
		remove()
		return nil, err
	}
	return conn, nil
}

func dockerOut(ctx context.Context, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	var out, errb bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", errors.New("docker " + args[0] + ": " + strings.TrimSpace(errb.String()))
	}
	return strings.TrimSpace(out.String()), nil
}
