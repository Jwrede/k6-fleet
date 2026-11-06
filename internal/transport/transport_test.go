package transport

import "testing"

func TestGoArch(t *testing.T) {
	for in, want := range map[string]string{"Linux x86_64": "linux/amd64", "Linux aarch64": "linux/arm64", "Darwin arm64": "darwin/arm64", "weird": "weird"} {
		if got := goArch(in); got != want {
			t.Errorf("%q: %q", in, got)
		}
	}
}
