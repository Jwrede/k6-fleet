package controller

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// externalScenario replaces the scenarios of a test with k6's
// externally-controlled executor, the one executor whose VUs can change during a
// run through the REST API. Each instance scales the numbers by its execution
// segment, so every agent gets the test total.
type externalScenario struct {
	VUs      int64
	Duration string // "0s" runs until stopped
}

func (s *externalScenario) endless() bool { return s != nil && s.Duration == "0s" }

// apply rewrites the options in a k6 archive. Options on the command line would
// not do: k6 derives no externally-controlled scenario from them.
func (s *externalScenario) apply(path string) error {
	in, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var out bytes.Buffer
	r, w := tar.NewReader(bytes.NewReader(in)), tar.NewWriter(&out)
	found := false
	for {
		h, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read k6 archive: %w", err)
		}
		body, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if h.Name == "metadata.json" {
			if body, err = s.metadata(body); err != nil {
				return err
			}
			h.Size = int64(len(body))
			found = true
		}
		if err := w.WriteHeader(h); err != nil {
			return err
		}
		if _, err := w.Write(body); err != nil {
			return err
		}
	}
	if !found {
		return errors.New("k6 archive has no metadata.json")
	}
	if err := w.Close(); err != nil {
		return err
	}
	return os.WriteFile(path, out.Bytes(), 0o644)
}

func (s *externalScenario) metadata(b []byte) ([]byte, error) {
	var meta map[string]any
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, fmt.Errorf("k6 archive metadata: %w", err)
	}
	opts, _ := meta["options"].(map[string]any)
	if opts == nil {
		opts = map[string]any{}
	}
	for _, k := range []string{"vus", "duration", "iterations", "stages"} {
		delete(opts, k)
	}
	opts["scenarios"] = map[string]any{"fleet": map[string]any{
		"executor": "externally-controlled", "vus": s.VUs, "maxVUs": s.VUs, "duration": s.Duration,
	}}
	meta["options"] = opts
	return json.Marshal(meta)
}
