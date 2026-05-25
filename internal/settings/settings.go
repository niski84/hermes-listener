// Package settings owns the user-editable hermes-listener configuration.
//
// Source of truth is the .env file at the project root. Settings get
// loaded from .env at startup AND re-read by the web UI on each render,
// so edits made by hand to .env are visible immediately. Writes go back
// to .env via the API; a restart is required for changes to take effect
// (the audio pipeline reads env once at SetupAudio time).
package settings

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Settings is a flat map of env-var name → value. We don't use a typed
// struct because the listener's env surface is small, ad-hoc, and
// regularly grows (new sidecars, new flags). A map keeps the wire
// format trivial and the form-handler generic.
type Settings struct {
	mu     sync.RWMutex
	envPath string
	values map[string]string
}

// New loads settings from the given .env path. Missing file is fine —
// values returns an empty map. Anything else (parse error, permission)
// is returned as an error.
func New(envPath string) (*Settings, error) {
	s := &Settings{
		envPath: envPath,
		values:  map[string]string{},
	}
	if err := s.Reload(); err != nil {
		return nil, err
	}
	return s, nil
}

// Reload re-reads the .env from disk. Safe to call from concurrent
// request handlers; reads are RLocked.
func (s *Settings) Reload() error {
	f, err := os.Open(s.envPath)
	if err != nil {
		if os.IsNotExist(err) {
			s.mu.Lock()
			s.values = map[string]string{}
			s.mu.Unlock()
			return nil
		}
		return fmt.Errorf("open %s: %w", s.envPath, err)
	}
	defer f.Close()

	parsed := map[string]string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(line[eq+1:])
		// Strip a single layer of matching quotes — common when users
		// hand-edit. Don't strip if values contain interpolation.
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') ||
				(val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		parsed[key] = val
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", s.envPath, err)
	}

	s.mu.Lock()
	s.values = parsed
	s.mu.Unlock()
	return nil
}

// Get returns a value or its fallback if unset.
func (s *Settings) Get(key, fallback string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.values[key]; ok && v != "" {
		return v
	}
	return fallback
}

// All returns a sorted copy of every key/value pair. Used by the web UI
// to render the raw view.
func (s *Settings) All() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.values))
	for k, v := range s.values {
		out[k] = v
	}
	return out
}

// Save persists the given updates to .env. Keys not in the update map
// are preserved at their existing values. We rewrite the file in place
// using a temp-file-rename for atomicity. Comments + blank lines from
// the original file are NOT preserved — that's a known limitation; for
// hermes-listener's small surface it's acceptable.
func (s *Settings) Save(updates map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range updates {
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k == "" {
			continue
		}
		if v == "" {
			// Explicit empty = unset. Remove from the file.
			delete(s.values, k)
			continue
		}
		s.values[k] = v
	}

	tmp, err := os.CreateTemp(filepath.Dir(s.envPath), ".env.tmp.*")
	if err != nil {
		return fmt.Errorf("temp: %w", err)
	}
	defer os.Remove(tmp.Name())

	// Sort keys so diffs are stable across saves.
	keys := make([]string, 0, len(s.values))
	for k := range s.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Fprintln(tmp, "# hermes-listener configuration")
	fmt.Fprintln(tmp, "# Saved by settings UI. Edit by hand or via http://localhost:$PORT/")
	fmt.Fprintln(tmp, "# After changes, restart the service for them to take effect.")
	fmt.Fprintln(tmp)
	for _, k := range keys {
		fmt.Fprintf(tmp, "%s=%s\n", k, s.values[k])
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}
	// Match the perm of an existing .env, or 0600 for new files.
	mode := os.FileMode(0o600)
	if st, err := os.Stat(s.envPath); err == nil {
		mode = st.Mode()
	}
	if err := os.Chmod(tmp.Name(), mode); err != nil {
		return fmt.Errorf("chmod: %w", err)
	}
	if err := os.Rename(tmp.Name(), s.envPath); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}
