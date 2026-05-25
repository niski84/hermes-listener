// Package web serves the hermes-listener HTTP UI + JSON API.
//
// Routes:
//
//	GET  /                 — settings UI (HTML)
//	GET  /api/health       — service health + channel telemetry (JSON)
//	GET  /api/settings     — current settings as flat JSON
//	POST /api/settings     — accept form-encoded updates, write .env
//	GET  /api/mics         — list ALSA/PulseAudio input devices
//	POST /api/restart      — request a graceful restart (returns; the
//	                         caller is expected to be systemd or similar)
//	GET  /api/probe        — probe sidecar URLs for reachability
package web

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"hermes-listener/internal/pipeline"
	"hermes-listener/internal/settings"
)

//go:embed templates/settings.html
var settingsHTML string

type Server struct {
	settings    *settings.Settings
	channelMgr  *pipeline.ChannelManager
	envPath     string
	tpl         *template.Template
}

func NewServer(s *settings.Settings, mgr *pipeline.ChannelManager, envPath string) (*Server, error) {
	tpl, err := template.New("settings").Funcs(template.FuncMap{
		"orDefault": func(v, fallback string) string {
			if v == "" {
				return fallback
			}
			return v
		},
	}).Parse(settingsHTML)
	if err != nil {
		return nil, fmt.Errorf("parse settings template: %w", err)
	}
	return &Server{
		settings:   s,
		channelMgr: mgr,
		envPath:    envPath,
		tpl:        tpl,
	}, nil
}

func (sv *Server) Mux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", sv.renderSettings)
	mux.HandleFunc("GET /api/health", sv.health)
	mux.HandleFunc("GET /api/settings", sv.getSettings)
	mux.HandleFunc("POST /api/settings", sv.saveSettings)
	mux.HandleFunc("GET /api/mics", sv.listMics)
	mux.HandleFunc("GET /api/probe", sv.probeSidecars)
	mux.HandleFunc("POST /api/restart", sv.restart)
	return mux
}

// ─── renderers ─────────────────────────────────────────────────────────

type settingsView struct {
	Values    map[string]string
	Mics      []micDevice
	Channels  any
	EnvPath   string
	Now       string
}

func (sv *Server) renderSettings(w http.ResponseWriter, r *http.Request) {
	// Re-read from disk on every render so a hand-edited .env shows up
	// immediately. Cheap operation, file is small.
	_ = sv.settings.Reload()
	mics, _ := listInputDevices()
	v := settingsView{
		Values:   sv.settings.All(),
		Mics:     mics,
		Channels: sv.channelMgr.List(),
		EnvPath:  sv.envPath,
		Now:      time.Now().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := sv.tpl.Execute(w, v); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

// ─── JSON API ──────────────────────────────────────────────────────────

func (sv *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{
		"status":   "ok",
		"service":  "hermes-listener",
		"channels": sv.channelMgr.List(),
	})
}

func (sv *Server) getSettings(w http.ResponseWriter, r *http.Request) {
	_ = sv.settings.Reload()
	writeJSON(w, 200, sv.settings.All())
}

func (sv *Server) saveSettings(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSON(w, 400, map[string]string{"error": "bad form: " + err.Error()})
		return
	}
	updates := map[string]string{}
	for k, vs := range r.PostForm {
		if len(vs) > 0 {
			updates[k] = vs[0]
		}
	}
	if err := sv.settings.Save(updates); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{
		"saved":   len(updates),
		"restart": true,
		"hint":    "Settings written to " + sv.envPath + ". Restart the service for changes to take effect.",
	})
}

type micDevice struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (sv *Server) listMics(w http.ResponseWriter, r *http.Request) {
	mics, err := listInputDevices()
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, mics)
}

// listInputDevices returns the list of PulseAudio source devices via
// `pactl list short sources`. Falls back to ALSA via `arecord -L` when
// pactl is unavailable.
func listInputDevices() ([]micDevice, error) {
	if out, err := exec.Command("pactl", "list", "short", "sources").Output(); err == nil {
		var devs []micDevice
		for _, line := range strings.Split(string(out), "\n") {
			parts := strings.Fields(line)
			if len(parts) < 2 {
				continue
			}
			// fields: index, name, driver, format, state
			name := parts[1]
			// Skip monitors (playback loopback) — capture only.
			if strings.Contains(name, ".monitor") {
				continue
			}
			devs = append(devs, micDevice{ID: name, Name: prettify(name)})
		}
		return devs, nil
	}
	// Fallback — ALSA names.
	if out, err := exec.Command("arecord", "-L").Output(); err == nil {
		var devs []micDevice
		for _, line := range strings.Split(string(out), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, " ") {
				continue
			}
			if line == "null" || strings.HasPrefix(line, "#") {
				continue
			}
			devs = append(devs, micDevice{ID: line, Name: line})
		}
		return devs, nil
	}
	return nil, fmt.Errorf("neither pactl nor arecord available")
}

func prettify(name string) string {
	// "alsa_input.usb-…_MCT244651021-01.analog-stereo" → "USB MCT24…"
	s := strings.TrimPrefix(name, "alsa_input.")
	s = strings.ReplaceAll(s, "_", " ")
	if len(s) > 64 {
		s = s[:60] + "…"
	}
	return s
}

// ─── sidecar probe ─────────────────────────────────────────────────────

type probeResult struct {
	Name string `json:"name"`
	URL  string `json:"url"`
	OK   bool   `json:"ok"`
	Err  string `json:"error,omitempty"`
}

func (sv *Server) probeSidecars(w http.ResponseWriter, r *http.Request) {
	probes := []struct {
		name, key, defaultURL, path string
	}{
		{"whisper", "WHISPER_URL", "http://localhost:9000", "/"},
		{"wake-word", "WAKE_WORD_URL", "http://localhost:9201", "/health"},
		{"speaker-filter", "SPEAKER_SIDECAR_URL", "http://localhost:9200", "/status"},
		{"smart-turn", "SMART_TURN_URL", "http://localhost:9202", "/"},
		{"plex", "PLEX_BASE_URL", "", ""},
	}
	results := make([]probeResult, 0, len(probes))
	client := &http.Client{Timeout: 2 * time.Second}
	for _, p := range probes {
		base := sv.settings.Get(p.key, p.defaultURL)
		if base == "" {
			results = append(results, probeResult{Name: p.name, URL: "", OK: false, Err: "not configured"})
			continue
		}
		url := base + p.path
		req, _ := http.NewRequestWithContext(r.Context(), "GET", url, nil)
		resp, err := client.Do(req)
		if err != nil {
			results = append(results, probeResult{Name: p.name, URL: url, OK: false, Err: err.Error()})
			continue
		}
		resp.Body.Close()
		results = append(results, probeResult{
			Name: p.name,
			URL:  url,
			OK:   resp.StatusCode < 500,
		})
	}
	writeJSON(w, 200, results)
}

// ─── restart ──────────────────────────────────────────────────────────

func (sv *Server) restart(w http.ResponseWriter, r *http.Request) {
	// We don't have authority to restart ourselves cleanly. Best-effort:
	// if running under systemd-user, the user can `systemctl --user
	// restart hermes-listener`. We just return a hint.
	writeJSON(w, 200, map[string]string{
		"hint": "Run: systemctl --user restart hermes-listener  (or kill -TERM $PID for non-service installs)",
	})
}

// ─── helpers ──────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// shutdown idle helper kept here so future packages can ImportSilently.
var _ = context.Background
