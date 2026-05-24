package startup

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"hermes-listener/internal/pipeline"
	"hermes-listener/internal/storage"
)

// AudioComponent is the minimal capture stack for hermes-listener:
// just hub, daily-transcript writer, and the channel manager that
// owns the mic. No watcher, no compression worker, no whisper
// watchdog, no recovery — features that ²nd-whisper-brain accumulated
// over 18 months and that Hermes doesn't need from a thin sensor.
type AudioComponent struct {
	Hub            *pipeline.Hub
	ChannelManager *pipeline.ChannelManager
	Transcript     *storage.DailyTranscript
	Store          *storage.Store // stub — pipeline holds it as a field
}

// SetupAudio wires the minimal capture path. Returns a started
// AudioComponent ready to receive events on Hub. Caller is responsible
// for blocking (typically: <-ctx.Done()).
//
// Required env (with sensible defaults):
//
//	WHISPER_URL        — whisper.cpp HTTP server (default http://localhost:9000)
//	VAULT_PATH         — where daily transcripts get written (default ~/Documents/vault)
//	MIC_DEVICE         — ALSA/PulseAudio device for the default mic (default "default")
//	AUDIO_DIR          — where to stage clip WAVs (default ./data/audio)
//	DATA_DIR           — channel persistence + scratch (default ./data)
func SetupAudio(ctx context.Context) (*AudioComponent, error) {
	whisperURL := envDefault("WHISPER_URL", "http://localhost:9000")
	vaultPath := expandHome(envDefault("VAULT_PATH", "~/Documents/vault"))
	micDevice := envDefault("MIC_DEVICE", "default")
	audioDir := envDefault("AUDIO_DIR", "./data/audio")
	dataDir := envDefault("DATA_DIR", "./data")

	// hermes-listener writes daily transcripts under a "listener/"
	// subdir of the user's vault. Keeps our output cleanly isolated from
	// anything else writing to the vault (manual notes, Hermes outputs,
	// nogura legacy files, etc.).
	transcriptDir := filepath.Join(vaultPath, "listener")
	if err := os.MkdirAll(transcriptDir, 0o755); err != nil {
		return nil, fmt.Errorf("create transcript dir %s: %w", transcriptDir, err)
	}
	if err := os.MkdirAll(audioDir, 0o755); err != nil {
		return nil, fmt.Errorf("create audio dir %s: %w", audioDir, err)
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", dataDir, err)
	}

	hub := pipeline.NewHub()
	transcript, err := storage.NewDailyTranscript(transcriptDir)
	if err != nil {
		return nil, fmt.Errorf("daily transcript: %w", err)
	}
	store := &storage.Store{} // stub; pipeline holds it but never invokes it

	mgr := pipeline.NewChannelManager(
		micDevice,
		whisperURL,
		audioDir,
		vaultPath,
		dataDir,
		hub,
		transcript,
		store,
	)

	log.Printf("[startup] hermes-listener ready: mic=%s whisper=%s vault=%s",
		micDevice, whisperURL, transcriptDir)

	return &AudioComponent{
		Hub:            hub,
		ChannelManager: mgr,
		Transcript:     transcript,
		Store:          store,
	}, nil
}

func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func expandHome(p string) string {
	if len(p) > 0 && p[0] == '~' {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, p[1:])
		}
	}
	return p
}
