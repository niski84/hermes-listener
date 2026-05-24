package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"hermes-listener/internal/models"
)

// NoiseSuppressStage strips non-speech noise (TV, HVAC, fans, keyboard
// clicks) from clip.PCM before it reaches whisper. Implemented as a per-clip
// ffmpeg subprocess using the arnndn filter (RNNoise) with a downloadable
// model file.
//
// We spawn one ffmpeg per clip. Startup overhead is ~30-50ms; the actual
// denoising runs ~100× realtime on a modern CPU, so a 5s clip costs well
// under 100ms end-to-end. If clip rate ever grows high enough to matter,
// swap this for a long-running ffmpeg with bounded in/out stdio buffers —
// the AudioStage interface stays the same.
//
// Graceful degradation: if the model file is missing at startup, the stage
// logs once and then becomes a no-op pass-through rather than failing every
// clip. Same on per-clip ffmpeg errors — we log, skip suppression, and let
// the raw PCM continue through the pipeline.
type NoiseSuppressStage struct {
	// ModelPath is the path to an RNNoise .rnnn model file (see
	// https://github.com/GregorR/rnnoise-models). cb.rnnn is a good default
	// general-purpose model; domain-specific ones exist if you want to tune.
	ModelPath string

	// Hub is optional — if set, we emit log events so UI activity panel
	// shows the stage is active and any errors surface.
	Hub *Hub

	disabled bool // set true if ModelPath doesn't exist; stage becomes a no-op
	checked  bool // lazy init guard
}

func (n *NoiseSuppressStage) Name() string { return "noise_suppress" }

func (n *NoiseSuppressStage) Process(ctx context.Context, clip *AudioClip) error {
	if !n.checked {
		n.checked = true
		if _, err := os.Stat(n.ModelPath); err != nil {
			n.disabled = true
			n.emit("warn", fmt.Sprintf("[noise_suppress] model missing (%s) — stage is a no-op until it's in place", n.ModelPath))
		}
	}
	if n.disabled {
		return nil
	}

	// arnndn operates at the native sample rate of the input. whisper expects
	// 16kHz s16le mono and our VAD buffer is already exactly that — so we can
	// pipe PCM straight in and out with no resampling step.
	start := time.Now()
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-f", "s16le", "-ar", "16000", "-ac", "1", "-i", "pipe:0",
		"-af", fmt.Sprintf("arnndn=m=%s", n.ModelPath),
		"-f", "s16le", "-ar", "16000", "-ac", "1", "pipe:1",
	)
	cmd.Stdin = bytes.NewReader(clip.PCM)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// Non-fatal: fall back to the original PCM so the clip still gets
		// transcribed. Log so a broken model surfaces instead of being silent.
		n.emit("warn", fmt.Sprintf("[noise_suppress] ffmpeg failed (%v) — using raw audio for this clip. stderr=%s", err, truncate(stderr.String(), 200)))
		return nil
	}

	denoised := out.Bytes()
	if len(denoised) == 0 {
		n.emit("warn", "[noise_suppress] ffmpeg returned empty output — using raw audio")
		return nil
	}

	clip.PCM = denoised
	clip.Meta["noise_suppressed"] = true
	clip.Meta["noise_suppress_ms"] = time.Since(start).Milliseconds()
	return nil
}

func (n *NoiseSuppressStage) emit(level, message string) {
	if n.Hub == nil {
		return
	}
	n.Hub.Broadcast(models.Event{
		Type:    "log",
		Payload: models.LogEntry{Level: level, Message: message},
	})
}
