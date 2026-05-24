package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"

	"hermes-listener/internal/models"
)

// WakeWordStage runs each VAD-closed clip through the openWakeWord sidecar
// at WAKE_WORD_SIDECAR_URL (default http://127.0.0.1:9201) and, if the
// sidecar reports a hit, annotates the clip with WakeWordDetected=true.
//
// The stage is FAIL-OPEN by design — any error (sidecar down, network blip,
// bad response) is logged at debug level and the clip continues unchanged.
// This is important because the wake-word path is an OPTIMISATION layer
// on top of the existing pipeline. If it's misbehaving, the worst case is
// that we lose the fast-track benefit; voice commands still work through
// the normal whisper + intent path.
//
// Downstream stages (SpeakerFilterStage, RMSGateStage) and the transcribe
// pool can check clip.WakeWordDetected to bypass their drop logic / jump
// the queue. Wake-word detection IS speaker validation for the purposes
// of the gate stages — if openWakeWord heard "hey jarvis", the user said
// it; we shouldn't second-guess that with an RMS check.
type WakeWordStage struct {
	SidecarURL string
	Hub        *Hub

	cl *http.Client
}

func (s *WakeWordStage) Name() string { return "wake_word" }

func (s *WakeWordStage) client() *http.Client {
	if s.cl != nil {
		return s.cl
	}
	// Short timeout — sidecar typically returns in <100ms on CPU. Don't
	// let it stall the pipeline if it hangs.
	s.cl = &http.Client{Timeout: 2 * time.Second}
	return s.cl
}

func (s *WakeWordStage) sidecarURL() string {
	if s.SidecarURL != "" {
		return strings.TrimRight(s.SidecarURL, "/")
	}
	if env := os.Getenv("WAKE_WORD_SIDECAR_URL"); env != "" {
		return strings.TrimRight(env, "/")
	}
	return "http://127.0.0.1:9201"
}

func (s *WakeWordStage) Process(ctx context.Context, clip *AudioClip) error {
	if clip == nil || len(clip.PCM) == 0 {
		return nil
	}
	// Don't pay the sidecar round-trip for clips that already got dropped
	// upstream (e.g. by VADFilterStage).
	if clip.Dropped {
		return nil
	}

	detected, model, confidence, ok := s.detect(ctx, clip.PCM)
	if !ok {
		// Sidecar call failed. Log once-in-a-while so users notice but
		// don't spam — failures are fail-OPEN: clip falls through to
		// normal whisper path.
		log.Printf("[wake_word] sidecar call failed for %.1fs clip; passing through (fail-open)", clip.Duration.Seconds())
		return nil
	}

	if clip.Meta == nil {
		clip.Meta = map[string]any{}
	}
	clip.Meta["wake_word_confidence"] = confidence
	clip.Meta["wake_word_model"] = model

	// Always log near-misses so you can see how close your voice came to
	// triggering. Below 0.05 is pure background noise; above 0.05 means
	// the model heard something resembling the wake word. Useful for
	// tuning the threshold or noticing mic problems.
	if !detected && confidence > 0.05 {
		log.Printf("[wake_word] near-miss %s (conf=%.4f, %.1fs clip) — below threshold, no fast-track", model, confidence, clip.Duration.Seconds())
	}

	if detected {
		clip.WakeWordDetected = true
		clip.WakeWordModel = model
		clip.WakeWordConfidence = confidence
		log.Printf("[wake_word] DETECTED %s (conf=%.2f, %.1fs clip) — fast-tracking", model, confidence, clip.Duration.Seconds())
		s.emit("info", fmt.Sprintf("[wake_word] DETECTED %s (conf=%.2f) — clip will fast-track", model, confidence))
		// Wake-word detection is the only moment in the pipeline where
		// identity is certain — it's definitively the user's voice.
		// Send to /adapt so the speaker pool grows from confirmed clips,
		// never from ambiguous scoring. Fail-open: don't block the clip.
		go s.adapt(clip.PCM)
		return nil
	}

	// Opt-in: drop long clips without a wake word. This was the default
	// behavior at one point but it conflicts with passive recording —
	// when the user is talking conversationally for 10-30s, the whole
	// clip gets discarded as "TV chatter." Enable only in command-only
	// mode where passive transcription is unwanted. Identity should be
	// gated by SpeakerFilterStage, not duration.
	if os.Getenv("WAKE_WORD_DROP_LONG_CLIPS") == "true" {
		const dropDurationThreshold = 8 * time.Second
		if clip.Duration > dropDurationThreshold {
			clip.Dropped = true
			clip.DropReason = fmt.Sprintf("no wake word in %.1fs clip (likely background)", clip.Duration.Seconds())
			log.Printf("[wake_word] dropped %.1fs clip — no wake word (conf=%.4f, likely TV/background)", clip.Duration.Seconds(), confidence)
			s.emit("info", fmt.Sprintf("[wake_word] dropped %.1fs clip — no wake word found (conf=%.4f)", clip.Duration.Seconds(), confidence))
		}
	}
	return nil
}

// detect POSTs the clip's PCM (as WAV) to /detect and returns the parsed
// result. Returns ok=false on any error so the caller can fail-open.
func (s *WakeWordStage) detect(ctx context.Context, pcm []byte) (detected bool, model string, confidence float64, ok bool) {
	wav := pcmToWAV(pcm)

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("file", "clip.wav")
	if err != nil {
		return false, "", 0, false
	}
	if _, err := fw.Write(wav); err != nil {
		return false, "", 0, false
	}
	mw.Close()

	req, err := http.NewRequestWithContext(ctx, "POST", s.sidecarURL()+"/detect", &body)
	if err != nil {
		return false, "", 0, false
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := s.client().Do(req)
	if err != nil {
		return false, "", 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return false, "", 0, false
	}
	var out struct {
		Detected   bool    `json:"detected"`
		Model      string  `json:"model"`
		Confidence float64 `json:"confidence"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, "", 0, false
	}
	return out.Detected, out.Model, out.Confidence, true
}

// adapt POSTs a confirmed-user clip to the speaker sidecar's /adapt endpoint.
// Called in a goroutine after wake-word detection — fire-and-forget, fail-open.
func (s *WakeWordStage) adapt(pcm []byte) {
	speakerURL := speakerSidecarURL()
	if speakerURL == "" {
		return
	}
	wav := pcmToWAV(pcm)
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("file", "clip.wav")
	if err != nil {
		return
	}
	if _, err := fw.Write(wav); err != nil {
		return
	}
	mw.Close()

	req, err := http.NewRequestWithContext(context.Background(), "POST", speakerURL+"/adapt", &body)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		log.Printf("[wake_word] /adapt call failed: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var out struct {
			OK       bool    `json:"ok"`
			Reason   string  `json:"reason,omitempty"`
			PoolSize int     `json:"pool_size"`
			Sim      float64 `json:"similarity"`
		}
		if json.NewDecoder(resp.Body).Decode(&out) == nil && out.OK {
			log.Printf("[wake_word] /adapt: pool now %d embeddings (sim=%.3f)", out.PoolSize, out.Sim)
		}
	}
}

func (s *WakeWordStage) emit(level, message string) {
	if s.Hub == nil {
		return
	}
	s.Hub.Broadcast(models.Event{
		Type:    "log",
		Payload: models.LogEntry{Level: level, Message: message},
	})
}
