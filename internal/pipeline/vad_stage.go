package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"
)

const defaultVADThreshold = 0.45

// VADFilterStage calls the Silero VAD endpoint on the speaker sidecar.
// Clips where speech_prob < Threshold are dropped before Whisper runs.
// Fail-open by design: if the endpoint is unreachable, the clip passes through.
type VADFilterStage struct {
	SidecarURL string  // e.g. "http://127.0.0.1:9200"
	Threshold  float64 // default 0.45
	client     *http.Client
}

// NewVADFilterStage creates a VADFilterStage. If threshold is <= 0, the default
// (0.45) is used. The HTTP client has a 3s timeout to avoid blocking the pipeline.
func NewVADFilterStage(sidecarURL string, threshold float64) *VADFilterStage {
	if threshold <= 0 {
		threshold = defaultVADThreshold
	}
	return &VADFilterStage{
		SidecarURL: sidecarURL,
		Threshold:  threshold,
		client:     &http.Client{Timeout: 3 * time.Second},
	}
}

func (v *VADFilterStage) Name() string { return "vad_filter" }

// Process sends the clip's PCM to the /vad endpoint as a WAV multipart upload.
// On success it stores speech_prob in clip.Meta and drops the clip if below threshold.
// On any failure (network, parse, non-200) the clip passes through unchanged (fail-open).
func (v *VADFilterStage) Process(ctx context.Context, clip *AudioClip) error {
	if len(clip.PCM) == 0 {
		return nil
	}

	wav := pcmToWAV(clip.PCM)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "audio.wav")
	if err != nil {
		return nil // fail-open
	}
	if _, err = io.Copy(fw, bytes.NewReader(wav)); err != nil {
		return nil // fail-open
	}
	mw.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.SidecarURL+"/vad", &buf)
	if err != nil {
		return nil // fail-open
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := v.client.Do(req)
	if err != nil {
		return nil // fail-open: sidecar unreachable
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return nil                     // fail-open
	}

	var result struct {
		SpeechProb float64 `json:"speech_prob"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil // fail-open
	}

	if clip.Meta == nil {
		clip.Meta = make(map[string]any)
	}
	clip.Meta["vad_speech_prob"] = result.SpeechProb

	if result.SpeechProb < v.Threshold {
		clip.Dropped = true
		clip.DropReason = fmt.Sprintf("vad: speech_prob %.2f < %.2f", result.SpeechProb, v.Threshold)
		// No Marker — silent drop, same pattern as SpeakerFilterStage.
	}
	return nil
}
