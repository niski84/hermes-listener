package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"time"
)

// SmartTurnClient queries the smart-turn sidecar at /score.
//
// The sidecar accepts a WAV file (multipart file=) and returns a JSON
// payload with {"complete": bool, "probability": float, "duration_seconds": float}.
//
// IsComplete returns complete=true on any error so the pipeline never stalls
// waiting on the sidecar (fail-open).
type SmartTurnClient struct {
	// BaseURL is the smart-turn sidecar base URL.
	// Defaults to SMART_TURN_URL env var, then "http://localhost:9202".
	// Set to "" to disable smart-turn checking entirely.
	BaseURL string

	// Timeout for each /score request. Defaults to 500ms.
	Timeout time.Duration

	cl *http.Client
}

func (c *SmartTurnClient) baseURL() string {
	if c.BaseURL != "" {
		return c.BaseURL
	}
	if env := os.Getenv("SMART_TURN_URL"); env != "" {
		return env
	}
	return "http://localhost:9202"
}

func (c *SmartTurnClient) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return 500 * time.Millisecond
}

func (c *SmartTurnClient) client() *http.Client {
	if c.cl != nil {
		return c.cl
	}
	c.cl = &http.Client{Timeout: c.timeout()}
	return c.cl
}

// IsComplete reads the WAV file at wavPath, POSTs it to /score, and returns
// (complete, probability).
//
// Returns complete=true on any error so the caller can fail-open.
// All errors are logged with the [smart_turn] prefix.
func (c *SmartTurnClient) IsComplete(ctx context.Context, wavPath string) (complete bool, prob float64) {
	data, err := os.ReadFile(wavPath)
	if err != nil {
		log.Printf("[smart_turn] read WAV %s: %v (fail-open)", wavPath, err)
		return true, 1.0
	}

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("file", "clip.wav")
	if err != nil {
		log.Printf("[smart_turn] create form file: %v (fail-open)", err)
		return true, 1.0
	}
	if _, err := fw.Write(data); err != nil {
		log.Printf("[smart_turn] write form data: %v (fail-open)", err)
		return true, 1.0
	}
	mw.Close()

	url := c.baseURL() + "/score"
	req, err := http.NewRequestWithContext(ctx, "POST", url, &body)
	if err != nil {
		log.Printf("[smart_turn] build request: %v (fail-open)", err)
		return true, 1.0
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := c.client().Do(req)
	if err != nil {
		log.Printf("[smart_turn] POST /score: %v (fail-open)", err)
		return true, 1.0
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		log.Printf("[smart_turn] POST /score returned HTTP %d (fail-open)", resp.StatusCode)
		return true, 1.0
	}

	var out struct {
		Complete        bool    `json:"complete"`
		Probability     float64 `json:"probability"`
		DurationSeconds float64 `json:"duration_seconds"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Printf("[smart_turn] decode response: %v (fail-open)", err)
		return true, 1.0
	}

	return out.Complete, out.Probability
}
