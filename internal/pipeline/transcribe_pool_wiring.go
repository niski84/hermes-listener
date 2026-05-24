package pipeline

import (
	"context"
	"log"
	"strings"
	"time"

	"hermes-listener/internal/pipeline/transcribepool"
)

// whisperBusy returns true when whisper-server rejected the request because
// it was already processing another clip (single-threaded, 1 concurrent
// inference). We retry rather than immediately dropping the clip.
func whisperBusy(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "whisper 500") && strings.Contains(s, "temporarily unavailable")
}

// NewWhisperTranscriber returns a Transcriber backed by the existing Whisper
// HTTP client. It is the production wiring used by api/server.go.
//
// The pool deliberately does not own the prompt context: each AudioChannel
// stamps the per-utterance prompt onto Utterance via its own ContextPrompt
// (see audio_channel.go). That keeps multi-channel deployments from cross-
// pollinating each other's conversation state.
func NewWhisperTranscriber(whisperURL string) transcribepool.Transcriber {
	return func(ctx context.Context, u transcribepool.Utterance) transcribepool.Result {
		prompt := ""
		if pj, ok := u.User.(*poolJob); ok {
			prompt = pj.prompt
		}

		// Retry when whisper is busy. With base model on CPU, most clips
		// finish in 2-4s. Short retry keeps voice commands snappy.
		const maxRetries = 5
		const retryWait = 1500 * time.Millisecond
		var out WhisperOutput
		var err error
		for attempt := range maxRetries + 1 {
			out, err = callWhisper(ctx, whisperClient, whisperURL, u.PCM, prompt)
			if err == nil || !whisperBusy(err) {
				break
			}
			if attempt < maxRetries {
				log.Printf("[transcribepool] whisper busy (attempt %d/%d) — retrying in %s", attempt+1, maxRetries+1, retryWait)
				select {
				case <-ctx.Done():
					err = ctx.Err()
					goto done
				case <-time.After(retryWait):
				}
			}
		}
	done:
		r := transcribepool.Result{
			Utterance: u,
			Text:      out.RawText,
			LatencyMs: out.LatencyMs,
		}
		if err != nil {
			r.Err = err
			return r
		}
		// Stamp Whisper confidence directly onto the clip so ClassifyStage
		// (running off the result dispatcher goroutine, after we return)
		// sees the same Meta keys it always has.
		if pj, ok := u.User.(*poolJob); ok && pj.clip != nil {
			applyWhisperOutput(pj.clip, out)
		}
		return r
	}
}

// RunTranscribeDispatcher reads pool results forever and invokes the per-job
// callback stashed on Utterance.User. This is the single consumer of
// pool.Results(); no two goroutines may race on the channel.
//
// Returns when the pool's results channel closes (Shutdown drained).
func RunTranscribeDispatcher(p *transcribepool.Pool) {
	for r := range p.Results() {
		pj, ok := r.Utterance.User.(*poolJob)
		if !ok || pj == nil {
			log.Printf("[transcribepool] result with no poolJob (id=%s) — discarding", r.Utterance.ID)
			continue
		}
		// Run the post-stages on a fresh goroutine so a slow downstream
		// stage can't backpressure the dispatcher (and therefore the pool).
		go pj.HandlePoolResult(r)
	}
}
