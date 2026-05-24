package pipeline

import (
	"strings"
	"sync"
)

// ContextPrompt is a small shared buffer used to seed the whisper decoder with
// prior-utterance text on each request. Whisper's `prompt` field is documented
// as "optional text to provide as a prompt for the first window" — in practice
// it meaningfully improves continuity across short clips (pronoun resolution,
// proper-noun consistency, punctuation style) without re-enabling the
// server-side state corruption that forced us onto `-nc`.
//
// WHY THIS IS A SHARED STRUCT (instead of logic inside TranscribeStage)
// -------------------------------------------------------------------
// The authoritative source of "this text is real speech, not a hallucination"
// is ClassifyStage, which runs AFTER TranscribeStage. So:
//
//   - ClassifyStage calls Record(clip.Text) when a clip passes filters.
//   - TranscribeStage calls For() before sending the next clip.
//
// A shared, mutex-guarded struct is the cleanest way to let those two stages
// talk without either one owning the other's lifetime.
//
// CUSTOM VOCABULARY (hints)
// -------------------------
// hints is a user-configurable list of proper nouns / domain jargon that gets
// prepended to every prompt. Whisper treats prompt tokens as bias, so listing
// "Nōgura", "Anthropic", "Tailscale" here makes the model far more likely to
// transcribe those correctly instead of falling back to phonetically-similar
// common words. Keep the total prompt under ~200 chars — whisper only uses
// the trailing window of the prompt, so over-stuffing crowds out the recent
// conversational context.
//
// Hot-swapping: SetHints replaces the list atomically under the same mutex
// that guards .For()/.Record(). ChannelManager.ReloadVocab() uses this to
// push a new vocab.txt across every live channel without restarting capture.
type ContextPrompt struct {
	// MaxRecent is how many recent accepted utterances to keep. 2 is a good
	// default: enough continuity for pronoun/topic carry-over, not so much
	// that stale context hangs around after a conversation shift.
	MaxRecent int

	// mu guards both hints and recent. RWMutex because For() is called on
	// every transcribe and is read-heavy, while SetHints/Record are bursty.
	mu     sync.RWMutex
	hints  []string
	recent []string
}

// SetHints atomically replaces the vocabulary hint list. Pass nil or empty
// to clear. The caller's slice is defensively copied so future mutations
// don't race with concurrent For() readers.
func (c *ContextPrompt) SetHints(h []string) {
	c.mu.Lock()
	if len(h) == 0 {
		c.hints = nil
	} else {
		c.hints = append([]string(nil), h...)
	}
	c.mu.Unlock()
}

// Hints returns a snapshot copy of the current hint list. Safe for callers
// to mutate; doesn't expose the internal slice.
func (c *ContextPrompt) Hints() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.hints) == 0 {
		return nil
	}
	return append([]string(nil), c.hints...)
}

// For assembles the prompt string to send to whisper. Shape:
//
//	"<hints joined by ', '>. <recent utterance 1> <recent utterance 2>"
//
// Returns "" if there's nothing to inject — TranscribeStage should skip the
// form field entirely in that case so whisper uses its own defaults.
func (c *ContextPrompt) For() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var parts []string
	if len(c.hints) > 0 {
		// Phrase hints as natural-language context; whisper's prompt is
		// token-matched, so a readable sentence works as well as a list.
		parts = append(parts, "Context: "+strings.Join(c.hints, ", ")+".")
	}
	if len(c.recent) > 0 {
		parts = append(parts, strings.Join(c.recent, " "))
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

// Record adds an accepted transcript to the rolling buffer.
func (c *ContextPrompt) Record(text string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	max := c.MaxRecent
	if max <= 0 {
		max = 2
	}
	c.recent = append(c.recent, text)
	if len(c.recent) > max {
		c.recent = c.recent[len(c.recent)-max:]
	}
}

// Reset clears the rolling buffer. Call on session boundaries so the first
// clip of a new session doesn't inherit context from the last one. Vocab
// hints are intentionally NOT cleared — they're configuration, not state.
func (c *ContextPrompt) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recent = nil
}
