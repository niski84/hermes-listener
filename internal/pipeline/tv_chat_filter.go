package pipeline

// TVChatFilterStage drops mic clips whose text strongly overlaps with the
// captions of whatever is currently playing on the user's TV.
//
// Rationale: mic channels in rooms with a TV pick up movie/show audio. The
// transcriber treats it as user speech, polluting transcripts and triggering
// false claims/Q&A. By comparing the clip's word-trigram set against a
// snapshot of the current TV captions, we can identify and suppress clips
// that look like they came from the TV instead of the user.
//
// Trigrams (3-word phrases) are robust to small STT word swaps and are rare
// in natural conversation but dense in scripted dialogue, so the score has
// strong separation between speech and TV chatter.
//
// Fail-open: any failure to fetch captions, an empty snapshot, or an empty
// clip results in no change to the clip. This stage NEVER returns an error.

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"hermes-listener/internal/models"
)

// puncRE strips all non-word, non-space, non-apostrophe characters before
// tokenizing. Apostrophes are kept so contractions like "don't" stay intact.
var puncRE = regexp.MustCompile(`[^\w\s']`)

// normalizeForGrams lower-cases s, strips punctuation, and returns the
// resulting whitespace-split tokens. Used on both the clip text and the
// caption text so the two sides hash identically.
func normalizeForGrams(s string) []string {
	s = strings.ToLower(s)
	s = puncRE.ReplaceAllString(s, " ")
	return strings.Fields(s)
}

// wordTrigrams returns the set of 3-word phrases (space-joined) found in
// words. For inputs shorter than 3 words it returns an empty (non-nil) map.
func wordTrigrams(words []string) map[string]struct{} {
	out := make(map[string]struct{})
	for i := 0; i+3 <= len(words); i++ {
		out[words[i]+" "+words[i+1]+" "+words[i+2]] = struct{}{}
	}
	return out
}

// CaptionsSnapshotter is the small interface TVChatFilterStage depends on.
// PlexCaptionsClient satisfies it; tests can substitute a stub so they don't
// need an HTTP server.
//
// CaptionsForTime returns the trigram set + movie title for whatever was
// playing at the supplied time. ok=false means there are no usable captions
// at that moment (nothing playing, fetch failed, etc.) — the stage interprets
// that as fail-open. MUST NEVER return an error.
type CaptionsSnapshotter interface {
	CaptionsForTime(ctx context.Context, at time.Time) (grams map[string]struct{}, title string, imdbID string, ok bool)
}

// TVChatFilterStage compares a clip's word-trigrams against the captions of
// whatever was playing on the TV at the clip's CapturedAt time, and drops the
// clip if the overlap exceeds Threshold.
type TVChatFilterStage struct {
	Client    CaptionsSnapshotter
	Threshold float64 // default 0.5 if zero or negative
	Hub       *Hub    // optional — if set, broadcasts a "tv_chatter_filtered" event on drop

	zeroTimeWarnOnce sync.Once
}

func (s *TVChatFilterStage) Name() string { return "tv_chat_filter" }

func (s *TVChatFilterStage) Process(ctx context.Context, clip *AudioClip) error {
	// Fail-open guards: no work to do.
	if clip == nil {
		return nil
	}
	if clip.Dropped || strings.TrimSpace(clip.RawText) == "" {
		return nil
	}
	if s.Client == nil {
		return nil
	}
	at := clip.CapturedAt
	if at.IsZero() {
		s.zeroTimeWarnOnce.Do(func() {
			log.Printf("[tv_filter] clip CapturedAt is zero — falling back to time.Now() (warning emitted once per stage)")
		})
		at = time.Now()
	}
	grams, title, _, ok := s.Client.CaptionsForTime(ctx, at)
	if !ok || len(grams) == 0 {
		// Diagnostic: a real utterance reached the TV filter but no captions
		// were available to compare against — the filter is a silent no-op.
		log.Printf("[tv_filter] eval SKIP — no captions for clip at %s (text=%q)",
			at.Format("15:04:05"), truncate(clip.RawText, 60))
		return nil
	}

	clipGrams := wordTrigrams(normalizeForGrams(clip.RawText))
	if len(clipGrams) == 0 {
		log.Printf("[tv_filter] eval SKIP — clip too short for trigrams (text=%q)", truncate(clip.RawText, 60))
		return nil
	}

	matches := 0
	for g := range clipGrams {
		if _, found := grams[g]; found {
			matches++
		}
	}
	score := float64(matches) / float64(len(clipGrams))

	if clip.Meta == nil {
		clip.Meta = make(map[string]any)
	}
	clip.Meta["tv_match_score"] = score
	clip.Meta["tv_match_movie"] = title

	threshold := s.Threshold
	if threshold <= 0 {
		threshold = 0.5
	}

	// Diagnostic: log every scored clip so the trigram-overlap distribution
	// is visible — needed to tell whether the 0.50 threshold is reachable by
	// mic-captured, Whisper-transcribed TV audio vs the pristine SRT.
	log.Printf("[tv_filter] eval score=%.3f thr=%.2f movie=%q trigrams=%d/%d text=%q",
		score, threshold, title, matches, len(clipGrams), truncate(clip.RawText, 80))

	if score >= threshold {
		clip.Dropped = true
		clip.DropReason = fmt.Sprintf("tv_chatter (score=%.2f, movie=%q)", score, title)
		// Explicit log line so TV-poisoning suppression is visible in the
		// log, not just on the SSE stream — requested for auditing what the
		// filter keeps out of transcripts/claims.
		log.Printf("[tv_filter] DROPPED clip as TV chatter (score=%.2f thr=%.2f movie=%q): %q",
			score, threshold, title, truncate(clip.RawText, 120))
		if s.Hub != nil {
			s.Hub.Broadcast(models.Event{
				Type: "tv_chatter_filtered",
				Payload: map[string]any{
					"channel_id": clip.ChannelID,
					"text":       clip.RawText,
					"score":      score,
					"movie":      title,
				},
			})
		}
	}
	return nil
}
