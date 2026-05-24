package pipeline

import (
	"context"
	"strings"
)

// MediaSignalStage annotates each clip with a media_confidence score that
// reflects how likely the clip is the user's own voice vs. ambient media.
// It never drops clips — that decision belongs to the broadcast site.
type MediaSignalStage struct {
	Captions  SimpleSnapshotter // nil = Plex signal disabled
	Threshold float64           // media_confidence below this → media_flagged=true
}

func (s *MediaSignalStage) Name() string { return "media_signal" }

func (s *MediaSignalStage) Process(ctx context.Context, clip *AudioClip) error {
	if clip.Dropped {
		return nil
	}

	if clip.Meta == nil {
		clip.Meta = make(map[string]any)
	}

	base := 1.0

	if sim, ok := clip.Meta["speaker_similarity"].(float64); ok {
		switch {
		case sim < 0.25:
			base -= 0.50
		case sim < 0.40:
			base -= 0.25
		}
	}

	if nsp, ok := clip.Meta["no_speech_prob"].(float64); ok {
		base -= nsp * 0.20
	}

	plexScore := 0.0
	if s.Captions != nil {
		grams, _, ok := s.Captions.Snapshot()
		if ok && len(grams) > 0 {
			plexScore = computePlexScore(clip.RawText, grams)
		}
	}
	base -= plexScore * 0.40

	// Text-pattern classifier — zero-latency, pure Go.
	// textScore near 0.0 = broadcast-like → add penalty (max -0.25).
	textScore := classifyText(clip.RawText)
	base -= (1.0 - textScore) * 0.25

	if base < 0.0 {
		base = 0.0
	} else if base > 1.0 {
		base = 1.0
	}

	clip.Meta["media_confidence"] = base
	clip.Meta["plex_match_score"] = plexScore
	clip.Meta["text_classifier_score"] = textScore
	clip.Meta["media_flagged"] = base < s.Threshold

	return nil
}

// computePlexScore returns the fraction of clip trigrams found in captionGrams.
// Returns 0 when the clip has no trigrams.
func computePlexScore(rawText string, captionGrams map[string]struct{}) float64 {
	if strings.TrimSpace(rawText) == "" {
		return 0.0
	}
	clipGrams := wordTrigrams(normalizeForGrams(rawText))
	if len(clipGrams) == 0 {
		return 0.0
	}
	matches := 0
	for g := range clipGrams {
		if _, found := captionGrams[g]; found {
			matches++
		}
	}
	return float64(matches) / float64(len(clipGrams))
}
