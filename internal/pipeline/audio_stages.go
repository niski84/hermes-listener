package pipeline

// This file defines the per-utterance processing pipeline used by Streamer.
//
// WHY A STAGE PIPELINE (vs. one big process() function)
// -----------------------------------------------------
// Each utterance (a chunk of PCM coming out of VAD) needs to flow through a
// growing list of concerns: save to disk, noise-suppress, speaker-filter,
// transcribe, classify/filter hallucinations, emit to transcript + SSE, etc.
// Hard-coding all of those inline made process() a 60-line switch that was
// painful to extend. The AudioStage interface gives us an ordered list of
// transformations each with a single responsibility.
//
// HOW TO ADD A NEW STAGE
// ----------------------
//   1. Write a type that implements AudioStage (Name + Process).
//   2. Decide where in the pipeline it runs — by convention:
//        early (raw-audio filters):  SaveAudioStage, NoiseSuppressStage
//        mid   (speaker / gating):   SpeakerFilterStage
//        main:                        TranscribeStage
//        late  (text filters):        ClassifyStage
//   3. Insert it in Streamer.buildPipeline() at the right slot.
//
// A stage may:
//   - mutate clip.PCM (preprocess audio)
//   - fill clip.RawText / clip.Text (transcribe / post-process)
//   - mark clip.Dropped = true with a DropReason to short-circuit the rest
//   - read/write clip.Meta for cross-stage data (e.g. speaker_similarity)
//
// Stages run sequentially on a single goroutine per utterance — they don't
// need internal locking for per-clip state. Shared state (like recent-output
// tracking for repetition detection) lives on the stage struct itself.

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
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// clarityAvgLogprobThresh and clarityNoSpeechProbThresh are the clip-level
// confidence gates applied by ClassifyStage. Values are overridden at startup
// from config (CLARITY_LOGPROB_THRESHOLD / CLARITY_NOSPEECH_THRESHOLD).
var (
	clarityAvgLogprobThresh   = -0.7 // more negative than this = "unclear"
	clarityNoSpeechProbThresh = 0.6  // higher than this        = "unclear"
)

// SetClarityThresholds wires the env-overridable confidence thresholds from
// config into the package-level vars used by ClassifyStage.
func SetClarityThresholds(avgLogprob, noSpeechProb float64) {
	clarityAvgLogprobThresh = avgLogprob
	clarityNoSpeechProbThresh = noSpeechProb
}

// AudioClip carries one utterance through the pipeline. Fields are filled in
// as stages run — early stages see mostly zero values, later stages see the
// accumulated result of everything upstream.
type AudioClip struct {
	// ChannelID identifies which AudioChannel produced this clip.
	// Stamped at creation; propagated into all hub event payloads.
	ChannelID string

	// Raw audio + capture metadata — set before the pipeline runs.
	PCM        []byte
	CapturedAt time.Time
	Duration   time.Duration
	RMS        float64

	// Populated by stages as they run.
	AudioPath string         // set by SaveAudioStage
	RawText   string         // set by TranscribeStage (unfiltered whisper output)
	Text      string         // set by ClassifyStage on pass; empty if dropped
	WhisperMs int64          // set by TranscribeStage — transcription latency
	Meta      map[string]any // extension slot: speaker_similarity, noise_suppressed, etc.

	// Drop semantics — any stage can short-circuit the pipeline.
	Dropped    bool
	DropReason string // human-readable reason for logs
	Marker     string // marker LABEL (e.g. "unclear") — writeMarker wraps it as "[~Xs, <label> | audio:...]". No surrounding brackets.

	// IsAmend signals that this clip's Text should replace the LAST line in the
	// in-progress session buffer rather than append a new one. Set by
	// ClassifyStage when a short continuation utterance is merged with the
	// previous accepted line.
	IsAmend bool

	// ForceClose is true when the VAD hard cap fired (maxSpeechFrames) rather
	// than natural silence. ClassifyStage uses this to decide whether a
	// full-length clip should be stitched to the previous line as a sentence
	// continuation.
	ForceClose bool

	// WakeWordDetected is true when the wake-word sidecar found a wake phrase
	// in this clip. Set by WakeWordStage. Downstream stages use it to
	// fast-track the clip — e.g. SpeakerFilterStage and RMSGateStage skip
	// their drop logic when this is true (wake-word IS speaker validation
	// for our purposes), and transcribepool can prioritise it in the queue.
	// Zero value (false) is safe — pipeline behaves identically to before
	// this field existed.
	WakeWordDetected bool

	// WakeWordModel is the name of the model that fired (e.g. "hey_jarvis_v0.1").
	// Empty when WakeWordDetected is false or the sidecar is unreachable.
	WakeWordModel string

	// WakeWordConfidence is the top score returned by the sidecar (0..1).
	WakeWordConfidence float64
}

// AudioStage is one step in the per-utterance pipeline. See file header.
type AudioStage interface {
	Name() string
	Process(ctx context.Context, clip *AudioClip) error
}

// runStages executes stages in order, stopping early if a stage marks the
// clip as Dropped or returns an error. Returns the first error (if any).
func runStages(ctx context.Context, stages []AudioStage, clip *AudioClip) error {
	for _, st := range stages {
		if clip.Dropped {
			return nil
		}
		if err := st.Process(ctx, clip); err != nil {
			return fmt.Errorf("%s: %w", st.Name(), err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// SaveAudioStage — writes the raw PCM as a timestamped WAV under audioDir.
// Runs first so every later stage (and future reprocessing) can reference the
// on-disk file by path, even for clips we end up dropping.
// ---------------------------------------------------------------------------

type SaveAudioStage struct {
	AudioDir string
}

func (s *SaveAudioStage) Name() string { return "save_audio" }

func (s *SaveAudioStage) Process(_ context.Context, clip *AudioClip) error {
	dir := filepath.Join(s.AudioDir, clip.CapturedAt.Format("2006-01-02"))
	if err := os.MkdirAll(dir, 0755); err != nil {
		clip.AudioPath = "unknown"
		return nil // non-fatal — keep processing even if we can't save
	}
	path := filepath.Join(dir, clip.CapturedAt.Format("15-04-05.000")+".wav")
	if err := os.WriteFile(path, pcmToWAV(clip.PCM), 0644); err != nil {
		clip.AudioPath = "unknown"
		return nil
	}
	clip.AudioPath = path
	return nil
}

// ---------------------------------------------------------------------------
// TranscribeStage — sends the clip to whisper-server and sets clip.RawText.
// If whisper errors, marks the clip Dropped with Marker="[whisper error]"
// so the transcript shows a placeholder instead of silently losing the clip.
// ---------------------------------------------------------------------------

type TranscribeStage struct {
	WhisperURL string
	Client     *http.Client
	// Context is an optional shared prompt buffer. If non-nil, its current
	// contents (domain hints + last N accepted utterances) are sent as the
	// whisper `prompt` field so the decoder has conversational context.
	// Safe to leave nil — whisper falls back to its own defaults.
	Context *ContextPrompt
}

func (t *TranscribeStage) Name() string { return "transcribe" }

func (t *TranscribeStage) Process(ctx context.Context, clip *AudioClip) error {
	prompt := ""
	if t.Context != nil {
		prompt = t.Context.For()
	}
	out, err := callWhisper(ctx, t.Client, t.WhisperURL, clip.PCM, prompt)
	clip.WhisperMs = out.LatencyMs
	if err != nil {
		clip.Dropped = true
		clip.DropReason = err.Error()
		clip.Marker = "whisper error"
		return nil
	}
	applyWhisperOutput(clip, out)
	return nil
}

// WhisperOutput is the structured result of one Whisper /inference call.
// Pulled out of TranscribeStage.Process so the transcribe worker pool can
// reuse the exact same HTTP shape.
type WhisperOutput struct {
	RawText      string
	AvgLogprob   float64
	NoSpeechProb float64
	SegmentCount int
	LatencyMs    int64
}

// callWhisper performs one POST /inference and decodes the verbose_json reply.
// On HTTP / decode error returns a "whisper error"-class error suitable for
// surfacing as clip.DropReason.
func callWhisper(ctx context.Context, client *http.Client, whisperURL string, pcm []byte, prompt string) (WhisperOutput, error) {
	wav := pcmToWAV(pcm)

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("file", "audio.wav")
	if err != nil {
		return WhisperOutput{}, err
	}
	if _, err := fw.Write(wav); err != nil {
		return WhisperOutput{}, err
	}
	mw.WriteField("temperature", "0")
	mw.WriteField("response_format", "verbose_json")
	if prompt != "" {
		mw.WriteField("prompt", prompt)
	}
	mw.Close()

	req, err := http.NewRequestWithContext(ctx, "POST", whisperURL+"/inference", &body)
	if err != nil {
		return WhisperOutput{}, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return WhisperOutput{LatencyMs: latency}, fmt.Errorf("whisper request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return WhisperOutput{LatencyMs: latency}, fmt.Errorf("whisper %d: %s", resp.StatusCode, string(b))
	}

	var raw struct {
		Text     string `json:"text"`
		Segments []struct {
			AvgLogprob   float64 `json:"avg_logprob"`
			NoSpeechProb float64 `json:"no_speech_prob"`
			Text         string  `json:"text"`
		} `json:"segments"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return WhisperOutput{LatencyMs: latency}, fmt.Errorf("whisper decode: %v", err)
	}

	out := WhisperOutput{
		RawText:      strings.TrimSpace(raw.Text),
		LatencyMs:    latency,
		SegmentCount: len(raw.Segments),
	}
	if len(raw.Segments) > 0 {
		var sumLP float64
		var maxNSP float64
		for _, seg := range raw.Segments {
			sumLP += seg.AvgLogprob
			if seg.NoSpeechProb > maxNSP {
				maxNSP = seg.NoSpeechProb
			}
		}
		out.AvgLogprob = sumLP / float64(len(raw.Segments))
		out.NoSpeechProb = maxNSP
	}
	return out, nil
}

// applyWhisperOutput copies a WhisperOutput onto an AudioClip in the form
// downstream stages (ClassifyStage etc.) expect.
func applyWhisperOutput(clip *AudioClip, out WhisperOutput) {
	clip.RawText = out.RawText
	if out.SegmentCount > 0 {
		if clip.Meta == nil {
			clip.Meta = make(map[string]any)
		}
		clip.Meta["avg_logprob"] = out.AvgLogprob
		clip.Meta["no_speech_prob"] = out.NoSpeechProb
		clip.Meta["segment_count"] = out.SegmentCount
	}
}

// ---------------------------------------------------------------------------
// ClassifyStage — runs the hallucination / repetition filters on RawText and
// either promotes it to clip.Text (pass) or drops it with Marker="[unclear]".
//
// State: a ring buffer of recent outputs for repetition-loop detection. Guarded
// by a mutex because the stage is shared across utterances.
// ---------------------------------------------------------------------------

// alwaysDropPatterns are checked against ALL clips regardless of duration.
// These are phrases that are never legitimate user speech.
var alwaysDropPatterns = []*regexp.Regexp{
	// Bracket/paren noise tags ([BLANK_AUDIO], (music), etc.)
	regexp.MustCompile(`(?i)^\[.*?\]$`),
	regexp.MustCompile(`(?i)^\(.*?\)$`),
	// Music/sound indicators
	regexp.MustCompile(`(?i)[♪♫🎵🎶]`),
	// YouTube/TV call-to-action phrases
	regexp.MustCompile(`(?i)\b(subscribe|like and share|follow us|click the bell|notification)\b`),
	// Percentage / price fragments (TV ads)
	regexp.MustCompile(`^\d+\s*%`),
	// Copyright / watermark text
	regexp.MustCompile(`(?i)(copyright|©|\(c\)\s*\d{4})`),
	// Lone punctuation or whitespace
	regexp.MustCompile(`^[[:punct:][:space:]]+$`),
	// Broadcast show-break phrases observed in production (any duration)
	regexp.MustCompile(`(?i)^(we'?ll be right back[\.,]?|stay tuned[\.,]?|don'?t go anywhere[\.,]?|see you next time[\.,]?|see you in the next one[\.,]?)(\s+(thank you\.?|thanks\.?))?$`),
	// "Thank you. We'll be right back." combos
	regexp.MustCompile(`(?i)^(thank you[\.,]?\s+)?we'?ll be right back`),
}

// hallucinationPatterns is a compiled set of regexps matched against SHORT
// (< 2s) clips only. Longer clips may legitimately contain these phrases.
var hallucinationPatterns = []*regexp.Regexp{
	// Exact short phrases Whisper emits on silence
	regexp.MustCompile(`(?i)^(thank you\.?|thanks for watching\.?|i'm sorry\.?|hello\.?|hi\.?|bye\.?|you\.|the\.|\.{2,})$`),
}

// continuationWindow is how long after the last accepted utterance a short
// clip is treated as a continuation (merged) rather than noise (dropped).
const (
	continuationWindow         = 15 * time.Second
	extendedContinuationWindow = 45 * time.Second
	continuationRmsFloor       = 120.0 // below noise floor of 150 but above silence
	rmsMatchRatio              = 0.35  // clip RMS must be >= lastAcceptedRMS * this ratio

	// hallucinationRmsHardFloor — RCA 008. Any clip with RMS below this
	// is dropped before any other classification, regardless of word
	// count or duration. This catches the Whisper-on-quiet-audio
	// hallucination flood ("Thank you.", "Okay.", "I'm sorry.") that
	// the existing word-count-gated RMS check missed because those
	// short phrases sometimes transcribe as 3+ words ("Thank you so
	// much.") and slip past the wordCount<3 branch.
	//
	// 80 is well below normal speech RMS (~200-500) and above true
	// silence (typically 5-30 ambient). Override with the
	// HALLUCINATION_RMS_FLOOR env var if your mic has unusual gain.
	hallucinationRmsHardFloor = 80.0
)

type ClassifyStage struct {
	// Context, if set, gets Record(clip.Text) called whenever a clip passes
	// filters. That feeds the prompt buffer used by TranscribeStage on the
	// NEXT clip, giving whisper conversational continuity.
	Context *ContextPrompt

	// MinWords drops utterances with fewer words than this. 1-2 word
	// transcriptions of ambient audio ("yeah", "ok", "hmm") are almost always
	// background noise or whisper hallucination — they carry no signal and
	// flood the downstream summarizer with junk. Default 3 if zero.
	MinWords int

	// SmartTurn, if non-nil, is called for wake-word clips to check whether
	// the utterance is complete. If the model returns complete=false AND
	// probability < 0.4, the clip is dropped with DropReason="smart_turn
	// incomplete". Only applies to the wake-word command path — normal passive
	// recording is never gated by smart turn. Nil = feature disabled (skip).
	SmartTurn *SmartTurnClient

	mu                   sync.Mutex
	recentOutputs        []string
	lastAcceptedAt       time.Time
	lastAcceptedText     string
	lastAcceptedRMS      float64       // RMS of last accepted full clip
	continuationRmsFloor float64       // min RMS to be "speaker" — default 120.0
	extendedWindow       time.Duration // extended merge window for RMS-matched clips — default 45s
}

func (c *ClassifyStage) Name() string { return "classify" }

func (c *ClassifyStage) Process(_ context.Context, clip *AudioClip) error {
	text := clip.RawText
	// Whisper verbose_json puts newlines between segments; collapse to one line.
	trimmed := strings.Join(strings.Fields(text), " ")

	if len(trimmed) < 3 {
		clip.Dropped = true
		clip.DropReason = "empty/too short"
		// No marker — don't pollute the transcript with "nothing happened".
		return nil
	}

	// RCA 008: hard RMS floor — runs BEFORE word-count branching.
	// Whisper hallucinates plausible English on quiet audio. The
	// existing RMS gate only fires for 1-2 word clips. This catches
	// 3+ word hallucinations on quiet audio (the "Thank you so much"
	// / "I'm sorry, what?" floods) by dropping anything below the
	// hard floor regardless of length. No marker — these are noise.
	floor := hallucinationRmsHardFloor
	if env := strings.TrimSpace(os.Getenv("HALLUCINATION_RMS_FLOOR")); env != "" {
		// Best-effort parse; bad values fall back to default.
		var v float64
		if _, err := fmt.Sscanf(env, "%f", &v); err == nil && v >= 0 {
			floor = v
		}
	}
	if clip.RMS > 0 && clip.RMS < floor {
		clip.Dropped = true
		clip.DropReason = fmt.Sprintf("hallucination-likely: rms=%.0f < hard floor %.0f", clip.RMS, floor)
		return nil
	}

	// Always-drop patterns: checked regardless of clip duration. These are
	// phrases that are never legitimate user speech (broadcast phrases, noise
	// tags, music indicators). Also catches bracket-only tokens.
	norm := strings.TrimSpace(trimmed)
	for _, pat := range alwaysDropPatterns {
		if pat.MatchString(norm) {
			clip.Dropped = true
			clip.DropReason = "always-drop pattern: " + pat.String()
			clip.Marker = "unclear"
			return nil
		}
	}
	if isBracketOnly(trimmed) {
		clip.Dropped = true
		clip.DropReason = "bracket noise"
		clip.Marker = "unclear"
		return nil
	}

	// Word-count gate. Short 1-2 word transcriptions of ambient audio are
	// almost always background noise or hallucination — UNLESS they arrive
	// within continuationWindow of the last accepted utterance, in which case
	// they're almost certainly trailing words of the same thought (speaker
	// paused mid-sentence). Merge them into the previous line instead of dropping.
	minWords := c.MinWords
	if minWords <= 0 {
		minWords = 3
	}
	wordCount := len(strings.Fields(trimmed))
	if wordCount < minWords {
		c.mu.Lock()
		prevText := c.lastAcceptedText
		since := time.Since(c.lastAcceptedAt)
		prevRMS := c.lastAcceptedRMS
		rmsFloor := c.continuationRmsFloor
		extWin := c.extendedWindow
		c.mu.Unlock()

		if rmsFloor <= 0 {
			rmsFloor = continuationRmsFloor
		}
		if extWin <= 0 {
			extWin = extendedContinuationWindow
		}
		clipRMS := clip.RMS

		// Gate 1: RMS too low to be speaker — always drop regardless of time.
		if clipRMS < rmsFloor {
			clip.Dropped = true
			clip.DropReason = fmt.Sprintf("noise-level short clip (%d word(s), rms=%.0f < floor=%.0f)", wordCount, clipRMS, rmsFloor)
			return nil
		}

		// Gate 2: within primary continuation window — merge (speaker trailing words).
		if prevText != "" && since < continuationWindow {
			merged := prevText + " " + trimmed
			clip.Text = merged
			clip.IsAmend = true
			c.mu.Lock()
			c.lastAcceptedAt = time.Now()
			c.lastAcceptedText = merged
			// Don't update lastAcceptedRMS on a merge — keep the reference from the full clip.
			c.mu.Unlock()
			if c.Context != nil {
				c.Context.Record(merged)
			}
			return nil
		}

		// Gate 3: extended window — merge only if RMS fingerprint matches speaker.
		if prevText != "" && prevRMS > 0 && clipRMS >= prevRMS*rmsMatchRatio && since < extWin {
			merged := prevText + " " + trimmed
			clip.Text = merged
			clip.IsAmend = true
			c.mu.Lock()
			c.lastAcceptedAt = time.Now()
			c.lastAcceptedText = merged
			c.mu.Unlock()
			if c.Context != nil {
				c.Context.Record(merged)
			}
			return nil
		}

		clip.Dropped = true
		clip.DropReason = fmt.Sprintf("short clip not attributed to speaker (%d word(s), rms=%.0f, since=%.0fs)", wordCount, clipRMS, since.Seconds())
		return nil
	}

	// For very short clips only: match against short-clip hallucination patterns.
	// Longer clips may legitimately say "thank you" or "hello".
	if clip.Duration < 2*time.Second {
		for _, pat := range hallucinationPatterns {
			if pat.MatchString(norm) {
				clip.Dropped = true
				clip.DropReason = "hallucination pattern: " + pat.String()
				clip.Marker = "unclear"
				return nil
			}
		}
	}

	// Intra-clip repetition: Whisper sometimes fills a clip with the same
	// short phrase repeated many times ("I don't know. I don't know. ×18").
	// This is a single clip, so inter-clip detection misses it.
	lower := strings.ToLower(trimmed)
	if isIntraClipRepetition(lower) {
		clip.Dropped = true
		clip.DropReason = "intra-clip repetition loop"
		clip.Marker = "unclear"
		return nil
	}

	// Inter-clip repetition loop detection — Whisper gets stuck emitting the
	// same phrase across successive clips. Three identical outputs in the last
	// four clips = stuck; drop this clip. Do NOT wipe recentOutputs — keeping
	// the window prevents the phrase from immediately re-entering on the next
	// clip (the original nil-reset caused the loop to break through every 3rd
	// occurrence).
	if c.isRepetition(lower) {
		clip.Dropped = true
		clip.DropReason = "repetition loop"
		clip.Marker = "unclear"
		return nil
	}

	// Low-confidence gate — use per-segment logprob/no_speech_prob stashed by
	// TranscribeStage. Missing = verbose_json not available; skip the check.
	if clip.Meta != nil {
		avgLP, hasLP := clip.Meta["avg_logprob"].(float64)
		noSP, hasNSP := clip.Meta["no_speech_prob"].(float64)
		if hasLP && hasNSP {
			if avgLP < clarityAvgLogprobThresh || noSP > clarityNoSpeechProbThresh {
				clip.Dropped = true
				clip.DropReason = fmt.Sprintf("low confidence (logprob=%.2f, nospeech=%.2f)", avgLP, noSP)
				clip.Marker = "unclear"
				return nil
			}
		}
	}

	// Smart-turn gate — wake-word command path only. When a SmartTurnClient
	// is configured, ask the ML model whether the speaker has finished their
	// utterance. If the model says incomplete AND the probability is below
	// 0.4 (i.e. it's fairly confident the speaker isn't done), drop the clip
	// so the pipeline can wait for more audio.
	//
	// Only runs on wake-word clips — passive recording is never gated here.
	// The client is fail-open: any sidecar error returns complete=true.
	if c.SmartTurn != nil && clip.WakeWordDetected && clip.AudioPath != "" && clip.AudioPath != "unknown" {
		complete, prob := c.SmartTurn.IsComplete(context.Background(), clip.AudioPath)
		if !complete && prob < 0.4 {
			clip.Dropped = true
			clip.DropReason = fmt.Sprintf("smart_turn incomplete utterance prob=%.2f", prob)
			log.Printf("[smart_turn] incomplete utterance prob=%.2f — dropping wake-word clip", prob)
			return nil
		}
	}

	// Force-close stitch: if the VAD hard cap fired on the previous clip (or
	// this one), the speaker may have been mid-sentence. When the previous
	// accepted text is an open sentence (no terminal punctuation) and this
	// clip arrived within forceCloseStitchWindow, merge them into one line
	// so the transcript reads as a single coherent utterance.
	if clip.ForceClose {
		c.mu.Lock()
		prevText := c.lastAcceptedText
		since := time.Since(c.lastAcceptedAt)
		c.mu.Unlock()
		const forceCloseStitchWindow = 15 * time.Second
		if prevText != "" && since < forceCloseStitchWindow && !endsWithSentencePunct(prevText) {
			merged := prevText + " " + trimmed
			clip.Text = merged
			clip.IsAmend = true
			c.trackRecent(merged)
			c.mu.Lock()
			c.lastAcceptedAt = time.Now()
			c.lastAcceptedText = merged
			c.lastAcceptedRMS = clip.RMS
			c.mu.Unlock()
			if c.Context != nil {
				c.Context.Record(merged)
			}
			return nil
		}
	}

	clip.Text = trimmed
	c.trackRecent(trimmed)
	c.mu.Lock()
	c.lastAcceptedAt = time.Now()
	c.lastAcceptedText = trimmed
	c.lastAcceptedRMS = clip.RMS
	c.mu.Unlock()
	if c.Context != nil {
		c.Context.Record(trimmed)
	}
	return nil
}

// endsWithSentencePunct returns true if s ends with a terminal punctuation
// mark (. ? !). Used to decide whether a force-closed chunk is an open
// sentence that should be stitched to the next chunk.
func endsWithSentencePunct(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return true
	}
	last := s[len(s)-1]
	return last == '.' || last == '?' || last == '!' || last == '"'
}

func (c *ClassifyStage) isRepetition(normalized string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.recentOutputs) < repetitionThresh {
		return false
	}
	last := c.recentOutputs
	if len(last) > repetitionWindow {
		last = last[len(last)-repetitionWindow:]
	}
	matches := 0
	for _, prev := range last {
		if strings.ToLower(strings.TrimSpace(prev)) == normalized {
			matches++
		}
	}
	return matches >= repetitionThresh
}

var sentenceBreakRe = regexp.MustCompile(`[.!?]+\s*`)

// isIntraClipRepetition detects clips where the same short phrase repeats 3+
// times within a single transcription — e.g. "I don't know. I don't know. ×18"
// or "This is a production of the U.S. Department of State. So, this is a..."
// Whisper hallucinates these on looping background audio (TV left on in room).
func isIntraClipRepetition(text string) bool {
	parts := sentenceBreakRe.Split(text, -1)
	if len(parts) < 4 {
		return false
	}
	counts := make(map[string]int, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		counts[p]++
		if counts[p] >= 3 {
			return true
		}
	}
	return false
}

func (c *ClassifyStage) trackRecent(text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recentOutputs = append(c.recentOutputs, text)
	if len(c.recentOutputs) > repetitionWindow*2 {
		c.recentOutputs = c.recentOutputs[len(c.recentOutputs)-repetitionWindow:]
	}
}

// ---------------------------------------------------------------------------
// Planned stages (not yet implemented) — here as a roadmap so the next person
// touching this pipeline knows where they belong:
//
//   NoiseSuppressStage     slot: before TranscribeStage
//                          RNNoise or Facebook Denoiser over clip.PCM.
//                          Sets clip.Meta["noise_suppressed"] = true.
//
//   SpeakerFilterStage     slot: before TranscribeStage
//                          Compute speaker embedding over clip.PCM, cosine-
//                          compare against the enrolled user voice profile.
//                          On low similarity, mark Dropped with Marker="" so
//                          other speakers / TV audio disappear silently.
//                          Sets clip.Meta["speaker_similarity"] = float64.
//
//   ContextBatchStage      slot: before TranscribeStage
//                          Whisper was trained on 30s windows. If the current
//                          clip is <5s, concat it with the prior 1-2 clips
//                          (and send only the newest portion's text through).
// ---------------------------------------------------------------------------
