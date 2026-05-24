package pipeline

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"hermes-listener/internal/models"
	"hermes-listener/internal/storage"
)

// VAD tuning
const (
	frameBytes        = 100 * 16000 / 1000 * 2 // 100ms frame at 16kHz s16le
	silenceRMS        = 110                      // noise floor ~40, background noise ~80-96, quiet brainstorm speech ~120-145, projected speech ~160+
	silenceFrameCount = 8                        // ~800ms silence = end of utterance
	minSpeechFrames   = 3                        // ~300ms minimum utterance
	// RCA 012: raised from 100 (~10s) to 300 (~30s). The original cap
	// existed because the speaker-filter sidecar processed clips
	// serially and longer clips meant longer queue. With the speaker
	// filter currently disabled (RCA 003/004) and the new RMS gate
	// (RCA 011) doing the speaker-discrimination work on whole clips,
	// the latency argument no longer applies. The cost was real: long
	// questions ("computer what is the capital of …") got cut at 10s,
	// trailing "..." in the live transcript, and live_qa_worker never
	// detected question form because the audio ended mid-sentence.
	maxSpeechFrames = 300 // ~30s max before forcing transcription
	repetitionWindow  = 4                        // check last N outputs for loops
	repetitionThresh  = 3                        // N-of-last-N identical = loop

	streamSampleRate    = 16000
	streamChannels      = 1
	streamBitsPerSample = 16
)

// Known whisper hallucinations for silence/noise — checked case-insensitively.
var knownHallucinations = []string{
	"[music]", "(music)", "[music.]",
	"[silence]", "(silence)",
	"[background noise]", "[noise]", "[blank_audio]",
	"thank you.", "thanks for watching.",
	"i'm sorry.", "i'm sorry", "sorry.", "sorry",
	"hello.", "hello", "hi.", "hi",
	"bye.", "bye", "bye bye.",
	"you.", "the.", "...", "..", ".",
	"(applause)", "[applause]",
}

type StreamerStatus struct {
	Running         bool   `json:"running"`
	Device          string `json:"device"`
	DurationSeconds int    `json:"duration_seconds"`
	UtterancesCount int64  `json:"utterances_count"`
	FilteredCount   int64  `json:"filtered_count"`
	ErrorCount      int64  `json:"error_count"`
}

// Streamer reads raw PCM from ffmpeg, applies energy-based VAD to find
// complete utterances, and runs each utterance through the per-clip pipeline
// defined in audio_stages.go.
//
// The Streamer itself owns only the capture loop and the VAD. Everything
// that happens AFTER VAD closes an utterance (save, transcribe, filter,
// emit) lives in AudioStage implementations. Add new audio-processing
// features by writing a stage, not by modifying the Streamer.
type Streamer struct {
	whisperURL string
	audioDir   string
	hub        *Hub
	transcript *storage.DailyTranscript

	stages     []AudioStage   // per-utterance pipeline; see audio_stages.go
	promptCtx  *ContextPrompt // shared whisper-prompt buffer used by Transcribe + Classify stages

	mu        sync.Mutex
	running   bool
	device    string
	startedAt time.Time

	utterances atomic.Int64
	filtered   atomic.Int64
	errors     atomic.Int64

	stopCh chan struct{}
	cmd    *exec.Cmd
}

func NewStreamer(whisperURL, audioDir string, hub *Hub, transcript *storage.DailyTranscript) *Streamer {
	s := &Streamer{
		whisperURL: whisperURL,
		audioDir:   audioDir,
		hub:        hub,
		transcript: transcript,
	}
	s.stages = s.buildPipeline()
	return s
}

// buildPipeline defines the stages each utterance flows through, in order.
// Add new stages (NoiseSuppressStage, SpeakerFilterStage, …) by inserting
// them at the appropriate slot here — stages are otherwise self-contained.
func (s *Streamer) buildPipeline() []AudioStage {
	// One shared prompt buffer wired into both Transcribe (reads) and Classify
	// (writes). See context_prompt.go for the why.
	s.promptCtx = &ContextPrompt{MaxRecent: 2}
	s.promptCtx.SetHints(loadVocabHints())
	return []AudioStage{
		&SaveAudioStage{AudioDir: s.audioDir},
		&NoiseSuppressStage{
			ModelPath: "data/models/cb.rnnn",
			Hub:       s.hub,
		},
		NewVADFilterStage(speakerSidecarURL(), 0), // 0 → uses defaultVADThreshold (0.45)
		// SpeakerFilterStage omitted — disabled by default (see audio_channel.go).
		// Enable with SPEAKER_FILTER_ENABLED=true.
		&TranscribeStage{
			WhisperURL: s.whisperURL,
			Client:     whisperClient,
			Context:    s.promptCtx,
		},
		&ClassifyStage{
			Context:  s.promptCtx,
			MinWords: 3, // drop "yeah"/"ok"/"hmm" noise — background audio artifacts
		},
	}
}

// loadVocabHints returns the user's custom vocabulary list. Sources, in order:
//  1. NOGURA_VOCAB env var (comma-separated) — quick override for dev
//  2. data/vocab.txt, one phrase per line (# comments allowed) — persistent
//  3. a small built-in default covering Nōgura's own stack so first-run
//     transcripts don't butcher the project's own name.
//
// Kept here (not in a config package) because it's a single feature's data
// source — if the list grows complex, promote it.
func loadVocabHints() []string {
	if v := os.Getenv("NOGURA_VOCAB"); v != "" {
		var out []string
		for _, part := range strings.Split(v, ",") {
			if p := strings.TrimSpace(part); p != "" {
				out = append(out, p)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	if data, err := os.ReadFile("data/vocab.txt"); err == nil {
		var out []string
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			out = append(out, line)
		}
		if len(out) > 0 {
			return out
		}
	}
	return []string{"Nōgura", "Nogura", "Claude", "Anthropic", "Tailscale", "whisper.cpp", "Obsidian"}
}

func (s *Streamer) Start(device string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return fmt.Errorf("already streaming")
	}
	s.device = device
	s.startedAt = time.Now()
	s.running = true
	s.stopCh = make(chan struct{})
	s.utterances.Store(0)
	s.filtered.Store(0)
	s.errors.Store(0)
	go s.run()
	s.log("info", fmt.Sprintf("[streamer] started on %q — VAD mode, saving audio to %s", device, s.audioDir))
	return nil
}

func (s *Streamer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return fmt.Errorf("not streaming")
	}
	close(s.stopCh)
	if s.cmd != nil && s.cmd.Process != nil {
		s.cmd.Process.Signal(os.Interrupt)
	}
	s.running = false
	// Clear the whisper-prompt context so the next Start() doesn't inherit
	// conversational context from an unrelated prior recording.
	if s.promptCtx != nil {
		s.promptCtx.Reset()
	}
	s.log("info", fmt.Sprintf("[streamer] stopped — utterances:%d filtered:%d errors:%d",
		s.utterances.Load(), s.filtered.Load(), s.errors.Load()))
	return nil
}

func (s *Streamer) Status() StreamerStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	var dur int
	if s.running {
		dur = int(time.Since(s.startedAt).Seconds())
	}
	return StreamerStatus{
		Running:         s.running,
		Device:          s.device,
		DurationSeconds: dur,
		UtterancesCount: s.utterances.Load(),
		FilteredCount:   s.filtered.Load(),
		ErrorCount:      s.errors.Load(),
	}
}

func (s *Streamer) run() {
	cmd := exec.Command("ffmpeg",
		"-y", "-f", "pulse", "-i", s.device,
		"-ar", "16000", "-ac", "1", "-f", "s16le", "-",
	)
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		s.log("error", fmt.Sprintf("[streamer] pipe: %v", err))
		s.mu.Lock(); s.running = false; s.mu.Unlock()
		return
	}
	s.mu.Lock(); s.cmd = cmd; s.mu.Unlock()

	if err := cmd.Start(); err != nil {
		s.log("error", fmt.Sprintf("[streamer] ffmpeg start: %v", err))
		s.mu.Lock(); s.running = false; s.mu.Unlock()
		return
	}
	defer func() {
		cmd.Wait()
		// Mark stopped if ffmpeg died unexpectedly (not via Stop()).
		s.mu.Lock()
		wasRunning := s.running
		s.running = false
		s.mu.Unlock()
		if wasRunning {
			s.log("error", "[streamer] ffmpeg exited unexpectedly — streaming stopped")
		}
	}()

	frame := make([]byte, frameBytes)
	var speech []byte
	var speechFrames, silentFrames int
	var inSpeech bool
	var frameCount int

	// Pre-roll ring buffer: keep the last preRollFrames of audio before the
	// speech onset so that the first word ("Computer") is never clipped even
	// if it starts below the RMS threshold.
	const preRollFrames = 4 // 4 × 100ms = 400ms of pre-roll
	preRoll := make([][]byte, preRollFrames)
	preRollIdx := 0
	preRollFull := false

	addPreRoll := func(f []byte) {
		cp := make([]byte, len(f))
		copy(cp, f)
		preRoll[preRollIdx] = cp
		preRollIdx = (preRollIdx + 1) % preRollFrames
		if preRollIdx == 0 {
			preRollFull = true
		}
	}
	flushPreRoll := func() []byte {
		if !preRollFull && preRollIdx == 0 {
			return nil
		}
		var buf []byte
		start := 0
		count := preRollIdx
		if preRollFull {
			start = preRollIdx
			count = preRollFrames
		}
		for i := 0; i < count; i++ {
			buf = append(buf, preRoll[(start+i)%preRollFrames]...)
		}
		return buf
	}

	dispatch := func(pcm []byte, forceClose bool) {
		// Copy before goroutine — VAD loop resets speech immediately after.
		buf := make([]byte, len(pcm))
		copy(buf, pcm)
		go s.process(buf, forceClose)
	}

	for {
		select {
		case <-s.stopCh:
			if speechFrames >= minSpeechFrames {
				dispatch(speech, false)
			}
			return
		default:
		}

		if _, err := io.ReadFull(stdout, frame); err != nil {
			if speechFrames >= minSpeechFrames {
				dispatch(speech, false)
			}
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				s.log("error", fmt.Sprintf("[streamer] ffmpeg read: %v", err))
			}
			return
		}

		rms := calcRMS(frame)
		frameCount++

		// Log RMS level every 50 frames (~5s) so we can see if the mic is actually picking up audio.
		if frameCount%50 == 0 {
			s.log("info", fmt.Sprintf("[vad] rms=%.0f threshold=%d speech=%v frames=%d",
				rms, silenceRMS, inSpeech, speechFrames))
		}

		if rms > silenceRMS {
			if !inSpeech {
				inSpeech = true
				// Prepend buffered pre-roll so leading audio (e.g. "Computer") isn't lost.
				if pre := flushPreRoll(); len(pre) > 0 {
					speech = append(pre, speech...)
					speechFrames += len(pre) / frameBytes
				}
				s.log("info", fmt.Sprintf("[vad] speech detected rms=%.0f", rms))
			}
			silentFrames = 0
			speechFrames++
			speech = append(speech, frame...)
			// Hard cap: if signal never dips below threshold, force a flush.
			if speechFrames >= maxSpeechFrames {
				s.log("info", fmt.Sprintf("[vad] max-length reached (%d frames), forcing utterance", speechFrames))
				dispatch(speech, true) // forceClose=true so ClassifyStage can stitch continuations
				speech = nil
				speechFrames = 0
				silentFrames = 0
				inSpeech = false
			}
		} else if speechFrames > 0 {
			silentFrames++
			speech = append(speech, frame...) // include trailing silence for natural word endings

			endOfUtterance := silentFrames >= silenceFrameCount
			tooLong := (speechFrames + silentFrames) >= maxSpeechFrames

			if endOfUtterance || tooLong {
				reason := "silence"
				if tooLong {
					reason = "max-length"
				}
				if speechFrames >= minSpeechFrames {
					s.log("info", fmt.Sprintf("[vad] utterance closed (%s) speech=%d silent=%d frames",
						reason, speechFrames, silentFrames))
					dispatch(speech, tooLong) // tooLong chunks are also force-closes
				} else {
					s.log("info", fmt.Sprintf("[vad] utterance too short (%d frames < %d min), skipped", speechFrames, minSpeechFrames))
				}
				speech = nil
				speechFrames = 0
				silentFrames = 0
				inSpeech = false
				// Reset pre-roll ring so old audio doesn't bleed into the next utterance.
				preRollIdx = 0
				preRollFull = false
			}
		} else {
			// Below threshold, not in speech — feed the pre-roll buffer.
			addPreRoll(frame)
		}
	}
}

// process runs one VAD-closed utterance through the stage pipeline and emits
// the result (or an [unclear] marker) to the transcript + SSE.
//
// All real work happens inside AudioStage implementations (audio_stages.go).
// This function is responsible only for wiring inputs, logging, metrics, and
// deciding what to emit based on the clip's post-pipeline state.
func (s *Streamer) process(pcm []byte, forceClose bool) {
	clip := &AudioClip{
		PCM:        pcm,
		CapturedAt: time.Now(),
		Duration:   time.Duration(float64(len(pcm)) / float64(streamSampleRate*streamChannels*(streamBitsPerSample/8)) * float64(time.Second)),
		RMS:        calcRMS(pcm),
		Meta:       map[string]any{},
		ForceClose: forceClose,
	}
	s.log("info", fmt.Sprintf("[streamer] processing %.1fs audio avgRMS=%.0f", clip.Duration.Seconds(), clip.RMS))

	ctx := context.Background()
	if err := runStages(ctx, s.stages, clip); err != nil {
		s.errors.Add(1)
		s.log("error", fmt.Sprintf("[streamer] pipeline error: %v", err))
		return
	}

	// Every clip that produced a RawText gets logged so the activity feed shows
	// what whisper heard, even for filtered clips.
	if clip.RawText != "" {
		s.log("info", fmt.Sprintf("[whisper] %.1fs → %q (%dms)", clip.Duration.Seconds(), truncate(clip.RawText, 80), clip.WhisperMs))
	}

	switch {
	case clip.Dropped && clip.Marker != "":
		// Filtered by a stage — write a placeholder marker so the gap is visible.
		s.filtered.Add(1)
		s.log("warn", fmt.Sprintf("[streamer] filtered (%s): %q → writing %s marker", clip.DropReason, clip.RawText, clip.Marker))
		s.writeMarker(clip, clip.Marker)

	case clip.Dropped:
		// Silently dropped (e.g. empty result, or speaker-filter reject).
		s.log("info", fmt.Sprintf("[streamer] dropped (%s) — no marker written", clip.DropReason))

	case clip.IsAmend && clip.Text != "":
		// Short continuation merged into the previous line — broadcast an amend
		// event so SessionDetector replaces the last buffer line (not appends).
		// We skip DailyTranscript here; the session file via SessionDetector is
		// the canonical record and will have the merged text when the session closes.
		s.utterances.Add(1)
		s.log("info", fmt.Sprintf("[streamer] amending last line with continuation: %q", clip.Text))
		s.hub.Broadcast(models.Event{
			Type: "transcript_amend",
			Payload: map[string]string{
				"time":      clip.CapturedAt.Format("15:04:05"),
				"text":      clip.Text,
				"audio_ref": clip.AudioPath,
			},
		})

	case clip.Text != "":
		s.utterances.Add(1)
		_ = s.transcript.Append(clip.Text, clip.CapturedAt)
		s.hub.Broadcast(models.Event{
			Type: "transcript_append",
			Payload: map[string]string{
				"time":      clip.CapturedAt.Format("15:04:05"),
				"text":      clip.Text,
				"audio_ref": clip.AudioPath,
			},
		})
	}
}

// writeMarker emits a `[~Xs, unclear | audio:ref]` placeholder line to the
// transcript and SSE hub. Used when a stage drops a clip but wants the
// transcript reader to see that something was filtered (so they can replay
// the saved audio file if curious).
func (s *Streamer) writeMarker(clip *AudioClip, label string) {
	text := fmt.Sprintf("[~%.0fs, %s | audio:%s]", clip.Duration.Seconds(), label, filepath.Base(clip.AudioPath))
	_ = s.transcript.Append(text, clip.CapturedAt)
	s.hub.Broadcast(models.Event{
		Type: "transcript_append",
		Payload: map[string]string{
			"time":      clip.CapturedAt.Format("15:04:05"),
			"text":      text,
			"marker":    "true",
			"audio_ref": clip.AudioPath,
		},
	})
}

// calcRMS returns the RMS amplitude of a s16le frame.
func calcRMS(frame []byte) float64 {
	n := len(frame) / 2
	if n == 0 {
		return 0
	}
	var sum float64
	for i := 0; i < len(frame)-1; i += 2 {
		s := int16(binary.LittleEndian.Uint16(frame[i:]))
		sum += float64(s) * float64(s)
	}
	return math.Sqrt(sum / float64(n))
}

// whisperClient is the shared HTTP client used by both TranscribeStage and
// the WhisperWatchdog. CPU mode (large-v3-turbo, 4 threads) takes ~2-3s per
// second of audio, so a 30s clip needs up to 90s. GPU was ~0.5s/clip total.
var whisperClient = &http.Client{Timeout: 120 * time.Second}

// speakerSidecarURL returns the speaker-sidecar base URL. Overridable via env
// so dev/prod/docker can point at a different host without a code change.
func speakerSidecarURL() string {
	if v := os.Getenv("SPEAKER_SIDECAR_URL"); v != "" {
		return v
	}
	return "http://127.0.0.1:9200"
}

// pcmToWAV wraps 16kHz mono s16le PCM in a WAV container.
func pcmToWAV(pcm []byte) []byte {
	dataSize := uint32(len(pcm))
	byteRate := uint32(streamSampleRate * streamChannels * streamBitsPerSample / 8)
	blockAlign := uint16(streamChannels * streamBitsPerSample / 8)

	var buf bytes.Buffer
	buf.WriteString("RIFF")
	binary.Write(&buf, binary.LittleEndian, 36+dataSize)
	buf.WriteString("WAVE")
	buf.WriteString("fmt ")
	binary.Write(&buf, binary.LittleEndian, uint32(16))
	binary.Write(&buf, binary.LittleEndian, uint16(1))
	binary.Write(&buf, binary.LittleEndian, uint16(streamChannels))
	binary.Write(&buf, binary.LittleEndian, uint32(streamSampleRate))
	binary.Write(&buf, binary.LittleEndian, byteRate)
	binary.Write(&buf, binary.LittleEndian, blockAlign)
	binary.Write(&buf, binary.LittleEndian, uint16(streamBitsPerSample))
	buf.WriteString("data")
	binary.Write(&buf, binary.LittleEndian, dataSize)
	buf.Write(pcm)
	return buf.Bytes()
}

// isBracketOnly returns true if every non-empty line of text is a bracket or
// parenthesis tag (e.g. "[BLANK_AUDIO]", "(music)", "[NOISE]").
func isBracketOnly(text string) bool {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "[") && !strings.HasPrefix(line, "(") {
			return false
		}
	}
	return true
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func (s *Streamer) log(level, message string) {
	log.Printf("[%s] %s", level, message)
	s.hub.Broadcast(models.Event{
		Type:    "log",
		Payload: models.LogEntry{Level: level, Message: message},
	})
}
