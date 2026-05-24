package pipeline

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"hermes-listener/internal/models"
	"hermes-listener/internal/pipeline/transcribepool"
	"hermes-listener/internal/storage"
)

// ChannelType identifies the audio source type.
type ChannelType string

const (
	ChannelTypeMic       ChannelType = "mic"
	ChannelTypeRTSP      ChannelType = "rtsp"
	ChannelTypeEdge      ChannelType = "edge"      // Android/remote device via /ws/edge
	ChannelTypeWebSocket ChannelType = "websocket" // reserved, not yet implemented
)

const (
	maxRTSPBackoffSec = 300 // 5 minutes max between reconnect attempts
)

// nextRTSPBackoff doubles the current backoff, capped at maxRTSPBackoffSec.
// Starts at 2s on first call (pass 0).
func nextRTSPBackoff(current int) int {
	if current == 0 {
		return 2
	}
	next := current * 2
	if next > maxRTSPBackoffSec {
		return maxRTSPBackoffSec
	}
	return next
}

// ChannelConfig holds type-specific configuration for an AudioChannel.
type ChannelConfig struct {
	Device         string `json:"device,omitempty"`           // mic: pulse device name
	URL            string `json:"url,omitempty"`              // rtsp: stream URL
	EnableTVFilter bool   `json:"enable_tv_filter,omitempty"` // mic: drop clips matching current TV captions
	PlexPlayerName string `json:"plex_player_name,omitempty"` // mic: optional plex player to query for captions
}

// ChannelStatus is the JSON-serialisable view of a running/stopped channel.
type ChannelStatus struct {
	ID              string      `json:"id"`
	Name            string      `json:"name"`
	Type            ChannelType `json:"type"`
	Config          ChannelConfig `json:"config"`
	Running         bool        `json:"running"`
	DurationSeconds int         `json:"duration_seconds"`
	UtterancesCount int64       `json:"utterances_count"`
	FilteredCount   int64       `json:"filtered_count"`
	ErrorCount      int64       `json:"error_count"`
	LastError       string      `json:"last_error,omitempty"`

	// SpeakerDroppedCount + VADClosedCount let the smoke test compute
	// "what fraction of detected speech was silently dropped by the
	// speaker filter". Without these counters the test had to parse
	// logs, which doesn't work because ffmpeg progress lines overlap
	// real log lines on the same buffer line. See
	// docs/every-time-i-fucked-up/004-...md.
	SpeakerDroppedCount int64 `json:"speaker_dropped_count"`
	VADClosedCount      int64 `json:"vad_closed_count"`

	// RMSGateDroppedCount + SpeakerBaseline expose the upstream
	// hallucination gate (RCA 011). Visible on /api/stream/status so
	// the user can see "dropped 47 silent clips before Whisper ran".
	RMSGateDroppedCount int64                    `json:"rms_gate_dropped_count"`
	SpeakerBaseline     *SpeakerBaselineSnapshot `json:"speaker_baseline,omitempty"`
}

// AudioChannel is a self-contained audio input source. It captures PCM (via
// ffmpeg for mic/RTSP, or directly for WebSocket), applies energy-based VAD,
// and runs each utterance through the shared stage pipeline. Every hub event
// emitted by this channel includes "channel_id" in the payload.
//
// RTSP channels automatically reconnect with exponential backoff (2s→5min).
// Speaker filtering is enabled for mic channels only.
type AudioChannel struct {
	ID     string
	Name   string
	Type   ChannelType
	Config ChannelConfig

	// injected at construction
	whisperURL string
	audioDir   string
	hub        *Hub
	transcript *storage.DailyTranscript
	store      *storage.Store
	vaultDir   string
	dataDir    string

	// TV chatter filter — populated by ChannelManager when the channel is a
	// mic with EnableTVFilter=true. Empty plexDashboardURL or non-mic types
	// disable the stage.
	plexDashboardURL    string
	tvFilterThreshold   float64
	tvFilterParentCtx   context.Context // root ctx for the captions client poll loop

	// mediaSignalThreshold is the minimum media_confidence score a clip must
	// reach before it is emitted without the [~media?] annotation. Populated
	// by ChannelManager from cfg.MediaSignalThreshold (default 0.6).
	mediaSignalThreshold float64

	// inlineExtractor is the optional InlineExtractorStage wired after
	// MediaSignalStage in the post-transcribe pipeline. Set by ChannelManager
	// when INLINE_EXTRACTOR_ENABLED=true. nil means the stage is not used.
	inlineExtractor *InlineExtractorStage

	// per-channel pipeline. The pre-transcribe stages run on the VAD goroutine;
	// transcription is offloaded to the shared TranscribePool; post-transcribe
	// stages run when the pool delivers a result. Splitting at the Whisper boundary
	// is what keeps a slow Whisper from backing audio capture up.
	preStages  []AudioStage
	postStages []AudioStage
	promptCtx  *ContextPrompt
	detector   *SessionDetector

	// pool is the shared transcription worker pool. May be nil (e.g. tests
	// that don't exercise transcription); in that case clips that pass the
	// pre-stages are simply not transcribed.
	pool *transcribepool.Pool

	// nextUttSeq generates per-channel unique utterance IDs for pool submission.
	nextUttSeq atomic.Uint64

	// transcribePromptCtx is the prompt buffer fed to the pool's transcriber so
	// the channel keeps conversational context across utterances even though
	// transcription no longer runs in-line.
	transcribePromptCtx *ContextPrompt

	// RCA 011: shared with RMSGateStage for floor lookup, and with
	// dispatch() so we record accepted-clip RMS values for adaptive
	// calibration. Owned by the channel; one instance per channel
	// because each channel has its own mic position / acoustic
	// fingerprint.
	baseline *SpeakerBaseline
	rmsGate  *RMSGateStage

	// smartTurnClient, if non-nil, is passed into ClassifyStage so that
	// wake-word clips are scored for turn completeness before being
	// finalised. Nil = smart-turn disabled (pipeline behaves as before).
	smartTurnClient *SmartTurnClient

	// lifecycle
	mu          sync.Mutex
	running     bool
	startedAt   time.Time
	stopCh      chan struct{}
	cmd         *exec.Cmd
	lastError   string
	dispatching atomic.Bool // cleared in Stop() to gate post-stop transcription bleed

	// stats
	utterances atomic.Int64
	filtered   atomic.Int64
	errors     atomic.Int64

	// speakerDropped counts utterances rejected by SpeakerFilterStage
	// (no marker written). The pre-existing `filtered` counter only
	// covers drops that DO write a marker, which made silent muting
	// invisible to /api/stream/status. RCA 003/004 was the cost of
	// that gap. Exposed as `speaker_dropped_count` in ChannelStatus.
	speakerDropped atomic.Int64
	// vadClosed counts utterance-closed events (the denominator we
	// want when computing drop rate). Without this the smoke test
	// has to parse logs, which is fragile because ffmpeg's progress
	// output overlaps real log lines.
	vadClosed atomic.Int64
}

// ffmpegArgs returns the ffmpeg command arguments appropriate for this channel type.
func (ch *AudioChannel) ffmpegArgs() []string {
	base := []string{"-y"}
	switch ch.Type {
	case ChannelTypeRTSP:
		return append(base,
			"-rtsp_transport", "tcp",
			"-i", ch.Config.URL,
			"-vn", // drop video
			"-ar", "16000", "-ac", "1", "-f", "s16le", "-",
		)
	default: // mic
		device := ch.Config.Device
		if device == "" {
			device = "default"
		}
		return append(base,
			"-f", "pulse", "-i", device,
			"-ar", "16000", "-ac", "1", "-f", "s16le", "-",
		)
	}
}

// buildPipeline constructs the ordered stage slices for this channel,
// returning (pre, post) — pre runs synchronously on the VAD goroutine, post
// runs after the TranscribePool returns a Whisper result. The split point
// (formerly TranscribeStage) is what makes a slow Whisper not back up audio.
//
// Edge channels get the RTSP-equivalent pipeline. RTSP channels skip
// SpeakerFilterStage — all audio from the source is trusted.
func (ch *AudioChannel) buildPipeline() ([]AudioStage, []AudioStage) {
	ch.promptCtx = &ContextPrompt{MaxRecent: 2}
	ch.promptCtx.SetHints(loadVocabHints())
	ch.transcribePromptCtx = ch.promptCtx

	// RCA 011: per-channel adaptive RMS baseline persisted to
	// data/audio-baseline-<channel-id>.json so calibration survives
	// restart. Mic channels get the gate; RTSP/edge channels don't
	// (their audio source is already trusted, no need to gate).
	if ch.Type == ChannelTypeMic && ch.baseline == nil {
		baselinePath := ""
		if ch.dataDir != "" {
			baselinePath = ch.dataDir + "/audio-baseline-" + ch.ID + ".json"
		}
		ch.baseline = NewSpeakerBaseline(DefaultSpeakerBaselineConfig(baselinePath))
		ch.rmsGate = &RMSGateStage{Baseline: ch.baseline}
	}

	stages := []AudioStage{
		&SaveAudioStage{AudioDir: ch.audioDir},
		// Wake-word detection runs early so downstream gates can fast-track
		// clips that contain the wake phrase. Fail-OPEN: if the sidecar is
		// down, this stage is a no-op and the clip falls through normally.
		&WakeWordStage{Hub: ch.hub},
		&NoiseSuppressStage{
			ModelPath: "data/models/cb.rnnn",
			Hub:       ch.hub,
		},
	}

	// DiarizeFilterStage (RCA 041) replaces the whole-clip VAD + speaker gate
	// with per-segment identity scoring. Opt-in, mic channels only:
	//   DIARIZE_FILTER_ENABLED=true → it enforces, standing in for BOTH
	//                                 VADFilterStage and SpeakerFilterStage.
	//   DIARIZE_FILTER_SHADOW=true  → it runs observe-only alongside the live
	//                                 VAD + speaker gate (logs the verdict it
	//                                 would reach, never drops). For
	//                                 validating before enforcing.
	diarizeEnforce := ch.Type == ChannelTypeMic && os.Getenv("DIARIZE_FILTER_ENABLED") == "true"
	diarizeShadow := ch.Type == ChannelTypeMic && os.Getenv("DIARIZE_FILTER_SHADOW") == "true"

	if diarizeEnforce {
		stages = append(stages, NewDiarizeFilterStage(speakerSidecarURL(), ch.hub, false))
	} else {
		stages = append(stages, NewVADFilterStage(speakerSidecarURL(), envFloat("VAD_FILTER_THRESHOLD", 0)))
	}

	if ch.Type == ChannelTypeMic {
		// Shadow-mode diarize: observe-only, runs next to the live gate.
		if diarizeShadow && !diarizeEnforce {
			stages = append(stages, NewDiarizeFilterStage(speakerSidecarURL(), ch.hub, true))
		}
		// Speaker filter uses voice-embedding similarity to reject TV audio and
		// strangers' voices while keeping the user's own voice — even when
		// whispering or talking softly. Disable entirely with
		// SPEAKER_FILTER_ENABLED=false; tune threshold via
		// SPEAKER_FILTER_THRESHOLD=<float>. Skipped entirely when the diarize
		// filter is enforcing — diarize already does per-segment identity.
		// Shadow mode: when SPEAKER_FILTER_SHADOW=true the stage is added even
		// if SPEAKER_FILTER_ENABLED=false — it scores and dumps every clip but
		// never drops. Diagnostic path for RCA 035.
		shadow := os.Getenv("SPEAKER_FILTER_SHADOW") == "true"
		if !diarizeEnforce && (os.Getenv("SPEAKER_FILTER_ENABLED") != "false" || shadow) {
			// 0.25 — measured 2026-05-19 (RCA 036): on clips ≥5s the enrolled
			// user scores 0.32–0.82, movie/TV audio scores ≤0.14. 0.25 sits in
			// the gap. The old 0.05 let TV through (it scored up to 0.143).
			threshold := 0.25
			if env := os.Getenv("SPEAKER_FILTER_THRESHOLD"); env != "" {
				var v float64
				if _, err := fmt.Sscanf(env, "%f", &v); err == nil && v >= 0 && v <= 1 {
					threshold = v
				}
			}
			stages = append(stages, &SpeakerFilterStage{
				SidecarURL: speakerSidecarURL(),
				Hub:        ch.hub,
				Threshold:  threshold,
				Shadow:     shadow,
			})
		}
		// RMS gate filters sub-baseline energy clips before Whisper sees them.
		if ch.rmsGate != nil {
			stages = append(stages, ch.rmsGate)
		}
	}
	preStages := stages

	// Post-transcribe stages run after the pool returns a Whisper result.
	var postStages []AudioStage
	// TV chatter filter — only for mic channels that opted in via config.
	// Inserted strictly between transcribe and classify so it sees the raw
	// whisper text but runs before hallucination/word-count gating.
	if ch.Type == ChannelTypeMic && ch.Config.EnableTVFilter && ch.plexDashboardURL != "" {
		parent := ch.tvFilterParentCtx
		if parent == nil {
			parent = context.Background()
		}
		client := NewPlexCaptionsClient(ch.plexDashboardURL, ch.Config.PlexPlayerName, 30*time.Second)
		client.Start(parent)
		postStages = append(postStages, &TVChatFilterStage{
			Client:    client,
			Threshold: ch.tvFilterThreshold,
			Hub:       ch.hub,
		})
		log.Printf("[channel:%s] tv chatter filter enabled (player=%q threshold=%.2f)",
			ch.ID, ch.Config.PlexPlayerName, ch.tvFilterThreshold)
	}
	cs := &ClassifyStage{
		Context:  ch.promptCtx,
		MinWords: 3,
	}
	if ch.smartTurnClient != nil {
		cs.SmartTurn = ch.smartTurnClient
	}
	postStages = append(postStages, cs)
	// MediaSignalStage — runs after ClassifyStage, before broadcast.
	// Reads speaker_similarity + no_speech_prob from Meta, optionally
	// polls Plex captions, and writes media_confidence + media_flagged.
	// The broadcast site below uses media_flagged to prefix the text
	// with [~media?] — clips are NEVER dropped here.
	{
		parent := ch.tvFilterParentCtx
		if parent == nil {
			parent = context.Background()
		}
		captionsClient := NewPlexCaptionsClient(ch.plexDashboardURL, ch.Config.PlexPlayerName, 30*time.Second)
		captionsClient.Start(parent)
		postStages = append(postStages, &MediaSignalStage{
			Captions:  captionsClient,
			Threshold: ch.mediaSignalThreshold,
		})
	}
	// InlineExtractorStage — fire-and-forget per-clip LLM extraction.
	// Runs after MediaSignalStage so clip.Text is fully populated.
	// Stage is a no-op when ch.inlineExtractor is nil or Enabled=false.
	if ch.inlineExtractor != nil {
		postStages = append(postStages, ch.inlineExtractor)
	}
	return preStages, postStages
}

// newClip creates an AudioClip pre-stamped with this channel's ID.
func (ch *AudioChannel) newClip(pcm []byte, forceClose bool) *AudioClip {
	return &AudioClip{
		ChannelID:  ch.ID,
		PCM:        pcm,
		CapturedAt: time.Now(),
		Duration: time.Duration(
			float64(len(pcm)) /
				float64(streamSampleRate*streamChannels*(streamBitsPerSample/8)) *
				float64(time.Second),
		),
		RMS:        calcRMS(pcm),
		Meta:       map[string]any{},
		ForceClose: forceClose,
	}
}

// Start begins audio capture. Returns an error if already running.
func (ch *AudioChannel) Start() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.running {
		return fmt.Errorf("channel %q already running", ch.ID)
	}
	if ch.Type == ChannelTypeWebSocket {
		return fmt.Errorf("websocket channels not yet implemented")
	}
	ch.running = true
	ch.startedAt = time.Now()
	ch.utterances.Store(0)
	ch.filtered.Store(0)
	ch.errors.Store(0)
	ch.lastError = ""
	if ch.Type == ChannelTypeEdge {
		// Edge channels are passive listeners — /ws/edge accepts frames when running=true.
		// dispatching=true allows vadLoop (audio mode) to dispatch utterances to process().
		ch.dispatching.Store(true)
		ch.logf("info", "[channel:%s] started (%s)", ch.ID, ch.Type)
		return nil
	}
	ch.stopCh = make(chan struct{})
	ch.dispatching.Store(true)
	go ch.runLoop()
	ch.logf("info", "[channel:%s] started (%s)", ch.ID, ch.Type)
	return nil
}

// Stop halts audio capture gracefully.
func (ch *AudioChannel) Stop() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if !ch.running {
		return fmt.Errorf("channel %q not running", ch.ID)
	}
	ch.running = false
	if ch.Type == ChannelTypeEdge {
		// Edge channels have no ffmpeg process. Clearing dispatching drops any
		// in-flight audio-mode vadLoop frames.
		ch.dispatching.Store(false)
		ch.logf("info", "[channel:%s] stopped — utterances:%d", ch.ID, ch.utterances.Load())
		return nil
	}
	ch.dispatching.Store(false)
	close(ch.stopCh)
	if ch.cmd != nil && ch.cmd.Process != nil {
		ch.cmd.Process.Signal(os.Interrupt)
	}
	if ch.promptCtx != nil {
		ch.promptCtx.Reset()
	}
	ch.logf("info", "[channel:%s] stopped — utterances:%d filtered:%d errors:%d",
		ch.ID, ch.utterances.Load(), ch.filtered.Load(), ch.errors.Load())
	return nil
}

// IsRunning reports whether the channel is currently active.
func (ch *AudioChannel) IsRunning() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.running
}

// RecordEdgeUtterance increments the utterances and vadClosed counters for
// text-mode edge frames. On-device whisper acts as the VAD gate, so bumping
// both maintains the accounting invariant (vad_closed = drops + utterances)
// that the pipeline smoke test checks.
func (ch *AudioChannel) RecordEdgeUtterance() {
	ch.utterances.Add(1)
	ch.vadClosed.Add(1)
}

// RunAudioFromReader feeds raw 16 kHz mono s16le PCM from r through the energy
// VAD and into the stage pipeline (noise suppress → Whisper → classify → hub).
// Used by EdgeHandler when the Android device streams audio instead of
// pre-transcribed text. Blocks until r is closed or returns EOF.
func (ch *AudioChannel) RunAudioFromReader(r io.Reader) {
	// Ensure dispatching is armed regardless of channel running state.
	// This lets an external caller (browser mic stream, edge WebSocket) feed
	// audio through the pipeline without requiring Start() to be called first.
	ch.dispatching.Store(true)
	ch.vadLoop(r)
}

// Status returns a snapshot of this channel's current state.
func (ch *AudioChannel) Status() ChannelStatus {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	var dur int
	if ch.running {
		dur = int(time.Since(ch.startedAt).Seconds())
	}
	st := ChannelStatus{
		ID:                  ch.ID,
		Name:                ch.Name,
		Type:                ch.Type,
		Config:              ch.Config,
		Running:             ch.running,
		DurationSeconds:     dur,
		UtterancesCount:     ch.utterances.Load(),
		FilteredCount:       ch.filtered.Load(),
		ErrorCount:          ch.errors.Load(),
		LastError:           ch.lastError,
		SpeakerDroppedCount: ch.speakerDropped.Load(),
		VADClosedCount:      ch.vadClosed.Load(),
	}
	if ch.rmsGate != nil {
		st.RMSGateDroppedCount = ch.rmsGate.Drops()
	}
	if ch.baseline != nil {
		snap := ch.baseline.Snapshot()
		st.SpeakerBaseline = &snap
	}
	return st
}

// runLoop is the outer loop. For mic channels it runs once; for RTSP channels
// it reconnects with exponential backoff until Stop() is called.
func (ch *AudioChannel) runLoop() {
	backoffSec := 0
	for {
		select {
		case <-ch.stopCh:
			return
		default:
		}

		if backoffSec > 0 {
			ch.logf("info", "[channel:%s] reconnecting in %ds…", ch.ID, backoffSec)
			select {
			case <-ch.stopCh:
				return
			case <-time.After(time.Duration(backoffSec) * time.Second):
			}
		}

		died, err := ch.runOnce()
		if err != nil {
			ch.errors.Add(1)
			ch.mu.Lock()
			ch.lastError = err.Error()
			ch.mu.Unlock()
			ch.logf("error", "[channel:%s] ffmpeg error: %v", ch.ID, err)
			ch.broadcastStatus()
		}

		// Check if Stop() was called.
		select {
		case <-ch.stopCh:
			return
		default:
		}

		if ch.Type != ChannelTypeRTSP {
			// Mic: unexpected death → mark stopped, don't reconnect.
			if died {
				ch.mu.Lock()
				ch.running = false
				ch.mu.Unlock()
				ch.logf("error", "[channel:%s] ffmpeg exited unexpectedly — streaming stopped", ch.ID)
				ch.broadcastStatus()
			}
			return
		}

		// RTSP: reconnect with backoff.
		backoffSec = nextRTSPBackoff(backoffSec)
		ch.logf("warn", "[channel:%s] RTSP stream lost — will retry in %ds", ch.ID, backoffSec)
		ch.broadcastStatus()
	}
}

// runOnce starts ffmpeg, runs the VAD loop until ffmpeg exits or Stop() is called.
// Returns (diedUnexpectedly, error).
func (ch *AudioChannel) runOnce() (bool, error) {
	args := ch.ffmpegArgs()
	cmd := exec.Command("ffmpeg", args...)
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return false, fmt.Errorf("pipe: %w", err)
	}
	ch.mu.Lock()
	ch.cmd = cmd
	ch.mu.Unlock()

	if err := cmd.Start(); err != nil {
		return false, fmt.Errorf("ffmpeg start: %w", err)
	}

	diedUnexpectedly := true
	defer func() {
		cmd.Wait()
		ch.mu.Lock()
		wasRunning := ch.running
		ch.mu.Unlock()
		if wasRunning && diedUnexpectedly {
			// Caller handles this.
		}
	}()

	ch.vadLoop(stdout)

	// If we reach here via stopCh, it's a clean stop, not unexpected death.
	select {
	case <-ch.stopCh:
		diedUnexpectedly = false
	default:
	}

	return diedUnexpectedly, nil
}

// vadLoop reads PCM frames from r, applies RMS VAD, and dispatches utterances
// to process(). Returns when r is exhausted or stopCh is closed.
func (ch *AudioChannel) vadLoop(r io.Reader) {
	frame := make([]byte, frameBytes)
	var speech []byte
	var speechFrames, silentFrames int
	var inSpeech bool
	var frameCount int

	const preRollFrames = 4
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
		start, count := 0, preRollIdx
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
		if !ch.dispatching.Load() {
			return
		}
		// Opt-in: drop force-closed clips longer than 12s as "TV chatter."
		// This conflicts with passive recording of long-form speech (the
		// user explaining something for 20-30s). Enable via env var only
		// when running in command-only mode.
		if os.Getenv("DROP_FORCE_CLOSED_LONG_CLIPS") == "true" {
			const maxAcceptedBytes = 12 * 16000 * 2 // 12s at 16kHz s16le mono
			if forceClose && len(pcm) > maxAcceptedBytes {
				ch.logf("info", "[vad:%s] dropping %ds force-closed clip (likely TV chatter)", ch.ID, len(pcm)/(16000*2))
				return
			}
		}
		buf := make([]byte, len(pcm))
		copy(buf, pcm)
		go ch.process(buf, forceClose)
	}

	for {
		select {
		case <-ch.stopCh:
			if speechFrames >= minSpeechFrames {
				dispatch(speech, false)
			}
			return
		default:
		}

		if _, err := io.ReadFull(r, frame); err != nil {
			if speechFrames >= minSpeechFrames {
				dispatch(speech, false)
			}
			return
		}

		rms := calcRMS(frame)
		frameCount++

		if frameCount%50 == 0 {
			ch.logf("info", "[vad:%s] rms=%.0f threshold=%d speech=%v frames=%d",
				ch.ID, rms, silenceRMS, inSpeech, speechFrames)
		}

		if rms > silenceRMS {
			if !inSpeech {
				inSpeech = true
				if pre := flushPreRoll(); len(pre) > 0 {
					speech = append(pre, speech...)
					speechFrames += len(pre) / frameBytes
				}
				ch.logf("info", "[vad:%s] speech detected rms=%.0f", ch.ID, rms)
			}
			silentFrames = 0
			speechFrames++
			speech = append(speech, frame...)
			if speechFrames >= maxSpeechFrames {
				ch.logf("info", "[vad:%s] max-length reached, forcing utterance", ch.ID)
				dispatch(speech, true)
				speech = nil
				speechFrames = 0
				silentFrames = 0
				inSpeech = false
			}
		} else if speechFrames > 0 {
			silentFrames++
			speech = append(speech, frame...)
			endOfUtterance := silentFrames >= silenceFrameCount
			tooLong := (speechFrames + silentFrames) >= maxSpeechFrames
			if endOfUtterance || tooLong {
				reason := "silence"
				if tooLong {
					reason = "max-length"
				}
				if speechFrames >= minSpeechFrames {
					// Counter for the smoke test's drop-rate denominator.
					// Bumped exactly once per VAD-close that goes to the
					// pipeline; ratio with speakerDropped gives the
					// "fraction of detected speech silently muted".
					ch.vadClosed.Add(1)
					ch.logf("info", "[vad:%s] utterance closed (%s) speech=%d silent=%d frames",
						ch.ID, reason, speechFrames, silentFrames)
					dispatch(speech, tooLong)
				}
				speech = nil
				speechFrames = 0
				silentFrames = 0
				inSpeech = false
				preRollIdx = 0
				preRollFull = false
			}
		} else {
			addPreRoll(frame)
		}
	}
}

// process runs one VAD-closed utterance through the pre-transcribe pipeline,
// then hands it off to the TranscribePool. Post-transcribe stages and the
// hub broadcast happen in onTranscribed (called when the pool result arrives).
//
// Splitting at the Whisper boundary is what makes this method return quickly
// even when Whisper is slow — VAD never blocks on transcription.
func (ch *AudioChannel) process(pcm []byte, forceClose bool) {
	clip := ch.newClip(pcm, forceClose)
	ch.logf("info", "[channel:%s] processing %.1fs audio avgRMS=%.0f",
		ch.ID, clip.Duration.Seconds(), clip.RMS)

	ctx := context.Background()
	if err := runStages(ctx, ch.preStages, clip); err != nil {
		ch.errors.Add(1)
		ch.logf("error", "[channel:%s] pre-pipeline error: %v", ch.ID, err)
		GlobalErrorLog.Add("channel:"+ch.ID, err)
		return
	}

	if clip.Dropped {
		ch.finalizeClip(clip)
		return
	}

	// Hand off to the transcribe pool. If the pool isn't wired or the queue
	// is full, fall back to running synchronously on this goroutine — that
	// preserves test paths and the legacy behaviour for clips that would
	// otherwise be silently lost when the queue is saturated.
	if ch.pool != nil {
		seq := ch.nextUttSeq.Add(1)
		uttID := fmt.Sprintf("%s-%d", ch.ID, seq)
		prompt := ""
		if ch.transcribePromptCtx != nil {
			prompt = ch.transcribePromptCtx.For()
		}
		u := transcribepool.Utterance{
			ID:         uttID,
			AudioPath:  clip.AudioPath,
			DurationMs: int(clip.Duration.Milliseconds()),
			Enqueued:   time.Now(),
			PCM:        clip.PCM,
			User: &poolJob{
				ch:     ch,
				clip:   clip,
				prompt: prompt,
			},
		}
		err := ch.pool.Submit(u)
		if err == nil {
			return
		}
		// Backpressure: log + drop. Phase 3 will surface ch.pool.Stats().Dropped via /api/doctor.
		ch.errors.Add(1)
		ch.logf("warn", "[channel:%s] transcribe pool full — dropping utterance %s (dropped_total=%d)",
			ch.ID, uttID, ch.pool.Stats().Dropped)
		return
	}

	ch.onTranscribed(clip, transcribepool.Result{}, nil)
}

// poolJob carries the per-utterance state the pool dispatcher needs to
// continue post-transcribe processing.
type poolJob struct {
	ch     *AudioChannel
	clip   *AudioClip
	prompt string
}

// HandlePoolResult is invoked by the shared pool dispatcher with one Result
// per submitted utterance. Runs off the worker goroutines so post-stage work
// (and the hub broadcast) doesn't backpressure the pool.
func (j *poolJob) HandlePoolResult(r transcribepool.Result) {
	j.ch.onTranscribed(j.clip, r, nil)
}

// onTranscribed applies the Whisper output to the clip, runs post-transcribe
// stages, and emits the hub event. Used both for pool-delivered results and
// for the synchronous fallback path when no pool is wired.
func (ch *AudioChannel) onTranscribed(clip *AudioClip, r transcribepool.Result, fallbackErr error) {
	if r.Err != nil {
		clip.Dropped = true
		clip.DropReason = r.Err.Error()
		clip.Marker = "whisper error"
	} else {
		clip.RawText = r.Text
		clip.WhisperMs = r.LatencyMs
		applyWhisperMeta(clip, r)
	}

	if !clip.Dropped {
		ctx := context.Background()
		if err := runStages(ctx, ch.postStages, clip); err != nil {
			ch.errors.Add(1)
			ch.logf("error", "[channel:%s] post-pipeline error: %v", ch.ID, err)
			GlobalErrorLog.Add("channel:"+ch.ID, err)
			return
		}
	}

	if clip.RawText != "" {
		ch.logf("info", "[whisper:%s] %.1fs → %q (%dms)",
			ch.ID, clip.Duration.Seconds(), truncate(clip.RawText, 80), clip.WhisperMs)
	}
	ch.finalizeClip(clip)
}

// applyWhisperMeta is the post-pool-result form of applyWhisperOutput; the
// pool returns Text directly so it has fewer fields than WhisperOutput.
func applyWhisperMeta(clip *AudioClip, r transcribepool.Result) {
	// Confidence metrics arrive via clip.Meta if the production transcriber
	// stamps them there directly (see api/server.go pool wiring). The pool
	// itself doesn't manipulate Meta.
	_ = r
	_ = clip
}

func (ch *AudioChannel) finalizeClip(clip *AudioClip) {
	switch {
	case clip.Dropped && clip.Marker != "":
		ch.filtered.Add(1)
		ch.logf("warn", "[channel:%s] filtered (%s): %q → writing %s marker",
			ch.ID, clip.DropReason, clip.RawText, clip.Marker)
		ch.writeMarker(clip, clip.Marker)

	case clip.Dropped:
		// A drop with no marker is the silent-mute path. Speaker filter
		// is by far the dominant source. Bumping speakerDropped here
		// rather than only in the filter stage is intentional: it
		// captures every "no marker, no transcript" outcome regardless
		// of which stage rejected, which is the right denominator-mate
		// for vadClosed when the smoke test computes drop rate.
		ch.speakerDropped.Add(1)
		ch.logf("info", "[channel:%s] dropped (%s) — no marker written", ch.ID, clip.DropReason)

	case clip.IsAmend && clip.Text != "":
		ch.utterances.Add(1)
		ch.logf("info", "[channel:%s] amending last line: %q", ch.ID, clip.Text)
		ch.hub.Broadcast(models.Event{
			Type: "transcript_amend",
			Payload: map[string]string{
				"channel_id": ch.ID,
				"time":       clip.CapturedAt.Format("15:04:05"),
				"text":       clip.Text,
				"audio_ref":  clip.AudioPath,
			},
		})

	case clip.Text != "":
		ch.utterances.Add(1)
		// RCA 011: feed accepted-clip RMS into the adaptive baseline.
		// We record HERE — at the broadcast site — rather than inside
		// ClassifyStage because this is the canonical "this clip
		// became real transcript text". A clip that classifies as a
		// continuation merge (above) doesn't get recorded because it
		// inherits the previous clip's reference; recording it would
		// double-weight short trailing words. (Amends could be
		// recorded if needed, but for now keeping the baseline
		// composed of full-utterance RMS is cleaner.)
		if ch.baseline != nil && clip.RMS > 0 {
			ch.baseline.Record(clip.RMS)
		}
		// MediaSignalStage sets media_flagged=true when the composite
		// media_confidence score is below the threshold. Prefix the
		// text with [~media?] so the transcript file and the summarizer
		// prompt can identify ambient media lines. clip.Text itself is
		// never mutated — only the broadcast/write path uses the
		// annotated form.
		broadcastText := clip.Text
		if flagged, _ := clip.Meta["media_flagged"].(bool); flagged {
			broadcastText = "[~media?] " + broadcastText
		}
		if ch.transcript != nil {
			// fix E: tag every write with the channel ID so the daily
			// transcript file can be filtered per-channel on read.
			_ = ch.transcript.AppendTagged(broadcastText, ch.ID, clip.CapturedAt)
		}
		mediaConf, _ := clip.Meta["media_confidence"].(float64)
		ch.hub.Broadcast(models.Event{
			Type: "transcript_append",
			Payload: map[string]any{
				"channel_id":       ch.ID,
				"time":             clip.CapturedAt.Format("15:04:05"),
				"text":             broadcastText,
				"audio_ref":        clip.AudioPath,
				"media_confidence": mediaConf,
			},
		})
	}
}

func (ch *AudioChannel) writeMarker(clip *AudioClip, label string) {
	audioRef := clip.AudioPath
	if audioRef == "" {
		audioRef = "unknown"
	}
	text := fmt.Sprintf("[~%.0fs, %s | audio:%s]", clip.Duration.Seconds(), label, truncate(audioRef, 40))
	if ch.transcript != nil {
		// fix E: markers carry the channel ID too so per-channel reads
		// see the right "(unclear)" / "(barge-in)" annotations.
		_ = ch.transcript.AppendTagged(text, ch.ID, clip.CapturedAt)
	}
	ch.hub.Broadcast(models.Event{
		Type: "transcript_append",
		Payload: map[string]string{
			"channel_id": ch.ID,
			"time":       clip.CapturedAt.Format("15:04:05"),
			"text":       text,
			"marker":     "true",
			"audio_ref":  clip.AudioPath,
		},
	})
}

func (ch *AudioChannel) broadcastStatus() {
	s := ch.Status()
	ch.hub.Broadcast(models.Event{
		Type:    "channel_status",
		Payload: s,
	})
}

func (ch *AudioChannel) logf(level, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("[%s] %s", level, msg)
	ch.hub.Broadcast(models.Event{
		Type:    "log",
		Payload: models.LogEntry{Level: level, Message: msg},
	})
}

// envFloat reads a float env var, falling back to def if unset/invalid.
func envFloat(name string, def float64) float64 {
	if env := os.Getenv(name); env != "" {
		var v float64
		if _, err := fmt.Sscanf(env, "%f", &v); err == nil {
			return v
		}
	}
	return def
}
