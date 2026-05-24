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
	"sync"
	"sync/atomic"
	"time"

	"hermes-listener/internal/models"
)

// SpeakerFilterStage rejects clips that don't sound like the enrolled user.
// It calls the Python sidecar at SidecarURL (see speaker_sidecar/server.py)
// which runs a SpeechBrain ECAPA-TDNN embedding and returns cosine similarity
// against the persisted enrollment.
//
// Fail-open by design: if the sidecar is unreachable, if /status reports no
// enrollment, or if a per-clip /score call fails, the stage is a no-op
// pass-through. The value of speaker filtering isn't worth dropping real
// speech whenever the sidecar hiccups.
//
// Tuning: with ECAPA on VoxCeleb, same-speaker clips typically land in
// [0.35, 0.85] and different-speaker in [-0.05, 0.25]. Threshold=0.25 is the
// conservative default (errs toward keeping audio). Lower it if you want to
// be stricter about TV/other voices; raise it if legit clips are getting
// dropped.
type SpeakerFilterStage struct {
	SidecarURL string        // e.g. "http://127.0.0.1:9200"
	Threshold  float64       // cosine similarity below this = drop. Default 0.25 if zero.
	Client     *http.Client  // if nil, a 10s-timeout client is created lazily
	Hub        *Hub          // optional — for surfacing "sidecar missing / no enrollment" warnings

	// Shadow mode: score every clip and dump the exact WAV bytes sent to
	// /score, but NEVER drop. Used to diagnose why live clips score far
	// lower than clean test clips (RCA 035) without risking muting the user.
	Shadow      bool
	dumpCounter atomic.Int32

	once    sync.Once
	enabled atomic.Bool // set during first call; if false, stage is a no-op
	// sem limits concurrent /score calls to 1. The Python sidecar is async but
	// single-threaded; bursts of simultaneous clips (e.g. post-reload flush) can
	// pile up and cause the 10s HTTP timeout to fire → fail-open → TV audio leaks.
	sem chan struct{}

	// Drop-rate guard. When the enrolled voiceprint stops matching current
	// conditions (mic moved, model reloaded, stale enrollment) the stage will
	// silently drop 100% of speech — the user becomes a black hole with no
	// signal (see RCA 003/004). The guard makes that impossible: after
	// guardConsecutiveDropLimit back-to-back drops it SUSPENDS enforcement
	// (pass-through) and emits a loud warning, then re-arms automatically the
	// moment a clip matches enrollment again. The guard can only ever relax
	// enforcement — it never causes a drop.
	guardMu          sync.Mutex
	consecutiveDrops int
	guardTripped     bool
}

// guardConsecutiveDropLimit is how many back-to-back drops trip the guard.
// At a normal speaking cadence this is roughly 30–60s of total muting —
// long enough not to trip on a few stray clips, short enough that a real
// silent-mute is caught fast.
const guardConsecutiveDropLimit = 12

// foreignContextLimit is how many consecutive ≥5s clips must score below
// threshold before short (unscoreable) clips are dropped on context. Two
// full-length foreign clips is a strong signal TV/YouTube is playing; the
// ≥5s zone is where ECAPA is reliable (RCA 036), so a false positive here
// would require the reliable zone to misjudge the user twice in a row.
const foreignContextLimit = 2

// foreignContextActive reports whether recent scored clips indicate foreign
// audio is playing, so short unscoreable clips should be dropped on context
// rather than leaked into the transcript (RCA 038). It is suppressed while
// the drop-rate guard is tripped — a tripped guard means we may already be
// wrongly muting the user, and contextual gating must never compound that.
func (s *SpeakerFilterStage) foreignContextActive() bool {
	s.guardMu.Lock()
	defer s.guardMu.Unlock()
	return !s.guardTripped && s.consecutiveDrops >= foreignContextLimit
}

func (s *SpeakerFilterStage) Name() string { return "speaker_filter" }

func (s *SpeakerFilterStage) threshold() float64 {
	if s.Threshold <= 0 {
		return 0.25
	}
	return s.Threshold
}

func (s *SpeakerFilterStage) client() *http.Client {
	if s.Client != nil {
		return s.Client
	}
	s.Client = &http.Client{Timeout: 10 * time.Second}
	return s.Client
}

// init checks sidecar reachability + enrollment once. We don't retry on every
// clip because a dead sidecar shouldn't cost us 10s of timeout per utterance.
// Users who start the sidecar after nogura will get filtering after a restart.
func (s *SpeakerFilterStage) init(ctx context.Context) {
	s.once.Do(func() {
		req, err := http.NewRequestWithContext(ctx, "GET", s.SidecarURL+"/status", nil)
		if err != nil {
			s.emit("warn", fmt.Sprintf("[speaker_filter] bad sidecar URL %q — stage disabled", s.SidecarURL))
			return
		}
		resp, err := s.client().Do(req)
		if err != nil {
			s.emit("warn", fmt.Sprintf("[speaker_filter] sidecar unreachable (%v) — stage is a no-op. Start it with scripts/start-speaker-sidecar.sh", err))
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			s.emit("warn", fmt.Sprintf("[speaker_filter] sidecar /status returned %d — stage disabled", resp.StatusCode))
			return
		}
		var st struct {
			Enrolled bool `json:"enrolled"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
			s.emit("warn", fmt.Sprintf("[speaker_filter] can't parse /status (%v) — stage disabled", err))
			return
		}
		if !st.Enrolled {
			s.emit("info", "[speaker_filter] no voice enrollment yet — stage is a pass-through. POST /api/speaker/enroll to activate.")
			return
		}
		s.enabled.Store(true)
		s.emit("info", fmt.Sprintf("[speaker_filter] active (threshold=%.2f)", s.threshold()))
	})
}

func (s *SpeakerFilterStage) semaphore() chan struct{} {
	if s.sem == nil {
		s.sem = make(chan struct{}, 1)
	}
	return s.sem
}

// pcmDurationSecs returns the wall-clock length of a 16kHz mono s16le PCM
// buffer in seconds.
func pcmDurationSecs(pcm []byte) float64 {
	return float64(len(pcm)) / float64(streamSampleRate*streamChannels*(streamBitsPerSample/8))
}

// speakerMinDurationSecs is the shortest clip the speaker filter will score
// and act on. Below this, ECAPA speaker embeddings are unreliable — measured
// scores for the enrolled user on 1.6–3.4s clips ranged 0.06 down to NEGATIVE
// (RCA 036). A clip that cannot be classified must never be dropped (RCA 003),
// so sub-threshold clips pass through unfiltered. Override with
// SPEAKER_FILTER_MIN_SECS.
const speakerMinDurationSecs = 5.0

func (s *SpeakerFilterStage) minDuration() float64 {
	if env := os.Getenv("SPEAKER_FILTER_MIN_SECS"); env != "" {
		var v float64
		if _, err := fmt.Sscanf(env, "%f", &v); err == nil && v >= 0 {
			return v
		}
	}
	return speakerMinDurationSecs
}

func (s *SpeakerFilterStage) Process(ctx context.Context, clip *AudioClip) error {
	s.init(ctx)
	if !s.enabled.Load() {
		return nil
	}

	// Duration gate: ECAPA can't reliably embed short clips (RCA 036). In
	// enforcing mode a too-short clip passes through unscored — never dropped.
	// Shadow mode still scores everything, to gather short-clip data.
	durSecs := pcmDurationSecs(clip.PCM)
	if !s.Shadow && durSecs < s.minDuration() {
		clip.Meta["speaker_skipped_short"] = durSecs
		// Contextual gating: a short clip can't be identity-scored on its
		// own (RCA 036), but if recent ≥5s clips all scored as a foreign
		// voice the channel is playing TV/YouTube — drop the short clip on
		// that context rather than leak it into the transcript (RCA 038).
		if s.foreignContextActive() {
			clip.Dropped = true
			clip.DropReason = "short clip dropped — foreign-voice context active"
			clip.Meta["speaker_context_dropped"] = true
		}
		return nil
	}

	// Serialize /score calls — sidecar is single-threaded; bursts time out → fail-open.
	select {
	case s.semaphore() <- struct{}{}:
		defer func() { <-s.semaphore() }()
	case <-time.After(8 * time.Second):
		s.emit("warn", "[speaker_filter] semaphore timeout — skipping clip to avoid pile-up")
		return nil // fail-open
	}

	sim, ok := s.score(ctx, clip.PCM)
	if !ok {
		return nil // fail-open
	}
	clip.Meta["speaker_similarity"] = sim

	if s.Shadow {
		s.dumpDebug(clip.PCM, sim)
		return nil // observe-only — never drop
	}

	below := sim < s.threshold()
	enforce := s.recordOutcome(below, sim)

	if below && enforce {
		// Silently drop — other voices / TV audio shouldn't pollute the transcript.
		// No Marker set → process() writes nothing.
		clip.Dropped = true
		clip.DropReason = fmt.Sprintf("speaker mismatch (sim=%.3f < %.2f)", sim, s.threshold())
	} else if below {
		// Below threshold but the guard has suspended enforcement — the clip
		// passes through rather than being silently muted.
		clip.Meta["speaker_guard_passthrough"] = true
	}
	return nil
}

// recordOutcome updates the drop-rate guard with one clip's result and reports
// whether the stage should still ENFORCE (actually drop) this clip.
//
//   - belowThreshold=false (a clip matched the enrolled voice): the counter
//     resets; if the guard was tripped it clears and filtering re-arms.
//   - belowThreshold=true: the counter climbs. Once it reaches the limit the
//     guard trips — enforcement is suspended and a loud warning is emitted —
//     so the user can never be silently muted past that point.
//
// The guard can only ever return false (relax). It never turns a kept clip
// into a dropped one.
func (s *SpeakerFilterStage) recordOutcome(belowThreshold bool, sim float64) (enforce bool) {
	s.guardMu.Lock()
	defer s.guardMu.Unlock()

	if !belowThreshold {
		if s.guardTripped {
			s.guardTripped = false
			s.emit("info", "[speaker_filter] guard cleared — a clip matched enrollment; filtering re-armed")
		}
		s.consecutiveDrops = 0
		return true
	}

	s.consecutiveDrops++
	if !s.guardTripped && s.consecutiveDrops >= guardConsecutiveDropLimit {
		s.guardTripped = true
		s.emit("warn", fmt.Sprintf(
			"[speaker_filter] GUARD TRIPPED — %d consecutive clips dropped as speaker mismatch. "+
				"Enforcement SUSPENDED (pass-through) so you are not silently muted. "+
				"If this is your voice, re-enroll; filtering re-arms automatically once a clip matches your enrollment.",
			s.consecutiveDrops))
		Degraded("speaker_filter", "guard_tripped", "warn", map[string]any{
			"consecutive_drops": s.consecutiveDrops,
			"last_similarity":   sim,
			"threshold":         s.threshold(),
		})
	}
	return !s.guardTripped
}

// dumpDebug (shadow mode only) logs the score for one clip and writes the
// exact WAV bytes that were POSTed to /score to /tmp/speaker-debug/, capped
// at the first 12 clips so a long session doesn't fill the disk. This is the
// instrumentation for RCA 035 — comparing a live pipeline clip against a
// clean test clip.
func (s *SpeakerFilterStage) dumpDebug(pcm []byte, sim float64) {
	durSecs := pcmDurationSecs(pcm)
	n := s.dumpCounter.Add(1)
	if n > 12 {
		log.Printf("[speaker_filter] SHADOW sim=%.4f pcm_bytes=%d dur=%.2fs (dump cap reached)", sim, len(pcm), durSecs)
		return
	}
	dir := "/tmp/speaker-debug"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("[speaker_filter] SHADOW mkdir: %v", err)
		return
	}
	path := fmt.Sprintf("%s/clip-%02d.wav", dir, n)
	if err := os.WriteFile(path, pcmToWAV(pcm), 0o644); err != nil {
		log.Printf("[speaker_filter] SHADOW write: %v", err)
		return
	}
	log.Printf("[speaker_filter] SHADOW sim=%.4f pcm_bytes=%d dur=%.2fs → %s", sim, len(pcm), durSecs, path)
}

// score POSTs the clip's PCM (as a WAV) to /score and returns the similarity.
// Returns ok=false on any failure so the caller can fail-open.
func (s *SpeakerFilterStage) score(ctx context.Context, pcm []byte) (float64, bool) {
	wav := pcmToWAV(pcm)

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("file", "clip.wav")
	if err != nil {
		return 0, false
	}
	if _, err := fw.Write(wav); err != nil {
		return 0, false
	}
	mw.Close()

	req, err := http.NewRequestWithContext(ctx, "POST", s.SidecarURL+"/score", &body)
	if err != nil {
		return 0, false
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := s.client().Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return 0, false
	}
	var out struct {
		Similarity float64 `json:"similarity"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, false
	}
	return out.Similarity, true
}

func (s *SpeakerFilterStage) emit(level, message string) {
	if s.Hub == nil {
		return
	}
	s.Hub.Broadcast(models.Event{
		Type:    "log",
		Payload: models.LogEntry{Level: level, Message: message},
	})
}
