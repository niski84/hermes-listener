package pipeline

import (
	"encoding/json"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

// speaker_baseline.go — adaptive RMS reference for the user's voice.
//
// Why this exists: see RCA 011 in docs/every-time-i-fucked-up/. The user
// wears the mic in a consistent place on the neck and speaks at a
// consistent volume. Their accepted-speech RMS distribution is stable.
// Whisper hallucinates on clips that fall well below that distribution
// (silence-tail, room noise, distant TV).
//
// The baseline tracks RMS values of clips that successfully reached
// transcript_append (i.e. real speech that classified clean). The
// RMSGateStage reads this baseline to decide whether a new clip even
// goes to Whisper. Below threshold → drop without transcribing → no
// hallucination, no GPU cost.
//
// State is bounded (last 100 samples) and persisted to disk so a
// restart doesn't lose the calibration. The first restart after this
// PR ships will start from an empty baseline; the static bootstrap
// floor (RCA 008's 80) protects until ~20 real samples land.

// SpeakerBaseline holds the rolling window of accepted-clip RMS values.
// All methods are safe for concurrent use.
type SpeakerBaseline struct {
	mu             sync.Mutex
	samples        []float64
	cap            int       // max ring size; FIFO eviction once full
	bootstrapMin   int       // # of samples required before going adaptive
	staticFloor    float64   // RMS floor used during bootstrap (RCA 008's 80)
	adaptiveCoef   float64   // adaptive floor = median × adaptiveCoef
	path           string    // persistence path; "" = no persistence
	dirty          bool      // true if samples changed since last save
	lastSavedAt    time.Time
	saveDebounce   time.Duration
}

// SpeakerBaselineConfig keeps the magic numbers explicit and tunable.
type SpeakerBaselineConfig struct {
	Cap          int
	BootstrapMin int
	StaticFloor  float64
	AdaptiveCoef float64
	Path         string
	SaveDebounce time.Duration
}

// DefaultSpeakerBaselineConfig is the production default.
//
//   - Cap=100 — last 100 accepted clips. ~30s of typical speech if user
//     averages one utterance per few seconds. Long enough to be stable;
//     short enough to follow mic-position changes within a session.
//   - BootstrapMin=20 — until we have 20 samples we use the static floor.
//     20 samples is ~30s-1m of speech, which is enough for the median to
//     be meaningful without being so high it delays activation.
//   - StaticFloor=80 — RCA 008's hard floor, kept as bootstrap default.
//   - AdaptiveCoef=0.45 — floor = median × 0.45. Median typical speech
//     RMS ~ 250; floor ~ 112. Soft brainstorm speech (120+) passes;
//     quiet hallucinations (60-100) still drop. Lowered from 0.55
//     (floor ~138) per RCA 033.
//   - SaveDebounce=10s — write to disk at most every 10s, reduces I/O.
func DefaultSpeakerBaselineConfig(path string) SpeakerBaselineConfig {
	return SpeakerBaselineConfig{
		Cap:          100,
		BootstrapMin: 20,
		StaticFloor:  80,
		AdaptiveCoef: 0.45,
		Path:         path,
		SaveDebounce: 10 * time.Second,
	}
}

// NewSpeakerBaseline constructs a baseline. If cfg.Path exists and is
// readable JSON, the previous samples are loaded so calibration
// survives restarts. Errors loading are logged but never fatal — a
// fresh start is always a safe fallback.
func NewSpeakerBaseline(cfg SpeakerBaselineConfig) *SpeakerBaseline {
	b := &SpeakerBaseline{
		cap:          cfg.Cap,
		bootstrapMin: cfg.BootstrapMin,
		staticFloor:  cfg.StaticFloor,
		adaptiveCoef: cfg.AdaptiveCoef,
		path:         cfg.Path,
		saveDebounce: cfg.SaveDebounce,
	}
	if cfg.Path != "" {
		b.load()
	}
	return b
}

// Record adds an RMS value from a clip that successfully reached the
// transcript. ONLY accepted clips contribute — rejected clips would
// pull the baseline toward whatever was rejecting (mostly silence/noise),
// which would lower the floor and weaken the gate over time.
func (b *SpeakerBaseline) Record(rms float64) {
	if rms <= 0 {
		return
	}
	b.mu.Lock()
	b.samples = append(b.samples, rms)
	if len(b.samples) > b.cap {
		// FIFO drop oldest. Slice trim is fine; tiny copies.
		b.samples = b.samples[len(b.samples)-b.cap:]
	}
	b.dirty = true
	shouldSave := b.path != "" && time.Since(b.lastSavedAt) >= b.saveDebounce
	b.mu.Unlock()
	if shouldSave {
		b.persist()
	}
}

// Floor returns the current RMS threshold. During bootstrap (fewer than
// bootstrapMin samples) the static floor is returned. Otherwise the
// adaptive floor (median × coef).
func (b *SpeakerBaseline) Floor() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.samples) < b.bootstrapMin {
		return b.staticFloor
	}
	return b.medianLocked() * b.adaptiveCoef
}

// Snapshot returns a read-only view of current state. Used by
// /api/stream/status so the UI can show "baseline 280 / floor 154 /
// 47 samples / 12 dropped".
type SpeakerBaselineSnapshot struct {
	SampleCount    int     `json:"sample_count"`
	StaticFloor    float64 `json:"static_floor"`
	AdaptiveFloor  float64 `json:"adaptive_floor"` // 0 if still bootstrapping
	EffectiveFloor float64 `json:"effective_floor"`
	Median         float64 `json:"median"`
	IsAdaptive     bool    `json:"is_adaptive"`
}

func (b *SpeakerBaseline) Snapshot() SpeakerBaselineSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	snap := SpeakerBaselineSnapshot{
		SampleCount: len(b.samples),
		StaticFloor: b.staticFloor,
	}
	if len(b.samples) >= b.bootstrapMin {
		median := b.medianLocked()
		snap.Median = median
		snap.AdaptiveFloor = median * b.adaptiveCoef
		snap.EffectiveFloor = snap.AdaptiveFloor
		snap.IsAdaptive = true
	} else {
		snap.EffectiveFloor = b.staticFloor
	}
	return snap
}

// medianLocked computes the median of samples. Caller must hold mu.
// We sort a copy because samples is the source of truth and other
// callers expect insertion order to be preserved (so FIFO eviction
// behaves predictably).
func (b *SpeakerBaseline) medianLocked() float64 {
	if len(b.samples) == 0 {
		return 0
	}
	cp := make([]float64, len(b.samples))
	copy(cp, b.samples)
	sort.Float64s(cp)
	mid := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[mid]
	}
	return (cp[mid-1] + cp[mid]) / 2
}

// persist writes the current samples to disk. Called from Record after
// the debounce window elapses. Errors are logged once and not retried —
// a baseline that can't be persisted still works in memory; we just
// lose the calibration on restart, which is what we already had.
func (b *SpeakerBaseline) persist() {
	b.mu.Lock()
	if b.path == "" || !b.dirty {
		b.mu.Unlock()
		return
	}
	cp := make([]float64, len(b.samples))
	copy(cp, b.samples)
	b.dirty = false
	b.lastSavedAt = time.Now()
	path := b.path
	b.mu.Unlock()

	tmp := path + ".tmp"
	data, err := json.Marshal(map[string]any{
		"samples":  cp,
		"saved_at": time.Now().Format(time.RFC3339),
	})
	if err != nil {
		log.Printf("[speaker_baseline] marshal: %v", err)
		return
	}
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		log.Printf("[speaker_baseline] write %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		log.Printf("[speaker_baseline] rename %s → %s: %v", tmp, path, err)
	}
}

// load reads samples from disk if the file exists. Missing file is
// fine (first boot). Corrupt file is fine (we start fresh — bootstrap
// will re-populate).
func (b *SpeakerBaseline) load() {
	data, err := os.ReadFile(b.path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[speaker_baseline] read %s: %v", b.path, err)
		}
		return
	}
	var raw struct {
		Samples []float64 `json:"samples"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		log.Printf("[speaker_baseline] parse %s: %v — starting fresh", b.path, err)
		return
	}
	b.mu.Lock()
	if len(raw.Samples) > b.cap {
		raw.Samples = raw.Samples[len(raw.Samples)-b.cap:]
	}
	b.samples = raw.Samples
	b.lastSavedAt = time.Now()
	b.mu.Unlock()
	log.Printf("[speaker_baseline] loaded %d samples from %s", len(raw.Samples), b.path)
}

// Flush writes pending samples to disk regardless of debounce. Called on
// graceful shutdown if we wire it; safe to call any time.
func (b *SpeakerBaseline) Flush() {
	b.persist()
}
