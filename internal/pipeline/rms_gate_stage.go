package pipeline

import (
	"context"
	"fmt"
	"sync/atomic"
)

// RMSGateStage is the upstream guard that decides whether a clip is
// loud enough to be the user's voice before we spend GPU cycles on
// Whisper. Lives between SpeakerFilterStage (or NoiseSuppressStage on
// non-mic channels) and TranscribeStage. See RCA 011.
//
// Why upstream of Whisper rather than in ClassifyStage (which is where
// RCA 008's static floor lives): once Whisper runs on quiet audio it
// emits hallucinations like "Thank you." or "Okay." Even if Classify
// drops them on the way out, we've still:
//
//   - paid the GPU cost on a clip that was always going to be garbage
//   - briefly emitted the hallucinated text into the pipeline (any
//     stage between Transcribe and Classify could observe it)
//
// The gate moves the guard upstream so Whisper never sees the clip.
// The downstream RCA 008 floor stays as a belt-and-suspenders safety
// net for clips that pass this gate but turn out to be hallucinations
// anyway (rare, but the cost is one regex check).
type RMSGateStage struct {
	Baseline *SpeakerBaseline

	// drops counts clips that this stage rejected. Read by the channel
	// to expose `rms_gate_dropped_count` on /api/stream/status.
	drops atomic.Int64
}

func (g *RMSGateStage) Name() string { return "rms_gate" }

func (g *RMSGateStage) Process(_ context.Context, clip *AudioClip) error {
	// clip.RMS == 0 means the upstream stages didn't measure (test
	// clips, edge WS path). Don't gate — let downstream handle it.
	if clip.RMS <= 0 {
		return nil
	}
	if g.Baseline == nil {
		return nil
	}
	floor := g.Baseline.Floor()
	if clip.RMS < floor {
		clip.Dropped = true
		clip.DropReason = fmt.Sprintf("rms_gate: %.0f < floor %.0f (baseline samples=%d)", clip.RMS, floor, g.Baseline.Snapshot().SampleCount)
		// No marker — don't pollute the transcript with "noise".
		g.drops.Add(1)
		return nil
	}
	return nil
}

// Drops returns the lifetime count of clips this stage rejected.
// Reset on process restart; intentionally not persisted (we want it
// to reflect "since boot" so a sudden uptick is visible).
func (g *RMSGateStage) Drops() int64 {
	return g.drops.Load()
}
