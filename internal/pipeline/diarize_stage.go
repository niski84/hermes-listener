package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"mime/multipart"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"hermes-listener/internal/models"
)

// Segment-similarity cutoffs. Measured 2026-05-21 against real clips: the
// enrolled user's reliable (>=1.5s) speech segments score 0.56-0.71; the
// documented non-speaker range is -0.05-0.25. diarizeKeepThreshold sits in the
// wide empty gap between them; diarizeForeignFloor is the documented foreign
// ceiling, used only for short clips ECAPA can't score reliably.
const (
	diarizeKeepThreshold = 0.40
	diarizeForeignFloor  = 0.25
)

// Floating keep threshold (AS-norm style, user-side). A fixed cosine cutoff is
// brittle: the wearer's own voice scored 0.37 under a fixed 0.40 threshold and
// was wrongly dropped (2026-05-21) because mic placement / volume / vocal
// effort shift the absolute score. Instead, the keep threshold floats off the
// wearer's OWN recent confident-match scores, so it tracks those conditions.
const (
	diarizeCalibAdmitBar   = diarizeKeepThreshold // a reliable seg must score >= this to anchor calibration
	diarizeCalibWindowSize = 40                   // most-recent N anchor scores kept
	diarizeCalibMinSamples = 10                   // below this, fall back to the fixed threshold
	diarizeFloatK          = 2.0                  // floating threshold = mean - K*stddev of the window
	diarizeFloatFloor      = 0.25                 // the floating threshold never drops below this
)

// diarizePresenceWindow is how long a confident user/foreign verdict stays
// "recent" for the purpose of breaking ties on ambiguous short clips.
const diarizePresenceWindow = 20 * time.Second

// DiarizeFilterStage replaces the whole-clip VAD + speaker gate (RCA 041).
// Instead of one averaged embedding per clip — which TV/music drags past the
// cosine threshold — it asks the sidecar's /diarize endpoint to split the clip
// into speech segments and score each against the enrolled voice. The fusion
// rule is keep-biased (PROJECT-GOALS: dropping real user speech is the worst
// outcome): the clip is kept if ANY segment matches the user.
//
// Fail-open: sidecar unreachable / not enrolled / bad response → pass-through.
//
// Rollout is opt-in. DIARIZE_FILTER_ENABLED=true makes it enforce (and replace
// VADFilterStage + SpeakerFilterStage). DIARIZE_FILTER_SHADOW=true runs it
// observe-only — it scores and logs the verdict it WOULD reach but never drops.
type DiarizeFilterStage struct {
	SidecarURL    string
	Hub           *Hub
	KeepThreshold float64 // segment sim >= this = the user. Default 0.40.
	ForeignFloor  float64 // short-only clip below this = confidently foreign. Default 0.25.
	Shadow        bool    // observe-only: score + log, never drop
	Client        *http.Client

	once    sync.Once
	enabled atomic.Bool
	sem     chan struct{}

	// Temporal context: when the stage last reached a CONFIDENT verdict.
	// Used only to break ties on ambiguous short clips — never to override a
	// confident segment verdict.
	mu            sync.Mutex
	lastUserAt    time.Time
	lastForeignAt time.Time

	// calibWindow holds the wearer's recent confident-match similarities; the
	// floating keep threshold is derived from it.
	calibWindow []float64
}

// NewDiarizeFilterStage builds the stage with an HTTP client and the
// concurrency semaphore pre-wired. Thresholds may be overridden via the
// DIARIZE_KEEP_THRESHOLD / DIARIZE_FOREIGN_FLOOR env vars.
func NewDiarizeFilterStage(sidecarURL string, hub *Hub, shadow bool) *DiarizeFilterStage {
	d := &DiarizeFilterStage{
		SidecarURL: sidecarURL,
		Hub:        hub,
		Shadow:     shadow,
		Client:     &http.Client{Timeout: 12 * time.Second},
		sem:        make(chan struct{}, 1),
	}
	if v := envFloat("DIARIZE_KEEP_THRESHOLD", 0); v > 0 {
		d.KeepThreshold = v
	}
	if v := envFloat("DIARIZE_FOREIGN_FLOOR", 0); v > 0 {
		d.ForeignFloor = v
	}
	return d
}

type diarizeSegment struct {
	Start      float64  `json:"start"`
	End        float64  `json:"end"`
	Duration   float64  `json:"duration"`
	Similarity *float64 `json:"similarity"` // null when there is no enrollment
	Reliable   bool     `json:"reliable"`
}

type diarizeResponse struct {
	Speech      bool             `json:"speech"`
	TotalSecs   float64          `json:"total_seconds"`
	SpeechSecs  float64          `json:"speech_seconds"`
	SpeechRatio float64          `json:"speech_ratio"`
	Enrolled    bool             `json:"enrolled"`
	VADSkipped  bool             `json:"vad_skipped"`
	Segments    []diarizeSegment `json:"segments"`
}

func (d *DiarizeFilterStage) Name() string { return "diarize_filter" }

func (d *DiarizeFilterStage) keepThreshold() float64 {
	if d.KeepThreshold > 0 {
		return d.KeepThreshold
	}
	return diarizeKeepThreshold
}

func (d *DiarizeFilterStage) foreignFloor() float64 {
	if d.ForeignFloor > 0 {
		return d.ForeignFloor
	}
	return diarizeForeignFloor
}

func (d *DiarizeFilterStage) client() *http.Client {
	if d.Client != nil {
		return d.Client
	}
	d.Client = &http.Client{Timeout: 12 * time.Second}
	return d.Client
}

func (d *DiarizeFilterStage) semaphore() chan struct{} {
	if d.sem == nil {
		d.sem = make(chan struct{}, 1)
	}
	return d.sem
}

// emit writes a line to both the process log (→ logs/nogura.log, greppable)
// and the hub (→ SSE/UI). Hub-only emit is invisible to log inspection, which
// made the shadow stage impossible to observe — the whole point of shadow mode.
func (d *DiarizeFilterStage) emit(level, message string) {
	log.Print(message)
	if d.Hub != nil {
		d.Hub.Broadcast(models.Event{
			Type:    "log",
			Payload: models.LogEntry{Level: level, Message: message},
		})
	}
}

// init checks sidecar reachability + enrollment once. A dead sidecar must not
// cost a per-clip timeout, so this runs a single time and caches the result.
func (d *DiarizeFilterStage) init(ctx context.Context) {
	d.once.Do(func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.SidecarURL+"/status", nil)
		if err != nil {
			d.emit("warn", fmt.Sprintf("[diarize_filter] bad sidecar URL %q — stage disabled", d.SidecarURL))
			return
		}
		resp, err := d.client().Do(req)
		if err != nil {
			d.emit("warn", fmt.Sprintf("[diarize_filter] sidecar unreachable (%v) — stage is a no-op", err))
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			d.emit("warn", fmt.Sprintf("[diarize_filter] sidecar /status returned %d — stage disabled", resp.StatusCode))
			return
		}
		var st struct {
			Enrolled bool `json:"enrolled"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
			d.emit("warn", fmt.Sprintf("[diarize_filter] can't parse /status (%v) — stage disabled", err))
			return
		}
		if !st.Enrolled {
			d.emit("info", "[diarize_filter] no voice enrollment yet — stage is a pass-through")
			return
		}
		d.enabled.Store(true)
		mode := "enforcing"
		if d.Shadow {
			mode = "shadow (observe-only)"
		}
		d.emit("info", fmt.Sprintf("[diarize_filter] active — %s, keep>=%.2f", mode, d.keepThreshold()))
	})
}

func (d *DiarizeFilterStage) Process(ctx context.Context, clip *AudioClip) error {
	d.init(ctx)
	if !d.enabled.Load() || len(clip.PCM) == 0 {
		return nil
	}

	// Serialize /diarize calls — the sidecar embeds N segments per clip and is
	// single-threaded; overlapping bursts time out. Fail-open on contention.
	select {
	case d.semaphore() <- struct{}{}:
		defer func() { <-d.semaphore() }()
	case <-time.After(8 * time.Second):
		d.emit("warn", "[diarize_filter] semaphore timeout — skipping clip (fail-open)")
		return nil
	}

	resp, ok := d.callDiarize(ctx, clip.PCM)
	if !ok {
		return nil // fail-open
	}

	verdict, reason := d.fuse(resp, time.Now())
	d.stampMeta(clip, resp, verdict, reason)

	if d.Shadow {
		d.emit("info", fmt.Sprintf("[diarize_filter] SHADOW would-%s: %s", verdict, reason))
		return nil
	}
	if verdict == "drop" {
		clip.Dropped = true
		clip.DropReason = "diarize: " + reason
		// No Marker — silent drop, consistent with the VAD/speaker filters.
	}
	return nil
}

// fuse applies the keep-biased segment fusion rule. PROJECT-GOALS: dropping
// real user speech is the worst outcome. Reliable (>=1.5s) segments yield a
// confident verdict; short clips fall back to recent temporal context, and
// only an unambiguously-foreign short clip — or one arriving during a recent
// foreign context — is dropped.
func (d *DiarizeFilterStage) fuse(r *diarizeResponse, now time.Time) (verdict, reason string) {
	if !r.Speech {
		// Silence or music. Deliberately not recorded as foreign context: a
		// "no speech" clip is ambiguous between the user being quiet and the
		// TV playing music, and recording it would bias-drop the user's next
		// soft utterance.
		return "drop", "no sustained speech (silence or music)"
	}
	if !r.Enrolled {
		return "keep", "no enrollment — pass-through"
	}

	keep, keepDesc := d.floatingKeep()
	const unset = -2.0
	bestReliable, bestAny := unset, unset
	hasReliable := false
	for _, seg := range r.Segments {
		if seg.Similarity == nil {
			continue
		}
		s := *seg.Similarity
		if s > bestAny {
			bestAny = s
		}
		if seg.Reliable {
			hasReliable = true
			if s > bestReliable {
				bestReliable = s
			}
		}
	}

	// Speech present but nothing was scored — can't judge identity; keep-biased.
	if bestAny == unset {
		return "keep", "speech present but no scored segments — pass-through"
	}

	// A reliable (>=1.5s) segment is ECAPA's trustworthy zone — a confident
	// verdict, which also sets the temporal context for later short clips.
	if hasReliable {
		if bestReliable >= keep {
			d.recordPresence(now, true)
			d.recordCalib(bestReliable)
			return "keep", fmt.Sprintf("user matched (reliable seg sim=%.2f >= keep %s)", bestReliable, keepDesc)
		}
		d.recordPresence(now, false)
		return "drop", fmt.Sprintf("foreign voice (best reliable seg sim=%.2f < keep %s)", bestReliable, keepDesc)
	}

	// Short-only clip: ECAPA is noisy here (RCA 036).
	if bestAny >= keep {
		d.recordPresence(now, true)
		return "keep", fmt.Sprintf("user matched (short seg sim=%.2f >= keep %s)", bestAny, keepDesc)
	}
	if bestAny < d.foreignFloor() {
		d.recordPresence(now, false)
		return "drop", fmt.Sprintf("short clip, foreign (best sim=%.2f < %.2f)", bestAny, d.foreignFloor())
	}

	// Genuinely ambiguous: a short clip scoring between the foreign floor and
	// the keep threshold. ECAPA can't call it — break the tie with recent
	// context. If the room was just confirmed foreign, this is likely more of
	// it; if the user was just here (or there is no recent context), keep.
	switch d.recentContext(now) {
	case "foreign":
		return "drop", fmt.Sprintf("short clip, ambiguous (sim=%.2f) — recent foreign context", bestAny)
	case "user":
		return "keep", fmt.Sprintf("short clip, ambiguous (sim=%.2f) — recent user presence", bestAny)
	default:
		return "keep", fmt.Sprintf("short clip, ambiguous (sim=%.2f) — keep-biased, no recent context", bestAny)
	}
}

// floatingKeep returns the current keep threshold and a short descriptor for
// logging. It floats K standard deviations below the mean of the wearer's
// recent confident-match scores, clamped to [diarizeFloatFloor, fixed default].
// It can only ever LOOSEN below the fixed default, never tighten above it —
// keep-biased. Until diarizeCalibMinSamples anchors exist it returns the fixed
// default so a cold start never wrongly drops the wearer.
func (d *DiarizeFilterStage) floatingKeep() (float64, string) {
	fixed := d.keepThreshold()
	d.mu.Lock()
	w := append([]float64(nil), d.calibWindow...)
	d.mu.Unlock()

	if len(w) < diarizeCalibMinSamples {
		return fixed, fmt.Sprintf("fixed %.2f (calibrating %d/%d)", fixed, len(w), diarizeCalibMinSamples)
	}
	var sum float64
	for _, v := range w {
		sum += v
	}
	mean := sum / float64(len(w))
	var sq float64
	for _, v := range w {
		sq += (v - mean) * (v - mean)
	}
	std := math.Sqrt(sq / float64(len(w)))

	thr := mean - diarizeFloatK*std
	if thr > fixed {
		thr = fixed
	}
	if thr < diarizeFloatFloor {
		thr = diarizeFloatFloor
	}
	return thr, fmt.Sprintf("float %.2f (n=%d mean=%.2f sd=%.2f)", thr, len(w), mean, std)
}

// recordCalib anchors the calibration window with a confident user-match
// score. Only clearly-the-user reliable segments are admitted, so background
// audio cannot drag the floating threshold down.
func (d *DiarizeFilterStage) recordCalib(sim float64) {
	if sim < diarizeCalibAdmitBar {
		return
	}
	d.mu.Lock()
	d.calibWindow = append(d.calibWindow, sim)
	if len(d.calibWindow) > diarizeCalibWindowSize {
		d.calibWindow = d.calibWindow[len(d.calibWindow)-diarizeCalibWindowSize:]
	}
	d.mu.Unlock()
}

// recordPresence stamps the time of a confident user or foreign verdict.
func (d *DiarizeFilterStage) recordPresence(now time.Time, user bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if user {
		d.lastUserAt = now
	} else {
		d.lastForeignAt = now
	}
}

// recentContext reports whether the most recent confident verdict within the
// presence window was "user" or "foreign" — or "" if neither is recent.
func (d *DiarizeFilterStage) recentContext(now time.Time) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	userRecent := !d.lastUserAt.IsZero() && now.Sub(d.lastUserAt) <= diarizePresenceWindow
	foreignRecent := !d.lastForeignAt.IsZero() && now.Sub(d.lastForeignAt) <= diarizePresenceWindow
	switch {
	case userRecent && foreignRecent:
		if d.lastUserAt.After(d.lastForeignAt) {
			return "user"
		}
		return "foreign"
	case userRecent:
		return "user"
	case foreignRecent:
		return "foreign"
	default:
		return ""
	}
}

func (d *DiarizeFilterStage) stampMeta(clip *AudioClip, r *diarizeResponse, verdict, reason string) {
	if clip.Meta == nil {
		clip.Meta = map[string]any{}
	}
	clip.Meta["diarize_speech"] = r.Speech
	clip.Meta["diarize_speech_ratio"] = r.SpeechRatio
	clip.Meta["diarize_segments"] = len(r.Segments)
	clip.Meta["diarize_verdict"] = verdict
	clip.Meta["diarize_reason"] = reason
	if d.Shadow {
		clip.Meta["diarize_shadow"] = true
	}
}

// callDiarize POSTs the clip's PCM as a WAV multipart upload to /diarize.
// Returns (nil, false) on any failure so the caller can fail-open.
func (d *DiarizeFilterStage) callDiarize(ctx context.Context, pcm []byte) (*diarizeResponse, bool) {
	wav := pcmToWAV(pcm)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "audio.wav")
	if err != nil {
		return nil, false
	}
	if _, err = io.Copy(fw, bytes.NewReader(wav)); err != nil {
		return nil, false
	}
	mw.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.SidecarURL+"/diarize", &buf)
	if err != nil {
		return nil, false
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := d.client().Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return nil, false
	}

	var out diarizeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, false
	}
	return &out, true
}
