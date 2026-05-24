package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"hermes-listener/internal/pipeline/transcribepool"
	"hermes-listener/internal/storage"
)

// channelRecord is the on-disk representation of a non-default channel.
type channelRecord struct {
	ID     string        `json:"id"`
	Name   string        `json:"name"`
	Type   ChannelType   `json:"type"`
	Config ChannelConfig `json:"config"`
}

// ChannelSpec is the user-supplied payload for adding a new channel.
type ChannelSpec struct {
	ID     string        `json:"id"`
	Name   string        `json:"name"`
	Type   ChannelType   `json:"type"`
	Config ChannelConfig `json:"config"`
}

// ChannelManager owns all AudioChannel instances. The "default" mic channel is
// always present and cannot be removed. Additional channels (RTSP, etc.) are
// added via Add() and persisted to data/channels.json across restarts.
type ChannelManager struct {
	mu      sync.RWMutex
	channels map[string]*AudioChannel

	// shared resources injected into each new channel
	whisperURL string
	audioDir   string
	hub        *Hub
	transcript *storage.DailyTranscript
	store      *storage.Store
	vaultDir   string
	dataDir    string

	// Optional TV chatter filter wiring — see TVChatFilterStage.
	plexDashboardURL  string
	tvFilterThreshold float64
	rootCtx           context.Context

	// mediaSignalThreshold is passed to MediaSignalStage in each channel's
	// buildPipeline. Default 0.6 means clips scoring below 0.6 media_confidence
	// are annotated with [~media?] but never dropped.
	mediaSignalThreshold float64

	// smartTurnClient, if non-nil, is forwarded to every new AudioChannel so
	// ClassifyStage can gate wake-word clips on turn completeness.
	smartTurnClient *SmartTurnClient

	// inlineExtractorCfg, if non-nil, is passed to each new channel's
	// InlineExtractorStage. Set via ConfigureInlineExtractor.
	inlineExtractorCfg *inlineExtractorConfig

	persistPath string // path to channels.json

	// pool, if non-nil, is the shared TranscribePool every channel submits
	// utterances into. Wired in by SetTranscribePool after construction so
	// the pool can be assembled lazily once Whisper config is known.
	pool *transcribepool.Pool
}

// SetTranscribePool wires the shared transcribe worker pool into this manager
// and into every existing channel. Channels created later inherit it via
// newAudioChannel.
func (m *ChannelManager) SetTranscribePool(p *transcribepool.Pool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pool = p
	for _, ch := range m.channels {
		ch.pool = p
	}
}

// NewChannelManager creates a ChannelManager and wires the default mic channel.
// defaultDevice is the PulseAudio device name for the mic channel (e.g. "default").
func NewChannelManager(
	defaultDevice string,
	whisperURL, audioDir, vaultDir, dataDir string,
	hub *Hub,
	transcript *storage.DailyTranscript,
	store *storage.Store,
) *ChannelManager {
	mgr := &ChannelManager{
		channels:    make(map[string]*AudioChannel),
		whisperURL:  whisperURL,
		audioDir:    audioDir,
		hub:         hub,
		transcript:  transcript,
		store:       store,
		vaultDir:    vaultDir,
		dataDir:     dataDir,
		rootCtx:     context.Background(),
		persistPath: filepath.Join(dataDir, "channels.json"),
	}
	mgr.addDefault(defaultDevice)
	mgr.loadPersisted()
	return mgr
}

// ConfigureTVFilter sets the plex-dashboard URL and match threshold used by
// any mic channel that has EnableTVFilter=true. Call this after construction
// and before any channel goes through buildPipeline (i.e. before Add()).
// Existing channels — including the default mic — are rebuilt so the new
// settings take effect.
func (m *ChannelManager) ConfigureTVFilter(plexDashboardURL string, threshold float64, ctx context.Context) {
	m.mu.Lock()
	m.plexDashboardURL = plexDashboardURL
	m.tvFilterThreshold = threshold
	if ctx != nil {
		m.rootCtx = ctx
	}
	// Re-stamp any already-constructed channels so their pipelines pick up
	// the TV filter settings if they opted in.
	for _, ch := range m.channels {
		ch.plexDashboardURL = plexDashboardURL
		ch.tvFilterThreshold = threshold
		ch.tvFilterParentCtx = m.rootCtx
		ch.preStages, ch.postStages = ch.buildPipeline()
	}
	m.mu.Unlock()
}

// ConfigureMediaSignal sets the media_confidence threshold for the
// MediaSignalStage wired into every channel's post-transcribe pipeline.
// Clips that score below threshold are annotated with [~media?] in the
// transcript but are never dropped. Call this after ConfigureTVFilter so
// the pipeline rebuild carries both settings.
func (m *ChannelManager) ConfigureMediaSignal(threshold float64) {
	m.mu.Lock()
	m.mediaSignalThreshold = threshold
	for _, ch := range m.channels {
		ch.mediaSignalThreshold = threshold
		ch.preStages, ch.postStages = ch.buildPipeline()
	}
	m.mu.Unlock()
}

// inlineExtractorConfig holds the shared dependencies for InlineExtractorStage
// across all channels. Set once via ConfigureInlineExtractor.
type inlineExtractorConfig struct {
	agents      *AgentClient
	store       ClaimStore
	embeds      *storage.EmbeddingStore
	vaultDir    string
	enabled     bool
	sessionIDFn func() int64 // returns the most recent session ID from the DB
}

// ConfigureInlineExtractor wires the InlineExtractorStage into all existing
// and future channels. Set enabled=true to activate (opt-in — doubles LLM
// calls per clip). Passing enabled=false installs the stage in disabled mode
// so it can be turned on at runtime without rebuilding pipelines.
//
// sessionIDFn is called per-clip to resolve the session ID to tag claims with.
// Typically a closure that calls store.ListSessions(1) — the most recent
// closed session is a good proxy for the live session (which has no DB record
// until it closes). If nil, inline extraction silently skips clips with no
// resolvable session.
func (m *ChannelManager) ConfigureInlineExtractor(agents *AgentClient, store ClaimStore, embeds *storage.EmbeddingStore, vaultDir string, enabled bool, sessionIDFn func() int64) {
	m.mu.Lock()
	m.inlineExtractorCfg = &inlineExtractorConfig{
		agents:      agents,
		store:       store,
		embeds:      embeds,
		vaultDir:    vaultDir,
		enabled:     enabled,
		sessionIDFn: sessionIDFn,
	}
	for _, ch := range m.channels {
		ch.inlineExtractor = &InlineExtractorStage{
			Agents:      agents,
			Store:       store,
			Embeds:      embeds,
			VaultDir:    vaultDir,
			Enabled:     enabled,
			sessionIDFn: sessionIDFn,
		}
		ch.preStages, ch.postStages = ch.buildPipeline()
	}
	m.mu.Unlock()
}

// SetSmartTurnClient wires a SmartTurnClient into all existing and future
// channels. Pass nil to disable smart-turn gating (the pipeline falls back
// to VAD-only turn detection, the behaviour before this feature existed).
func (m *ChannelManager) SetSmartTurnClient(c *SmartTurnClient) {
	m.mu.Lock()
	m.smartTurnClient = c
	for _, ch := range m.channels {
		ch.smartTurnClient = c
		ch.preStages, ch.postStages = ch.buildPipeline()
	}
	m.mu.Unlock()
}

// addDefault wires the built-in mic channel with id "default". When the
// env var MIC_ENABLE_TV_FILTER=1 is set, the TV chatter filter activates
// on the default channel — clips that match current Plex captions are
// dropped before Whisper runs. Reduces "fairy story" / movie-dialogue
// hallucinations leaking from background TV.
func (m *ChannelManager) addDefault(device string) {
	cfg := ChannelConfig{Device: device}
	if v := os.Getenv("MIC_ENABLE_TV_FILTER"); v == "1" || v == "true" {
		cfg.EnableTVFilter = true
	}
	ch := m.newAudioChannel(ChannelSpec{
		ID:     "default",
		Name:   "Microphone",
		Type:   ChannelTypeMic,
		Config: cfg,
	})
	m.channels["default"] = ch
}

// Add registers a new channel and persists it. Returns an error if the ID
// is already taken or the spec is invalid.
func (m *ChannelManager) Add(spec ChannelSpec) error {
	if spec.ID == "" {
		return fmt.Errorf("channel ID is required")
	}
	if spec.Type == "" {
		return fmt.Errorf("channel type is required")
	}
	if spec.Type == ChannelTypeRTSP && spec.Config.URL == "" {
		return fmt.Errorf("RTSP channel requires a URL")
	}
	if spec.Type == ChannelTypeMic && spec.Config.Device == "" {
		spec.Config.Device = "default"
	}
	// Edge channels need no extra config — they're passive WebSocket listeners.

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.channels[spec.ID]; exists {
		return fmt.Errorf("channel %q already exists", spec.ID)
	}
	ch := m.newAudioChannel(spec)
	m.channels[spec.ID] = ch
	m.persist()
	log.Printf("[channels] added %s channel %q (%s)", spec.Type, spec.ID, spec.Name)
	return nil
}

// Remove stops and deletes a channel. The "default" channel cannot be removed.
func (m *ChannelManager) Remove(id string) error {
	if id == "default" {
		return fmt.Errorf("cannot remove the default mic channel")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	ch, ok := m.channels[id]
	if !ok {
		return fmt.Errorf("channel %q not found", id)
	}
	_ = ch.Stop() // ignore "not running" error
	// Force-close any open session before removing.
	if ch.detector != nil {
		ch.detector.ManualClose()
	}
	delete(m.channels, id)
	m.persist()
	log.Printf("[channels] removed channel %q", id)
	return nil
}

// StartChannel starts the named channel's audio capture.
func (m *ChannelManager) StartChannel(id string) error {
	m.mu.RLock()
	ch, ok := m.channels[id]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("channel %q not found", id)
	}
	return ch.Start()
}

// StopChannel stops the named channel's audio capture.
func (m *ChannelManager) StopChannel(id string) error {
	m.mu.RLock()
	ch, ok := m.channels[id]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("channel %q not found", id)
	}
	return ch.Stop()
}

// Get returns the named channel or nil.
func (m *ChannelManager) Get(id string) *AudioChannel {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.channels[id]
}

// List returns a status snapshot of every channel, default first.
func (m *ChannelManager) List() []ChannelStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]ChannelStatus, 0, len(m.channels))
	// Default first, then alphabetical.
	if ch, ok := m.channels["default"]; ok {
		out = append(out, ch.Status())
	}
	for id, ch := range m.channels {
		if id == "default" {
			continue
		}
		out = append(out, ch.Status())
	}
	return out
}

// CloseAllSessions force-closes the active session on every channel's
// SessionDetector. Returns the number of detectors that actually had an
// open session (i.e. ManualClose returned true). Safe to call when no
// session is active anywhere — returns 0 in that case.
//
// This is the per-channel replacement for the legacy global
// detector.ManualClose() invoked by /api/sessions/close. See fix D.
func (m *ChannelManager) CloseAllSessions() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	closed := 0
	for _, ch := range m.channels {
		if ch.detector == nil {
			continue
		}
		if ch.detector.ManualClose() {
			closed++
		}
	}
	return closed
}

// ReloadVocab re-reads data/vocab.txt (via loadVocabHints) and pushes
// the resulting hint list into every live channel's ContextPrompt. The
// swap is atomic per channel — utterances already mid-flight in the
// transcribe pool keep the old prompt; the next utterance picks up the
// new list. Returns the number of channels that were updated.
//
// Called by POST/PUT/DELETE /api/vocab handlers after the file is
// rewritten so the user gets immediate effect without restarting a
// channel.
func (m *ChannelManager) ReloadVocab() int {
	hints := loadVocabHints()
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := 0
	for _, ch := range m.channels {
		if ch.promptCtx != nil {
			ch.promptCtx.SetHints(hints)
			n++
		}
		// transcribePromptCtx is currently aliased to promptCtx in
		// buildPipeline, but guard for the future where they diverge.
		if ch.transcribePromptCtx != nil && ch.transcribePromptCtx != ch.promptCtx {
			ch.transcribePromptCtx.SetHints(hints)
		}
	}
	return n
}

// DefaultChannel returns the built-in mic channel.
func (m *ChannelManager) DefaultChannel() *AudioChannel {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.channels["default"]
}

// ActiveEdgeChannelID returns the ID of the first running edge-type channel,
// or an empty string if none exists or none is running.
func (m *ChannelManager) ActiveEdgeChannelID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, ch := range m.channels {
		if ch.Type == ChannelTypeEdge && ch.IsRunning() {
			return id
		}
	}
	return ""
}

// EnsureEdgeChannel ensures the edge channel exists and is started.
// Safe to call multiple times — idempotent.
func (m *ChannelManager) EnsureEdgeChannel() {
	m.mu.RLock()
	var found *AudioChannel
	for _, ch := range m.channels {
		if ch.Type == ChannelTypeEdge {
			found = ch
			break
		}
	}
	m.mu.RUnlock()

	if found == nil {
		spec := ChannelSpec{ID: "edge", Name: "Remote Mic", Type: ChannelTypeEdge}
		if err := m.Add(spec); err != nil {
			log.Printf("[channels] EnsureEdgeChannel add: %v", err)
			return
		}
	}

	// Start it if it was restored from disk but not yet started.
	if err := m.StartChannel("edge"); err != nil {
		// "already running" is not an error here.
		if err.Error() != fmt.Sprintf("channel %q already running", "edge") {
			log.Printf("[channels] EnsureEdgeChannel start: %v", err)
		}
	}
}

// ── Internal ──────────────────────────────────────────────────────────────────

// newAudioChannel constructs an AudioChannel from a spec and creates its
// dedicated SessionDetector. The detector is started immediately.
func (m *ChannelManager) newAudioChannel(spec ChannelSpec) *AudioChannel {
	ch := &AudioChannel{
		ID:                spec.ID,
		Name:              spec.Name,
		Type:              spec.Type,
		Config:            spec.Config,
		whisperURL:        m.whisperURL,
		audioDir:          m.audioDir,
		hub:               m.hub,
		transcript:        m.transcript,
		store:             m.store,
		vaultDir:          m.vaultDir,
		dataDir:           m.dataDir,
		plexDashboardURL:     m.plexDashboardURL,
		tvFilterThreshold:    m.tvFilterThreshold,
		tvFilterParentCtx:    m.rootCtx,
		mediaSignalThreshold: m.mediaSignalThreshold,
		pool:                 m.pool,
		smartTurnClient:      m.smartTurnClient,
	}
	// Wire inline extractor if configured.
	if cfg := m.inlineExtractorCfg; cfg != nil {
		ch.inlineExtractor = &InlineExtractorStage{
			Agents:      cfg.agents,
			Store:       cfg.store,
			Embeds:      cfg.embeds,
			VaultDir:    cfg.vaultDir,
			Enabled:     cfg.enabled,
			sessionIDFn: cfg.sessionIDFn,
		}
	}
	ch.preStages, ch.postStages = ch.buildPipeline()

	// Per-channel SessionDetector. Each channel gets its own session gap timer
	// and session files. The detector filters hub events by channel_id.
	if m.store != nil {
		det := newSessionDetectorForChannel(ch.ID, m.hub, m.store, m.audioDir, m.vaultDir, m.dataDir)
		ch.detector = det
		det.Start()
	}
	return ch
}

// persist writes non-default channels to data/channels.json.
// Must be called with m.mu held (write lock).
func (m *ChannelManager) persist() {
	var records []channelRecord
	for id, ch := range m.channels {
		if id == "default" {
			continue // built-in, always recreated from env config
		}
		records = append(records, channelRecord{
			ID:     ch.ID,
			Name:   ch.Name,
			Type:   ch.Type,
			Config: ch.Config,
		})
	}
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		log.Printf("[channels] persist marshal: %v", err)
		return
	}
	tmp := m.persistPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		log.Printf("[channels] persist write: %v", err)
		return
	}
	if err := os.Rename(tmp, m.persistPath); err != nil {
		log.Printf("[channels] persist rename: %v", err)
	}
}

// loadPersisted reads data/channels.json and adds any saved channels.
// Called once at startup after addDefault.
func (m *ChannelManager) loadPersisted() {
	data, err := os.ReadFile(m.persistPath)
	if err != nil {
		return // no file yet — normal on first run
	}
	var records []channelRecord
	if err := json.Unmarshal(data, &records); err != nil {
		log.Printf("[channels] load persisted: invalid JSON: %v", err)
		return
	}
	for _, r := range records {
		spec := ChannelSpec{ID: r.ID, Name: r.Name, Type: r.Type, Config: r.Config}
		ch := m.newAudioChannel(spec)
		m.channels[r.ID] = ch
		log.Printf("[channels] restored %s channel %q from disk", r.Type, r.ID)
	}
}
