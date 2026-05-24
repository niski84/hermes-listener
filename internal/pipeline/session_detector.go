package pipeline

import (
	"hermes-listener/internal/storage"
)

// SessionDetector is a stub in hermes-listener.
//
// In ²nd-whisper-brain this watched transcript_append events, grouped
// lines into "sessions" by silence gap, wrote per-session markdown
// files, and enqueued background pipeline jobs for summarization /
// extraction. All of that is intelligence-layer work that hermes-listener
// hands off to the consuming agent (Hermes reads the daily file directly
// and can do its own segmentation if needed).
//
// We keep the type so audio_channel.go's `*SessionDetector` field
// compiles unchanged. Constructor returns nil — channel code checks for
// nil before invoking detector methods.
type SessionDetector struct{}

// NewSessionDetector returns nil — capture path never needs a real one.
func NewSessionDetector(hub *Hub, store *storage.Store, audioDir, vaultDir string) *SessionDetector {
	return nil
}

// newSessionDetectorForChannel is referenced by channel_manager.go.
// Returns nil — same reasoning as above.
func newSessionDetectorForChannel(channelID string, hub *Hub, store *storage.Store, audioDir, vaultDir, dataDir string) *SessionDetector {
	return nil
}

// Start / Stop are no-ops. Real detector has goroutine + DB writes.
func (d *SessionDetector) Start() {}
func (d *SessionDetector) Stop()  {}

// ManualClose was called by channel_manager.go on shutdown to flush any
// open session. With detector=nil, channel_manager checks before calling,
// so this is defensive — kept for completeness when constructed.
func (d *SessionDetector) ManualClose() bool { return false }
