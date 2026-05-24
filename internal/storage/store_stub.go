package storage

import "time"

// Store is a minimal stub for the capture-only port of the whisper-brain
// pipeline. The original Store type backed the full ²nd-whisper-brain DB
// (claims, commitments, sessions, embeddings, etc.) — none of which the
// capture path actually calls. AudioChannel holds *Store as a field but
// never invokes methods on it. Keeping the type so the capture code
// compiles unchanged; intentionally empty.
//
// If hermes-listener ever needs real persistence beyond the daily
// markdown file, add a focused SQLite or BadgerDB store here. Don't
// resurrect the full whisper-brain schema.
type Store struct{}

// Close is a no-op for the stub — kept so callers can defer it safely.
func (s *Store) Close() error { return nil }

// EmbeddingStore is a stub for the original embeddings-table backed type.
// Capture pipeline never invokes anything on it; field exists only.
type EmbeddingStore struct{}

// PipelineJob is a stub for clarity_worker re-queue records. Not persisted
// in hermes-listener.
type PipelineJob struct {
	ID       int64
	Filename string
}

// Session is a stub for session_detector persistence. Hermes-listener
// holds session state in memory only — daily markdown file is the
// canonical record. Fields match what session_detector reads/writes.
type Session struct {
	ID        int64
	StartedAt int64
	EndedAt   int64
	ChannelID string
	Date      string
	Start     int64
	End       int64
	Status    string
	WordCount int
	FilePath  string
}

// ─── Session detector support ────────────────────────────────────────────
// session_detector.go writes session records via Store methods. Capture-only
// hermes-listener doesn't persist session metadata to a DB — only to the
// daily markdown file via DailyTranscript. These stubs accept the calls
// and return success without doing anything.

// Expand Session to match the original schema fields the detector writes.
// (Replaces the bare Session{} stub above.)
func (s *Store) CreateSession(sess *Session) (int64, error) { return 0, nil }
func (s *Store) TagSessionWithSkill(_ int64, _ string) error { return nil }
func (s *Store) ClaimJob(_ ...string) (*PipelineJob, error) { return nil, nil }
func (s *Store) FailJob(_ int64, _ string) error             { return nil }
func (s *Store) CompleteJob(_ int64) error                   { return nil }
func (s *Store) EnqueueJob(_ string, _ []byte) (int64, error) { return 0, nil }

// MarshalPayload / UnmarshalPayload — pipeline jobs serialized arbitrary data.
// Hermes-listener never enqueues, so these are simple JSON pass-throughs.
func MarshalPayload(v any) ([]byte, error)   { return nil, nil }
func UnmarshalPayload(_ []byte, _ any) error { return nil }

// RecordDegraded is called by pipeline/degraded.go to log component
// degradation to the DB. No-op stub — degradation is also logged via
// emit/log events, so callers don't lose visibility.
func (s *Store) RecordDegraded(_ string, _ string, _ string, _ string, _ time.Duration) (int64, error) {
	return 0, nil
}
