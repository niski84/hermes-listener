package models

import "time"

type Status string

const (
	StatusPending      Status = "pending"
	StatusTranscribing Status = "transcribing"
	StatusProcessing   Status = "processing"
	StatusDone         Status = "done"
	StatusError        Status = "error"
	// StatusFiltered marks a recovered audio clip that the
	// pre-Whisper RMS+VAD guard dropped (see worker.go). Stored in
	// the chunks table so the next startup's recoverOrphanedWAVs
	// scan knows to skip the file — without this, we'd re-feed the
	// same hallucinations to Whisper on every restart.
	StatusFiltered Status = "filtered"
)

type Chunk struct {
	ID          string    `json:"id"`
	Filename    string    `json:"filename"`
	AudioPath   string    `json:"audio_path"`
	CreatedAt   time.Time `json:"created_at"`
	Status      Status    `json:"status"`
	RawText     string    `json:"raw_text,omitempty"`
	Summary     string    `json:"summary,omitempty"`
	Keywords    []string  `json:"keywords,omitempty"`
	ActionItems []string  `json:"action_items,omitempty"`
	ErrorMsg    string    `json:"error_msg,omitempty"`

	// IsRecovered is true when this chunk was sourced from the
	// crash-recovery scan or a pre-existing ingest file rather than
	// the live recorder. Used by the worker pool to (a) emit a
	// transcript_backfill SSE event instead of transcript_append, so
	// the live UI doesn't show stale lines as if just spoken, and
	// (b) gate the clip through the pre-Whisper RMS+VAD filter so
	// hallucinations don't leak through. Not persisted in the chunks
	// table — derived from filename / startup origin at runtime.
	IsRecovered bool `json:"is_recovered,omitempty"`
}

type AgentResult struct {
	Summary     string   `json:"summary"`
	Keywords    []string `json:"keywords"`
	ActionItems []string `json:"action_items"`
}

type Event struct {
	Seq     uint64 `json:"seq"`
	ID      string `json:"id"`
	Type    string `json:"type"`
	Payload any    `json:"payload"`
	TS      int64  `json:"ts"`
}

type LogEntry struct {
	Level   string `json:"level"`
	Message string `json:"message"`
	ChunkID string `json:"chunk_id,omitempty"`
}

type Stats struct {
	Total      int `json:"total"`
	Done       int `json:"done"`
	Errors     int `json:"errors"`
	QueueDepth int `json:"queue_depth"`
}

// ClaimEvidence is a pin from a claim back to the raw transcript line that
// produced it. Block IDs look like "t140322" matching the ^anchors in
// 90-Raw session files, so the UI can wikilink straight to the source.
type ClaimEvidence struct {
	BlockID string `json:"block_id"`
	Quote   string `json:"quote"`
}

// Claim is a single structured extraction from a session, carrying provenance
// (Evidence), a sincerity tag (so jokes/hypotheticals don't get treated as
// truth), and a temporal tag (so evolving commitments aren't frozen in place).
//
// Sincerity values:   direct | hypothetical | sarcastic | third-party | uncertain
// Temporal values:    stable   | evolving     | pending   | resolved
// Type values:        preference | commitment | belief | person | project | decision
// MemoryTier values:  lasting | session | transient
type Claim struct {
	ID          int64           `json:"id"`
	SessionID   int64           `json:"session_id,omitempty"`
	Type        string          `json:"type"`
	Entity      string          `json:"entity,omitempty"`
	Claim       string          `json:"claim"`
	Evidence    []ClaimEvidence `json:"evidence,omitempty"`
	Sincerity   string          `json:"sincerity"`
	Temporal    string          `json:"temporal"`
	Confidence  float64         `json:"confidence"`
	Status      string          `json:"status"`
	RevisedFrom *int64          `json:"revised_from,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	// MemoryTier classifies the expected lifetime of this claim:
	//   "lasting"   — long-term preference or identity fact unlikely to change
	//   "session"   — relevant only to this work session
	//   "transient" — one-off clarification or noise
	MemoryTier string `json:"memory_tier,omitempty"`

	// Proposal 002 — commitment fulfillment. Set when an auto- or
	// manually-triggered actor marks the commitment done. Nil/empty
	// = still open. Surfaces these on the Commitments UI.
	FulfilledAt       *time.Time `json:"fulfilled_at,omitempty"`
	FulfilledEvidence string     `json:"fulfilled_evidence,omitempty"`
	FulfilledBy       string     `json:"fulfilled_by,omitempty"` // 'auto:fulfillment' | 'user:web'
}

// Dispatch is the audit record for one skill dispatch. The payload_json is
// the full Payload sent to the skill (so we can re-dispatch or debug what
// the context router picked). response_status/body/error capture what came
// back; success = response_status >= 200 && < 300 && error_msg == "".
type Dispatch struct {
	ID             int64     `json:"id"`
	WorkItemID     string    `json:"work_item_id"`
	SessionID      int64     `json:"session_id,omitempty"`
	SkillName      string    `json:"skill_name"`
	PayloadJSON    string    `json:"payload_json"`
	ResponseStatus int       `json:"response_status,omitempty"`
	ResponseBody   string    `json:"response_body,omitempty"`
	ErrorMsg       string    `json:"error_msg,omitempty"`
	DispatchedAt   time.Time `json:"dispatched_at"`
}
