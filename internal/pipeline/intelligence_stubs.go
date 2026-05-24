package pipeline

import (
	"hermes-listener/internal/storage"
)

// This file holds minimal stubs for intelligence-layer types that the
// capture pipeline references in field declarations or constructor
// signatures but never actually invokes in hermes-listener's
// capture-only configuration. Each stub is a no-op so the original
// ²nd-whisper-brain code compiles unchanged.
//
// If/when hermes-listener wants to grow an intelligence layer, replace
// these with real implementations (or, more likely, delete the
// references — Hermes is meant to own the intelligence layer here).

// AgentClient stub — original lived in pipeline/agents.go and held LLM
// model routing. Capture pipeline never invokes its methods.
type AgentClient struct{}

// ClaimStore stub — original interface lived in inline_extractor_stage.go.
// Defines what the (now-stubbed) InlineExtractorStage would have called.
// Capture pipeline never invokes anything on this.
type ClaimStore interface {
	// Intentionally empty — anyone needing real claim storage should
	// implement against their own interface, not this stub.
}

// PipelineJob is referenced by clarity_worker.go for re-queueing unclear
// audio. Hermes-listener's clarity_worker is wired to no-op (no recovery
// store), but the type is referenced in field declarations.
type PipelineJob = storage.PipelineJob

// Session is referenced by session_detector.go for persisting session
// metadata. Hermes-listener doesn't persist sessions to a DB.
type Session = storage.Session
