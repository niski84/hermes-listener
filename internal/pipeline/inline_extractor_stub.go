package pipeline

import "context"

// InlineExtractorStage is intentionally stubbed in hermes-listener.
// See intelligence_stubs.go for the broader rationale.
//
// Fields mirror the original so channel_manager.go's struct literal
// compiles. The fields are never read because the channel checks
// ch.inlineExtractor != nil only in the original; here we still construct
// one but Process() is a no-op so it's effectively dead weight.
type InlineExtractorStage struct {
	Agents      *AgentClient
	Store       interface{} // was *storage.Store; intentionally typed wide
	Embeds      interface{} // was *storage.EmbeddingStore
	VaultDir    string
	Enabled     bool
	sessionIDFn func() int64
}

// Name satisfies any caller that probes for AudioStage.Name().
func (s *InlineExtractorStage) Name() string { return "inline_extractor (stubbed)" }

// Process satisfies the AudioStage interface — deliberate no-op.
func (s *InlineExtractorStage) Process(_ context.Context, _ *AudioClip) error {
	return nil
}
