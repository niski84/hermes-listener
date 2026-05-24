package pipeline

// SimpleSnapshotter is the interface MediaSignalStage depends on for Plex
// caption data. PlexCaptionsClient satisfies it via its Snapshot() shim;
// tests can substitute a stub without needing an HTTP server.
//
// Snapshot returns the current trigram set and movie title. ok=false means
// nothing is playing or the source is unavailable — the caller treats that
// as fail-open (no penalty applied).
type SimpleSnapshotter interface {
	Snapshot() (grams map[string]struct{}, title string, ok bool)
}
