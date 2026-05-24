package pipeline

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"hermes-listener/internal/models"
	"hermes-listener/internal/storage"
)

// degradedSink is the singleton wired in api/server.go at startup. We
// keep it as a package-level pointer rather than threading it through
// every constructor so call-sites read like `Degraded(...)` and the
// surface stays light. Nil-safe: if the sink isn't initialized yet
// (very early boot), the call falls through to a log line.
var (
	degradedMu  sync.RWMutex
	degradedDB  *storage.Store
	degradedHub *Hub
)

// SetDegradedSink wires the persistent backend used by Degraded(). Called
// once from api/server.go after the store is initialized. Safe to call
// multiple times.
func SetDegradedSink(s *storage.Store, h *Hub) {
	degradedMu.Lock()
	defer degradedMu.Unlock()
	degradedDB = s
	degradedHub = h
}

// Degraded records a non-fatal "kept going but it wasn't great" event.
// component is the subsystem ("synthesizer", "speaker_filter", "mem0_push"
// etc). reason is short, stable, and machine-readable enough that the
// (component, reason) tuple can be deduped across many calls.
// payload is optional structured detail; pass anything JSON-marshalable.
// severity is one of "info", "warn", "error".
//
// Errors writing the row are themselves logged to the GlobalErrorLog
// rather than returned — observability must not be the cause of a fault.
func Degraded(component, reason, severity string, payload any) {
	degradedMu.RLock()
	store := degradedDB
	hub := degradedHub
	degradedMu.RUnlock()

	if store == nil {
		log.Printf("[degraded] %s/%s severity=%s (sink not initialized): %v",
			component, reason, severity, payload)
		return
	}

	var payloadJSON string
	if payload != nil {
		if b, err := json.Marshal(payload); err == nil {
			payloadJSON = string(b)
		}
	}

	id, err := store.RecordDegraded(component, reason, payloadJSON, severity, time.Minute)
	if err != nil {
		GlobalErrorLog.AddMessage("degraded_sink", err.Error())
		return
	}
	if hub != nil {
		hub.Broadcast(models.Event{
			Type: "degraded_event",
			Payload: map[string]any{
				"id":        id,
				"component": component,
				"reason":    reason,
				"severity":  severity,
			},
		})
	}
}
