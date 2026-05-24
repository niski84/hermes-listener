package pipeline

import (
	"sync"
	"time"
)

// ErrorEntry is a timestamped worker error kept in the ring buffer.
type ErrorEntry struct {
	Worker  string    `json:"worker"`
	Message string    `json:"message"`
	At      time.Time `json:"at"`
}

// ErrorLog is a thread-safe ring buffer of the last N pipeline errors.
// Workers call Add() instead of (or in addition to) log.Printf so errors
// are visible through /api/pipeline/status without reading log files.
type ErrorLog struct {
	mu      sync.Mutex
	entries []ErrorEntry
	cap     int
}

// NewErrorLog creates an ErrorLog that retains the last cap entries.
func NewErrorLog(cap int) *ErrorLog {
	if cap <= 0 {
		cap = 50
	}
	return &ErrorLog{cap: cap, entries: make([]ErrorEntry, 0, cap)}
}

// Add appends an error entry, dropping the oldest when the buffer is full.
func (el *ErrorLog) Add(worker string, err error) {
	if err == nil {
		return
	}
	el.AddMessage(worker, err.Error())
}

// AddMessage appends a plain-text error entry.
func (el *ErrorLog) AddMessage(worker, message string) {
	el.mu.Lock()
	defer el.mu.Unlock()
	if len(el.entries) >= el.cap {
		el.entries = el.entries[1:]
	}
	el.entries = append(el.entries, ErrorEntry{
		Worker:  worker,
		Message: message,
		At:      time.Now(),
	})
}

// Last returns the most recent n entries (or all if n > len).
func (el *ErrorLog) Last(n int) []ErrorEntry {
	el.mu.Lock()
	defer el.mu.Unlock()
	if n <= 0 || n >= len(el.entries) {
		out := make([]ErrorEntry, len(el.entries))
		copy(out, el.entries)
		return out
	}
	src := el.entries[len(el.entries)-n:]
	out := make([]ErrorEntry, len(src))
	copy(out, src)
	return out
}

// GlobalErrorLog is the shared error sink for all pipeline workers.
var GlobalErrorLog = NewErrorLog(50)
