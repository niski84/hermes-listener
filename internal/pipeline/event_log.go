package pipeline

import "hermes-listener/internal/models"

// eventRing is a fixed-capacity ring buffer over models.Event keyed by Seq.
// Older events fall off when capacity is reached. Caller must hold the hub
// mutex; the ring itself is not internally synchronized.
type eventRing struct {
	buf    []models.Event
	cap    int
	head   int // index of next write
	count  int // number of valid entries (<= cap)
}

func newEventRing(capacity int) *eventRing {
	if capacity <= 0 {
		capacity = 1024
	}
	return &eventRing{buf: make([]models.Event, capacity), cap: capacity}
}

func (r *eventRing) push(e models.Event) {
	r.buf[r.head] = e
	r.head = (r.head + 1) % r.cap
	if r.count < r.cap {
		r.count++
	}
}

// snapshot returns events ordered oldest-first.
func (r *eventRing) snapshot() []models.Event {
	if r.count == 0 {
		return nil
	}
	out := make([]models.Event, r.count)
	start := (r.head - r.count + r.cap) % r.cap
	for i := 0; i < r.count; i++ {
		out[i] = r.buf[(start+i)%r.cap]
	}
	return out
}

// since returns events with Seq strictly greater than seq, ordered oldest-first.
// truncated is true when seq is older than the oldest retained event (i.e. the
// caller asked for events the ring no longer remembers).
func (r *eventRing) since(seq uint64) (events []models.Event, truncated bool) {
	if r.count == 0 {
		return nil, false
	}
	start := (r.head - r.count + r.cap) % r.cap
	oldest := r.buf[start].Seq
	if seq != 0 && seq < oldest-1 {
		truncated = true
	}
	// seq < oldest also means we may have lost events between seq+1 and oldest-1.
	if seq+1 < oldest {
		truncated = true
	}
	for i := 0; i < r.count; i++ {
		ev := r.buf[(start+i)%r.cap]
		if ev.Seq > seq {
			events = append(events, ev)
		}
	}
	return events, truncated
}

func (r *eventRing) oldestSeq() uint64 {
	if r.count == 0 {
		return 0
	}
	start := (r.head - r.count + r.cap) % r.cap
	return r.buf[start].Seq
}
