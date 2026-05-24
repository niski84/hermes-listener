package pipeline

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"hermes-listener/internal/models"
)

const (
	defaultRingSize    = 1024
	subscriberBufSize  = 64
	stuckDetachAfter   = 2 * time.Second
	lagEventDebounce   = 500 * time.Millisecond
	lagLogDebounce     = 1 * time.Second
)

// HubStats is exposed via /api/doctor.
type HubStats struct {
	Subscribers   int              `json:"subscribers"`
	LastSeq       uint64           `json:"last_seq"`
	OldestRingSeq uint64           `json:"oldest_ring_seq"`
	RingSize      int              `json:"ring_size"`
	RingCapacity  int              `json:"ring_capacity"`
	TotalDropped  uint64           `json:"total_dropped"`
	PerSubscriber []SubscriberStats `json:"per_subscriber"`
}

type SubscriberStats struct {
	ID            string `json:"id"`
	Dropped       uint64 `json:"dropped"`
	BufferedNow   int    `json:"buffered_now"`
	HighWaterMark int    `json:"high_water_mark"`
	AgeOfOldestMs int64  `json:"age_of_oldest_ms"`
}

// Subscription is the v2 shape (Phase 4 SSE handler will use this).
type Subscription struct {
	Events <-chan models.Event
	Cancel func()
}

// subState tracks per-subscriber backpressure metrics. The channel itself is
// the map key in the hub (preserves the legacy Subscribe() shape) and we keep
// state pointer-aside so HWM/Dropped are updated under the hub's write lock
// during fan-out.
type subState struct {
	id            string
	hwm           int
	dropped       uint64
	firstFullAt   time.Time
	lastLagEvent  time.Time
	lastLagLog    time.Time
	oldestSendAt  time.Time // wall time of oldest event currently buffered
}

type Hub struct {
	mu          sync.RWMutex
	subscribers map[chan models.Event]*subState
	ring        *eventRing
	seq         uint64 // atomic; assigned by Broadcast
	subCounter  uint64 // atomic; for subscriber IDs
	totalDrop   uint64 // atomic
}

func NewHub() *Hub {
	return NewHubWithRingSize(ringSizeFromEnv())
}

func NewHubWithRingSize(size int) *Hub {
	return &Hub{
		subscribers: make(map[chan models.Event]*subState),
		ring:        newEventRing(size),
	}
}

func ringSizeFromEnv() int {
	if v := os.Getenv("HUB_RING_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultRingSize
}

// Subscribe returns a buffered channel of events broadcast after subscribe time.
func (h *Hub) Subscribe() chan models.Event {
	ch := make(chan models.Event, subscriberBufSize)
	id := fmt.Sprintf("sub-%d", atomic.AddUint64(&h.subCounter, 1))
	st := &subState{id: id}
	h.mu.Lock()
	h.subscribers[ch] = st
	h.mu.Unlock()
	return ch
}

// SubscribeV2 is the new preferred shape. Cancel is idempotent.
func (h *Hub) SubscribeV2() Subscription {
	ch := h.Subscribe()
	var once sync.Once
	cancel := func() { once.Do(func() { h.Unsubscribe(ch) }) }
	return Subscription{Events: ch, Cancel: cancel}
}

func (h *Hub) Unsubscribe(ch chan models.Event) {
	h.mu.Lock()
	if _, ok := h.subscribers[ch]; !ok {
		h.mu.Unlock()
		return
	}
	delete(h.subscribers, ch)
	h.mu.Unlock()
	close(ch)
}

// Broadcast assigns Seq, ID, and TS (when zero), records the event in the ring
// buffer, and fans out to subscribers with bounded backpressure. Returns the
// assigned Seq. Slow subscribers do not block fast ones — sends are non-blocking
// and a subscriber stuck (channel full) for >2s is detached.
func (h *Hub) Broadcast(event models.Event) uint64 {
	if event.TS == 0 {
		event.TS = time.Now().UnixMilli()
	}
	h.mu.Lock()
	seq := atomic.AddUint64(&h.seq, 1)
	event.Seq = seq
	event.ID = strconv.FormatUint(seq, 10)
	h.ring.push(event)

	now := time.Now()
	// Pessimistic but simple: hold write lock during fan-out so we can mutate
	// per-subscriber state and detach stuck ones in-place. Sends are still
	// non-blocking via select/default, so a slow consumer cannot stall this.
	type lagEmit struct {
		subID         string
		dropped       uint64
		oldestAgeMs   int64
	}
	var lagEmits []lagEmit
	var detached []chan models.Event

	for ch, st := range h.subscribers {
		select {
		case ch <- event:
			// Successful send: update HWM and clear stuck-clock.
			n := len(ch)
			if n > st.hwm {
				st.hwm = n
			}
			if n == 0 {
				st.oldestSendAt = time.Time{}
			} else if st.oldestSendAt.IsZero() {
				st.oldestSendAt = now
			}
			st.firstFullAt = time.Time{}
		default:
			// Channel full — count, log, maybe emit lag, maybe detach.
			st.dropped++
			atomic.AddUint64(&h.totalDrop, 1)
			if st.firstFullAt.IsZero() {
				st.firstFullAt = now
			}
			if cap(ch) > st.hwm {
				st.hwm = cap(ch)
			}
			if now.Sub(st.lastLagLog) >= lagLogDebounce {
				log.Printf("[hub] subscriber %s dropping events (dropped=%d, full for %v)",
					st.id, st.dropped, now.Sub(st.firstFullAt).Truncate(time.Millisecond))
				st.lastLagLog = now
			}
			// Debounced lag emit. Skip if the event we're broadcasting IS a lag
			// event (don't recurse into emitting a lag-about-a-lag).
			if event.Type != "pipeline.lag" && now.Sub(st.lastLagEvent) >= lagEventDebounce {
				st.lastLagEvent = now
				oldestAge := int64(0)
				if !st.oldestSendAt.IsZero() {
					oldestAge = now.Sub(st.oldestSendAt).Milliseconds()
				}
				lagEmits = append(lagEmits, lagEmit{
					subID:       st.id,
					dropped:     st.dropped,
					oldestAgeMs: oldestAge,
				})
			}
			if now.Sub(st.firstFullAt) > stuckDetachAfter {
				detached = append(detached, ch)
			}
		}
	}

	// Detach stuck subscribers under the same write lock.
	for _, ch := range detached {
		st := h.subscribers[ch]
		if st != nil {
			log.Printf("[hub] detaching stuck subscriber %s after %v (dropped=%d)",
				st.id, stuckDetachAfter, st.dropped)
		}
		delete(h.subscribers, ch)
		close(ch)
	}
	h.mu.Unlock()

	// Re-broadcast lag events outside the lock. Recursive call is safe — the
	// recursion depth is bounded to 1 because we suppress lag-about-lag above.
	for _, le := range lagEmits {
		h.Broadcast(models.Event{
			Type: "pipeline.lag",
			Payload: map[string]any{
				"subscriber_id": le.subID,
				"dropped":       le.dropped,
				"oldest_age_ms": le.oldestAgeMs,
			},
		})
	}

	return seq
}

// BroadcastRaw is a convenience wrapper for callers that don't import models.
func (h *Hub) BroadcastRaw(eventType string, payload map[string]any) {
	h.Broadcast(models.Event{Type: eventType, Payload: payload})
}

// ReplaySince returns events with Seq strictly greater than seq, oldest-first.
func (h *Hub) ReplaySince(seq uint64) []models.Event {
	events, _ := h.ReplaySinceWithFlag(seq)
	return events
}

func (h *Hub) ReplaySinceWithFlag(seq uint64) ([]models.Event, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ring.since(seq)
}

func (h *Hub) SubscriberCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscribers)
}

func (h *Hub) Stats() HubStats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	now := time.Now()
	per := make([]SubscriberStats, 0, len(h.subscribers))
	for ch, st := range h.subscribers {
		ageMs := int64(0)
		if !st.oldestSendAt.IsZero() {
			ageMs = now.Sub(st.oldestSendAt).Milliseconds()
		}
		per = append(per, SubscriberStats{
			ID:            st.id,
			Dropped:       st.dropped,
			BufferedNow:   len(ch),
			HighWaterMark: st.hwm,
			AgeOfOldestMs: ageMs,
		})
	}
	return HubStats{
		Subscribers:   len(h.subscribers),
		LastSeq:       atomic.LoadUint64(&h.seq),
		OldestRingSeq: h.ring.oldestSeq(),
		RingSize:      h.ring.count,
		RingCapacity:  h.ring.cap,
		TotalDropped:  atomic.LoadUint64(&h.totalDrop),
		PerSubscriber: per,
	}
}
