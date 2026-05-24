// Package transcribepool decouples Whisper transcription from the VAD
// goroutine. VAD submits Utterances to a bounded queue; a fixed worker pool
// drains it, calling an injected Transcriber. Results land on a single
// channel that the pipeline consumes asynchronously.
//
// Why the indirection: a slow Whisper call used to block the VAD loop, which
// backed up audio capture and cascaded into dropped SSE events for the user.
// See PLAN-pipeline-resilience.md.
package transcribepool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrPoolFull is returned by Submit when the bounded queue is at capacity.
// Callers must NOT block — they should log+drop (and Stats().Dropped will
// reflect the loss).
var ErrPoolFull = errors.New("transcribe pool: queue full")

// ErrPoolClosed is returned by Submit after Shutdown has been called.
var ErrPoolClosed = errors.New("transcribe pool: closed")

// Utterance is one VAD-closed audio clip awaiting transcription.
type Utterance struct {
	ID         string
	AudioPath  string
	DurationMs int
	Enqueued   time.Time

	// PCM is the raw audio for the transcriber. Optional — transcribers that
	// read AudioPath off disk can ignore it.
	PCM []byte

	// Ctx is forwarded to the Transcriber. nil = context.Background().
	Ctx context.Context

	// Done, if non-nil, is closed by the pool after the matching Result has
	// been delivered. Lets callers correlate completion without scanning the
	// shared results channel.
	Done chan struct{}

	// User is an opaque slot for caller-side data that needs to flow with the
	// Utterance to the result consumer (e.g. *AudioClip).
	User any
}

// Result is the outcome of one transcription. Err non-nil = failure;
// downstream code is expected to handle that path explicitly.
type Result struct {
	Utterance Utterance
	Text      string
	LangProb  float64
	LatencyMs int64
	Err       error
}

// Transcriber is the injected work function. Tests pass a fake; production
// passes a Whisper HTTP wrapper.
type Transcriber func(ctx context.Context, u Utterance) Result

// Stats is a snapshot of the pool's lifetime counters.
type Stats struct {
	Submitted int64
	Dropped   int64 // ErrPoolFull
	Processed int64
	InFlight  int64
}

// Config configures a new Pool.
type Config struct {
	Workers     int
	Queue       int
	Transcriber Transcriber
}

// Pool is the bounded-queue worker pool.
type Pool struct {
	cfg     Config
	jobs    chan Utterance
	results chan Result

	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
	closed    atomic.Bool

	submitted atomic.Int64
	dropped   atomic.Int64
	processed atomic.Int64
	inflight  atomic.Int64
}

// New constructs a Pool. Call Start before Submit.
func New(cfg Config) *Pool {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.Queue <= 0 {
		cfg.Queue = 1
	}
	// Results buffer must absorb everything ever produced (queue + in-flight)
	// so a non-draining caller cannot wedge Shutdown.
	return &Pool{
		cfg:     cfg,
		jobs:    make(chan Utterance, cfg.Queue),
		results: make(chan Result, cfg.Queue+cfg.Workers),
	}
}

// Start spawns the worker goroutines. Idempotent.
func (p *Pool) Start() {
	p.startOnce.Do(func() {
		for i := 0; i < p.cfg.Workers; i++ {
			p.wg.Add(1)
			go p.workerLoop()
		}
	})
}

func (p *Pool) workerLoop() {
	defer p.wg.Done()
	for u := range p.jobs {
		p.inflight.Add(1)
		ctx := u.Ctx
		if ctx == nil {
			ctx = context.Background()
		}
		start := time.Now()
		res := p.cfg.Transcriber(ctx, u)
		if res.LatencyMs == 0 {
			res.LatencyMs = time.Since(start).Milliseconds()
		}
		res.Utterance = u
		p.results <- res
		if u.Done != nil {
			close(u.Done)
		}
		p.processed.Add(1)
		p.inflight.Add(-1)
	}
}

// Submit enqueues an utterance. Newest-wins: if the queue is full, the
// oldest queued utterance is dropped so the new one always lands. This
// keeps voice commands snappy under TV/background chatter — without it,
// a real command spoken into a backed-up queue would itself be dropped
// while stale TV chatter ahead of it got transcribed.
// Returns ErrPoolClosed after Shutdown. Never blocks.
func (p *Pool) Submit(u Utterance) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}
	if u.Enqueued.IsZero() {
		u.Enqueued = time.Now()
	}
	for {
		select {
		case p.jobs <- u:
			p.submitted.Add(1)
			return nil
		default:
			// Queue is full — discard one oldest item to make room.
			select {
			case <-p.jobs:
				p.dropped.Add(1)
			default:
				// Race: a worker drained it. Loop and try again.
			}
		}
	}
}

// Results returns the channel of completed transcriptions. The channel is
// closed after Shutdown has fully drained.
func (p *Pool) Results() <-chan Result {
	return p.results
}

// Stats returns a lifetime counter snapshot.
func (p *Pool) Stats() Stats {
	return Stats{
		Submitted: p.submitted.Load(),
		Dropped:   p.dropped.Load(),
		Processed: p.processed.Load(),
		InFlight:  p.inflight.Load(),
	}
}

// Shutdown stops accepting new submissions, waits for in-flight workers
// (bounded by ctx), then closes the results channel.
func (p *Pool) Shutdown(ctx context.Context) error {
	var err error
	p.stopOnce.Do(func() {
		p.closed.Store(true)
		close(p.jobs)

		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			err = ctx.Err()
		}
		close(p.results)
	})
	return err
}
