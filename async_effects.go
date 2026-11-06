package cordis

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// AsyncCleanup is an asynchronous cleanup callback: it resolves once the
// resource has been released. In Go it is a blocking function receiving a
// context.Context; cancelling that context is the analogue of dropping the Rust
// cleanup future.
type AsyncCleanup func(ctx context.Context) error

// AsyncEffect is the result of an async effect body: zero or more cleanup
// callbacks, awaited in reverse registration order.
type AsyncEffect []AsyncCleanup

// AsyncCleanupOf wraps a closure as an AsyncCleanup.
func AsyncCleanupOf(fn func(ctx context.Context) error) AsyncCleanup { return fn }

// drainFrameKey is the context key carrying the set of async disposers currently
// being drained on the calling goroutine's stack. It makes self-join detection
// possible without inspecting the goroutine stack: a cleanup that calls back
// into its own disposer passes the received context and the kernel short-circuits
// instead of waiting for a teardown it is part of.
type drainFrameKey struct{}

type drainFrames map[*AsyncDisposer]struct{}

func withDrainFrame(ctx context.Context, d *AsyncDisposer) context.Context {
	frames, _ := ctx.Value(drainFrameKey{}).(drainFrames)
	next := make(drainFrames, len(frames)+1)
	for key := range frames {
		next[key] = struct{}{}
	}
	next[d] = struct{}{}
	return context.WithValue(ctx, drainFrameKey{}, next)
}

func drainingInContext(ctx context.Context, d *AsyncDisposer) bool {
	if ctx == nil {
		return false
	}
	frames, _ := ctx.Value(drainFrameKey{}).(drainFrames)
	_, ok := frames[d]
	return ok
}

// asyncDisposeState is the join state shared by every handle of one async effect.
//
// The claimed bit is the teardown ownership token: exactly one caller drains the
// ordered cleanup list, everybody else joins. It also makes a cancelled teardown
// retryable (S-1): cancellation restores the unstarted cleanups and marks the
// state cancelled, and the next Dispose/DisposeBlocking claims it again.
type asyncDisposeState struct {
	done      atomic.Bool
	cancelled atomic.Bool
	claimed   atomic.Bool

	mu sync.Mutex
	ch chan struct{}
}

func (s *asyncDisposeState) claim() bool {
	if !s.claimed.CompareAndSwap(false, true) {
		return false
	}
	s.done.Store(false)
	s.cancelled.Store(false)
	s.mu.Lock()
	s.ch = make(chan struct{})
	s.mu.Unlock()
	return true
}

func (s *asyncDisposeState) release() {
	s.claimed.Store(false)
	s.wake()
}

func (s *asyncDisposeState) finish() {
	s.done.Store(true)
	s.cancelled.Store(false)
	s.release()
}

func (s *asyncDisposeState) cancel() {
	s.cancelled.Store(true)
	s.done.Store(true)
	s.release()
}

func (s *asyncDisposeState) settled() bool {
	return s.done.Load() && !s.claimed.Load()
}

func (s *asyncDisposeState) waitChan() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch == nil {
		s.ch = make(chan struct{})
	}
	return s.ch
}

func (s *asyncDisposeState) wake() {
	s.mu.Lock()
	if s.ch != nil {
		close(s.ch)
		s.ch = nil
	}
	s.mu.Unlock()
}

// AsyncDisposer is a single-shot handle to one async effect's cleanups.
//
// DisposeAsync awaits the cleanups in reverse registration order exactly once;
// concurrent callers join the in-flight teardown instead of returning early. If
// the passed context is cancelled mid-teardown, the cleanups that had not
// started are restored and the next Dispose retries them (S-1). A cleanup that
// calls back into its own disposer with the context it received returns
// immediately (self-join short-circuit, S-2).
type AsyncDisposer struct {
	state *asyncDisposeState

	mu       sync.Mutex
	cleanups []AsyncCleanup
}

// NewAsyncDisposer creates a disposer owning the given cleanups.
func NewAsyncDisposer(cleanups []AsyncCleanup) *AsyncDisposer {
	return &AsyncDisposer{state: &asyncDisposeState{}, cleanups: cleanups}
}

// DisposeAsync awaits every cleanup in reverse registration order.
//
// The cleanups run sequentially, never concurrently, so teardown order is
// deterministic. A panicking cleanup does not skip the remaining ones: the panic
// is captured, the rest of the list still runs, and the first captured panic is
// re-raised after the drain (same policy as the sync Disposer). Cleanup errors
// are logged and the first one is returned after the drain.
func (d *AsyncDisposer) DisposeAsync(ctx context.Context) error {
	if d == nil || d.state == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if d.state.claim() {
			return d.drain(ctx)
		}
		if drainingInContext(ctx, d) {
			// A cleanup awaiting its own disposer: waiting for ourselves can
			// never complete, so return; the outer drain finishes the teardown.
			return nil
		}
		wait := d.state.waitChan()
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
		if !d.state.cancelled.Load() {
			return nil
		}
		// The teardown we joined was cancelled and restored its cleanups: retry.
	}
}

// drain runs a claimed teardown.
func (d *AsyncDisposer) drain(ctx context.Context) (err error) {
	d.mu.Lock()
	remaining := d.cleanups
	d.cleanups = nil
	d.mu.Unlock()

	var firstPanic any
	var firstErr error
	for {
		if ctx.Err() != nil {
			// Cancelled before the next cleanup starts: restore the unstarted
			// cleanups (reverse order preserved) and let a later call retry.
			d.restore(remaining)
			d.state.cancel()
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			break
		}
		if len(remaining) == 0 {
			d.state.finish()
			break
		}
		cleanup := remaining[len(remaining)-1]
		remaining = remaining[:len(remaining)-1]
		frameCtx := withDrainFrame(ctx, d)
		var cleanupErr error
		panicValue := callContained(func() {
			if cleanup != nil {
				cleanupErr = cleanup(frameCtx)
			}
		})
		switch {
		case panicValue != nil:
			if firstPanic == nil {
				firstPanic = panicValue
			}
			slog.Error("async cleanup panicked; continuing with the remaining cleanups",
				"panic", panicText(panicValue))
		case cleanupErr != nil:
			if ctx.Err() != nil {
				// The in-flight cleanup was cancelled and cannot be recovered;
				// the remaining ones are restored for the retry (S-1 boundary).
				d.restore(remaining)
				d.state.cancel()
				if firstErr == nil {
					firstErr = ctx.Err()
				}
				remaining = nil
			} else {
				slog.Error("async cleanup failed; continuing with the remaining cleanups",
					"error", cleanupErr)
				if firstErr == nil {
					firstErr = cleanupErr
				}
			}
		}
		if d.state.cancelled.Load() {
			break
		}
	}
	if firstPanic != nil {
		panic(firstPanic)
	}
	return firstErr
}

func (d *AsyncDisposer) restore(cleanups []AsyncCleanup) {
	d.mu.Lock()
	if d.cleanups == nil {
		d.cleanups = cleanups
	}
	d.mu.Unlock()
}

// DisposeBlocking runs the teardown on the calling goroutine.
//
// It joins an in-flight teardown, retries a cancelled one, and returns
// immediately when called from inside one of its own cleanups (pass the context
// received by the cleanup so the drain frame is visible; a nil context disables
// self-join detection and would block).
func (d *AsyncDisposer) DisposeBlocking(ctx context.Context) error {
	if d == nil || d.state == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if drainingInContext(ctx, d) {
		return nil
	}
	if d.state.settled() && !d.state.cancelled.Load() {
		return nil
	}
	return d.DisposeAsync(ctx)
}

// disposeBlockingContained is the fiber drain form: panics are captured and the
// error is reported.
func (d *AsyncDisposer) disposeBlockingContained(ctx context.Context) (firstPanic any, err error) {
	firstPanic = callContained(func() {
		err = d.DisposeBlocking(ctx)
	})
	return firstPanic, err
}

// IsDisposed reports whether the ordered teardown completed (a cancelled
// teardown is not complete: its restored cleanups still own resources).
func (d *AsyncDisposer) IsDisposed() bool {
	if d == nil || d.state == nil {
		return true
	}
	d.mu.Lock()
	empty := d.cleanups == nil
	d.mu.Unlock()
	return d.state.settled() && empty && !d.state.cancelled.Load()
}

// discard drops the cleanups without running them and marks the effect settled
// (used when a child fiber disposed itself and the parent detaches the
// ownership effect, R3-A4).
func (d *AsyncDisposer) discard() {
	if d == nil || d.state == nil {
		return
	}
	d.mu.Lock()
	d.cleanups = nil
	d.mu.Unlock()
	d.state.finish()
}

// --- XT-01: bounded recovery of async cleanups a Drop safety net could not run ---

// OrphanCapacity is the maximum number of unrecovered async effects retained
// for an embedder drain.
const OrphanCapacity = 256

var (
	orphanMu      sync.Mutex
	orphans       []*AsyncDisposer
	orphanCount   atomic.Uint64
	orphanFailed  atomic.Uint64
	orphanPending atomic.Int64
)

// orphanAsyncDisposer hands an async effect to the orphan queue instead of
// dropping it.
func orphanAsyncDisposer(disposer *AsyncDisposer) {
	orphanMu.Lock()
	if len(orphans) < OrphanCapacity {
		orphans = append(orphans, disposer)
		orphanPending.Add(1)
		orphanMu.Unlock()
		return
	}
	orphanMu.Unlock()
	orphanCount.Add(1)
}

// OrphanAsyncCleanupCount returns the cumulative number of async effects the
// kernel had to leak because the bounded orphan queue was full. 0 in a healthy
// process.
func OrphanAsyncCleanupCount() uint64 { return orphanCount.Load() }

// OrphanAsyncCleanupFailureCount returns the cumulative number of drained
// orphan async effects whose cleanup panicked or failed.
func OrphanAsyncCleanupFailureCount() uint64 { return orphanFailed.Load() }

// OrphanAsyncCleanupsPending returns the number of orphaned async effects
// waiting for DrainOrphanAsyncCleanups.
func OrphanAsyncCleanupsPending() int { return int(orphanPending.Load()) }

// DrainOrphanAsyncCleanups runs every async cleanup the kernel orphaned during
// Fiber.Abandon. It returns how many effects were processed (not how many
// succeeded). Call it from a goroutine that may block.
func DrainOrphanAsyncCleanups() int {
	orphanMu.Lock()
	taken := orphans
	orphans = nil
	orphanPending.Add(-int64(len(taken)))
	orphanMu.Unlock()
	for _, disposer := range taken {
		panicValue, err := disposer.disposeBlockingContained(context.Background())
		if panicValue != nil || err != nil {
			orphanFailed.Add(1)
			slog.Error("orphan async cleanup failed during drain",
				"panic", panicText(panicValue), "error", err)
		}
	}
	return len(taken)
}
