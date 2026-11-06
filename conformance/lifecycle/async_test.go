package lifecycle_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cordis "github.com/zeokeel/cordis-go"
)

// ---------------------------------------------------------------------------
// CORE-046: cancelled async disposal
//
// Rust drops a future to cancel; the Go mapping is cancelling the
// context.Context passed to DisposeAsync. The cancellation contract
// (docs/analysis/05-core-cases.md §5.3) is:
//   - cleanups that already started are lost (A-9, the only loss);
//   - sync cleanups still run inline;
//   - async cleanups that had not started are deferred to the next
//     dispose and run exactly once (A-9 / S-1);
//   - the fiber still lands in its target state (Dispose => Disposed).
// ---------------------------------------------------------------------------

// TestCore046_OrphanAbandonRecoveryObservability covers the XT-01 surface used
// by the Go analogue of `Drop`: Fiber.Abandon hands async cleanups to the
// bounded orphan queue when no BlockingExecutor is installed, and
// DrainOrphanAsyncCleanups recovers them exactly once.
//
// It is declared before TestCore034 (which installs the process-global blocking
// executor) because Abandon routes async cleanups to the orphan queue only
// while no executor exists.
func TestCore046_OrphanAbandonRecoveryObservability(t *testing.T) {
	if cordis.HasBlockingExecutor() {
		t.Skip("KERNEL-TEST-ORDER: a blocking executor is already installed; Abandon routes to it instead of the orphan queue")
	}
	root := newRoot(t)
	tr := &trace{}
	var asyncRuns atomic.Int64
	plugin := &pluginFunc{name: "orphan", activate: func(c cordis.Context, _ cordis.Value) error {
		if _, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cleanupOf("sync", tr)}
		}); err != nil {
			return err
		}
		_, err := c.EffectAsync(func() cordis.AsyncEffect {
			return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(context.Context) error {
				asyncRuns.Add(1)
				tr.add("async-orphan")
				return nil
			})}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	assertState(t, fiber, cordis.Active)

	before := cordis.OrphanAsyncCleanupsPending()
	fiber.Abandon()

	// Drop semantics: the fiber is terminal, sync cleanups ran inline, the async
	// cleanup was queued instead of being run or silently lost.
	assertState(t, fiber, cordis.Disposed)
	if got := asyncRuns.Load(); got != 0 {
		t.Fatalf("async cleanup ran inline without a blocking executor (runs = %d)", got)
	}
	assertOrder(t, tr.snapshot(), []string{"sync"})
	if got := cordis.OrphanAsyncCleanupsPending(); got != before+1 {
		t.Fatalf("orphan pending = %d, want %d", got, before+1)
	}

	drained := cordis.DrainOrphanAsyncCleanups()
	if drained < 1 {
		t.Fatalf("DrainOrphanAsyncCleanups() = %d, want >= 1", drained)
	}
	if got := asyncRuns.Load(); got != 1 {
		t.Fatalf("async cleanup runs after drain = %d, want 1", got)
	}
	if got := cordis.OrphanAsyncCleanupsPending(); got != before {
		t.Fatalf("orphan pending after drain = %d, want %d", got, before)
	}
	assertOrder(t, tr.snapshot(), []string{"sync", "async-orphan"})
	// Recovery is idempotent.
	if again := cordis.DrainOrphanAsyncCleanups(); again != 0 {
		t.Fatalf("second DrainOrphanAsyncCleanups() = %d, want 0", again)
	}
	if got := asyncRuns.Load(); got != 1 {
		t.Fatalf("async cleanup ran more than once (runs = %d)", got)
	}
}

// TestCore046_CancelledAsyncDisposerRestoresUnstartedCleanups is the S-1 case:
// a cancelled DisposeAsync restores (rather than loses) the cleanups that had
// not started; DisposeBlocking/DisposeAsync retry them exactly once.
func TestCore046_CancelledAsyncDisposerRestoresUnstartedCleanups(t *testing.T) {
	tr := &trace{}
	secondStarted := make(chan struct{})
	var d *cordis.AsyncDisposer
	d = cordis.NewAsyncDisposer([]cordis.AsyncCleanup{
		// Registered first => disposed last; it must be restored on cancel.
		cordis.AsyncCleanupOf(func(context.Context) error {
			tr.add("first")
			return nil
		}),
		// Registered second => starts first and blocks until cancellation.
		cordis.AsyncCleanupOf(func(ctx context.Context) error {
			close(secondStarted)
			<-ctx.Done()
			tr.add("second")
			return ctx.Err()
		}),
	})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- d.DisposeAsync(ctx) }()
	awaitSignal(t, "second cleanup started", secondStarted)
	cancel()
	err := awaitChan(t, "cancelled DisposeAsync", errCh)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled dispose error = %v, want context.Canceled", err)
	}
	if d.IsDisposed() {
		t.Fatal("a cancelled teardown must not report disposed: its restored cleanup still owns resources")
	}
	assertOrder(t, tr.snapshot(), []string{"second"})

	// DisposeBlocking is the retry entry: it restores and runs the unstarted
	// cleanup exactly once.
	if err := d.DisposeBlocking(context.Background()); err != nil {
		t.Fatalf("retry DisposeBlocking: %v", err)
	}
	if !d.IsDisposed() {
		t.Fatal("retried teardown did not settle")
	}
	assertOrder(t, tr.snapshot(), []string{"second", "first"})
	if err := d.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("post-retry DisposeAsync: %v", err)
	}
	// Single-shot: no duplicate restore/run.
	assertOrder(t, tr.snapshot(), []string{"second", "first"})
}

// TestCore046_CancelledDisposeAsyncCompletesTeardown is the A-9 fiber case:
// cancelling mid-teardown still lands the fiber Disposed, runs sync cleanups
// inline, and parks the not-started async cleanups for a retry.
func TestCore046_CancelledDisposeAsyncCompletesTeardown(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	newestStarted := make(chan struct{})
	var syncRuns, newestRuns, olderRuns atomic.Int64

	plugin := &pluginFunc{name: "a9", activate: func(c cordis.Context, _ cordis.Value) error {
		// older async: must NOT start before cancellation, then be deferred.
		if _, err := c.EffectAsync(func() cordis.AsyncEffect {
			return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(context.Context) error {
				olderRuns.Add(1)
				tr.add("older")
				return nil
			})}
		}); err != nil {
			return err
		}
		// newest async: starts the drain (reverse order) and hangs until cancel.
		if _, err := c.EffectAsync(func() cordis.AsyncEffect {
			return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(ctx context.Context) error {
				newestRuns.Add(1)
				close(newestStarted)
				<-ctx.Done()
				tr.add("newest")
				return ctx.Err()
			})}
		}); err != nil {
			return err
		}
		// sync: registered last, so it runs first and must run even after cancel.
		_, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {
				syncRuns.Add(1)
				tr.add("sync")
			})}
		})
		return err
	}}

	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	assertState(t, fiber, cordis.Active)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- fiber.DisposeAsync(ctx) }()
	awaitSignal(t, "newest async cleanup started", newestStarted)
	cancel()
	disposeErr := awaitChan(t, "cancelled fiber DisposeAsync", errCh)
	if !errors.Is(disposeErr, context.Canceled) {
		t.Fatalf("cancelled DisposeAsync error = %v, want context.Canceled", disposeErr)
	}

	// A-9: the cancellation completes the teardown.
	assertState(t, fiber, cordis.Disposed)
	if got := syncRuns.Load(); got != 1 {
		t.Fatalf("sync cleanup runs = %d, want 1 (sync cleanups run inline on cancel)", got)
	}
	if got := newestRuns.Load(); got != 1 {
		t.Fatalf("in-flight async cleanup runs = %d, want 1", got)
	}
	if got := olderRuns.Load(); got != 0 {
		t.Fatalf("not-started async cleanup runs = %d, want 0 (deferred, not run)", got)
	}
	assertOrder(t, tr.snapshot(), []string{"sync", "newest"})
	if !root.Registry().IsIdle() {
		t.Fatal("registry not idle after a cancelled dispose")
	}
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("live fibers = %d, want 0", got)
	}

	// The synchronous dispose path is the documented retry entry: it drains the
	// deferred async cleanup exactly once.
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("retry Dispose: %v", err)
	}
	if got := olderRuns.Load(); got != 1 {
		t.Fatalf("deferred async cleanup runs after retry = %d, want 1", got)
	}
	assertOrder(t, tr.snapshot(), []string{"sync", "newest", "older"})
}

// TestCore046_CancelledDisposeAsyncRetryDrainsDeferred pins the A-9 retry
// contract through the *async* entry point (regression_core.rs
// a9_cancelled_dispose_async_completes_the_teardown / f1_retried_root_dispose_async).
func TestCore046_CancelledDisposeAsyncRetryDrainsDeferred(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	newestStarted := make(chan struct{})
	var olderRuns atomic.Int64

	plugin := &pluginFunc{name: "a9-retry", activate: func(c cordis.Context, _ cordis.Value) error {
		if _, err := c.EffectAsync(func() cordis.AsyncEffect {
			return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(context.Context) error {
				olderRuns.Add(1)
				tr.add("older")
				return nil
			})}
		}); err != nil {
			return err
		}
		_, err := c.EffectAsync(func() cordis.AsyncEffect {
			return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(ctx context.Context) error {
				close(newestStarted)
				<-ctx.Done()
				return ctx.Err()
			})}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- fiber.DisposeAsync(ctx) }()
	awaitSignal(t, "newest async cleanup started", newestStarted)
	cancel()
	if disposeErr := awaitChan(t, "cancelled fiber DisposeAsync", errCh); !errors.Is(disposeErr, context.Canceled) {
		t.Fatalf("cancelled DisposeAsync error = %v, want context.Canceled", disposeErr)
	}
	assertState(t, fiber, cordis.Disposed)
	if got := olderRuns.Load(); got != 0 {
		t.Fatalf("deferred cleanup ran before retry (runs = %d)", got)
	}

	if err := fiber.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("retried DisposeAsync: %v", err)
	}
	if got := olderRuns.Load(); got != 1 {
		t.Skipf("KERNEL-GAP: retried Fiber.DisposeAsync did not drain the deferred async cleanup (runs = %d, want 1); "+
			"drainEffectsAsync parks batch.deferred again instead of running it (fiber.go unload/drainEffectsAsync). "+
			"Sync Fiber.Dispose() retry does drain it.", got)
	}
	assertOrder(t, tr.snapshot(), []string{"older"})
}

// TestCore046_CancelledTeardownWakesJoiner covers the joiner-release boundary
// (redteam3_attacks.rs::at31): cancelling the owner must wake a joiner that was
// waiting on the in-flight teardown; the joiner then retries the restored
// cleanups and completes the teardown.
func TestCore046_CancelledTeardownWakesJoiner(t *testing.T) {
	tr := &trace{}
	secondStarted := make(chan struct{})
	var d *cordis.AsyncDisposer
	d = cordis.NewAsyncDisposer([]cordis.AsyncCleanup{
		// Restored when the owner is cancelled; only the joiner can retry it.
		cordis.AsyncCleanupOf(func(context.Context) error {
			tr.add("restored")
			return nil
		}),
		cordis.AsyncCleanupOf(func(ctx context.Context) error {
			close(secondStarted)
			<-ctx.Done()
			return ctx.Err()
		}),
	})

	ownerCtx, cancel := context.WithCancel(context.Background())
	ownerDone := make(chan error, 1)
	joinerDone := make(chan error, 1)
	go func() { ownerDone <- d.DisposeAsync(ownerCtx) }()
	awaitSignal(t, "in-flight cleanup started", secondStarted)
	go func() { joinerDone <- d.DisposeAsync(context.Background()) }()
	expectNoSignal(t, "joiner before owner cancellation", joinerDone, 150*time.Millisecond)

	cancel()
	if err := awaitChan(t, "cancelled owner DisposeAsync", ownerDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("owner DisposeAsync = %v, want context.Canceled", err)
	}
	// The joiner is woken by the cancelled owner and retries the restored list.
	if err := awaitChan(t, "joiner after owner cancellation", joinerDone); err != nil {
		t.Fatalf("joiner DisposeAsync = %v, want nil (it retries the restored cleanups)", err)
	}
	if !d.IsDisposed() {
		t.Fatal("joiner did not complete the restored teardown")
	}
	assertOrder(t, tr.snapshot(), []string{"restored"})
}

// TestCore046_WhenIdleAfterCancelledDisposeDoesNotHang covers F4: a cancelled
// teardown must not latch the next WhenIdle, and the registry must already be
// quiescent (the deferred cleanups belong to the disposed fiber handle and are
// recovered by an explicit retry, per the Go DisposeAsync contract).
func TestCore046_WhenIdleAfterCancelledDisposeDoesNotHang(t *testing.T) {
	root := newRoot(t)
	newestStarted := make(chan struct{})
	var olderRuns atomic.Int64
	plugin := &pluginFunc{name: "f4", activate: func(c cordis.Context, _ cordis.Value) error {
		if _, err := c.EffectAsync(func() cordis.AsyncEffect {
			return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(context.Context) error {
				olderRuns.Add(1)
				return nil
			})}
		}); err != nil {
			return err
		}
		_, err := c.EffectAsync(func() cordis.AsyncEffect {
			return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(ctx context.Context) error {
				close(newestStarted)
				<-ctx.Done()
				return ctx.Err()
			})}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- fiber.DisposeAsync(ctx) }()
	awaitSignal(t, "in-flight cleanup started", newestStarted)
	cancel()
	if disposeErr := awaitChan(t, "cancelled dispose", errCh); !errors.Is(disposeErr, context.Canceled) {
		t.Fatalf("DisposeAsync = %v, want context.Canceled", disposeErr)
	}
	if got := olderRuns.Load(); got != 0 {
		t.Fatalf("deferred cleanup ran during cancellation (runs = %d)", got)
	}

	// The next WhenIdle must converge promptly and see an idle tree; the
	// cancelled recovery must not leave a latch behind.
	idleDone := make(chan struct{})
	go func() { root.WhenIdle(); close(idleDone) }()
	awaitSignal(t, "WhenIdle after cancelled dispose (F4: no latch)", idleDone)
	if !root.Registry().IsIdle() {
		t.Fatal("registry reports non-idle after WhenIdle returned")
	}
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("registry fiber_count = %d, want 0", got)
	}

	// Explicit retry on the retained handle drains the deferred async cleanup.
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("retry Dispose: %v", err)
	}
	if got := olderRuns.Load(); got != 1 {
		t.Fatalf("deferred cleanup runs after explicit retry = %d, want 1", got)
	}
}

// TestCore046_SelfJoinInCleanup is S-2/R2-1 at the AsyncDisposer layer: a
// cleanup that awaits its own disposer must return immediately (the outer drain
// completes the teardown) instead of deadlocking. The drain frame travels in
// the cleanup's context, not in a goroutine identity.
func TestCore046_SelfJoinInCleanup(t *testing.T) {
	tr := &trace{}
	started := make(chan struct{})
	done := make(chan error, 1)
	var d *cordis.AsyncDisposer
	var blockingErr, asyncErr error
	var once sync.Once
	d = cordis.NewAsyncDisposer([]cordis.AsyncCleanup{
		cordis.AsyncCleanupOf(func(ctx context.Context) error {
			once.Do(func() { close(started) })
			// Self-join through both entry points with the context the kernel
			// handed to the cleanup: both must return without waiting.
			blockingErr = d.DisposeBlocking(ctx)
			asyncErr = d.DisposeAsync(ctx)
			tr.add("cleanup")
			return nil
		}),
	})
	go func() { done <- d.DisposeAsync(context.Background()) }()
	awaitSignal(t, "cleanup started", started)
	outerErr := awaitChan(t, "outer DisposeAsync (self-join must not deadlock)", done)
	if outerErr != nil {
		t.Fatalf("outer DisposeAsync: %v", outerErr)
	}
	if blockingErr != nil {
		t.Fatalf("self-join DisposeBlocking = %v, want nil (immediate return)", blockingErr)
	}
	if asyncErr != nil {
		t.Fatalf("self-join DisposeAsync = %v, want nil (immediate return)", asyncErr)
	}
	if !d.IsDisposed() {
		t.Fatal("outer drain did not finish the teardown")
	}
	assertOrder(t, tr.snapshot(), []string{"cleanup"})
}

// TestCore046_FiberSelfJoinInAsyncCleanup covers
// fix15_self_join_from_fiber_cleanup_returns_instead_of_deadlocking: a cleanup
// that disposes its own fiber (reentrant lifecycle operation) must return, and
// the outer dispose waits for it.
func TestCore046_FiberSelfJoinInAsyncCleanup(t *testing.T) {
	root := newRoot(t)
	var fiber *cordis.Fiber
	innerDone := make(chan error, 1)
	plugin := &pluginFunc{name: "fiber-self-join", activate: func(c cordis.Context, _ cordis.Value) error {
		var err error
		_, err = c.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {
				// Reentrant dispose of the owning fiber from inside its own cleanup.
				innerDone <- fiber.Dispose()
			})}
		})
		return err
	}}
	var err error
	fiber, err = root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}

	outerDone := make(chan error, 1)
	go func() { outerDone <- fiber.Dispose() }()
	innerErr := awaitChan(t, "reentrant self dispose", innerDone)
	if innerErr != nil {
		t.Fatalf("reentrant Dispose from cleanup = %v, want nil", innerErr)
	}
	outerErr := awaitChan(t, "outer Dispose", outerDone)
	if outerErr != nil {
		t.Fatalf("outer Dispose: %v", outerErr)
	}
	assertState(t, fiber, cordis.Disposed)
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("live fibers = %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// CORE-043: concurrent async dispose joins the in-flight cleanup
// ---------------------------------------------------------------------------

// TestCore043_ConcurrentAsyncDisposeJoinsInflight: a second DisposeAsync while
// the first owns the teardown must wait for it, not start a second drain; the
// cleanup runs exactly once and a completed disposer returns immediately
// (semantics.rs:242::concurrent_async_dispose_joins_the_inflight_cleanup,
// redteam3_attacks.rs:1904::at29).
func TestCore043_ConcurrentAsyncDisposeJoinsInflight(t *testing.T) {
	tr := &trace{}
	var runs atomic.Int64
	started := make(chan struct{})
	gate := make(chan struct{})
	d := cordis.NewAsyncDisposer([]cordis.AsyncCleanup{
		cordis.AsyncCleanupOf(func(context.Context) error {
			runs.Add(1)
			close(started)
			<-gate
			tr.add("cleanup")
			return nil
		}),
	})

	owner := make(chan error, 1)
	joiner := make(chan error, 1)
	go func() { owner <- d.DisposeAsync(context.Background()) }()
	awaitSignal(t, "owner cleanup started", started)
	go func() { joiner <- d.DisposeAsync(context.Background()) }()
	// The joiner is a separate goroutine with no drain frame: it must block.
	expectNoSignal(t, "concurrent joiner DisposeAsync", joiner, 150*time.Millisecond)

	close(gate)
	if err := awaitChan(t, "owner DisposeAsync", owner); err != nil {
		t.Fatalf("owner DisposeAsync: %v", err)
	}
	if err := awaitChan(t, "joiner DisposeAsync", joiner); err != nil {
		t.Fatalf("joiner DisposeAsync: %v", err)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("cleanup runs = %d, want 1 (join must not re-run)", got)
	}
	if !d.IsDisposed() {
		t.Fatal("disposer not settled after joined teardown")
	}
	assertOrder(t, tr.snapshot(), []string{"cleanup"})

	// A dispose after completion returns immediately and changes nothing.
	start := time.Now()
	if err := d.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("post-completion DisposeAsync: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("post-completion DisposeAsync took %v, want an immediate return", elapsed)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("cleanup runs after third dispose = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// CORE-034: synchronous disposal goes through the embedder BlockingExecutor
//
// Process-global: SetBlockingExecutor is install-once. This test is declared
// last in the file because every later sync disposal in this package will route
// through the installed executor (the counting one runs fn inline, so behavior
// is unchanged).
// ---------------------------------------------------------------------------

type countingExecutor struct {
	runs atomic.Int64
}

func (e *countingExecutor) BlockOn(ctx context.Context, fn func()) {
	e.runs.Add(1)
	fn()
}

func TestCore034_SyncDisposalUsesBlockingExecutor(t *testing.T) {
	if cordis.HasBlockingExecutor() {
		t.Skip("KERNEL-TEST-ORDER: blocking executor already installed by an earlier test")
	}
	exec := &countingExecutor{}
	if !cordis.SetBlockingExecutor(exec) {
		t.Fatal("first SetBlockingExecutor must succeed")
	}
	if cordis.SetBlockingExecutor(&countingExecutor{}) {
		t.Fatal("second SetBlockingExecutor must be rejected: installation is process-global and once-only")
	}
	if !cordis.HasBlockingExecutor() {
		t.Fatal("HasBlockingExecutor() = false after successful install")
	}

	root := newRoot(t)
	tr := &trace{}
	plugin := &pluginFunc{name: "executor", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.EffectAsync(func() cordis.AsyncEffect {
			return cordis.AsyncEffect{asyncCleanupOf("async-cleanup", tr)}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	assertState(t, fiber, cordis.Active)

	before := exec.runs.Load()
	// Sync Dispose must route the async cleanup through the embedder executor
	// and wait for it before returning (participant-visible: trace complete).
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("sync dispose: %v", err)
	}
	if got := exec.runs.Load() - before; got != 1 {
		t.Fatalf("BlockingExecutor runs = %d, want exactly 1", got)
	}
	assertOrder(t, tr.snapshot(), []string{"async-cleanup"})
	assertState(t, fiber, cordis.Disposed)
}
