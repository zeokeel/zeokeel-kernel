package runtime_test

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
// CORE-049: quiescence — Registry.IsIdle is strict, WhenIdle waits for slow
// activations and async disposals, and the caller's own registration window is
// the only excluded in-flight work (Go models the Rust "registration frame"
// with the registering goroutine id, so a create-hook WhenIdle does not wait
// for itself while an external waiter does).
// Rust: quiescence.rs:28/35/86, regression_semantics.rs:1395/1341
// ---------------------------------------------------------------------------

func TestCore049_FreshRegistryIsIdle(t *testing.T) {
	root := newRoot(t)
	if !root.Registry().IsIdle() {
		t.Fatal("fresh registry is not idle")
	}
	// Both the Context and the raw Registry waiter return immediately.
	callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := root.WhenIdleContext(callCtx); err != nil {
		t.Fatalf("Context.WhenIdleContext on a fresh tree = %v, want nil", err)
	}
	if err := root.Registry().WhenIdleContext(callCtx); err != nil {
		t.Fatalf("Registry.WhenIdleContext on a fresh tree = %v, want nil", err)
	}
	done := make(chan struct{})
	go func() {
		root.WhenIdle()
		close(done)
	}()
	waitClosed(t, done, 2*time.Second, "WhenIdle on a fresh tree")
}

// blockingActivationPlugin stays inside Activate until the test releases it.
type blockingActivationPlugin struct {
	name    string
	started chan struct{}
	release chan struct{}
}

func (p *blockingActivationPlugin) Name() string { return p.name }

func (p *blockingActivationPlugin) Activate(cordis.Context, cordis.Value) error {
	close(p.started)
	<-p.release
	return nil
}

func TestCore049_WhenIdleWaitsForSlowActivation(t *testing.T) {
	root := newRoot(t)
	started := make(chan struct{})
	release := make(chan struct{})
	fiberCh := make(chan *cordis.Fiber, 1)
	pluginDone := make(chan struct{})
	go func() {
		defer close(pluginDone)
		fiber, err := root.Plugin(&blockingActivationPlugin{name: "slow-activation", started: started, release: release}, nil)
		if err != nil {
			t.Errorf("plugin: %v", err)
			return
		}
		fiberCh <- fiber
	}()
	waitClosed(t, started, 2*time.Second, "slow activation start")

	// IsIdle is strict: in-flight activation is not idle.
	if root.Registry().IsIdle() {
		t.Fatal("IsIdle reported idle while an activation is in flight")
	}

	whenIdleDone := make(chan struct{})
	whenIdleCalled := make(chan struct{})
	go func() {
		close(whenIdleCalled)
		root.Registry().WhenIdle()
		close(whenIdleDone)
	}()
	waitClosed(t, whenIdleCalled, 2*time.Second, "WhenIdle start")
	// The waiter must not resolve while apply is still blocked.
	assertBlocked(t, whenIdleDone, 50*time.Millisecond, "WhenIdle during slow activation")

	close(release)
	waitClosed(t, pluginDone, 2*time.Second, "slow activation completion")
	waitClosed(t, whenIdleDone, 2*time.Second, "WhenIdle after activation completion")

	fiber := <-fiberCh
	if got := fiber.State(); got != cordis.Active {
		t.Fatalf("fiber state after WhenIdle = %v, want Active", got)
	}
	if !root.Registry().IsIdle() {
		t.Fatal("registry is not idle after activation completed")
	}
}

// blockingCleanupPlugin owns a blocking async cleanup.
type blockingCleanupPlugin struct {
	name    string
	started chan struct{}
	release chan struct{}
	runs    *atomic.Int64
}

func (p *blockingCleanupPlugin) Name() string { return p.name }

func (p *blockingCleanupPlugin) Activate(c cordis.Context, _ cordis.Value) error {
	_, err := c.EffectAsync(func() cordis.AsyncEffect {
		return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(context.Context) error {
			close(p.started)
			<-p.release
			p.runs.Add(1)
			return nil
		})}
	})
	return err
}

func TestCore049_WhenIdleWaitsForAsyncDisposal(t *testing.T) {
	root := newRoot(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int64
	fiber, err := root.Plugin(&blockingCleanupPlugin{name: "slow-disposal", started: started, release: release, runs: &runs}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	disposeDone := make(chan error, 1)
	go func() { disposeDone <- fiber.DisposeAsync(context.Background()) }()
	waitClosed(t, started, 2*time.Second, "async cleanup start")

	if root.Registry().IsIdle() {
		t.Fatal("IsIdle reported idle while an async disposal is in flight")
	}
	whenIdleDone := make(chan struct{})
	whenIdleCalled := make(chan struct{})
	go func() {
		close(whenIdleCalled)
		root.WhenIdle()
		close(whenIdleDone)
	}()
	waitClosed(t, whenIdleCalled, 2*time.Second, "WhenIdle start")
	assertBlocked(t, whenIdleDone, 50*time.Millisecond, "WhenIdle during async disposal")

	close(release)
	if err := waitErr(t, disposeDone, 2*time.Second, "async disposal"); err != nil {
		t.Fatalf("DisposeAsync: %v", err)
	}
	// A direct DisposeAsync has no registration window: the idle waiter must be
	// woken by the fiber-level pendingOps dropping to zero as well, not only by
	// the final state transition.
	waitClosed(t, whenIdleDone, 2*time.Second, "WhenIdle after async disposal")

	// WhenIdle only resolved after the teardown finished.
	if got := fiber.State(); got != cordis.Disposed {
		t.Fatalf("fiber state when WhenIdle returned = %v, want Disposed", got)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("async cleanup runs = %d, want 1", got)
	}
	if !root.Registry().IsIdle() {
		t.Fatal("registry is not idle after the async disposal finished")
	}
}

// TestCore049_RegistrationWindowSelfExemption covers the Go adaptation of the
// Rust "own registration frame" exclusion: an internal/plugin create hook runs
// inside the registration window; WhenIdle from that same goroutine does not
// wait for itself, while an external waiter must wait for the window to close.
func TestCore049_RegistrationWindowSelfExemption(t *testing.T) {
	root := newRoot(t)
	selfResult := make(chan error, 1)
	inCreate := make(chan struct{})
	releaseCreate := make(chan struct{})
	if _, err := root.OnInternalPlugin(func(f *cordis.Fiber) {
		if f.Name() != "reg-window" {
			return
		}
		waiterCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		selfResult <- root.WhenIdleContext(waiterCtx)
		close(inCreate)
		<-releaseCreate
	}); err != nil {
		t.Fatalf("internal/plugin: %v", err)
	}

	pluginDone := make(chan struct{})
	go func() {
		defer close(pluginDone)
		if _, err := root.Plugin(setupPlugin{name: "reg-window"}, nil); err != nil {
			t.Errorf("plugin: %v", err)
		}
	}()
	waitClosed(t, inCreate, 2*time.Second, "create hook registration window")

	// The create hook's own WhenIdle did not block on its registration.
	if err := waitErr(t, selfResult, 2*time.Second, "create-hook WhenIdle"); err != nil {
		t.Fatalf("WhenIdle inside the create hook = %v, want nil (self-registration exempt)", err)
	}
	// External observers see the registration window as non-idle and wait.
	if root.Registry().IsIdle() {
		t.Fatal("IsIdle reported idle inside the registration window")
	}
	// Several independent external waiters: every one of them must observe the
	// window closing (a single waiter makes a lost-wakeup race hard to hit).
	const externalWaiters = 8
	externalDone := make(chan struct{})
	externalStarted := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Add(externalWaiters)
		for i := 0; i < externalWaiters; i++ {
			go func() {
				defer wg.Done()
				root.Registry().WhenIdle()
			}()
		}
		close(externalStarted)
		wg.Wait()
		close(externalDone)
	}()
	waitClosed(t, externalStarted, 2*time.Second, "external WhenIdle start")
	assertBlocked(t, externalDone, 50*time.Millisecond, "external WhenIdle inside the registration window")

	close(releaseCreate)
	waitClosed(t, pluginDone, 2*time.Second, "plugin registration completion")
	waitClosed(t, externalDone, 2*time.Second, "external WhenIdle after registration window")
	if !root.Registry().IsIdle() {
		t.Fatal("registry is not idle after the registration window closed")
	}
}

// TestCore049_IsIdleStrictInsideApply pins the documented boundary: inside
// Activate the fiber is Loading and counts as in-flight even for itself, so a
// synchronous WhenIdle there waits (and must be cancelled by the caller); only
// the internal/plugin create hook has the self-exemption.
func TestCore049_IsIdleStrictInsideApply(t *testing.T) {
	root := newRoot(t)
	sawIdle := make(chan bool, 1)
	waitErrCh := make(chan error, 1)
	plugin := &applyIdleProbePlugin{isIdle: sawIdle, waitErr: waitErrCh}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	insideErr := waitErr(t, waitErrCh, 2*time.Second, "apply WhenIdleContext")
	if !errors.Is(insideErr, context.DeadlineExceeded) {
		t.Fatalf("WhenIdleContext inside Apply = %v, want context.DeadlineExceeded (it must wait for itself)", insideErr)
	}
	if idle := <-sawIdle; idle {
		t.Fatal("IsIdle inside Apply = true, want false (strict, no self-exemption in Apply)")
	}
	if got := fiber.State(); got != cordis.Active {
		t.Fatalf("fiber state after Apply = %v, want Active", got)
	}
	if !root.Registry().IsIdle() {
		t.Fatal("registry is not idle after Apply returned")
	}
}

type applyIdleProbePlugin struct {
	isIdle  chan bool
	waitErr chan error
}

func (p *applyIdleProbePlugin) Name() string { return "apply-idle-probe" }

func (p *applyIdleProbePlugin) Activate(c cordis.Context, _ cordis.Value) error {
	p.isIdle <- c.Registry().IsIdle()
	waiterCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	p.waitErr <- c.WhenIdleContext(waiterCtx)
	return nil
}

// TestCore049_WhenIdleContextCancellation checks that a cancelled waiter
// releases immediately and the tree still settles once the in-flight work ends.
func TestCore049_WhenIdleContextCancellation(t *testing.T) {
	root := newRoot(t)
	started := make(chan struct{})
	release := make(chan struct{})
	pluginDone := make(chan struct{})
	go func() {
		defer close(pluginDone)
		if _, err := root.Plugin(&blockingActivationPlugin{name: "cancelled-waiter", started: started, release: release}, nil); err != nil {
			t.Errorf("plugin: %v", err)
		}
	}()
	waitClosed(t, started, 2*time.Second, "activation start")

	waiterCtx, cancel := context.WithCancel(context.Background())
	waiterErr := make(chan error, 1)
	go func() { waiterErr <- root.WhenIdleContext(waiterCtx) }()
	// Give the waiter a chance to enter the wait, then cancel it.
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := waitErr(t, waiterErr, 2*time.Second, "cancelled WhenIdleContext"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled WhenIdleContext = %v, want context.Canceled", err)
	}
	if root.Registry().IsIdle() {
		t.Fatal("IsIdle reported idle before the slow activation was released")
	}

	close(release)
	waitClosed(t, pluginDone, 2*time.Second, "activation completion")
	settled := make(chan struct{})
	go func() {
		root.WhenIdle()
		close(settled)
	}()
	waitClosed(t, settled, 2*time.Second, "WhenIdle after cancelled waiter")
}
