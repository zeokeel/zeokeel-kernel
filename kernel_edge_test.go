package cordis_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cordis "github.com/zeokeel/cordis-go"
)

// waitFor polls cond until it returns true or the deadline elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not reached: %s", what)
}

func disposeAllQuietly(root cordis.Context) {
	defer func() { _ = recover() }()
	_ = root.DisposeAll()
}

// blockingPlugin's Activate blocks until release is closed.
type blockingPlugin struct {
	started chan struct{}
	release chan struct{}
	applied atomic.Int32
}

func (p *blockingPlugin) Name() string { return "blocking" }
func (p *blockingPlugin) Activate(cordis.Context, cordis.Value) error {
	p.applied.Add(1)
	if p.started != nil {
		select {
		case <-p.started:
		default:
			close(p.started)
		}
	}
	if p.release != nil {
		<-p.release
	}
	return nil
}

func TestLifecycleBusyDuringBlockedCleanup(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	blocked := make(chan struct{})
	entered := make(chan struct{})
	fiber, err := root.Plugin(cleanupBlockPlugin{entered: entered, blocked: blocked}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var disposeErr error
	go func() {
		defer wg.Done()
		disposeErr = fiber.Dispose()
	}()
	<-entered
	// The cleanup is running; a concurrent dispose from another goroutine must
	// report LifecycleBusy instead of racing.
	if err := fiber.Dispose(); !errors.Is(err, cordis.ErrLifecycleBusy) {
		t.Fatalf("concurrent dispose = %v, want LifecycleBusy", err)
	}
	close(blocked)
	wg.Wait()
	if disposeErr != nil {
		t.Fatalf("first dispose: %v", disposeErr)
	}
	if fiber.State() != cordis.Disposed {
		t.Fatalf("state = %v", fiber.State())
	}
}

type cleanupBlockPlugin struct {
	entered chan struct{}
	blocked chan struct{}
}

func (p cleanupBlockPlugin) Name() string { return "cleanup-block" }
func (p cleanupBlockPlugin) Activate(ctx cordis.Context, _ cordis.Value) error {
	_, err := ctx.Effect(func() cordis.Effect {
		return cordis.Effect{cordis.CleanupOf(func() {
			close(p.entered)
			<-p.blocked
		})}
	})
	return err
}

func TestReentrantDisposeFromCleanup(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	var fiberRef *cordis.Fiber
	var reenterErr error
	plugin := &reentrantDisposePlugin{onCleanup: func(f *cordis.Fiber) { reenterErr = f.Dispose() }}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	fiberRef = fiber
	plugin.target = fiberRef
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	if reenterErr != nil {
		t.Fatalf("reentrant dispose = %v, want nil", reenterErr)
	}
	if fiber.State() != cordis.Disposed {
		t.Fatalf("state = %v", fiber.State())
	}
}

type reentrantDisposePlugin struct {
	target    *cordis.Fiber
	onCleanup func(*cordis.Fiber)
}

func (p *reentrantDisposePlugin) Name() string { return "reentrant" }
func (p *reentrantDisposePlugin) Activate(ctx cordis.Context, _ cordis.Value) error {
	_, err := ctx.Effect(func() cordis.Effect {
		return cordis.Effect{cordis.CleanupOf(func() {
			if p.target != nil && p.onCleanup != nil {
				p.onCleanup(p.target)
			}
		})}
	})
	return err
}

// counterPlugin counts activations and carries a configurable name.
type counterPlugin struct {
	name     string
	applied  atomic.Int32
	configs  []cordis.Value
	configMu sync.Mutex
}

func (p *counterPlugin) Name() string { return p.name }
func (p *counterPlugin) Activate(_ cordis.Context, config cordis.Value) error {
	p.applied.Add(1)
	p.configMu.Lock()
	p.configs = append(p.configs, config)
	p.configMu.Unlock()
	return nil
}

func TestRestartReapplies(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	plugin := &counterPlugin{name: "counter"}
	fiber, err := root.Plugin(plugin, map[string]cordis.Value{"n": 1})
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if plugin.applied.Load() != 1 {
		t.Fatalf("applied = %d", plugin.applied.Load())
	}
	if err := fiber.Restart(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if plugin.applied.Load() != 2 {
		t.Fatalf("applied after restart = %d", plugin.applied.Load())
	}
	if fiber.State() != cordis.Active {
		t.Fatalf("state = %v", fiber.State())
	}
}

func TestUpdateValidatesAndKeepsOldConfigOnFailure(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	schema := cordis.NewJsonConfigSchema().
		Required("model").
		Property("model", cordis.JSONString)
	plugin := &schemaCounterPlugin{schema: schema}
	fiber, err := root.Plugin(plugin, map[string]cordis.Value{"model": "m1"})
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if fiber.State() != cordis.Active {
		t.Fatalf("state = %v", fiber.State())
	}
	// Invalid update: Validation error, fiber untouched.
	err = fiber.Update(map[string]cordis.Value{"model": 42})
	if code, ok := cordis.CodeOf(err); !ok || code != cordis.CodeValidation {
		t.Fatalf("update err = %v (code %v), want Validation", err, code)
	}
	if fiber.State() != cordis.Active {
		t.Fatalf("state after invalid update = %v", fiber.State())
	}
	raw, _ := fiber.RawConfig().(map[string]cordis.Value)
	if raw["model"] != "m1" {
		t.Fatalf("raw config = %v, want retained m1", raw)
	}
	// Valid update restarts with the new raw config.
	if err := fiber.Update(map[string]cordis.Value{"model": "m2"}); err != nil {
		t.Fatalf("valid update: %v", err)
	}
	if got := plugin.applied.Load(); got != 2 {
		t.Fatalf("applied = %d, want 2", got)
	}
	raw, _ = fiber.RawConfig().(map[string]cordis.Value)
	if raw["model"] != "m2" {
		t.Fatalf("raw config after update = %v", raw)
	}
}

type schemaCounterPlugin struct {
	schema  cordis.ConfigSchema
	applied atomic.Int32
}

func (p *schemaCounterPlugin) Name() string                      { return "schema-counter" }
func (p *schemaCounterPlugin) ConfigSchema() cordis.ConfigSchema { return p.schema }
func (p *schemaCounterPlugin) Activate(cordis.Context, cordis.Value) error {
	p.applied.Add(1)
	return nil
}

func TestUpdateVetoDoesNotRestart(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	plugin := &counterPlugin{name: "veto"}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	// Install an internal/update hook that never calls next: veto.
	if _, err := root.OnInternalUpdate(func(f *cordis.Fiber, config cordis.Value, noSave bool, next *cordis.UpdateNext) {
		// veto: return without calling next.Next
	}); err != nil {
		t.Fatalf("hook: %v", err)
	}
	if err := fiber.Update(map[string]cordis.Value{"x": 1}); err != nil {
		t.Fatalf("vetoed update err = %v", err)
	}
	if got := plugin.applied.Load(); got != 1 {
		t.Fatalf("applied = %d, want 1 (no restart)", got)
	}
}

func TestInternalStatusAndLifecycleHooks(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	var mu sync.Mutex
	var transitions []string
	if _, err := root.OnInternalStatus(func(f *cordis.Fiber, old cordis.FiberState) {
		mu.Lock()
		transitions = append(transitions, old.String()+"->"+f.State().String())
		mu.Unlock()
	}); err != nil {
		t.Fatalf("status hook: %v", err)
	}
	var lifecycle []cordis.LifecycleEvent
	if _, err := root.OnLifecycle(func(event *cordis.LifecycleEvent) {
		mu.Lock()
		lifecycle = append(lifecycle, *event)
		mu.Unlock()
	}); err != nil {
		t.Fatalf("lifecycle hook: %v", err)
	}
	fiber, err := root.Plugin(&counterPlugin{name: "observed"}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	mu.Lock()
	seq := append([]cordis.LifecycleEvent(nil), lifecycle...)
	mu.Unlock()
	if len(seq) < 2 {
		t.Fatalf("lifecycle events = %d, want >= 2", len(seq))
	}
	// Per-fiber seq must be monotonic.
	last := uint64(0)
	for _, event := range seq {
		if event.FiberID != fiber.ID() {
			continue
		}
		if event.Seq < last {
			t.Fatalf("non-monotonic seq: %v after %d", event.Seq, last)
		}
		last = event.Seq
	}
	mu.Lock()
	count := len(transitions)
	mu.Unlock()
	if count == 0 {
		t.Fatal("no internal/status transitions observed")
	}
}

func TestInternalGetHookOverrideAndAbsence(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	key := cordis.NewServiceKey[*string]("hooked")
	value := "real"
	if _, err := cordis.Provide(root, key, &value); err != nil {
		t.Fatalf("provide: %v", err)
	}
	override := "virtual"
	if _, err := root.OnInternalGet(func(c cordis.Context, name string, next *cordis.GetNext) (cordis.ServiceValue, bool) {
		if name == "hooked" {
			return &override, true
		}
		return next.Next()
	}); err != nil {
		t.Fatalf("get hook: %v", err)
	}
	got, ok := cordis.Get(root, key)
	if !ok || got != &override {
		t.Fatalf("hooked get = %v/%v", got, ok)
	}
	// The hook can also report absence even though the store has the service.
	absentRoot := cordis.Root()
	defer disposeAllQuietly(absentRoot)
	if _, err := cordis.Provide(absentRoot, key, &value); err != nil {
		t.Fatalf("provide: %v", err)
	}
	if _, err := absentRoot.OnInternalGet(func(c cordis.Context, name string, next *cordis.GetNext) (cordis.ServiceValue, bool) {
		return nil, false
	}); err != nil {
		t.Fatalf("get hook: %v", err)
	}
	if _, ok := cordis.Get(absentRoot, key); ok {
		t.Fatal("get hook should be able to report absence")
	}
}

func TestAvailabilityGateRefresh(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	gate := cordis.NewServiceGate(false)
	key := cordis.NewServiceKey[*int]("gated")
	n := 1
	if _, err := cordis.ProvideGated(root, key, &n, gate); err != nil {
		t.Fatalf("provide gated: %v", err)
	}
	consumer := &consumerPlugin{injected: []string{key.Name()}}
	fiber, err := root.Plugin(consumer, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if consumer.ready || fiber.State() != cordis.Pending {
		t.Fatalf("consumer should be pending: ready=%v state=%v", consumer.ready, fiber.State())
	}
	gate.Set(true)
	waitFor(t, time.Second, func() bool { return fiber.State() == cordis.Active }, "consumer active after gate open")
	if !consumer.ready {
		t.Fatal("consumer did not run apply")
	}
	gate.Set(false)
	waitFor(t, time.Second, func() bool { return fiber.State() == cordis.Pending }, "consumer pending after gate close")
}

func TestProviderWithdrawalOrder(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	var mu sync.Mutex
	var order []string
	providerKey := cordis.NewServiceKey[*int]("dep-order")
	provider := &funcPlugin{name: "provider", activate: func(ctx cordis.Context) error {
		value := 1
		if _, err := cordis.Provide(ctx, providerKey, &value); err != nil {
			return err
		}
		_, err := ctx.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {
				mu.Lock()
				order = append(order, "provider-cleanup")
				mu.Unlock()
			})}
		})
		return err
	}}
	consumer := &funcPlugin{name: "consumer", inject: []string{providerKey.Name()}, activate: func(ctx cordis.Context) error {
		_, err := ctx.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {
				mu.Lock()
				order = append(order, "consumer-cleanup")
				mu.Unlock()
			})}
		})
		return err
	}}
	providerFiber, err := root.Plugin(provider, nil)
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	consumerFiber, err := root.Plugin(consumer, nil)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	if consumerFiber.State() != cordis.Active {
		t.Fatalf("consumer state = %v", consumerFiber.State())
	}
	if err := providerFiber.Dispose(); err != nil {
		t.Fatalf("dispose provider: %v", err)
	}
	waitFor(t, time.Second, func() bool { return consumerFiber.State() == cordis.Pending }, "consumer unloaded")
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "consumer-cleanup" || order[1] != "provider-cleanup" {
		t.Fatalf("withdrawal order = %v, want [consumer-cleanup provider-cleanup]", order)
	}
}

type funcPlugin struct {
	name     string
	inject   []string
	activate func(cordis.Context) error
}

func (p *funcPlugin) Name() string     { return p.name }
func (p *funcPlugin) Inject() []string { return p.inject }
func (p *funcPlugin) Activate(ctx cordis.Context, _ cordis.Value) error {
	return p.activate(ctx)
}

func TestRegistryDedupesRuntimeAndReclaims(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	key := cordis.NamedPluginKey("dup")
	f1, err := root.Plugin(namedPlugin{name: "dup"}, nil)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	f2, err := root.Plugin(namedPlugin{name: "dup"}, nil)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	_ = f1
	runtime, ok := root.Registry().Get(key)
	if !ok {
		t.Fatal("runtime missing")
	}
	if runtime.FiberCount() != 2 {
		t.Fatalf("fiber count = %d, want 2", runtime.FiberCount())
	}
	if err := f1.Dispose(); err != nil {
		t.Fatalf("dispose f1: %v", err)
	}
	if _, ok := root.Registry().Get(key); !ok {
		t.Fatal("runtime should survive while one fiber remains")
	}
	if err := f2.Dispose(); err != nil {
		t.Fatalf("dispose f2: %v", err)
	}
	if _, ok := root.Registry().Get(key); ok {
		t.Fatal("runtime should be reclaimed after last fiber disposed")
	}
}

type namedPlugin struct{ name string }

func (p namedPlugin) Name() string                                { return p.name }
func (p namedPlugin) Activate(cordis.Context, cordis.Value) error { return nil }

func TestReplaceKeepsFiberIdentity(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	impl := &implPlugin{}
	fiber, err := root.Plugin(impl, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	id := fiber.ID()
	oldKey := cordis.NamedPluginKey("impl")
	newKey := cordis.NamedPluginKey("impl-v2")
	if _, err := root.Registry().Replace(oldKey, &implPluginV2{}); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if fiber.ID() != id {
		t.Fatalf("fiber identity changed: %d -> %d", id, fiber.ID())
	}
	if _, ok := root.Registry().Get(newKey); !ok {
		t.Fatal("new runtime key missing after replace")
	}
	if _, ok := root.Registry().Get(oldKey); ok {
		t.Fatal("old runtime key should be gone after replace")
	}
	if fiber.State() != cordis.Active {
		t.Fatalf("state = %v", fiber.State())
	}
}

type implPlugin struct{ applied atomic.Int32 }

func (p *implPlugin) Name() string                                { return "impl" }
func (p *implPlugin) Activate(cordis.Context, cordis.Value) error { p.applied.Add(1); return nil }

type implPluginV2 struct{ applied atomic.Int32 }

func (p *implPluginV2) Name() string                                { return "impl-v2" }
func (p *implPluginV2) Activate(cordis.Context, cordis.Value) error { p.applied.Add(1); return nil }

func TestWhenIdleWaitsForRegistrationWindow(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	started := make(chan struct{})
	release := make(chan struct{})
	plugin := &blockingPlugin{started: started, release: release}
	pluginDone := make(chan struct{})
	go func() {
		defer close(pluginDone)
		_, _ = root.Plugin(plugin, nil)
	}()
	<-started
	idleReturned := make(chan struct{})
	go func() {
		defer close(idleReturned)
		root.WhenIdle()
	}()
	select {
	case <-idleReturned:
		t.Fatal("WhenIdle returned while a registration/activation was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-pluginDone
	select {
	case <-idleReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("WhenIdle did not return after activation finished")
	}
}

func TestSelfRegistrationWhenIdleExemption(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	var waitErr error
	// The internal/plugin create hook runs while the registration window is held
	// by the calling goroutine; when_idle must exclude that own registration
	// instead of waiting for itself (CORE-049 C4). Note: inside apply the fiber's
	// own activation is an in-flight operation, so blocking on quiescence there
	// is intentional (XT-04) and is not covered by this exemption.
	if _, err := root.OnInternalPlugin(func(f *cordis.Fiber) {
		ctx, ok := f.Context()
		if !ok {
			return
		}
		waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		waitErr = ctx.WhenIdleContext(waitCtx)
	}); err != nil {
		t.Fatalf("plugin hook: %v", err)
	}
	if _, err := root.Plugin(&funcPlugin{name: "self-idle", activate: func(cordis.Context) error { return nil }}, nil); err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if waitErr != nil {
		t.Fatalf("WhenIdle inside create hook = %v, want nil (own registration excluded)", waitErr)
	}
}

func TestAsyncDisposerCancellationRestoresRemaining(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	disposer := cordis.NewAsyncDisposer([]cordis.AsyncCleanup{
		func(ctx context.Context) error {
			mu.Lock()
			ran = append(ran, "one")
			mu.Unlock()
			return nil
		},
		func(ctx context.Context) error {
			close(firstEntered)
			<-releaseFirst
			return ctx.Err()
		},
		func(ctx context.Context) error {
			mu.Lock()
			ran = append(ran, "three")
			mu.Unlock()
			return nil
		},
	})
	cancelCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- disposer.DisposeAsync(cancelCtx) }()
	<-firstEntered
	// Cancel the teardown; the in-flight cleanup observes cancellation and the
	// not-yet-started cleanups are restored for a retry.
	cancel()
	close(releaseFirst)
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled dispose err = %v, want context.Canceled", err)
	}
	if disposer.IsDisposed() {
		t.Fatal("cancelled teardown must not be settled")
	}
	// Retry: the restored cleanups run (and only once).
	if err := disposer.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("retry dispose: %v", err)
	}
	mu.Lock()
	got := append([]string(nil), ran...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "three" || got[1] != "one" {
		t.Fatalf("ran = %v", got)
	}
	if !disposer.IsDisposed() {
		t.Fatal("retry should settle the disposer")
	}
}

func TestFiberDisposeAsyncCancellationCompletesTeardown(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	entered := make(chan struct{})
	release := make(chan struct{})
	tr := &trace{}
	fiber, err := root.PluginAsync(cleanupBlockPluginAsync{entered: entered, release: release, tr: tr}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	cancelCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fiber.DisposeAsync(cancelCtx) }()
	<-entered
	cancel()
	close(release)
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled dispose async err = %v, want context.Canceled", err)
	}
	// A-9: the teardown completes instead of restoring the old state.
	if fiber.State() != cordis.Disposed {
		t.Fatalf("state = %v, want Disposed", fiber.State())
	}
	// A retry dispose drains the deferred tail.
	_ = fiber.Dispose()
}

type cleanupBlockPluginAsync struct {
	entered chan struct{}
	release chan struct{}
	tr      *trace
}

func (p cleanupBlockPluginAsync) Name() string { return "cleanup-block-async" }
func (p cleanupBlockPluginAsync) ActivateAsync(_ context.Context, ctx cordis.Context, _ cordis.Value) error {
	_, err := ctx.EffectAsync(func() cordis.AsyncEffect {
		return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(cleanupCtx context.Context) error {
			if p.tr != nil {
				p.tr.add("async-cleanup")
			}
			close(p.entered)
			<-p.release
			return cleanupCtx.Err()
		})}
	})
	return err
}
