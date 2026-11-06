package cordis_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cordis "github.com/zeokeel/cordis-go"
)

func fiberWithMarkers(t *testing.T, root cordis.Context, cleanup func()) *cordis.Fiber {
	t.Helper()
	fiber, err := root.Plugin(&funcPlugin{name: "marker", activate: func(ctx cordis.Context) error {
		_, err := ctx.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(cleanup)}
		})
		return err
	}}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	return fiber
}

func TestCleanupPanicDoesNotSkipRemaining(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	var mu sync.Mutex
	var order []string
	fiber, err := root.Plugin(&funcPlugin{name: "panic-cleanup", activate: func(ctx cordis.Context) error {
		// Registered first: runs last.
		if _, err := ctx.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {
				mu.Lock()
				order = append(order, "panic")
				mu.Unlock()
				panic("cleanup boom")
			})}
		}); err != nil {
			return err
		}
		// Registered second: runs first.
		if _, err := ctx.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {
				mu.Lock()
				order = append(order, "ok")
				mu.Unlock()
			})}
		}); err != nil {
			return err
		}
		return nil
	}}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	var panicValue any
	func() {
		defer func() { panicValue = recover() }()
		_ = fiber.Dispose()
	}()
	if panicValue == nil {
		t.Fatal("dispose should re-raise the first cleanup panic")
	}
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "ok" || got[1] != "panic" {
		t.Fatalf("cleanup order = %v, want [ok panic]", got)
	}
	if fiber.State() != cordis.Disposed {
		t.Fatalf("state = %v, want Disposed after drain", fiber.State())
	}
}

func TestApplyFailureRollsBackEffects(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	cleaned := 0
	fiber, err := root.Plugin(&funcPlugin{name: "failing", activate: func(ctx cordis.Context) error {
		if _, err := ctx.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() { cleaned++ })}
		}); err != nil {
			return err
		}
		return errors.New("apply failed")
	}}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if fiber.State() != cordis.Failed {
		t.Fatalf("state = %v, want Failed", fiber.State())
	}
	if cleaned != 1 {
		t.Fatalf("cleanup count = %d, want 1 (rollback)", cleaned)
	}
}

func TestApplyPanicRollsBackEffects(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	cleaned := 0
	fiber, err := root.Plugin(&funcPlugin{name: "panicking", activate: func(ctx cordis.Context) error {
		if _, err := ctx.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() { cleaned++ })}
		}); err != nil {
			return err
		}
		panic("apply boom")
	}}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if fiber.State() != cordis.Failed {
		t.Fatalf("state = %v, want Failed", fiber.State())
	}
	if cleaned != 1 {
		t.Fatalf("cleanup count = %d, want 1 (panic rollback)", cleaned)
	}
}

func TestListenerPanicDoesNotAbortDispatch(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	if _, err := root.On("panic", func([]cordis.Value) cordis.EventResult {
		panic("listener boom")
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	after := 0
	if _, err := root.On("panic", func([]cordis.Value) cordis.EventResult {
		after++
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	root.Emit("panic", nil) // must not panic
	if after != 1 {
		t.Fatalf("listener after panicking one ran %d times, want 1", after)
	}
}

func TestWaterfallPanicIsFailClosed(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	terminalRan := false
	if _, err := root.OnWaterfall("wf", func([]cordis.Value, *cordis.WaterfallNext) cordis.Value {
		panic("middleware boom")
	}); err != nil {
		t.Fatalf("waterfall: %v", err)
	}
	var panicValue any
	func() {
		defer func() { panicValue = recover() }()
		root.Waterfall("wf", nil, func([]cordis.Value) cordis.Value {
			terminalRan = true
			return "terminal"
		})
	}()
	if panicValue == nil {
		t.Fatal("a panicking waterfall middleware must abort the chain and re-raise")
	}
	if terminalRan {
		t.Fatal("the terminal must not run after a middleware panic (fail-closed)")
	}
}

func TestListenerIsolatedScopeFiltering(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	isolated := root.Isolate("some-service")
	rootCalls := 0
	isolatedCalls := 0
	globalCalls := 0
	if _, err := root.On("filtered", func([]cordis.Value) cordis.EventResult {
		rootCalls++
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	if _, err := isolated.On("filtered", func([]cordis.Value) cordis.EventResult {
		isolatedCalls++
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on isolated: %v", err)
	}
	if _, err := root.OnWithOptions("filtered", cordis.EventOptions{Global: true}, func([]cordis.Value) cordis.EventResult {
		globalCalls++
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on global: %v", err)
	}
	root.Emit("filtered", nil)
	if rootCalls != 1 || isolatedCalls != 0 || globalCalls != 1 {
		t.Fatalf("after root emit: root=%d isolated=%d global=%d", rootCalls, isolatedCalls, globalCalls)
	}
	rootCalls, isolatedCalls, globalCalls = 0, 0, 0
	isolated.Emit("filtered", nil)
	if rootCalls != 0 || isolatedCalls != 1 || globalCalls != 1 {
		t.Fatalf("after isolated emit: root=%d isolated=%d global=%d", rootCalls, isolatedCalls, globalCalls)
	}
}

func TestEventPrependOrder(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	var order []string
	if _, err := root.On("order", func([]cordis.Value) cordis.EventResult {
		order = append(order, "normal")
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	if _, err := root.OnWithOptions("order", cordis.EventOptions{Prepend: true}, func([]cordis.Value) cordis.EventResult {
		order = append(order, "prepended")
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	root.Emit("order", nil)
	if len(order) != 2 || order[0] != "prepended" || order[1] != "normal" {
		t.Fatalf("order = %v", order)
	}
}

func TestListenerRemovedWithOwningFiber(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	tr := &trace{}
	fiber, err := root.Plugin(&funcPlugin{name: "owner", activate: func(ctx cordis.Context) error {
		_, err := ctx.On("owned", func([]cordis.Value) cordis.EventResult {
			tr.add("called")
			return cordis.Continue()
		})
		return err
	}}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	root.Emit("owned", nil)
	if len(tr.snapshot()) != 1 {
		t.Fatalf("expected listener to run once")
	}
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	root.Emit("owned", nil)
	if got := len(tr.snapshot()); got != 1 {
		t.Fatalf("listener ran %d times after owner dispose, want 1", got)
	}
}

func TestInternalListenerHookRewrite(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	if _, err := root.OnInternalListener(func(req cordis.ListenerRequest) *cordis.ListenerOverride {
		if req.Name != "rewrite" {
			return nil
		}
		return &cordis.ListenerOverride{
			Callback: func([]cordis.Value) cordis.EventResult {
				return cordis.Bail("rewritten")
			},
			Global: true,
		}
	}); err != nil {
		t.Fatalf("hook: %v", err)
	}
	if _, err := root.On("rewrite", func([]cordis.Value) cordis.EventResult {
		return cordis.Bail("original")
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	value, ok := root.Bail("rewrite", nil)
	if !ok || value != "rewritten" {
		t.Fatalf("bail = %v/%v, want rewritten", value, ok)
	}
}

func TestInternalDispatchObserver(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	var mu sync.Mutex
	var observed []cordis.DispatchInfo
	if _, err := root.OnInternalDispatch(func(info cordis.DispatchInfo) {
		mu.Lock()
		observed = append(observed, info)
		mu.Unlock()
	}); err != nil {
		t.Fatalf("hook: %v", err)
	}
	root.Emit("public", 1, 2)
	// internal/ names must never recurse into the dispatch observer.
	root.Emit("internal/dispatch", nil)
	mu.Lock()
	defer mu.Unlock()
	if len(observed) != 1 {
		t.Fatalf("observed = %v, want exactly the public dispatch", observed)
	}
	if observed[0].Mode != cordis.DispatchEmit || observed[0].Name != "public" || len(observed[0].Args) != 2 {
		t.Fatalf("info = %+v", observed[0])
	}
}

type fixedClock struct {
	unixMs int64
	monoMs int64
}

func (c fixedClock) NowUnixMs() int64      { return c.unixMs }
func (c fixedClock) NowMonotonicMs() int64 { return c.monoMs }

func TestInjectableClockDrivesLogTimestamps(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	clock := fixedClock{unixMs: 123456789, monoMs: 42}
	if err := root.SetClock(clock); err != nil {
		t.Fatalf("set clock: %v", err)
	}
	logger := root.LoggerNamed("clocked")
	logger.Info("with injected time", nil)
	records := root.LoggerService().Records()
	if len(records) == 0 {
		t.Fatal("no records")
	}
	if got := records[0].TsUnixMs; got != 123456789 {
		t.Fatalf("ts = %d, want injected 123456789", got)
	}
}

func TestLoggerInvalidNumericLevelClampsToError(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	intercepted := root.Intercept("logger", map[string]cordis.Value{"level": 99})
	logger := intercepted.LoggerNamed("clamped")
	logger.Debug("hidden", nil)
	logger.Error("visible", nil)
	records := root.LoggerService().Records()
	for _, record := range records {
		if record.Message == "hidden" {
			t.Fatalf("debug record leaked through a clamped threshold: %+v", record)
		}
	}
	found := false
	for _, record := range records {
		if record.Message == "visible" {
			found = true
		}
	}
	if !found {
		t.Fatal("error record missing")
	}
}

func TestConfigTypedNullOptionalField(t *testing.T) {
	schema := cordis.NewJsonConfigSchema().
		Property("optional", cordis.JSONNumber).
		DefaultValue("optional", nil)
	value, err := schema.Validate(map[string]cordis.Value{"other": true})
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	object := value.(map[string]cordis.Value)
	if _, present := object["optional"]; present {
		t.Fatalf("typed nil default must not be injected: %v", object)
	}
	// A provided value is type-checked.
	if _, err := schema.Validate(map[string]cordis.Value{"optional": "string"}); err == nil {
		t.Fatal("expected a type mismatch for the provided value")
	}
}

func TestConfigResolveIntercepts(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	base := map[string]cordis.Value{"timeout": 1.0, "retries": 2.0}
	child := root.Intercept("svc", map[string]cordis.Value{"timeout": 5.0})
	child = child.Intercept("svc", map[string]cordis.Value{"retries": 7.0})
	resolved := child.ResolveConfig("svc", base, nil).(map[string]cordis.Value)
	if resolved["timeout"] != 5.0 {
		t.Fatalf("timeout = %v, want 5 (root intercept first, leaf last)", resolved["timeout"])
	}
	if resolved["retries"] != 7.0 {
		t.Fatalf("retries = %v, want 7 (leaf intercept wins)", resolved["retries"])
	}
}

func TestDisposeAllContinuesAfterCleanupPanic(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	secondCleaned := false
	if _, err := root.Plugin(&funcPlugin{name: "panicky", activate: func(ctx cordis.Context) error {
		_, err := ctx.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() { panic("cleanup boom") })}
		})
		return err
	}}, nil); err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if _, err := root.Plugin(&funcPlugin{name: "survivor", activate: func(ctx cordis.Context) error {
		_, err := ctx.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() { secondCleaned = true })}
		})
		return err
	}}, nil); err != nil {
		t.Fatalf("plugin: %v", err)
	}
	var panicValue any
	func() {
		defer func() { panicValue = recover() }()
		_ = root.DisposeAll()
	}()
	if panicValue == nil {
		t.Fatal("DisposeAll should re-raise the first cleanup panic after attempting every fiber")
	}
	if !secondCleaned {
		t.Fatal("DisposeAll stopped at the panicking fiber instead of attempting the rest")
	}
}

func TestCheckpointCancellation(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	var fiber *cordis.Fiber
	fiber, err := root.Plugin(&funcPlugin{name: "checkpoints", activate: func(ctx cordis.Context) error {
		if err := ctx.Checkpoint(); err != nil {
			return err
		}
		return nil
	}}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	fiber.Cancel()
	if !fiber.IsCancelled() {
		t.Fatal("fiber should be cancelled")
	}
	// Checkpoint through a fresh activation context reports Cancelled.
	ctx, ok := fiber.Context()
	if !ok {
		t.Fatal("fiber context missing")
	}
	if err := ctx.Checkpoint(); !errors.Is(err, cordis.ErrCancelled) {
		t.Fatalf("checkpoint = %v, want ErrCancelled", err)
	}
}

func TestDeepChainCascadeDispose(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	const depth = 100
	var disposed atomic.Int32
	var chainPlugin func(level int) cordis.Plugin
	chainPlugin = func(level int) cordis.Plugin {
		return &funcPlugin{name: fmt.Sprintf("chain-%d", level), activate: func(ctx cordis.Context) error {
			if _, err := ctx.Effect(func() cordis.Effect {
				return cordis.Effect{cordis.CleanupOf(func() { disposed.Add(1) })}
			}); err != nil {
				return err
			}
			if level > 0 {
				if _, err := ctx.Plugin(chainPlugin(level-1), nil); err != nil {
					return err
				}
			}
			return nil
		}}
	}
	top, err := root.Plugin(chainPlugin(depth), nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if top.State() != cordis.Active {
		t.Fatalf("top state = %v, error = %v", top.State(), top.ErrorDetail())
	}
	if err := top.Dispose(); err != nil {
		t.Fatalf("dispose chain: %v", err)
	}
	if got := disposed.Load(); got != depth+1 {
		t.Fatalf("disposed = %d, want %d", got, depth+1)
	}
}

func TestPluginDepthLimit(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	var build func(level int) cordis.Plugin
	build = func(level int) cordis.Plugin {
		return &funcPlugin{name: "deep", activate: func(ctx cordis.Context) error {
			if _, err := ctx.Plugin(build(level+1), nil); err != nil {
				return err
			}
			return nil
		}}
	}
	if _, err := root.Plugin(build(0), nil); err != nil {
		t.Fatalf("root plugin: %v", err)
	}
	// The chain is truncated at the loading-depth bound (XT-11): the last
	// successful fiber reports a failed activation with InvalidPlugin instead of
	// overflowing the stack, and no more than MAX_PLUGIN_DEPTH fibers load.
	fibers := root.Registry().Fibers()
	if got := len(fibers); got > cordis.MaxPluginDepth+1 {
		t.Fatalf("loaded %d fibers, want <= %d", got, cordis.MaxPluginDepth+1)
	}
	if len(fibers) < cordis.MaxPluginDepth-2 {
		t.Fatalf("chain truncated too early: %d fibers", len(fibers))
	}
	foundInvalid := false
	for _, loaded := range fibers {
		if code, ok := cordis.CodeOf(loaded.ErrorDetail()); ok && code == cordis.CodeInvalidPlugin {
			foundInvalid = true
		}
	}
	if !foundInvalid {
		t.Fatal("expected an InvalidPlugin activation error at the depth bound")
	}
}

func TestChildSelfDisposeDetachesOwnership(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	parentCtxCh := make(chan cordis.Context, 1)
	parent, err := root.Plugin(&funcPlugin{name: "dynamic-parent", activate: func(ctx cordis.Context) error {
		parentCtxCh <- ctx
		return nil
	}}, nil)
	if err != nil {
		t.Fatalf("parent: %v", err)
	}
	parentCtx := <-parentCtxCh
	baseline := len(parent.Effects())
	for i := 0; i < 20; i++ {
		child, err := parentCtx.Plugin(namedPlugin{name: fmt.Sprintf("child-%d", i)}, nil)
		if err != nil {
			t.Fatalf("child %d: %v", i, err)
		}
		if err := child.Dispose(); err != nil {
			t.Fatalf("child dispose %d: %v", i, err)
		}
	}
	if got := len(parent.Effects()); got > baseline+2 {
		t.Fatalf("parent effect bookkeeping grew: %d (baseline %d) after child churn", got, baseline)
	}
	if err := parent.Dispose(); err != nil {
		t.Fatalf("parent dispose: %v", err)
	}
}

func TestAsyncDisposerJoin(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	disposer := cordis.NewAsyncDisposer([]cordis.AsyncCleanup{
		func(context.Context) error {
			runs.Add(1)
			close(entered)
			<-release
			return nil
		},
	})
	const joiners = 3
	var wg sync.WaitGroup
	wg.Add(joiners)
	for i := 0; i < joiners; i++ {
		go func() {
			defer wg.Done()
			_ = disposer.DisposeAsync(context.Background())
		}()
	}
	<-entered
	// Give the joiners a chance to enter the join path.
	time.Sleep(10 * time.Millisecond)
	close(release)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent DisposeAsync callers did not return")
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("cleanup ran %d times, want 1", got)
	}
	if !disposer.IsDisposed() {
		t.Fatal("disposer should be settled")
	}
}

func TestAsyncDisposerSelfJoinFromCleanup(t *testing.T) {
	var disposer *cordis.AsyncDisposer
	selfJoinReturned := make(chan struct{})
	disposer = cordis.NewAsyncDisposer([]cordis.AsyncCleanup{
		func(cleanupCtx context.Context) error {
			// Calling back into our own disposer with the context received must
			// return immediately instead of waiting for ourselves.
			done := make(chan error, 1)
			go func() { done <- disposer.DisposeAsync(cleanupCtx) }()
			select {
			case err := <-done:
				if err != nil {
					return err
				}
			case <-time.After(time.Second):
				return errors.New("self-join did not short-circuit")
			}
			close(selfJoinReturned)
			return nil
		},
	})
	if err := disposer.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	select {
	case <-selfJoinReturned:
	default:
		t.Fatal("cleanup did not run to completion")
	}
}

func TestDisposeDuringApplyWins(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var live *cordis.Fiber
	if _, err := root.OnInternalPlugin(func(f *cordis.Fiber) {
		mu.Lock()
		live = f
		mu.Unlock()
	}); err != nil {
		t.Fatalf("hook: %v", err)
	}
	applyDone := make(chan struct{})
	go func() {
		defer close(applyDone)
		_, _ = root.Plugin(&funcPlugin{name: "slow", activate: func(ctx cordis.Context) error {
			close(started)
			<-release
			// Registrations made after a concurrent dispose are rejected; a
			// plugin may observe the error and unwind.
			_, _ = ctx.Effect(func() cordis.Effect {
				return cordis.Effect{cordis.CleanupOf(func() {})}
			})
			return nil
		}}, nil)
	}()
	<-started
	mu.Lock()
	fiber := live
	mu.Unlock()
	if fiber == nil {
		t.Fatal("internal/plugin hook did not observe the fiber")
	}
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose during apply: %v", err)
	}
	if fiber.State() != cordis.Disposed {
		t.Fatalf("state after dispose = %v, want Disposed", fiber.State())
	}
	close(release)
	<-applyDone
	// The late activation result must be discarded: still Disposed, not Active.
	if fiber.State() != cordis.Disposed {
		t.Fatalf("late activation resurrected the fiber: %v", fiber.State())
	}
}

func TestAwaitSettledWaitsForActivation(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	release := make(chan struct{})
	var mu sync.Mutex
	var live *cordis.Fiber
	if _, err := root.OnInternalPlugin(func(f *cordis.Fiber) {
		mu.Lock()
		live = f
		mu.Unlock()
	}); err != nil {
		t.Fatalf("hook: %v", err)
	}
	pluginDone := make(chan struct{})
	go func() {
		defer close(pluginDone)
		_, _ = root.Plugin(&funcPlugin{name: "settle", activate: func(cordis.Context) error {
			<-release
			return nil
		}}, nil)
	}()
	var fiber *cordis.Fiber
	waitFor(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		fiber = live
		return fiber != nil
	}, "fiber observed")
	settled := make(chan error, 1)
	go func() { settled <- fiber.AwaitSettled() }()
	select {
	case err := <-settled:
		t.Fatalf("AwaitSettled returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-pluginDone
	if err := <-settled; err != nil {
		t.Fatalf("AwaitSettled: %v", err)
	}
	if fiber.State() != cordis.Active {
		t.Fatalf("state = %v", fiber.State())
	}
}

func TestFailedActivationAwaitSettledRethrows(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	fiber, err := root.Plugin(&funcPlugin{name: "settle-fail", activate: func(cordis.Context) error {
		return cordis.PluginError("settle-fail", "config", "bad key")
	}}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	settleErr := fiber.AwaitSettled()
	if settleErr == nil {
		t.Fatal("AwaitSettled should rethrow the activation error")
	}
	var structured *cordis.CordisError
	if !errors.As(settleErr, &structured) || structured.Code() != cordis.CodePlugin {
		t.Fatalf("settle err = %v, want structured Plugin error", settleErr)
	}
	if structured.PluginKind() != "config" {
		t.Fatalf("plugin kind = %q", structured.PluginKind())
	}
}

func TestAccessorSemantics(t *testing.T) {
	root := cordis.Root()
	defer disposeAllQuietly(root)
	flag := false
	if _, err := root.Accessor("computed", func(cordis.Context) cordis.ServiceValue {
		if flag {
			return "on"
		}
		return nil
	}); err != nil {
		t.Fatalf("accessor: %v", err)
	}
	if value, ok := root.GetNamed("computed"); ok || value != nil {
		t.Fatalf("accessor get = %v/%v, want absent", value, ok)
	}
	flag = true
	if value, ok := root.GetNamed("computed"); !ok || value != "on" {
		t.Fatalf("accessor get = %v/%v, want on", value, ok)
	}
	// Read-only accessor rejects writes with AccessorReadOnly.
	if err := root.SetNamed("computed", "x"); codeOf(err) != cordis.CodeAccessorReadOnly {
		t.Fatalf("write err = %v, want AccessorReadOnly", err)
	}
	// A setter may reject the write with AccessorWriteRejected.
	writeRoot := cordis.Root()
	defer disposeAllQuietly(writeRoot)
	if _, err := writeRoot.AccessorWithSetter("writable",
		func(cordis.Context) cordis.ServiceValue { return "v" },
		func(cordis.Context, cordis.ServiceValue) bool { return false },
	); err != nil {
		t.Fatalf("accessor: %v", err)
	}
	if err := writeRoot.SetNamed("writable", "new"); codeOf(err) != cordis.CodeAccessorWriteRejected {
		t.Fatalf("write err = %v, want AccessorWriteRejected", err)
	}
}

func codeOf(err error) cordis.ErrCode {
	code, _ := cordis.CodeOf(err)
	return code
}
