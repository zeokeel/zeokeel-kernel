package runtime_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	cordis "github.com/zeokeel/zeokeel-kernel"
)

// ---------------------------------------------------------------------------
// CORE-017: listeners are owned by the registering fiber and are recycled on
// unload, on both the sync and the async lane.
// Rust: lifecycle.rs:137::event_bail_and_owner_disposal,
//       async_conformance.rs:565::async_listeners_unload_with_their_fiber
// ---------------------------------------------------------------------------

func TestCore017_SyncListenerRecycledWithOwner(t *testing.T) {
	root := newRoot(t)
	hits := 0
	owner, err := root.Plugin(setupPlugin{name: "listener-owner", setup: func(c cordis.Context) error {
		_, err := c.On("evt", func([]cordis.Value) cordis.EventResult {
			hits++
			return cordis.Bail("stop")
		})
		return err
	}}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := root.Events().ListenerCount(); got != 1 {
		t.Fatalf("listener count after registration = %d, want 1", got)
	}
	if value, ok := root.Bail("evt"); !ok || value != "stop" {
		t.Fatalf("bail before dispose = %v/%v, want stop/true", value, ok)
	}
	if hits != 1 {
		t.Fatalf("listener hits = %d, want 1", hits)
	}
	if err := owner.Dispose(); err != nil {
		t.Fatalf("dispose owner: %v", err)
	}
	if got := root.Events().ListenerCount(); got != 0 {
		t.Fatalf("listener count after owner disposal = %d, want 0", got)
	}
	if value, ok := root.Bail("evt"); ok || value != nil {
		t.Fatalf("bail after owner disposal = %v/%v, want nil/false", value, ok)
	}
	if hits != 1 {
		t.Fatalf("disposed listener still ran (hits = %d, want 1)", hits)
	}
}

func TestCore017_AsyncListenerRecycledWithOwner(t *testing.T) {
	root := newRoot(t)
	var hits atomic.Int64
	owner, err := root.Plugin(setupPlugin{name: "async-listener-owner", setup: func(c cordis.Context) error {
		_, err := c.OnAsync("evt", func(context.Context, []cordis.Value) cordis.EventResult {
			hits.Add(1)
			return cordis.Continue()
		})
		return err
	}}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := root.Events().AsyncListenerCount(); got != 1 {
		t.Fatalf("async listener count = %d, want 1", got)
	}
	root.ParallelAsync("evt")
	if got := hits.Load(); got != 1 {
		t.Fatalf("async listener hits = %d, want 1", got)
	}
	if err := owner.Dispose(); err != nil {
		t.Fatalf("dispose owner: %v", err)
	}
	if got := root.Events().AsyncListenerCount(); got != 0 {
		t.Fatalf("async listener count after owner disposal = %d, want 0", got)
	}
	root.ParallelAsync("evt")
	if got := hits.Load(); got != 1 {
		t.Fatalf("async listener hits after owner disposal = %d, want 1", got)
	}
}

// TestCore017_ListenerRemovedOnActivationRollback covers the second owner
// recovery path: a failed activation rolls back the listeners it registered
// (Rust: fail_fiber_* / discard_activation_effects remove_owner).
func TestCore017_ListenerRemovedOnActivationRollback(t *testing.T) {
	root := newRoot(t)
	var hits atomic.Int64
	fiber, err := root.Plugin(setupPlugin{name: "rollback-owner", setup: func(c cordis.Context) error {
		if _, err := c.OnAsync("evt", func(context.Context, []cordis.Value) cordis.EventResult {
			hits.Add(1)
			return cordis.Continue()
		}); err != nil {
			return err
		}
		return errors.New("apply failed after registering a listener")
	}}, nil)
	if err != nil {
		t.Fatalf("plugin creation returned %v, want a Failed fiber with nil error", err)
	}
	if got := fiber.State(); got != cordis.Failed {
		t.Fatalf("fiber state = %v, want Failed", got)
	}
	if got := root.Events().ListenerCount(); got != 0 {
		t.Fatalf("listener count after failed activation = %d, want 0 (rolled back)", got)
	}
	root.ParallelAsync("evt")
	if got := hits.Load(); got != 0 {
		t.Fatalf("rolled-back listener ran %d times, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// CORE-018: prepend/global/once options and Bail value semantics.
// Rust: events.rs:42::event_options_prepend_and_global,
//       events.rs:23::event_once_fires_exactly_once,
//       events.rs:156::bail_ignores_null_and_false_values
// ---------------------------------------------------------------------------

func TestCore018_PrependAndGlobalOptions(t *testing.T) {
	root := newRoot(t)

	// prepend inserts at the head of the registration order.
	var order []string
	if _, err := root.On("demo", func([]cordis.Value) cordis.EventResult {
		order = append(order, "append")
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on append: %v", err)
	}
	if _, err := root.OnWithOptions("demo", cordis.EventOptions{Prepend: true}, func([]cordis.Value) cordis.EventResult {
		order = append(order, "prepend")
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on prepend: %v", err)
	}
	root.Emit("demo")
	assertTrace(t, "prepend order", order, []string{"prepend", "append"})

	// global listeners receive events from every scope; scoped listeners only
	// receive events dispatched in an equal scope.
	globalHits, scopedHits := 0, 0
	if _, err := root.OnWithOptions("g", cordis.EventOptions{Global: true}, func([]cordis.Value) cordis.EventResult {
		globalHits++
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("global listener: %v", err)
	}
	if _, err := root.On("g", func([]cordis.Value) cordis.EventResult {
		scopedHits++
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("scoped listener: %v", err)
	}
	isolated := root.Isolate("demo")
	root.Emit("g")
	if globalHits != 1 || scopedHits != 1 {
		t.Fatalf("after root emit: global=%d scoped=%d, want 1/1", globalHits, scopedHits)
	}
	isolated.Emit("g")
	if globalHits != 2 {
		t.Fatalf("global listener hits after isolated emit = %d, want 2", globalHits)
	}
	if scopedHits != 1 {
		t.Fatalf("scoped root listener hits after isolated emit = %d, want 1 (no cross-scope leak)", scopedHits)
	}
}

func TestCore018_OnceFiresExactlyOnce(t *testing.T) {
	root := newRoot(t)
	hits := 0
	disposer, err := root.Once("once", func([]cordis.Value) cordis.EventResult {
		hits++
		return cordis.Continue()
	})
	if err != nil {
		t.Fatalf("once: %v", err)
	}
	root.Emit("once", 1)
	root.Emit("once", 2)
	if hits != 1 {
		t.Fatalf("once listener hits = %d, want 1", hits)
	}
	if got := root.Events().ListenerCount(); got != 0 {
		t.Fatalf("once listener count = %d, want 0 (auto-removed)", got)
	}
	// The disposer stays safe and idempotent after the automatic removal.
	disposer.Dispose()
	disposer.Dispose()
	root.Emit("once", 3)
	if hits != 1 {
		t.Fatalf("once listener hits after disposer = %d, want 1", hits)
	}
}

func TestCore018_OnceIsAtMostOnceUnderConcurrentDispatch(t *testing.T) {
	root := newRoot(t)
	var hits atomic.Int64
	if _, err := root.Once("concurrent-once", func([]cordis.Value) cordis.EventResult {
		hits.Add(1)
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("once: %v", err)
	}
	var wg sync.WaitGroup
	const dispatchers = 16
	wg.Add(dispatchers)
	for i := 0; i < dispatchers; i++ {
		go func() {
			defer wg.Done()
			root.Emit("concurrent-once")
		}()
	}
	wg.Wait()
	if got := hits.Load(); got != 1 {
		t.Fatalf("concurrent once hits = %d, want 1", got)
	}
}

func TestCore018_BailIgnoresNullAndFalse(t *testing.T) {
	root := newRoot(t)
	var seen []string
	for _, tc := range []struct {
		label string
		value cordis.Value
	}{
		{"null", nil},
		{"false", false},
		{"stop", "stop"},
	} {
		tc := tc
		if _, err := root.On("bail", func([]cordis.Value) cordis.EventResult {
			seen = append(seen, tc.label)
			return cordis.Bail(tc.value)
		}); err != nil {
			t.Fatalf("on %s: %v", tc.label, err)
		}
	}
	if _, err := root.On("bail", func([]cordis.Value) cordis.EventResult {
		seen = append(seen, "after-bail")
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on after: %v", err)
	}

	value, ok := root.Bail("bail")
	if !ok || value != "stop" {
		t.Fatalf("bail result = %v/%v, want stop/true", value, ok)
	}
	assertTrace(t, "sync bail seen", seen, []string{"null", "false", "stop"})

	// serial_async reuses the same bail semantics and stops at the first real
	// bail (remaining listeners do not run).
	seen = nil
	value, ok = root.SerialAsync("bail")
	if !ok || value != "stop" {
		t.Fatalf("serial_async result = %v/%v, want stop/true", value, ok)
	}
	assertTrace(t, "serial_async bail seen", seen, []string{"null", "false", "stop"})

	// emit ignores Bail values and runs every listener.
	seen = nil
	root.Emit("bail")
	assertTrace(t, "emit ignores bail", seen, []string{"null", "false", "stop", "after-bail"})
}

// ---------------------------------------------------------------------------
// CORE-019: event dispatch scope filter — dispatch scope must equal the
// listener registration scope, except for global listeners. Events never
// propagate to ancestors.
// Rust: events.rs:80::event_context_filter_matches_scopes,
//       events.rs:119::event_waterfall_respects_context_filter
// ---------------------------------------------------------------------------

func TestCore019_EventScopeFilter(t *testing.T) {
	root := newRoot(t)
	isolated := root.Isolate("demo")
	rootHits, isoHits := 0, 0
	if _, err := root.On("evt", func([]cordis.Value) cordis.EventResult {
		rootHits++
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("root on: %v", err)
	}
	if _, err := isolated.On("evt", func([]cordis.Value) cordis.EventResult {
		isoHits++
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("isolated on: %v", err)
	}
	root.Emit("evt")
	if rootHits != 1 || isoHits != 0 {
		t.Fatalf("after root emit: root=%d isolated=%d, want 1/0", rootHits, isoHits)
	}
	isolated.Emit("evt")
	if rootHits != 1 || isoHits != 1 {
		t.Fatalf("after isolated emit: root=%d isolated=%d, want 1/1 (no upward propagation)", rootHits, isoHits)
	}
}

func TestCore019_WaterfallScopeFilter(t *testing.T) {
	root := newRoot(t)
	isolated := root.Isolate("demo")
	rootSeen, isoSeen := 0, 0
	if _, err := root.OnWaterfall("wf", func(_ []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		rootSeen++
		return next.Next()
	}); err != nil {
		t.Fatalf("root waterfall: %v", err)
	}
	if _, err := isolated.OnWaterfall("wf", func(_ []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		isoSeen++
		return next.Next()
	}); err != nil {
		t.Fatalf("isolated waterfall: %v", err)
	}

	if got := root.Waterfall("wf", []cordis.Value{1}, func(args []cordis.Value) cordis.Value {
		return args[0]
	}); got != 1 {
		t.Fatalf("root waterfall value = %v, want 1", got)
	}
	if rootSeen != 1 || isoSeen != 0 {
		t.Fatalf("after root waterfall: rootSeen=%d isolatedSeen=%d, want 1/0", rootSeen, isoSeen)
	}
	if got := isolated.Waterfall("wf", []cordis.Value{2}, func(args []cordis.Value) cordis.Value {
		return args[0]
	}); got != 2 {
		t.Fatalf("isolated waterfall value = %v, want 2", got)
	}
	if rootSeen != 1 || isoSeen != 1 {
		t.Fatalf("after isolated waterfall: rootSeen=%d isolatedSeen=%d, want 1/1", rootSeen, isoSeen)
	}
}

// ---------------------------------------------------------------------------
// CORE-020: waterfall middlewares — explicit Next chain, onion order, veto,
// repeatable Next, terminal shortcut, fiber ownership, and fail-closed panics.
// Rust: async_conformance.rs:253..817, events.rs:119, waterfall.rs
// ---------------------------------------------------------------------------

func TestCore020_WaterfallOrderAndRewrite(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	args := []cordis.Value{"payload", 1}
	if _, err := root.OnWaterfall("chain", func(gotArgs []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		tr.add("outer:in")
		if !reflect.DeepEqual(gotArgs, args) {
			t.Errorf("outer got args %v, want %v", gotArgs, args)
		}
		if !reflect.DeepEqual(next.Args(), args) {
			t.Errorf("outer next args %v, want %v", next.Args(), args)
		}
		if got := next.Remaining(); got != 1 {
			t.Errorf("outer remaining = %d, want 1", got)
		}
		inner := next.Next()
		tr.add("outer:out")
		return map[string]cordis.Value{"outer": inner}
	}); err != nil {
		t.Fatalf("outer middleware: %v", err)
	}
	if _, err := root.OnWaterfall("chain", func(_ []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		tr.add("inner:in")
		if got := next.Remaining(); got != 0 {
			t.Errorf("inner remaining = %d, want 0", got)
		}
		terminal := next.Next()
		tr.add("inner:out")
		return map[string]cordis.Value{"inner": terminal}
	}); err != nil {
		t.Fatalf("inner middleware: %v", err)
	}

	terminalCalls := 0
	value := root.Waterfall("chain", args, func(termArgs []cordis.Value) cordis.Value {
		terminalCalls++
		if !reflect.DeepEqual(termArgs, args) {
			t.Errorf("terminal got args %v, want %v", termArgs, args)
		}
		return "terminal"
	})
	if terminalCalls != 1 {
		t.Fatalf("terminal calls = %d, want 1", terminalCalls)
	}
	assertTrace(t, "waterfall onion order", tr.snapshot(),
		[]string{"outer:in", "inner:in", "inner:out", "outer:out"})
	assertDeepEqual(t, "waterfall rewritten value", value,
		map[string]cordis.Value{"outer": map[string]cordis.Value{"inner": "terminal"}})
}

func TestCore020_WaterfallVetoStopsChain(t *testing.T) {
	root := newRoot(t)
	laterRuns, terminalRuns := 0, 0
	if _, err := root.OnWaterfall("veto", func([]cordis.Value, *cordis.WaterfallNext) cordis.Value {
		return "vetoed"
	}); err != nil {
		t.Fatalf("veto middleware: %v", err)
	}
	if _, err := root.OnWaterfall("veto", func(_ []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		laterRuns++
		return next.Next()
	}); err != nil {
		t.Fatalf("later middleware: %v", err)
	}
	value := root.Waterfall("veto", nil, func([]cordis.Value) cordis.Value {
		terminalRuns++
		return "base"
	})
	if value != "vetoed" {
		t.Fatalf("vetoed waterfall value = %v, want vetoed", value)
	}
	if laterRuns != 0 {
		t.Fatalf("middleware after veto ran %d times, want 0", laterRuns)
	}
	if terminalRuns != 0 {
		t.Fatalf("terminal ran %d times after veto, want 0", terminalRuns)
	}
}

func TestCore020_WaterfallNextMayBeCalledTwice(t *testing.T) {
	root := newRoot(t)
	terminalRuns := 0
	if _, err := root.OnWaterfall("twice", func(_ []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		first := next.Next()
		second := next.Next()
		return []cordis.Value{first, second}
	}); err != nil {
		t.Fatalf("middleware: %v", err)
	}
	value := root.Waterfall("twice", nil, func([]cordis.Value) cordis.Value {
		terminalRuns++
		return terminalRuns
	})
	if terminalRuns != 2 {
		t.Fatalf("terminal runs = %d, want 2 (Next is not once)", terminalRuns)
	}
	assertDeepEqual(t, "next-twice terminal results", value, []cordis.Value{1, 2})
}

func TestCore020_WaterfallTerminalSkipsRemaining(t *testing.T) {
	root := newRoot(t)
	laterRuns := 0
	if _, err := root.OnWaterfall("term", func(_ []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		return next.Terminal()
	}); err != nil {
		t.Fatalf("terminal middleware: %v", err)
	}
	if _, err := root.OnWaterfall("term", func(_ []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		laterRuns++
		return next.Next()
	}); err != nil {
		t.Fatalf("later middleware: %v", err)
	}
	value := root.Waterfall("term", nil, func([]cordis.Value) cordis.Value { return "base" })
	if value != "base" {
		t.Fatalf("terminal-shortcut value = %v, want base", value)
	}
	if laterRuns != 0 {
		t.Fatalf("later middleware ran %d times after Terminal(), want 0", laterRuns)
	}
}

func TestCore020_WaterfallUnloadsWithOwner(t *testing.T) {
	root := newRoot(t)
	owner, err := root.Plugin(setupPlugin{name: "waterfall-owner", setup: func(c cordis.Context) error {
		_, err := c.OnWaterfall("owned", func(_ []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
			return "owned(" + next.Next().(string) + ")"
		})
		return err
	}}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := root.Events().WaterfallListenerCount(); got != 1 {
		t.Fatalf("waterfall listener count = %d, want 1", got)
	}
	if err := owner.Dispose(); err != nil {
		t.Fatalf("dispose owner: %v", err)
	}
	if got := root.Events().WaterfallListenerCount(); got != 0 {
		t.Fatalf("waterfall listener count after disposal = %d, want 0", got)
	}
	if value := root.Waterfall("owned", nil, func([]cordis.Value) cordis.Value { return "base" }); value != "base" {
		t.Fatalf("waterfall after owner disposal = %v, want base", value)
	}
}

func TestCore020_AsyncWaterfallOrderVetoAndAwait(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	if _, err := root.OnWaterfallAsync("awf", func(ctx context.Context, _ []cordis.Value, next *cordis.AsyncWaterfallNext) cordis.Value {
		tr.add("outer:in")
		value := next.NextWith(ctx)
		tr.add("outer:out")
		return "outer(" + value.(string) + ")"
	}); err != nil {
		t.Fatalf("outer async middleware: %v", err)
	}
	if _, err := root.OnWaterfallAsync("awf", func(ctx context.Context, _ []cordis.Value, next *cordis.AsyncWaterfallNext) cordis.Value {
		tr.add("inner:in")
		value := next.Next()
		tr.add("inner:out")
		return "inner(" + value.(string) + ")"
	}); err != nil {
		t.Fatalf("inner async middleware: %v", err)
	}
	value := root.WaterfallAsync(context.Background(), "awf", nil, func(context.Context, []cordis.Value) cordis.Value {
		return "terminal"
	})
	assertTrace(t, "async waterfall onion order", tr.snapshot(),
		[]string{"outer:in", "inner:in", "inner:out", "outer:out"})
	if value != "outer(inner(terminal))" {
		t.Fatalf("async waterfall value = %v, want outer(inner(terminal))", value)
	}

	// A vetoing async middleware blocks later middleware and the terminal.
	vetoRoot := newRoot(t)
	laterRuns, terminalRuns := 0, 0
	if _, err := vetoRoot.OnWaterfallAsync("veto", func(context.Context, []cordis.Value, *cordis.AsyncWaterfallNext) cordis.Value {
		return "async-vetoed"
	}); err != nil {
		t.Fatalf("async veto middleware: %v", err)
	}
	if _, err := vetoRoot.OnWaterfallAsync("veto", func(ctx context.Context, _ []cordis.Value, next *cordis.AsyncWaterfallNext) cordis.Value {
		laterRuns++
		return next.Next()
	}); err != nil {
		t.Fatalf("async later middleware: %v", err)
	}
	value = vetoRoot.WaterfallAsync(context.Background(), "veto", nil, func(context.Context, []cordis.Value) cordis.Value {
		terminalRuns++
		return "base"
	})
	if value != "async-vetoed" {
		t.Fatalf("async veto value = %v, want async-vetoed", value)
	}
	if laterRuns != 0 || terminalRuns != 0 {
		t.Fatalf("async veto: later=%d terminal=%d, want 0/0", laterRuns, terminalRuns)
	}
}

func TestCore020_WaterfallPanicIsFailClosed(t *testing.T) {
	// Sync lane: a panicking middleware aborts the chain (remaining middleware
	// and terminal must not run) and the panic is re-raised to the dispatcher.
	root := newRoot(t)
	laterRuns, terminalRuns := 0, 0
	if _, err := root.OnWaterfall("panicky", func([]cordis.Value, *cordis.WaterfallNext) cordis.Value {
		panic("sync waterfall boom")
	}); err != nil {
		t.Fatalf("panic middleware: %v", err)
	}
	if _, err := root.OnWaterfall("panicky", func(_ []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		laterRuns++
		return next.Next()
	}); err != nil {
		t.Fatalf("later middleware: %v", err)
	}
	syncPanic := func() (recovered any) {
		defer func() { recovered = recover() }()
		root.Waterfall("panicky", nil, func([]cordis.Value) cordis.Value {
			terminalRuns++
			return "base"
		})
		return nil
	}()
	if syncPanic != "sync waterfall boom" {
		t.Fatalf("sync waterfall panic = %v, want the original payload", syncPanic)
	}
	if laterRuns != 0 || terminalRuns != 0 {
		t.Fatalf("sync waterfall panic: later=%d terminal=%d, want 0/0 (fail-closed)", laterRuns, terminalRuns)
	}

	// Async lane is fail-closed as well.
	asyncRoot := newRoot(t)
	asyncTerminal, asyncLater := 0, 0
	if _, err := asyncRoot.OnWaterfallAsync("panicky", func(context.Context, []cordis.Value, *cordis.AsyncWaterfallNext) cordis.Value {
		panic("async waterfall boom")
	}); err != nil {
		t.Fatalf("async panic middleware: %v", err)
	}
	if _, err := asyncRoot.OnWaterfallAsync("panicky", func(ctx context.Context, _ []cordis.Value, next *cordis.AsyncWaterfallNext) cordis.Value {
		asyncLater++
		return next.Next()
	}); err != nil {
		t.Fatalf("async later middleware: %v", err)
	}
	asyncPanic := func() (recovered any) {
		defer func() { recovered = recover() }()
		asyncRoot.WaterfallAsync(context.Background(), "panicky", nil, func(context.Context, []cordis.Value) cordis.Value {
			asyncTerminal++
			return "base"
		})
		return nil
	}()
	if asyncPanic != "async waterfall boom" {
		t.Fatalf("async waterfall panic = %v, want the original payload", asyncPanic)
	}
	if asyncLater != 0 || asyncTerminal != 0 {
		t.Fatalf("async waterfall panic: later=%d terminal=%d, want 0/0 (fail-closed)", asyncLater, asyncTerminal)
	}
}

// ---------------------------------------------------------------------------
// CORE-021: the nine typed internal/* hooks — plugin, status, service, config,
// update, get, set, listener, dispatch — with fixed order and veto/rewrite
// semantics. Rust: internal.rs:34..307, api_surface.rs:430
// ---------------------------------------------------------------------------

// recorderPlugin captures the config it was activated with (for config-hook
// rewrite assertions).
type recorderPlugin struct {
	name    string
	applied []cordis.Value
}

func (p *recorderPlugin) Name() string { return p.name }

func (p *recorderPlugin) Activate(_ cordis.Context, config cordis.Value) error {
	p.applied = append(p.applied, config)
	return nil
}

func TestCore021_InternalLifecycleHooksOrder(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	if _, err := root.OnInternalPlugin(func(f *cordis.Fiber) {
		tr.add("plugin:" + f.Name())
	}); err != nil {
		t.Fatalf("internal/plugin: %v", err)
	}
	if _, err := root.OnInternalStatus(func(f *cordis.Fiber, old cordis.FiberState) {
		// The status hook observes the *old* state; the new one is readable from
		// the fiber at notification time.
		tr.add("status:" + old.String())
	}); err != nil {
		t.Fatalf("internal/status: %v", err)
	}
	if _, err := root.OnInternalConfig(func(f *cordis.Fiber, config cordis.Value, next *cordis.ConfigNext) cordis.Value {
		tr.add("config:" + f.Name() + ":" + f.State().String())
		object, ok := config.(map[string]cordis.Value)
		if !ok {
			t.Errorf("config hook got %T, want object", config)
			return next.Next(config)
		}
		object["from_hook"] = true
		return next.Next(object)
	}); err != nil {
		t.Fatalf("internal/config: %v", err)
	}

	plugin := &recorderPlugin{name: "recorder"}
	fiber, err := root.Plugin(plugin, map[string]cordis.Value{"a": 1})
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := fiber.State(); got != cordis.Active {
		t.Fatalf("fiber state = %v, want Active", got)
	}
	if len(plugin.applied) != 1 {
		t.Fatalf("activation count = %d, want 1", len(plugin.applied))
	}
	applied, ok := plugin.applied[0].(map[string]cordis.Value)
	if !ok {
		t.Fatalf("apply saw %T, want object", plugin.applied[0])
	}
	// The config hook's injected field is visible to apply and in fiber.Config.
	if applied["a"] != 1 || applied["from_hook"] != true {
		t.Fatalf("apply config = %v, want map[a:1 from_hook:true]", applied)
	}
	assertDeepEqual(t, "fiber.Config vs apply config", fiber.Config(), plugin.applied[0])

	// First four lifecycle events are fixed: create hook (fiber still Pending),
	// transition to Loading (status receives old=Pending), config waterfall
	// while Loading, transition to Active (status receives old=Loading).
	assertOrderedPrefix(t, "load lifecycle trace", tr.snapshot(), []string{
		"plugin:recorder",
		"status:pending",
		"config:recorder:loading",
		"status:loading",
	})

	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	// Disposal transitions are Active→Unloading→Disposed; the status hook sees
	// the old states. In reverse order the last two events are
	// ["status:unloading", "status:active"].
	got := tr.snapshot()
	if len(got) < 2 {
		t.Fatalf("lifecycle trace too short: %v", got)
	}
	assertTrace(t, "dispose lifecycle tail", got[len(got)-2:], []string{"status:active", "status:unloading"})
}

func TestCore021_InternalConfigHookTransformsConfig(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	// Two hooks run in registration order; each may rewrite the config before
	// passing it on. A hook that returns without calling Next is terminal for
	// the config waterfall.
	for _, name := range []string{"first", "second"} {
		name := name
		if _, err := root.OnInternalConfig(func(f *cordis.Fiber, config cordis.Value, next *cordis.ConfigNext) cordis.Value {
			tr.add(name + ":" + f.State().String())
			object := config.(map[string]cordis.Value)
			object[name] = true
			return next.Next(object)
		}); err != nil {
			t.Fatalf("internal/config %s: %v", name, err)
		}
	}
	plugin := &recorderPlugin{name: "config-target"}
	if _, err := root.Plugin(plugin, map[string]cordis.Value{"raw": 1}); err != nil {
		t.Fatalf("plugin: %v", err)
	}
	assertTrace(t, "config hook order/state", tr.snapshot(), []string{"first:loading", "second:loading"})
	applied := plugin.applied[0].(map[string]cordis.Value)
	if applied["raw"] != 1 || applied["first"] != true || applied["second"] != true {
		t.Fatalf("config after waterfall = %v, want raw + both hook fields", applied)
	}
}

func TestCore021_InternalUpdateVetoAndRewrite(t *testing.T) {
	root := newRoot(t)
	var runs int32
	seen := make([]cordis.Value, 0, 4)
	plugin := &countingPlugin{name: "update-target", runs: &runs, seen: &seen}
	fiber, err := root.Plugin(plugin, map[string]cordis.Value{"v": 1})
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := fiber.State(); got != cordis.Active {
		t.Fatalf("fiber state = %v, want Active", got)
	}

	// A hook that never calls Next vetoes the update: Ok, no restart, config
	// unchanged.
	calls := 0
	veto, err := root.OnInternalUpdate(func(_ *cordis.Fiber, _ cordis.Value, _ bool, _ *cordis.UpdateNext) {
		calls++
	})
	if err != nil {
		t.Fatalf("internal/update veto hook: %v", err)
	}
	if err := fiber.Update(map[string]cordis.Value{"v": 2}); err != nil {
		t.Fatalf("vetoed update returned %v, want nil (Ok)", err)
	}
	if calls != 1 {
		t.Fatalf("veto hook calls = %d, want 1", calls)
	}
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("activation count after veto = %d, want 1", got)
	}
	if cfg, _ := fiber.Config().(map[string]cordis.Value); cfg["v"] != 1 {
		t.Fatalf("config after veto = %v, want v=1", fiber.Config())
	}
	veto.Dispose()

	// A rewriting hook passes a modified config to the terminal: the new raw
	// config is committed and the fiber restarts once.
	rewrite, err := root.OnInternalUpdate(func(_ *cordis.Fiber, config cordis.Value, noSave bool, next *cordis.UpdateNext) {
		object := config.(map[string]cordis.Value)
		object["rewritten"] = true
		next.Next(object, noSave)
	})
	if err != nil {
		t.Fatalf("internal/update rewrite hook: %v", err)
	}
	defer rewrite.Dispose()
	if err := fiber.Update(map[string]cordis.Value{"v": 3}); err != nil {
		t.Fatalf("rewritten update returned %v, want nil", err)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("activation count after rewrite = %d, want 2", got)
	}
	cfg, _ := fiber.Config().(map[string]cordis.Value)
	if cfg["v"] != 3 || cfg["rewritten"] != true {
		t.Fatalf("config after rewrite = %v, want v=3 rewritten=true", cfg)
	}
	raw, _ := fiber.RawConfig().(map[string]cordis.Value)
	if raw["rewritten"] != true {
		t.Fatalf("raw config after rewrite = %v, want the rewritten config committed", fiber.RawConfig())
	}
}

// countingPlugin counts activations and records the configs it sees.
type countingPlugin struct {
	name string
	runs *int32
	seen *[]cordis.Value
}

func (p *countingPlugin) Name() string { return p.name }

func (p *countingPlugin) Activate(_ cordis.Context, config cordis.Value) error {
	atomic.AddInt32(p.runs, 1)
	*p.seen = append(*p.seen, config)
	return nil
}

func TestCore021_InternalServiceHookObservesProvideAndRemove(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	if _, err := root.OnInternalService(func(_ cordis.Context, name string, value cordis.ServiceValue, present bool) {
		if present {
			tr.add(name + ":provided")
			if value == nil {
				t.Errorf("internal/service provided %q with nil value", name)
			}
		} else {
			tr.add(name + ":removed")
		}
	}); err != nil {
		t.Fatalf("internal/service: %v", err)
	}
	key := cordis.NewServiceKey[string]("greeting")
	disposer, err := cordis.Provide(root, key, "hi")
	if err != nil {
		t.Fatalf("provide: %v", err)
	}
	disposer.Dispose()
	assertTrace(t, "service hook trace", tr.snapshot(), []string{"greeting:provided", "greeting:removed"})
}

func TestCore021_InternalGetWaterfall(t *testing.T) {
	root := newRoot(t)
	calls := 0
	if _, err := root.OnInternalGet(func(_ cordis.Context, name string, next *cordis.GetNext) (cordis.ServiceValue, bool) {
		calls++
		if name == "injected" {
			return "from-get-hook", true
		}
		// Not handled here: continue the chain down to the service store.
		return next.Next()
	}); err != nil {
		t.Fatalf("internal/get: %v", err)
	}
	key := cordis.NewServiceKey[string]("real")
	if _, err := cordis.Provide(root, key, "real-value"); err != nil {
		t.Fatalf("provide: %v", err)
	}
	if value, ok := root.GetNamed("injected"); !ok || value != "from-get-hook" {
		t.Fatalf("get(injected) = %v/%v, want from-get-hook/true", value, ok)
	}
	if value, ok := root.GetNamed("real"); !ok || value != "real-value" {
		t.Fatalf("get(real) = %v/%v, want real-value/true (fallthrough to terminal)", value, ok)
	}
	if _, ok := root.GetNamed("missing"); ok {
		t.Fatal("get(missing) reported present")
	}
	if calls != 3 {
		t.Fatalf("get hook calls = %d, want 3", calls)
	}
}

func TestCore021_InternalSetWaterfallForwardAndVeto(t *testing.T) {
	t.Run("forward", func(t *testing.T) {
		root := newRoot(t)
		key := cordis.NewServiceKey[string]("svc")
		if _, err := cordis.Provide(root, key, "orig"); err != nil {
			t.Fatalf("provide: %v", err)
		}
		calls := 0
		if _, err := root.OnInternalSet(func(_ cordis.Context, _ string, _ cordis.ServiceValue, next *cordis.SetNext) bool {
			calls++
			return next.Next()
		}); err != nil {
			t.Fatalf("internal/set: %v", err)
		}
		if err := root.SetNamed("svc", "new"); err != nil {
			t.Fatalf("set: %v", err)
		}
		if calls != 1 {
			t.Fatalf("forward hook calls = %d, want 1", calls)
		}
		if value, _ := root.GetNamed("svc"); value != "new" {
			t.Fatalf("service value after forwarding set = %v, want new", value)
		}
	})
	t.Run("veto", func(t *testing.T) {
		root := newRoot(t)
		key := cordis.NewServiceKey[string]("svc")
		if _, err := cordis.Provide(root, key, "orig"); err != nil {
			t.Fatalf("provide: %v", err)
		}
		calls := 0
		if _, err := root.OnInternalSet(func(_ cordis.Context, _ string, _ cordis.ServiceValue, _ *cordis.SetNext) bool {
			calls++
			return false // veto the write
		}); err != nil {
			t.Fatalf("internal/set: %v", err)
		}
		// A vetoed write is still Ok; the value is simply not written.
		if err := root.SetNamed("svc", "other"); err != nil {
			t.Fatalf("vetoed set returned %v, want nil (Ok)", err)
		}
		if calls != 1 {
			t.Fatalf("veto hook calls = %d, want 1", calls)
		}
		if value, _ := root.GetNamed("svc"); value != "orig" {
			t.Fatalf("service value after vetoed set = %v, want orig", value)
		}
	})
}

func TestCore021_InternalListenerOverride(t *testing.T) {
	root := newRoot(t)
	origHits, replacementHits := 0, 0
	var requests []cordis.ListenerRequest
	if _, err := root.OnInternalListener(func(req cordis.ListenerRequest) *cordis.ListenerOverride {
		requests = append(requests, req)
		if req.Name != "demo" {
			return nil
		}
		return &cordis.ListenerOverride{
			Callback: func([]cordis.Value) cordis.EventResult {
				replacementHits++
				return cordis.Continue()
			},
		}
	}); err != nil {
		t.Fatalf("internal/listener: %v", err)
	}
	if _, err := root.On("demo", func([]cordis.Value) cordis.EventResult {
		origHits++
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	if len(requests) != 1 {
		t.Fatalf("listener hook requests = %d, want 1", len(requests))
	}
	if requests[0].Name != "demo" || requests[0].Owner != root.Fiber().ID() {
		t.Fatalf("listener request = %+v, want name=demo owner=%d", requests[0], root.Fiber().ID())
	}
	root.Emit("demo")
	if replacementHits != 1 || origHits != 0 {
		t.Fatalf("listener override: replacement=%d original=%d, want 1/0", replacementHits, origHits)
	}
}

func TestCore021_InternalDispatchDoesNotRecurse(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	var modes []cordis.DispatchMode
	var modesMu sync.Mutex
	if _, err := root.OnInternalDispatch(func(info cordis.DispatchInfo) {
		tr.add("dispatch:" + info.Mode.String() + ":" + info.Name)
		modesMu.Lock()
		modes = append(modes, info.Mode)
		modesMu.Unlock()
		// Nested public dispatch and internal/* names must not be reported.
		root.Emit("inner")
		root.Emit("internal/whatever")
	}); err != nil {
		t.Fatalf("internal/dispatch: %v", err)
	}
	root.Emit("outer")
	root.Bail("outer")
	root.Waterfall("outer", nil, func([]cordis.Value) cordis.Value { return nil })
	root.ParallelAsync("outer")
	root.SerialAsync("outer")
	assertTrace(t, "internal/dispatch non-recursion", tr.snapshot(),
		[]string{
			"dispatch:emit:outer",
			"dispatch:bail:outer",
			"dispatch:waterfall:outer",
			"dispatch:parallel:outer",
			"dispatch:serial:outer",
		})
	modesMu.Lock()
	defer modesMu.Unlock()
	assertDeepEqual(t, "dispatch modes", modes, []cordis.DispatchMode{
		cordis.DispatchEmit, cordis.DispatchBail, cordis.DispatchWaterfall,
		cordis.DispatchParallel, cordis.DispatchSerial,
	})
}

// TestCore021_RawInternalBusIsNotFiberOwned checks the advanced surface:
// Events().Internal() registrations are caller-owned and outlive fibers.
func TestCore021_RawInternalBusIsNotFiberOwned(t *testing.T) {
	root := newRoot(t)
	var hits atomic.Int64
	raw := root.Events().Internal().OnStatus(func(*cordis.Fiber, cordis.FiberState) {
		hits.Add(1)
	})
	// Loading and disposing a plugin fiber is observed by the raw hook...
	first, err := root.Plugin(setupPlugin{name: "raw-first"}, nil)
	if err != nil {
		t.Fatalf("first plugin: %v", err)
	}
	if err := first.Dispose(); err != nil {
		t.Fatalf("dispose first: %v", err)
	}
	if got := hits.Load(); got == 0 {
		t.Fatal("raw internal/status hook did not observe any transition")
	}
	// ...and it survives the fiber unload (it is not fiber-owned).
	before := hits.Load()
	second, err := root.Plugin(setupPlugin{name: "raw-second"}, nil)
	if err != nil {
		t.Fatalf("second plugin: %v", err)
	}
	_ = second.Dispose()
	if got := hits.Load(); got <= before {
		t.Fatalf("raw hook hits = %d after second fiber, want > %d (survives fiber unload)", got, before)
	}
	// Only the caller's disposer removes it.
	raw.Dispose()
	raw.Dispose()
	settled := hits.Load()
	third, err := root.Plugin(setupPlugin{name: "raw-third"}, nil)
	if err != nil {
		t.Fatalf("third plugin: %v", err)
	}
	_ = third.Dispose()
	if got := hits.Load(); got != settled {
		t.Fatalf("raw hook fired after dispose: %d -> %d", settled, got)
	}
}

// ---------------------------------------------------------------------------
// CORE-031: a panicking sync listener does not abort dispatch; later listeners
// still run. (Waterfall middleware is the fail-closed exception, see
// TestCore020_WaterfallPanicIsFailClosed.)
// Rust: hardening.rs:190::event_listener_panic_does_not_abort_dispatch
// ---------------------------------------------------------------------------

func TestCore031_ListenerPanicDoesNotAbortDispatch(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	if _, err := root.On("evt", func([]cordis.Value) cordis.EventResult {
		panic("listener boom")
	}); err != nil {
		t.Fatalf("panic listener: %v", err)
	}
	if _, err := root.On("evt", func([]cordis.Value) cordis.EventResult {
		tr.add("later")
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("later listener: %v", err)
	}
	if _, err := root.On("evt", func([]cordis.Value) cordis.EventResult {
		tr.add("last")
		return cordis.Bail("stop")
	}); err != nil {
		t.Fatalf("bailing listener: %v", err)
	}

	// Emit must not unwind, and every listener after the panicking one runs.
	root.Emit("evt")
	assertTrace(t, "sync dispatch after listener panic", tr.snapshot(), []string{"later", "last"})

	// The same containment applies when the panic happens inside a bail
	// dispatch: the dispatch continues and the bail value is returned.
	tr2 := &trace{}
	panicRoot := newRoot(t)
	if _, err := panicRoot.On("evt", func([]cordis.Value) cordis.EventResult {
		panic("bail listener boom")
	}); err != nil {
		t.Fatalf("panic listener: %v", err)
	}
	if _, err := panicRoot.On("evt", func([]cordis.Value) cordis.EventResult {
		tr2.add("later")
		return cordis.Bail("stop")
	}); err != nil {
		t.Fatalf("bailing listener: %v", err)
	}
	value, ok := panicRoot.Bail("evt")
	if !ok || value != "stop" {
		t.Fatalf("bail after panicking listener = %v/%v, want stop/true", value, ok)
	}
	assertTrace(t, "bail dispatch after listener panic", tr2.snapshot(), []string{"later"})
}

func TestCore031_AsyncListenerPanicIsContained(t *testing.T) {
	root := newRoot(t)
	var laterRuns atomic.Int64
	if _, err := root.OnAsync("evt", func(context.Context, []cordis.Value) cordis.EventResult {
		panic("async listener boom")
	}); err != nil {
		t.Fatalf("panic async listener: %v", err)
	}
	if _, err := root.OnAsync("evt", func(context.Context, []cordis.Value) cordis.EventResult {
		laterRuns.Add(1)
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("later async listener: %v", err)
	}
	root.ParallelAsync("evt")
	if got := laterRuns.Load(); got != 1 {
		t.Fatalf("parallel async later listener runs = %d, want 1 (panic contained)", got)
	}
	// Serial lane: a panicking async listener is contained too.
	serialRoot := newRoot(t)
	var serialLater atomic.Int64
	if _, err := serialRoot.OnAsync("evt", func(context.Context, []cordis.Value) cordis.EventResult {
		panic("serial async listener boom")
	}); err != nil {
		t.Fatalf("serial panic listener: %v", err)
	}
	if _, err := serialRoot.OnAsync("evt", func(context.Context, []cordis.Value) cordis.EventResult {
		serialLater.Add(1)
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("serial later listener: %v", err)
	}
	serialRoot.SerialAsync("evt")
	if got := serialLater.Load(); got != 1 {
		t.Fatalf("serial async later listener runs = %d, want 1 (panic contained)", got)
	}
}
