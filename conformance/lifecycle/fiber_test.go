package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cordis "github.com/zeokeel/cordis-go"
)

// ---------------------------------------------------------------------------
// CORE-001: activation / unload / cleanup flow, is_active tracking
// (lifecycle.rs::effects_run_in_reverse_order_once,
//  ::event_bail_and_owner_disposal,
//  api_surface.rs::api_fiber_is_active_tracks_lifecycle_states)
// ---------------------------------------------------------------------------

func TestCore001_EffectsRunReverseOrderOnce(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	plugin := &pluginFunc{name: "effects", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cleanupOf("first", tr), cleanupOf("second", tr)}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	assertState(t, fiber, cordis.Active)
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	assertOrder(t, tr.snapshot(), []string{"second", "first"})
	// Dispose is idempotent: no duplicate cleanups.
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("second dispose: %v", err)
	}
	assertOrder(t, tr.snapshot(), []string{"second", "first"})
	assertState(t, fiber, cordis.Disposed)
}

func TestCore001_EventBailAndOwnerDisposal(t *testing.T) {
	root := newRoot(t)
	var hits atomic.Int64
	plugin := &pluginFunc{name: "events", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.On("demo", func([]cordis.Value) cordis.EventResult {
			hits.Add(1)
			return cordis.Bail("stop")
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}

	if value, ok := root.Bail("demo", 1); !ok || value != "stop" {
		t.Fatalf("bail = %v/%v, want stop/true", value, ok)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("listener hits = %d, want 1", got)
	}

	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	if value, ok := root.Bail("demo", 1); ok {
		t.Fatalf("bail after owner disposal = %v/true, want nil/false (listener must unload with its fiber)", value)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("listener hits after disposal = %d, want 1", got)
	}
}

func TestCore001_IsActiveTracksLifecycleStates(t *testing.T) {
	root := newRoot(t)
	// The root fiber is always active.
	if !root.Fiber().IsActive() {
		t.Fatalf("root fiber state = %v, want Active", root.Fiber().State())
	}

	key := cordis.NewServiceKey[string]("core001.greeting")
	var started atomic.Int64
	plugin := &pluginFunc{name: "pending", deps: []string{key.Name()}, activate: func(cordis.Context, cordis.Value) error {
		started.Add(1)
		return nil
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	// Pending must never run apply and is_active must be false.
	assertState(t, fiber, cordis.Pending)
	if fiber.IsActive() {
		t.Fatal("pending fiber reports is_active = true")
	}
	if got := started.Load(); got != 0 {
		t.Fatalf("apply ran while Pending (started = %d)", got)
	}

	if _, err := cordis.Provide(root, key, "hello"); err != nil {
		t.Fatalf("provide: %v", err)
	}
	assertState(t, fiber, cordis.Active)
	if !fiber.IsActive() {
		t.Fatal("active fiber reports is_active = false")
	}
	if got := started.Load(); got != 1 {
		t.Fatalf("apply runs = %d, want 1", got)
	}

	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	assertState(t, fiber, cordis.Disposed)
	if fiber.IsActive() {
		t.Fatal("disposed fiber reports is_active = true")
	}
}

// ---------------------------------------------------------------------------
// CORE-026: fiber diagnostics (uid/parent/assert_active/await_settled/settled)
// (fiber.rs::fiber_uid_parent_and_assert_active,
//  ::await_settled_rethrows_startup_errors,
//  ::await_settled_waits_for_async_effect_cleanup,
//  regression_core.rs::awaiting_settled_observes_refresh_unload)
// ---------------------------------------------------------------------------

func TestCore026_UIDParentAssertActive(t *testing.T) {
	root := newRoot(t)
	fiber, err := root.Plugin(noopPlugin("simple"), nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	uid, ok := fiber.UID()
	if !ok || uid != fiber.ID() {
		t.Fatalf("uid = %d/%v, want %d/true", uid, ok, fiber.ID())
	}
	if err := fiber.AssertActive(); err != nil {
		t.Fatalf("assert_active on Active fiber: %v", err)
	}
	parent, ok := fiber.Parent()
	if !ok {
		t.Fatal("fiber.parent missing")
	}
	if got, want := parent.FiberID(), root.Fiber().ID(); got != want {
		t.Fatalf("parent fiber id = %d, want %d (the context the plugin was loaded from)", got, want)
	}
}

// TestCore026_UIDClearedOnDispose pins the CORE-026 assertion `uid()==None`
// after disposal. The Go handle keeps the fiberData alive by design, but the
// observable diagnostic must reflect that the fiber left the registry.
func TestCore026_UIDClearedOnDispose(t *testing.T) {
	root := newRoot(t)
	fiber, err := root.Plugin(noopPlugin("uid-fiber"), nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	if uid, ok := fiber.UID(); ok {
		t.Fatalf("Fiber.UID() = %d/true after dispose, want 0/false (CORE-026 clears the uid)", uid)
	}
}

// TestCore026_AssertActiveAfterDisposeIsInactiveEffect pins the CORE-026
// assertion `assert_active()==Err(InactiveEffect)` after disposal.
func TestCore026_AssertActiveAfterDisposeIsInactiveEffect(t *testing.T) {
	root := newRoot(t)
	fiber, err := root.Plugin(noopPlugin("assert-fiber"), nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	err = fiber.AssertActive()
	if err == nil {
		t.Fatal("AssertActive on a disposed fiber returned nil")
	}
	assertCode(t, err, cordis.CodeInactiveEffect)
}

func TestCore026_AwaitSettledRethrowsStartupErrors(t *testing.T) {
	root := newRoot(t)
	schema := cordis.NewJsonConfigSchema().Required("model")
	plugin := &pluginFunc{name: "needs-model", schema: schema, activate: func(cordis.Context, cordis.Value) error {
		return nil
	}}
	fiber, err := root.Plugin(plugin, map[string]cordis.Value{})
	if err != nil {
		t.Fatalf("plugin handle: %v", err)
	}
	assertState(t, fiber, cordis.Failed)

	settledErr := fiber.AwaitSettled()
	assertCode(t, settledErr, cordis.CodeValidation)
	if msg := settledErr.Error(); !containsAll(msg, "model") {
		t.Fatalf("await_settled error = %q, want a message containing \"model\"", msg)
	}
	// The structured activation error is retained as-is.
	if detail := fiber.ErrorDetail(); detail == nil || detail.Error() != settledErr.Error() {
		t.Fatalf("error_detail = %v, want the same Validation error", detail)
	}
	if raw := fiber.Error(); raw == nil || !containsAll(raw.Error(), "model") {
		t.Fatalf("fiber.error() = %v, want a message containing \"model\"", raw)
	}
}

func TestCore026_AwaitSettledWaitsForAsyncEffectCleanup(t *testing.T) {
	root := newRoot(t)
	cleanupStarted := make(chan struct{})
	gate := make(chan struct{})
	plugin := &pluginFunc{name: "async-effect", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.EffectAsyncLabeled("async-effect", func() cordis.AsyncEffect {
			return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(context.Context) error {
				close(cleanupStarted)
				<-gate
				return nil
			})}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if labels := effectLabels(fiber.Effects()); len(labels) != 1 || labels[0] != "async-effect" {
		t.Fatalf("effects = %v, want [async-effect]", labels)
	}

	disposeDone := make(chan error, 1)
	go func() { disposeDone <- fiber.Dispose() }()
	awaitSignal(t, "async cleanup started", cleanupStarted)

	// While the unload is in flight, AwaitSettled must not report settled.
	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := fiber.AwaitSettledContext(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AwaitSettledContext during in-flight unload = %v, want deadline exceeded (settled == false)", err)
	}

	close(gate)
	if err := awaitChan(t, "dispose", disposeDone); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	if err := fiber.AwaitSettled(); err != nil {
		t.Fatalf("AwaitSettled after disposal: %v", err)
	}
	if got := fiber.Effects(); len(got) != 0 {
		t.Fatalf("effects after disposal = %s, want empty", effectTree(got))
	}
	assertState(t, fiber, cordis.Disposed)
}

// TestCore026_AwaitSettledObservesRefreshUnload covers
// regression_core.rs::awaiting_settled_observes_refresh_unload: a refresh-driven
// unload is in-flight work, so awaiting settlement converges with the fiber no
// longer Active (here: back to Pending).
func TestCore026_AwaitSettledObservesRefreshUnload(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("core026.gate")
	gate, err := cordis.Provide(root, key, "on")
	if err != nil {
		t.Fatalf("provide: %v", err)
	}
	consumer := &pluginFunc{name: "core026-consumer", deps: []string{key.Name()}, activate: func(cordis.Context, cordis.Value) error {
		return nil
	}}
	fiber, err := root.Plugin(consumer, nil)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	assertState(t, fiber, cordis.Active)

	gate.Dispose()
	if err := fiber.AwaitSettled(); err != nil {
		t.Fatalf("AwaitSettled after refresh unload: %v", err)
	}
	if got := fiber.State(); got == cordis.Active {
		t.Fatalf("fiber state = %v after refresh unload, want != Active", got)
	}
	assertState(t, fiber, cordis.Pending)
}

// ---------------------------------------------------------------------------
// CORE-027: async effect bodies/streams and restart teardown
// (async_effect_body.rs::async_effect_body_and_stream,
//  ::restart_awaits_async_effect_cleanups, ::async_effect_body_nests_labeled_effects;
//  async_conformance.rs::restart_async_drains_and_reapplies_async_effects;
//  lifecycle.rs::restart_reapplies_plugin_and_update_config)
//
// Go mapping note: ctx.EffectFuture takes the already-computed AsyncEffect and
// EffectStream feeds a channel; the async "body" of the Rust effect_future is a
// synchronous closure in Go, so the async-body *error* case is covered by
// CORE-036 (AsyncPlugin failure rollback).
// ---------------------------------------------------------------------------

func TestCore027_AsyncEffectBodyAndStream(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	plugin := &pluginFunc{name: "async-bodies", activate: func(c cordis.Context, _ cordis.Value) error {
		if _, err := c.EffectFuture(cordis.AsyncEffect{asyncCleanupOf("future", tr)}, "future-body"); err != nil {
			return err
		}
		_, err := c.EffectStream(func(ctx context.Context) <-chan cordis.Effect {
			ch := make(chan cordis.Effect, 2)
			ch <- cordis.Effect{cleanupOf("stream-1", tr)}
			ch <- cordis.Effect{cleanupOf("stream-2", tr)}
			close(ch)
			return ch
		}, "stream-body")
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	assertState(t, fiber, cordis.Active)

	// The stream is consumed on its own goroutine; wait for both nested items
	// before disposing so the teardown order is deterministic.
	waitFor(t, "stream effects registered", func() bool {
		labels := effectLabels(fiber.Effects())
		items := 0
		for _, label := range labels {
			if label == "ctx.effect_stream.item" {
				items++
			}
		}
		return items == 2 && hasLabel(fiber.Effects(), "future-body") && hasLabel(fiber.Effects(), "stream-body")
	})

	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	// Stream items are disposed before the future body: reverse registration
	// across the stream's own items and across effects.
	assertOrder(t, tr.snapshot(), []string{"stream-2", "stream-1", "future"})
	assertState(t, fiber, cordis.Disposed)
}

func TestCore027_RestartAwaitsAsyncEffectCleanups(t *testing.T) {
	root := newRoot(t)
	var runs, cleanups atomic.Int64
	plugin := &pluginFunc{name: "restartable", activate: func(c cordis.Context, _ cordis.Value) error {
		runs.Add(1)
		_, err := c.EffectFuture(cordis.AsyncEffect{
			cordis.AsyncCleanupOf(func(context.Context) error {
				cleanups.Add(1)
				return nil
			}),
		}, "restartable-effect")
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("runs = %d, want 1", got)
	}

	if err := fiber.RestartAsync(context.Background()); err != nil {
		t.Fatalf("RestartAsync: %v", err)
	}
	if got := cleanups.Load(); got != 1 {
		t.Fatalf("cleanups after restart = %d, want 1 (restart awaited the previous async cleanup)", got)
	}
	if got := runs.Load(); got != 2 {
		t.Fatalf("runs after restart = %d, want 2", got)
	}
	assertState(t, fiber, cordis.Active)

	// update_async drains too, and dispose_async drains the last one.
	if err := fiber.UpdateAsync(nil); err != nil {
		t.Fatalf("UpdateAsync: %v", err)
	}
	if got := cleanups.Load(); got != 2 {
		t.Fatalf("cleanups after update_async = %d, want 2", got)
	}
	if got := runs.Load(); got != 3 {
		t.Fatalf("runs after update_async = %d, want 3", got)
	}
	if err := fiber.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("DisposeAsync: %v", err)
	}
	if got := cleanups.Load(); got != 3 {
		t.Fatalf("cleanups after dispose_async = %d, want 3", got)
	}
	assertState(t, fiber, cordis.Disposed)
}

func TestCore027_AsyncBodyNestsLabeledEffects(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	plugin := &pluginFunc{name: "nested-async", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.EffectAsyncLabeled("outer-async", func() cordis.AsyncEffect {
			// A nested effect registered by the body belongs to the outer node.
			if _, nestedErr := c.EffectLabeled("inner", func() cordis.Effect {
				return cordis.Effect{cleanupOf("inner", tr)}
			}); nestedErr != nil {
				return nil
			}
			return cordis.AsyncEffect{asyncCleanupOf("outer", tr)}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := effectTree(fiber.Effects()); got != "[outer-async[inner[]]]" {
		t.Fatalf("effect tree = %s, want [outer-async[inner[]]]", got)
	}
}

// ---------------------------------------------------------------------------
// CORE-028: failed activation rolls back effects/services and keeps the
// structured error (hardening.rs::failed_apply_rolls_back_effects_and_services,
// api_surface.rs::api_structured_plugin_error_constructor_and_display,
// ::api_structured_plugin_error_survives_sync_activation,
// regression_core.rs::xt02_activation_dependency_lost_rolls_back_to_pending)
// ---------------------------------------------------------------------------

func TestCore028_FailedApplyRollsBackEffectsAndServices(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	key := cordis.NewServiceKey[string]("hardening.text")
	plugin := &pluginFunc{name: "fails-after-registering", activate: func(c cordis.Context, _ cordis.Value) error {
		if _, err := cordis.Provide(c, key, "stale"); err != nil {
			return err
		}
		if _, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cleanupOf("rolled-back", tr)}
		}); err != nil {
			return err
		}
		return cordis.PluginFailedError("apply failed")
	}}

	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin call itself must succeed, got: %v", err)
	}
	assertState(t, fiber, cordis.Failed)
	if fiber.Error() == nil {
		t.Fatal("failed fiber has no error")
	}
	assertCode(t, fiber.ErrorDetail(), cordis.CodePluginFailed)
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("failed activation leaked a service")
	}
	assertOrder(t, tr.snapshot(), []string{"rolled-back"})

	// The kernel keeps working: a healthy sibling still activates.
	sibling, err := root.Plugin(noopPlugin("healthy-sibling"), nil)
	if err != nil {
		t.Fatalf("sibling plugin: %v", err)
	}
	assertState(t, sibling, cordis.Active)

	// A dead provider must not be injectable: the dependent stays Pending.
	dependent := &pluginFunc{name: "needs-text", deps: []string{key.Name()}, activate: func(cordis.Context, cordis.Value) error {
		return errors.New("must not activate")
	}}
	dependentFiber, err := root.Plugin(dependent, nil)
	if err != nil {
		t.Fatalf("dependent plugin: %v", err)
	}
	assertState(t, dependentFiber, cordis.Pending)
}

func TestCore028_StructuredPluginError(t *testing.T) {
	root := newRoot(t)
	structured := cordis.PluginError("api-failing-sync", "config", "boom")
	if structured.Name() != "api-failing-sync" || structured.PluginKind() != "config" || structured.Detail() != "boom" {
		t.Fatalf("PluginError fields = name %q kind %q detail %q", structured.Name(), structured.PluginKind(), structured.Detail())
	}
	wantMessage := "plugin error: api-failing-sync (config): boom"
	if got := structured.Error(); got != wantMessage {
		t.Fatalf("PluginError.Error() = %q, want %q", got, wantMessage)
	}
	assertCode(t, structured, cordis.CodePlugin)

	plugin := &pluginFunc{name: "api-failing-sync", activate: func(cordis.Context, cordis.Value) error {
		return cordis.PluginError("api-failing-sync", "config", "boom")
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin handle: %v", err)
	}
	assertState(t, fiber, cordis.Failed)
	settled := fiber.AwaitSettled()
	assertCode(t, settled, cordis.CodePlugin)
	if got := settled.Error(); got != wantMessage {
		t.Fatalf("await_settled error = %q, want %q (structured error must survive activation unchanged)", got, wantMessage)
	}
	if got := fiber.Error(); got == nil || got.Error() != wantMessage {
		t.Fatalf("fiber.error() = %v, want %q", got, wantMessage)
	}
}

// TestCore028_ActivationDependencyLostRollsBackToPending is XT-02: an async
// activation that loses its injected provider mid-apply must roll back to
// Pending instead of publishing Active against a dead service.
func TestCore028_ActivationDependencyLostRollsBackToPending(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	key := cordis.NewServiceKey[string]("core028.gate")

	provider := &pluginFunc{name: "xt02-provider", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := cordis.Provide(c, key, "on")
		return err
	}}
	providerFiber, err := root.Plugin(provider, nil)
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	assertState(t, providerFiber, cordis.Active)

	loading := make(chan *cordis.Fiber, 1)
	release := make(chan struct{})
	consumer := &pluginFunc{name: "xt02-consumer", deps: []string{key.Name()}, activate: func(c cordis.Context, _ cordis.Value) error {
		if _, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cleanupOf("consumer-rollback", tr)}
		}); err != nil {
			return err
		}
		loading <- c.Fiber()
		<-release
		return nil
	}}
	type pluginResult struct {
		fiber *cordis.Fiber
		err   error
	}
	consumerDone := make(chan pluginResult, 1)
	go func() {
		fiber, err := root.Plugin(consumer, nil)
		consumerDone <- pluginResult{fiber: fiber, err: err}
	}()
	consumerFiber := awaitChan(t, "consumer activation started", loading)
	assertState(t, consumerFiber, cordis.Loading)

	// Provider disappears while the consumer apply is still running.
	if err := providerFiber.Dispose(); err != nil {
		t.Fatalf("provider dispose: %v", err)
	}
	close(release)
	result := awaitChan(t, "consumer plugin call", consumerDone)
	if result.err != nil {
		t.Fatalf("consumer plugin call: %v", result.err)
	}
	// The rolled-back activation must not publish Active; it returns to Pending.
	assertState(t, result.fiber, cordis.Pending)
	assertOrder(t, tr.snapshot(), []string{"consumer-rollback"})
	if got := result.fiber.Effects(); len(got) != 0 {
		t.Fatalf("rollback left effects behind: %s", effectTree(got))
	}
	// Failed/Pending rollback does not block disposal.
	if err := result.fiber.Dispose(); err != nil {
		t.Fatalf("dispose rolled-back consumer: %v", err)
	}
}

// ---------------------------------------------------------------------------
// CORE-029: apply panic is isolated, the fiber fails and rolls back, the caller
// does not panic (hardening.rs::panicking_apply_is_isolated_and_rolled_back)
// ---------------------------------------------------------------------------

func TestCore029_PanickingApplyIsolatedAndRolledBack(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	key := cordis.NewServiceKey[string]("core029.text")
	plugin := &pluginFunc{name: "panicking-apply", activate: func(c cordis.Context, _ cordis.Value) error {
		if _, err := cordis.Provide(c, key, "stale"); err != nil {
			return err
		}
		if _, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cleanupOf("rolled-back", tr)}
		}); err != nil {
			return err
		}
		panic("apply boom")
	}}

	var fiber *cordis.Fiber
	var err error
	if panicValue := catchPanic(func() { fiber, err = root.Plugin(plugin, nil) }); panicValue != nil {
		t.Fatalf("apply panic escaped ctx.plugin: %v", panicValue)
	}
	if err != nil {
		t.Fatalf("plugin handle: %v", err)
	}
	assertState(t, fiber, cordis.Failed)
	if got := fiber.Error(); got == nil || !containsAll(got.Error(), "panic", "apply boom") {
		t.Fatalf("fiber.error() = %v, want a message containing panic/apply boom", got)
	}
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("panicking apply leaked a service")
	}
	assertOrder(t, tr.snapshot(), []string{"rolled-back"})

	// The kernel is still usable.
	sibling, err := root.Plugin(noopPlugin("after-panic"), nil)
	if err != nil {
		t.Fatalf("plugin after panic: %v", err)
	}
	assertState(t, sibling, cordis.Active)
}

// ---------------------------------------------------------------------------
// CORE-035: AsyncPlugin activates/disposes through the *_async APIs
// (hardening.rs::async_plugin_activates_and_disposes,
//  api_surface.rs::api_structured_plugin_error_survives_async_activation)
// ---------------------------------------------------------------------------

func TestCore035_AsyncPluginActivatesAndDisposes(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	key := cordis.NewServiceKey[string]("hardening.text")
	plugin := &asyncPluginFunc{name: "async-init", activate: func(_ context.Context, c cordis.Context, _ cordis.Value) error {
		if _, err := cordis.Provide(c, key, "async"); err != nil {
			return err
		}
		_, err := c.EffectAsyncLabeled("async-effect", func() cordis.AsyncEffect {
			return cordis.AsyncEffect{asyncCleanupOf("async-cleanup", tr)}
		})
		return err
	}}
	fiber, err := root.PluginAsync(plugin, nil)
	if err != nil {
		t.Fatalf("PluginAsync: %v", err)
	}
	assertState(t, fiber, cordis.Active)
	if got, ok := cordis.Get(root, key); !ok || got != "async" {
		t.Fatalf("service = %v/%v, want async/true", got, ok)
	}

	if err := fiber.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("DisposeAsync: %v", err)
	}
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("service survived async disposal")
	}
	assertOrder(t, tr.snapshot(), []string{"async-cleanup"})
	assertState(t, fiber, cordis.Disposed)
}

func TestCore035_AsyncPluginStructuredError(t *testing.T) {
	root := newRoot(t)
	plugin := &asyncPluginFunc{name: "api-failing-async", activate: func(_ context.Context, _ cordis.Context, _ cordis.Value) error {
		return cordis.PluginError("api-failing-async", "io", "boom")
	}}
	fiber, err := root.PluginAsync(plugin, nil)
	if err != nil {
		t.Fatalf("PluginAsync handle: %v", err)
	}
	assertState(t, fiber, cordis.Failed)
	settled := fiber.AwaitSettled()
	assertCode(t, settled, cordis.CodePlugin)
	structured, ok := settled.(*cordis.CordisError)
	if !ok {
		t.Fatalf("await_settled error = %T, want *CordisError", settled)
	}
	if structured.PluginKind() != "io" || structured.Name() != "api-failing-async" {
		t.Fatalf("structured error = %+v, want plugin api-failing-async kind io", structured)
	}
}

// ---------------------------------------------------------------------------
// CORE-036: async plugin failure rolls back and awaits async cleanup
// (hardening.rs::async_plugin_failure_rolls_back)
// ---------------------------------------------------------------------------

func TestCore036_AsyncPluginFailureRollsBack(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	key := cordis.NewServiceKey[string]("hardening.text")
	plugin := &asyncPluginFunc{name: "async-fail", activate: func(_ context.Context, c cordis.Context, _ cordis.Value) error {
		if _, err := cordis.Provide(c, key, "stale"); err != nil {
			return err
		}
		if _, err := c.EffectAsyncLabeled("async-effect", func() cordis.AsyncEffect {
			return cordis.AsyncEffect{asyncCleanupOf("async-cleanup", tr)}
		}); err != nil {
			return err
		}
		cordis.YieldNow()
		return cordis.PluginFailedError("async failed")
	}}
	fiber, err := root.PluginAsync(plugin, nil)
	if err != nil {
		t.Fatalf("PluginAsync handle: %v", err)
	}
	assertState(t, fiber, cordis.Failed)
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("failed async plugin leaked a service")
	}
	// The rollback awaited the async cleanup (not fire-and-forget).
	assertOrder(t, tr.snapshot(), []string{"async-cleanup"})
	if got := fiber.Error(); got == nil || !containsAll(got.Error(), "async failed") {
		t.Fatalf("fiber.error() = %v, want a message containing \"async failed\"", got)
	}

	// Sync lifecycle APIs reject async plugins instead of blocking.
	assertCode(t, fiber.Restart(), cordis.CodeAsyncPluginNeedsAsync)
	assertCode(t, fiber.Update(nil), cordis.CodeAsyncPluginNeedsAsync)
	// A Failed fiber is still disposable.
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose failed fiber: %v", err)
	}
	assertState(t, fiber, cordis.Disposed)
	assertOrder(t, tr.snapshot(), []string{"async-cleanup"})
}

// ---------------------------------------------------------------------------
// CORE-038: a failed sync activation drains async cleanup inline (cross-lane
// reverse order), and a later dispose does not repeat it
// (hardening.rs::failed_sync_activation_drains_async_cleanup_inline)
// ---------------------------------------------------------------------------

func TestCore038_FailedSyncActivationDrainsAsyncCleanupInline(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	plugin := &pluginFunc{name: "sync-fails-with-async-cleanup", activate: func(c cordis.Context, _ cordis.Value) error {
		if _, err := c.EffectAsyncLabeled("deferred", func() cordis.AsyncEffect {
			return cordis.AsyncEffect{asyncCleanupOf("deferred", tr)}
		}); err != nil {
			return err
		}
		return cordis.PluginFailedError("sync failure")
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin handle: %v", err)
	}
	assertState(t, fiber, cordis.Failed)
	// The function returned only after the async cleanup ran.
	assertOrder(t, tr.snapshot(), []string{"deferred"})

	if err := fiber.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("DisposeAsync: %v", err)
	}
	assertOrder(t, tr.snapshot(), []string{"deferred"})
	assertState(t, fiber, cordis.Disposed)
}

// ---------------------------------------------------------------------------
// CORE-039: restart rechecks the inject gate instead of force-activating
// (hardening.rs::restart_rechecks_inject_gate)
// ---------------------------------------------------------------------------

func TestCore039_RestartRechecksInjectGate(t *testing.T) {
	root := newRoot(t)
	gateKey := cordis.NewServiceKey[string]("core039.gate2")
	textKey := cordis.NewServiceKey[string]("core039.text")
	provider, err := cordis.Provide(root, gateKey, "on")
	if err != nil {
		t.Fatalf("provide gate: %v", err)
	}
	plugin := &pluginFunc{name: "gate-user", deps: []string{gateKey.Name()}, activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := cordis.Provide(c, textKey, "gated")
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	assertState(t, fiber, cordis.Active)
	if _, ok := cordis.Get(root, textKey); !ok {
		t.Fatal("gated service missing while provider active")
	}

	// Removing the injected service unloads the dependent back to Pending.
	provider.Dispose()
	assertState(t, fiber, cordis.Pending)

	// Restart must re-check inject: Ok but still Pending, and no service.
	if err := fiber.Restart(); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	assertState(t, fiber, cordis.Pending)
	if _, ok := cordis.Get(root, textKey); ok {
		t.Fatal("restart force-activated a fiber whose inject gate is closed")
	}
}

// ---------------------------------------------------------------------------
// CORE-044: a disposed fiber is terminal; lifecycle broadcasts report every
// transition, contain listener panics and support unsubscribe
// (redteam.rs::rt12_restart_after_dispose_fails_safely,
//  redteam.rs::rt01_sync_dispose_during_apply_must_not_end_active,
//  api_surface.rs::api_on_lifecycle_*)
// ---------------------------------------------------------------------------

func TestCore044_DisposedFiberIsTerminal(t *testing.T) {
	root := newRoot(t)
	fiber, err := root.Plugin(noopPlugin("terminal"), nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	assertState(t, fiber, cordis.Disposed)

	// rt12: restart/update after dispose fail safely (no panic, no revival).
	if panicValue := catchPanic(func() { _ = fiber.Restart() }); panicValue != nil {
		t.Fatalf("Restart after dispose panicked: %v", panicValue)
	}
	assertCode(t, fiber.Restart(), cordis.CodeDisposed)
	assertCode(t, fiber.RestartAsync(context.Background()), cordis.CodeDisposed)
	assertCode(t, fiber.Update(nil), cordis.CodeDisposed)
	assertCode(t, fiber.UpdateAsync(nil), cordis.CodeDisposed)
	assertState(t, fiber, cordis.Disposed)
}

// TestCore044_DisposeDuringApplyCannotEndActive is rt01: a concurrent dispose
// while apply is running must let ctx.plugin return without panicking, leave
// the fiber in a terminal state (never Active), reject late effect
// registrations with InactiveEffect, and leave the registry empty.
func TestCore044_DisposeDuringApplyCannotEndActive(t *testing.T) {
	root := newRoot(t)
	started := make(chan *cordis.Fiber, 1)
	release := make(chan struct{})
	var lateErr error
	plugin := &pluginFunc{name: "slow-apply", activate: func(c cordis.Context, _ cordis.Value) error {
		started <- c.Fiber()
		<-release
		_, lateErr = c.Effect(func() cordis.Effect { return nil })
		return nil
	}}
	type pluginResult struct {
		fiber *cordis.Fiber
		err   error
	}
	done := make(chan pluginResult, 1)
	go func() {
		fiber, err := root.Plugin(plugin, nil)
		done <- pluginResult{fiber: fiber, err: err}
	}()
	fiber := awaitChan(t, "apply started", started)
	assertState(t, fiber, cordis.Loading)

	// Dispose from another goroutine while apply is in progress.
	if panicValue := catchPanic(func() { _ = fiber.Dispose() }); panicValue != nil {
		t.Fatalf("dispose during apply panicked: %v", panicValue)
	}
	close(release)
	result := awaitChan(t, "plugin call", done)
	if result.err != nil {
		t.Fatalf("plugin call: %v", result.err)
	}
	// rt01: final state in {Disposed, Failed}, never Active.
	switch got := result.fiber.State(); got {
	case cordis.Disposed, cordis.Failed:
	default:
		t.Fatalf("final state after dispose-during-apply = %v, want Disposed or Failed (never Active)", got)
	}
	assertState(t, result.fiber, cordis.Disposed)
	if got := result.fiber.Effects(); len(got) != 0 {
		t.Fatalf("effects after dispose-during-apply = %s, want empty", effectTree(got))
	}
	assertCode(t, lateErr, cordis.CodeInactiveEffect)
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("registry fiber_count = %d, want 0", got)
	}
	if got := root.Registry().Size(); got != 0 {
		t.Fatalf("registry size = %d, want 0", got)
	}
}

// eventLog collects lifecycle broadcasts (delivered on the transition
// goroutine, so it is lock-protected).
type eventLog struct {
	mu     sync.Mutex
	events []cordis.LifecycleEvent
}

func (l *eventLog) add(event *cordis.LifecycleEvent) {
	l.mu.Lock()
	l.events = append(l.events, *event)
	l.mu.Unlock()
}

func (l *eventLog) snapshot() []cordis.LifecycleEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]cordis.LifecycleEvent(nil), l.events...)
}

func (l *eventLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.events)
}

func (l *eventLog) forFiber(id uint64) []cordis.LifecycleEvent {
	var out []cordis.LifecycleEvent
	for _, event := range l.snapshot() {
		if event.FiberID == id {
			out = append(out, event)
		}
	}
	return out
}

func TestCore044_LifecycleBroadcast(t *testing.T) {
	root := newRoot(t)
	first := &eventLog{}
	second := &eventLog{}
	if _, err := root.OnLifecycle(first.add); err != nil {
		t.Fatalf("OnLifecycle(first): %v", err)
	}
	// A panicking subscriber must not break other subscribers or the kernel.
	if _, err := root.OnLifecycle(func(*cordis.LifecycleEvent) { panic("lifecycle listener boom") }); err != nil {
		t.Fatalf("OnLifecycle(panicker): %v", err)
	}
	if _, err := root.OnLifecycle(second.add); err != nil {
		t.Fatalf("OnLifecycle(second): %v", err)
	}

	fiber, err := root.Plugin(noopPlugin("lifecycle-plugin"), nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	active, ok := findEvent(first.forFiber(fiber.ID()), func(event cordis.LifecycleEvent) bool {
		return event.To == cordis.Active
	})
	if !ok {
		t.Fatalf("no transition to Active broadcast for fiber %d: %v", fiber.ID(), first.forFiber(fiber.ID()))
	}
	if active.From != cordis.Loading || active.Name != "lifecycle-plugin" {
		t.Fatalf("Active event = %+v, want from Loading name lifecycle-plugin", active)
	}

	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	transitions := first.forFiber(fiber.ID())
	// Exact chain: Pending --Loading--> Active --Unloading--> Disposed, with a
	// per-fiber monotonic sequence.
	wantFromTo := [][2]cordis.FiberState{
		{cordis.Pending, cordis.Loading},
		{cordis.Loading, cordis.Active},
		{cordis.Active, cordis.Unloading},
		{cordis.Unloading, cordis.Disposed},
	}
	if len(transitions) != len(wantFromTo) {
		t.Fatalf("lifecycle transitions = %v, want %d transitions", transitions, len(wantFromTo))
	}
	for i, want := range wantFromTo {
		if transitions[i].From != want[0] || transitions[i].To != want[1] {
			t.Fatalf("transition[%d] = %v -> %v, want %v -> %v", i, transitions[i].From, transitions[i].To, want[0], want[1])
		}
		if transitions[i].Seq != uint64(i) {
			t.Fatalf("transition[%d].Seq = %d, want %d (per-fiber monotonic sequence)", i, transitions[i].Seq, i)
		}
	}
	// The panicking subscriber did not stop the others: both logs saw the same
	// transitions.
	if got, want := second.count(), first.count(); got != want {
		t.Fatalf("second subscriber saw %d events, first saw %d; a panicking subscriber must not break the rest", got, want)
	}

	// Unsubscribing via the disposer stops delivery (Go's explicit Drop).
	unsubscribed := &eventLog{}
	unsub, err := root.OnLifecycle(unsubscribed.add)
	if err != nil {
		t.Fatalf("OnLifecycle(unsub): %v", err)
	}
	unsub.Dispose()
	if _, err := root.Plugin(noopPlugin("after-unsubscribe"), nil); err != nil {
		t.Fatalf("plugin after unsubscribe: %v", err)
	}
	if got := unsubscribed.count(); got != 0 {
		t.Fatalf("unsubscribed listener received %d events, want 0", got)
	}

	// Root fiber transitions: id 0, name "root".
	if _, err := disposeAllCatching(root); err != nil {
		t.Fatalf("DisposeAll: %v", err)
	}
	rootEvent, ok := findEvent(first.snapshot(), func(event cordis.LifecycleEvent) bool {
		return event.FiberID == 0 && event.To == cordis.Disposed
	})
	if !ok {
		t.Fatal("no Disposed transition broadcast for the root fiber")
	}
	if rootEvent.Name != "root" {
		t.Fatalf("root fiber event name = %q, want \"root\"", rootEvent.Name)
	}

	// LifecycleEvent is a value snapshot; sorting by (fiber_id, seq) reproduces
	// the migration chain.
	events := []cordis.LifecycleEvent{
		{FiberID: 2, Name: "b", From: cordis.Pending, To: cordis.Loading, Seq: 1},
		{FiberID: 1, Name: "a", From: cordis.Pending, To: cordis.Loading, Seq: 0},
		{FiberID: 1, Name: "a", From: cordis.Loading, To: cordis.Active, Seq: 1},
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].FiberID != events[j].FiberID {
			return events[i].FiberID < events[j].FiberID
		}
		return events[i].Seq < events[j].Seq
	})
	if events[0].FiberID != 1 || events[0].Seq != 0 || events[1].Seq != 1 || events[2].FiberID != 2 {
		t.Fatalf("sorted lifecycle events = %+v, want [1/0 1/1 2/1]", events)
	}
}

// ---------------------------------------------------------------------------
// CORE-045: concurrent lifecycle calls are exclusive (LifecycleBusy), while a
// reentrant call from the running operation is allowed
// (concurrency.rs::concurrent_dispose_returns_lifecycle_busy,
//  ::reentrant_lifecycle_calls_are_allowed,
//  redteam4_attacks.rs::r4_01/r4_03/r4_04,
//  regression_core.rs::p2a_inflight_async_unload_reports_lifecycle_busy)
// ---------------------------------------------------------------------------

func TestCore045_ConcurrentLifecycleCallsAreBusy(t *testing.T) {
	root := newRoot(t)
	cleanupStarted := make(chan struct{})
	gate := make(chan struct{})
	plugin := &pluginFunc{name: "busy", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {
				close(cleanupStarted)
				<-gate
			})}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}

	ownerDone := make(chan error, 1)
	go func() { ownerDone <- fiber.Dispose() }()
	awaitSignal(t, "blocking cleanup started", cleanupStarted)

	// Every non-reentrant lifecycle call must be rejected instead of racing.
	assertCode(t, fiber.Dispose(), cordis.CodeLifecycleBusy)
	assertCode(t, fiber.Restart(), cordis.CodeLifecycleBusy)
	assertCode(t, fiber.Update(nil), cordis.CodeLifecycleBusy)
	assertCode(t, fiber.DisposeAsync(context.Background()), cordis.CodeLifecycleBusy)

	close(gate)
	if err := awaitChan(t, "owner dispose", ownerDone); err != nil {
		t.Fatalf("owner dispose: %v", err)
	}
	assertState(t, fiber, cordis.Disposed)
	// A rejected call did not leak the lifecycle slot: the next operation is not
	// busy (it is just a no-op/terminal error).
	assertCode(t, fiber.Restart(), cordis.CodeDisposed)
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("idempotent dispose after owner: %v", err)
	}
}

func TestCore045_ReentrantDisposeFromCleanupIsAllowed(t *testing.T) {
	root := newRoot(t)
	var fiber *cordis.Fiber
	innerErr := make(chan error, 1)
	plugin := &pluginFunc{name: "reentrant", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {
				innerErr <- fiber.Dispose()
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
	if got := awaitChan(t, "reentrant dispose from cleanup", innerErr); got != nil {
		t.Fatalf("reentrant dispose = %v, want Ok (same operation frame)", got)
	}
	if err := awaitChan(t, "outer dispose", outerDone); err != nil {
		t.Fatalf("outer dispose: %v", err)
	}
	assertState(t, fiber, cordis.Disposed)
}

func TestCore045_AsyncUnloadInFlightIsBusy(t *testing.T) {
	root := newRoot(t)
	cleanupStarted := make(chan struct{})
	gate := make(chan struct{})
	var cleanups atomic.Int64
	plugin := &pluginFunc{name: "p2a", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.EffectAsync(func() cordis.AsyncEffect {
			return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(context.Context) error {
				cleanups.Add(1)
				close(cleanupStarted)
				<-gate
				return nil
			})}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	disposeDone := make(chan error, 1)
	go func() { disposeDone <- fiber.DisposeAsync(context.Background()) }()
	awaitSignal(t, "async cleanup started", cleanupStarted)

	// While an async unload owns the lifecycle slot, sync calls from another
	// goroutine report LifecycleBusy (p2a semantics).
	assertCode(t, fiber.Dispose(), cordis.CodeLifecycleBusy)
	assertCode(t, fiber.Restart(), cordis.CodeLifecycleBusy)
	assertCode(t, fiber.DisposeAsync(context.Background()), cordis.CodeLifecycleBusy)

	close(gate)
	if err := awaitChan(t, "async dispose", disposeDone); err != nil {
		t.Fatalf("DisposeAsync: %v", err)
	}
	assertState(t, fiber, cordis.Disposed)
	if got := cleanups.Load(); got != 1 {
		t.Fatalf("cleanup runs = %d, want 1", got)
	}
}

// TestCore045_CleanupPanicReleasesLifecycleSlot is r4_04/rt05: a cleanup panic
// escaping Dispose still finalizes the fiber and releases the slot.
func TestCore045_CleanupPanicReleasesLifecycleSlot(t *testing.T) {
	root := newRoot(t)
	boost := &panicPayload{message: "cleanup boom"}
	plugin := &pluginFunc{name: "panic-slot", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() { panic(boost) })}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := catchPanic(func() { _ = fiber.Dispose() }); got != boost {
		t.Fatalf("dispose panic = %v, want original payload", got)
	}
	assertState(t, fiber, cordis.Disposed)
	// The slot is released: another operation is not LifecycleBusy.
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose after panicking cleanup: %v", err)
	}
}

// ---------------------------------------------------------------------------
// CORE-047: child fibers are owned by their parent (cascade + rollback); deep
// chains unload dependents-first; churn keeps bookkeeping bounded
// (child_ownership.rs::*; regression_core.rs::c1_*/r3a4_*)
// ---------------------------------------------------------------------------

func newLeaf(name string, tr *trace, cleanups *atomic.Int64) *pluginFunc {
	return &pluginFunc{name: name, activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {
				cleanups.Add(1)
				tr.add(name)
			})}
		})
		return err
	}}
}

func TestCore047_ParentDisposeDisposesChildren(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	var leafCleanups atomic.Int64
	var children []*cordis.Fiber
	var mu sync.Mutex
	parent := &pluginFunc{name: "parent", activate: func(c cordis.Context, _ cordis.Value) error {
		for i := 0; i < 3; i++ {
			leaf, err := c.Plugin(newLeaf(fmt.Sprintf("leaf-%d", i), tr, &leafCleanups), nil)
			if err != nil {
				return err
			}
			mu.Lock()
			children = append(children, leaf)
			mu.Unlock()
		}
		return nil
	}}
	parentFiber, err := root.Plugin(parent, nil)
	if err != nil {
		t.Fatalf("parent: %v", err)
	}
	assertState(t, parentFiber, cordis.Active)
	if got := root.Registry().FiberCount(); got != 4 {
		t.Fatalf("registry fiber_count = %d, want 4 (parent + 3 leaves)", got)
	}

	if err := parentFiber.Dispose(); err != nil {
		t.Fatalf("dispose parent: %v", err)
	}
	if got := root.Registry().Size(); got != 0 {
		t.Fatalf("registry size after parent dispose = %d, want 0", got)
	}
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("registry fiber_count after parent dispose = %d, want 0", got)
	}
	if got := leafCleanups.Load(); got != 3 {
		t.Fatalf("leaf cleanups = %d, want exactly 3 (one per leaf)", got)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, child := range children {
		if got := child.State(); got != cordis.Disposed {
			t.Fatalf("leaf %d state = %v, want Disposed", i, got)
		}
	}
}

func TestCore047_FailedParentActivationRollsBackChildren(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	var leafCleanups atomic.Int64
	var children []*cordis.Fiber
	parent := &pluginFunc{name: "failing-parent", activate: func(c cordis.Context, _ cordis.Value) error {
		for i := 0; i < 2; i++ {
			leaf, err := c.Plugin(newLeaf(fmt.Sprintf("rollback-leaf-%d", i), tr, &leafCleanups), nil)
			if err != nil {
				return err
			}
			children = append(children, leaf)
		}
		return cordis.PluginFailedError("parent failed")
	}}
	parentFiber, err := root.Plugin(parent, nil)
	if err != nil {
		t.Fatalf("parent handle: %v", err)
	}
	assertState(t, parentFiber, cordis.Failed)
	if got := leafCleanups.Load(); got != 2 {
		t.Fatalf("leaf cleanups = %d, want 2 (children rolled back)", got)
	}
	if len(children) != 2 {
		t.Fatalf("children = %d, want 2", len(children))
	}
	for i, child := range children {
		if got := child.State(); got != cordis.Disposed {
			t.Fatalf("rollback leaf %d state = %v, want Disposed", i, got)
		}
	}
	// The failed parent itself remains registered (CORE-028).
	if got := root.Registry().FiberCount(); got != 1 {
		t.Fatalf("registry fiber_count = %d, want 1 (the Failed parent)", got)
	}
	if got := root.Registry().Size(); got != 1 {
		t.Fatalf("registry size = %d, want 1 (the Failed parent runtime)", got)
	}
}

func TestCore047_NestedCascadeAndRootFiberDispose(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	var leafCleanups atomic.Int64
	parent := &pluginFunc{name: "cascade-parent", activate: func(c cordis.Context, _ cordis.Value) error {
		for i := 0; i < 2; i++ {
			if _, err := c.Plugin(newLeaf(fmt.Sprintf("cascade-leaf-%d", i), tr, &leafCleanups), nil); err != nil {
				return err
			}
		}
		return nil
	}}
	grandparent := &pluginFunc{name: "cascade-grandparent", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.Plugin(parent, nil)
		return err
	}}
	grandFiber, err := root.Plugin(grandparent, nil)
	if err != nil {
		t.Fatalf("grandparent: %v", err)
	}
	if got := root.Registry().FiberCount(); got != 4 {
		t.Fatalf("registry fiber_count = %d, want 4 (grandparent + parent + 2 leaves)", got)
	}

	// Root fiber disposal cascades the whole tree the same way.
	if err := root.Fiber().Dispose(); err != nil {
		t.Fatalf("root fiber dispose: %v", err)
	}
	assertState(t, grandFiber, cordis.Disposed)
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("registry fiber_count after root fiber dispose = %d, want 0", got)
	}
	if got := leafCleanups.Load(); got != 2 {
		t.Fatalf("leaf cleanups = %d, want 2", got)
	}
	assertState(t, root.Fiber(), cordis.Disposed)
	// The root context is terminal: no new plugins.
	if _, err := root.Plugin(noopPlugin("after-root-dispose"), nil); err == nil {
		t.Fatal("plugin creation succeeded on a disposed root")
	} else {
		assertCode(t, err, cordis.CodeDisposed)
	}
}

func TestCore047_DeepDependencyChainDisposeOrder(t *testing.T) {
	const depth = 1500
	root := newRoot(t)
	tr := &trace{}
	fibers := make([]*cordis.Fiber, depth)
	for i := 0; i < depth; i++ {
		i := i
		var deps []string
		if i > 0 {
			deps = []string{fmt.Sprintf("core047.chain.%d", i-1)}
		}
		plugin := &pluginFunc{
			name: fmt.Sprintf("chain-%04d", i),
			deps: deps,
			activate: func(c cordis.Context, _ cordis.Value) error {
				key := cordis.NewServiceKey[int](fmt.Sprintf("core047.chain.%d", i))
				if _, err := cordis.Provide(c, key, i); err != nil {
					return err
				}
				_, err := c.Effect(func() cordis.Effect {
					return cordis.Effect{cleanupOf(fmt.Sprintf("L%d", i), tr)}
				})
				return err
			},
		}
		fiber, err := root.Plugin(plugin, nil)
		if err != nil {
			t.Fatalf("chain link %d: %v", i, err)
		}
		assertState(t, fiber, cordis.Active)
		fibers[i] = fiber
	}

	// Disposing the base withdraws its service; every transitive dependent is
	// unloaded dependents-first, deepest first.
	if err := fibers[0].Dispose(); err != nil {
		t.Fatalf("dispose base: %v", err)
	}
	snapshot := tr.snapshot()
	if len(snapshot) != depth {
		t.Fatalf("cleanup count = %d, want %d", len(snapshot), depth)
	}
	for i := 0; i < depth; i++ {
		if want := fmt.Sprintf("L%d", depth-1-i); snapshot[i] != want {
			t.Fatalf("cleanup[%d] = %q, want %q (trace should be L1499..L0)", i, snapshot[i], want)
		}
	}
	for i := 1; i < depth; i++ {
		if got := fibers[i].State(); got != cordis.Pending {
			t.Fatalf("dependent chain link %d state = %v, want Pending", i, got)
		}
	}
	if err := root.DisposeAll(); err != nil {
		t.Fatalf("DisposeAll: %v", err)
	}
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("registry fiber_count = %d, want 0", got)
	}
	if got := root.Registry().Size(); got != 0 {
		t.Fatalf("registry size = %d, want 0", got)
	}
	if !root.Registry().IsIdle() {
		t.Fatal("registry not idle after DisposeAll")
	}
}

func TestCore047_ChildChurnKeepsBookkeepingBounded(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	var cleanups atomic.Int64
	child := newLeaf("churn-child", tr, &cleanups)
	var parentCtx cordis.Context
	parent := &pluginFunc{name: "churn-parent", activate: func(c cordis.Context, _ cordis.Value) error {
		parentCtx = c
		_, err := c.Plugin(child, nil)
		return err
	}}
	parentFiber, err := root.Plugin(parent, nil)
	if err != nil {
		t.Fatalf("parent: %v", err)
	}
	baseline := len(parentFiber.Effects())
	if baseline != 1 {
		t.Fatalf("parent baseline effects = %d, want 1 (one ownership effect)", baseline)
	}

	for i := 0; i < 100; i++ {
		childFiber, err := parentCtx.Plugin(child, nil)
		if err != nil {
			t.Fatalf("churn iteration %d: %v", i, err)
		}
		if err := childFiber.Dispose(); err != nil {
			t.Fatalf("churn iteration %d dispose: %v", i, err)
		}
		if got := len(parentFiber.Effects()); got != baseline {
			t.Fatalf("iteration %d: parent effects = %d, want baseline %d (self-disposed child must detach its ownership effect)", i, got, baseline)
		}
	}
	// The parent's own activation loaded one long-lived child, still alive: the
	// registry holds the parent plus that child (every churned child disposed
	// itself). Detached ownership effects are what stays bounded.
	if got := root.Registry().FiberCount(); got != 2 {
		t.Fatalf("registry fiber_count = %d, want 2 (parent + its live initial child)", got)
	}

	// Parent disposal is idempotent even after the child already disposed itself.
	if err := parentFiber.Dispose(); err != nil {
		t.Fatalf("parent dispose: %v", err)
	}
	if err := parentFiber.Dispose(); err != nil {
		t.Fatalf("second parent dispose: %v", err)
	}
	if got := root.Registry().Size(); got != 0 {
		t.Fatalf("registry size = %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// CORE-050: cooperative cancellation through Context.Checkpoint
// (quiescence.rs::checkpoint_observes_cooperative_cancellation)
// ---------------------------------------------------------------------------

func TestCore050_CheckpointObservesCooperativeCancellation(t *testing.T) {
	root := newRoot(t)
	var captured cordis.Context
	plugin := &pluginFunc{name: "cancellable", activate: func(c cordis.Context, _ cordis.Value) error {
		captured = c
		return nil
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if err := captured.Checkpoint(); err != nil {
		t.Fatalf("checkpoint before cancel: %v, want nil", err)
	}
	if fiber.IsCancelled() {
		t.Fatal("fiber reports cancelled before Cancel")
	}

	fiber.Cancel()
	if !fiber.IsCancelled() {
		t.Fatal("fiber does not report cancelled after Cancel")
	}
	assertCode(t, captured.Checkpoint(), cordis.CodeCancelled)
	// Cancellation is cooperative: the kernel does not force the fiber down.
	assertState(t, fiber, cordis.Active)
}

// TestCore050_CheckpointHotLoopObservesCancellation: a plugin that observes
// Checkpoint unwinds through the normal failure path; the kernel never kills it.
func TestCore050_CheckpointHotLoopObservesCancellation(t *testing.T) {
	root := newRoot(t)
	started := make(chan *cordis.Fiber, 1)
	release := make(chan struct{})
	plugin := &pluginFunc{name: "hot-loop", activate: func(c cordis.Context, _ cordis.Value) error {
		started <- c.Fiber()
		<-release
		return c.Checkpoint()
	}}
	type pluginResult struct {
		fiber *cordis.Fiber
		err   error
	}
	done := make(chan pluginResult, 1)
	go func() {
		fiber, err := root.Plugin(plugin, nil)
		done <- pluginResult{fiber: fiber, err: err}
	}()
	fiber := awaitChan(t, "hot loop plugin started", started)
	fiber.Cancel()
	close(release)
	result := awaitChan(t, "hot loop plugin call", done)
	if result.err != nil {
		t.Fatalf("plugin call: %v", result.err)
	}
	// The plugin returned the cancellation error, which fails the activation.
	assertState(t, result.fiber, cordis.Failed)
	assertCode(t, result.fiber.Error(), cordis.CodeCancelled)
}

// ---------------------------------------------------------------------------
// CORE-051: dispose_all shutdown path
// (child_ownership.rs::dispose_all_reclaims_the_whole_tree,
//  regression_core.rs::fix09_dispose_all_reports_the_panic_when_a_fiber_errors,
//  redteam9_final.rs::t01/t02/t03/t06/t07/t08)
// ---------------------------------------------------------------------------

func TestCore051_DisposeAllReclaimsTree(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	var leafCleanups atomic.Int64
	parent := &pluginFunc{name: "shutdown-parent", activate: func(c cordis.Context, _ cordis.Value) error {
		for i := 0; i < 3; i++ {
			if _, err := c.Plugin(newLeaf(fmt.Sprintf("shutdown-leaf-%d", i), tr, &leafCleanups), nil); err != nil {
				return err
			}
		}
		return nil
	}}
	if _, err := root.Plugin(parent, nil); err != nil {
		t.Fatalf("parent: %v", err)
	}
	if got := root.Registry().FiberCount(); got != 4 {
		t.Fatalf("registry fiber_count = %d, want 4", got)
	}

	if err := root.DisposeAll(); err != nil {
		t.Fatalf("DisposeAll: %v", err)
	}
	if got := root.Registry().Size(); got != 0 {
		t.Fatalf("registry size = %d, want 0", got)
	}
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("registry fiber_count = %d, want 0", got)
	}
	if !root.Registry().IsIdle() {
		t.Fatal("registry not idle after DisposeAll")
	}
	if got := leafCleanups.Load(); got != 3 {
		t.Fatalf("leaf cleanups = %d, want exactly 3", got)
	}
	assertState(t, root.Fiber(), cordis.Disposed)
	// Second call is a no-op.
	if err := root.DisposeAll(); err != nil {
		t.Fatalf("second DisposeAll: %v", err)
	}
	// The root is terminal.
	if _, err := root.Plugin(noopPlugin("after-dispose-all"), nil); err == nil {
		t.Fatal("plugin creation succeeded after DisposeAll, want \"fiber already disposed\"")
	} else {
		assertCode(t, err, cordis.CodeDisposed)
	}
	// A fresh root is unaffected.
	fresh := newRoot(t)
	freshFiber, err := fresh.Plugin(noopPlugin("fresh"), nil)
	if err != nil {
		t.Fatalf("fresh root plugin: %v", err)
	}
	assertState(t, freshFiber, cordis.Active)
}

// TestCore051_DisposeAllDrainsNeverDisposedAsyncCleanup is t07: shutdown must
// run async cleanups that no explicit dispose ever touched.
func TestCore051_DisposeAllDrainsNeverDisposedAsyncCleanup(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	plugin := &pluginFunc{name: "never-disposed-async", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.EffectAsyncLabeled("async-cleanup", func() cordis.AsyncEffect {
			return cordis.AsyncEffect{asyncCleanupOf("async-cleanup", tr)}
		})
		return err
	}}
	if _, err := root.Plugin(plugin, nil); err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if err := root.DisposeAll(); err != nil {
		t.Fatalf("DisposeAll: %v", err)
	}
	assertOrder(t, tr.snapshot(), []string{"async-cleanup"})
	if !root.Registry().IsIdle() {
		t.Fatal("registry not idle after DisposeAll")
	}
}

// TestCore051_DisposeAllPanicPriority is fix09/t08: a cleanup panic is re-raised
// after every fiber was attempted, and it wins over collected errors.
func TestCore051_DisposeAllPanicPriority(t *testing.T) {
	root := newRoot(t)
	boom := &panicPayload{message: "shutdown boom"}
	var erroredCleanups, panickingCleanups atomic.Int64
	errored := &pluginFunc{name: "erroring", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.EffectAsync(func() cordis.AsyncEffect {
			return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(context.Context) error {
				erroredCleanups.Add(1)
				return errors.New("cleanup error")
			})}
		})
		return err
	}}
	panicking := &pluginFunc{name: "panicking", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {
				panickingCleanups.Add(1)
				panic(boom)
			})}
		})
		return err
	}}
	if _, err := root.Plugin(errored, nil); err != nil {
		t.Fatalf("errored plugin: %v", err)
	}
	if _, err := root.Plugin(panicking, nil); err != nil {
		t.Fatalf("panicking plugin: %v", err)
	}

	recovered, err := disposeAllCatching(root)
	if recovered != boom {
		t.Fatalf("DisposeAll panic = %v, want the original cleanup panic payload %p", recovered, boom)
	}
	if err != nil {
		t.Fatalf("DisposeAll returned error %v together with a panic, want panic priority", err)
	}
	if got := erroredCleanups.Load(); got != 1 {
		t.Fatalf("errored cleanup runs = %d, want 1 (panic must not abort the sweep)", got)
	}
	if got := panickingCleanups.Load(); got != 1 {
		t.Fatalf("panicking cleanup runs = %d, want 1", got)
	}
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("registry fiber_count = %d, want 0", got)
	}
}

// TestCore051_ReentrantDisposeAllFromCleanup is t02: dispose_all called from a
// cleanup during another dispose_all must not deadlock; the registry still ends
// up empty.
func TestCore051_ReentrantDisposeAllFromCleanup(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	var nested atomic.Int64
	plugin := &pluginFunc{name: "reentrant-dispose-all", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {
				if nested.Add(1) == 1 {
					// Reenter the shutdown path from a cleanup.
					_, _ = disposeAllCatching(root)
				}
				tr.add("cleanup")
			})}
		})
		return err
	}}
	if _, err := root.Plugin(plugin, nil); err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if _, err := root.Plugin(noopPlugin("sibling"), nil); err != nil {
		t.Fatalf("sibling: %v", err)
	}

	type shutdownResult struct {
		panicValue any
		err        error
	}
	done := make(chan shutdownResult, 1)
	go func() {
		panicValue, err := disposeAllCatching(root)
		done <- shutdownResult{panicValue: panicValue, err: err}
	}()
	result := awaitChan(t, "reentrant DisposeAll (deadlock watchdog)", done)
	if result.panicValue != nil {
		t.Fatalf("reentrant DisposeAll panicked: %v", result.panicValue)
	}
	if result.err != nil {
		t.Fatalf("reentrant DisposeAll error: %v", result.err)
	}
	if got := nested.Load(); got != 1 {
		t.Fatalf("cleanup runs = %d, want 1", got)
	}
	assertOrder(t, tr.snapshot(), []string{"cleanup"})
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("registry fiber_count = %d, want 0", got)
	}
	if !root.Registry().IsIdle() {
		t.Fatal("registry not idle after reentrant DisposeAll")
	}
}

// TestCore051_ConcurrentDisposeAll is t03: concurrent shutdown calls must not
// panic, every cleanup runs exactly once, and the tree ends empty and idle.
func TestCore051_ConcurrentDisposeAll(t *testing.T) {
	root := newRoot(t)
	var cleanups [4]atomic.Int64
	for i := 0; i < len(cleanups); i++ {
		i := i
		plugin := &pluginFunc{name: fmt.Sprintf("concurrent-shutdown-%d", i), activate: func(c cordis.Context, _ cordis.Value) error {
			_, err := c.Effect(func() cordis.Effect {
				return cordis.Effect{cordis.CleanupOf(func() { cleanups[i].Add(1) })}
			})
			return err
		}}
		if _, err := root.Plugin(plugin, nil); err != nil {
			t.Fatalf("plugin %d: %v", i, err)
		}
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var errsMu sync.Mutex
	var errs []error
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 50; i++ {
				err := root.DisposeAll()
				errsMu.Lock()
				errs = append(errs, err)
				errsMu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		if err == nil {
			continue
		}
		var cordisErr *cordis.CordisError
		if errors.As(err, &cordisErr) && cordisErr.Code() == cordis.CodeLifecycleBusy {
			continue // expected under concurrency; callers retry
		}
		t.Fatalf("concurrent DisposeAll error = %v, want nil or LifecycleBusy", err)
	}

	// Retry until quiescent (the contract allows a busy call to leave fibers).
	waitFor(t, "registry quiescent after concurrent DisposeAll", func() bool {
		_ = root.DisposeAll()
		return root.Registry().FiberCount() == 0 && root.Registry().Size() == 0 && root.Registry().IsIdle()
	})
	for i := range cleanups {
		if got := cleanups[i].Load(); got != 1 {
			t.Fatalf("plugin %d cleanup runs = %d, want exactly 1 (dispose is single-shot)", i, got)
		}
	}
}

// containsAll reports whether s contains every substring.
func containsAll(s string, substrings ...string) bool {
	for _, sub := range substrings {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

// findEvent returns the first event matching pred.
func findEvent(events []cordis.LifecycleEvent, pred func(cordis.LifecycleEvent) bool) (cordis.LifecycleEvent, bool) {
	for _, event := range events {
		if pred(event) {
			return event, true
		}
	}
	return cordis.LifecycleEvent{}, false
}
