package lifecycle_test

import (
	"context"
	"testing"

	cordis "github.com/zeokeel/cordis-go"
)

// ---------------------------------------------------------------------------
// CORE-007: effect registration is reverse-ordered and single-shot across the
// sync and async lanes; attached/future effects are owned by the fiber.
// (Rust: async_conformance.rs::sync_and_async_effects_share_reverse_registration_order,
// ::async_cleanup_runs_in_reverse_order_once,
// api_surface.rs::api_attach_effect_owns_an_external_disposer,
// ::api_effect_future_async_registers_async_cleanups)
// ---------------------------------------------------------------------------

func TestCore007_SyncAndAsyncEffectsShareReverseRegistrationOrder(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	plugin := &pluginFunc{name: "mixed-effects", activate: func(c cordis.Context, _ cordis.Value) error {
		if _, err := c.EffectLabeled("sync-a", func() cordis.Effect {
			return cordis.Effect{cleanupOf("sync-a", tr)}
		}); err != nil {
			return err
		}
		if _, err := c.EffectAsyncLabeled("async-b", func() cordis.AsyncEffect {
			return cordis.AsyncEffect{asyncCleanupOf("async-b", tr)}
		}); err != nil {
			return err
		}
		_, err := c.EffectLabeled("sync-c", func() cordis.Effect {
			return cordis.Effect{cleanupOf("sync-c", tr)}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	assertState(t, fiber, cordis.Active)

	if err := fiber.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("DisposeAsync: %v", err)
	}
	assertOrder(t, tr.snapshot(), []string{"sync-c", "async-b", "sync-a"})
	assertState(t, fiber, cordis.Disposed)
	if err := fiber.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("second DisposeAsync: %v", err)
	}
	assertOrder(t, tr.snapshot(), []string{"sync-c", "async-b", "sync-a"})
}

func TestCore007_AsyncCleanupRunsReverseOrderOnce(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	plugin := &pluginFunc{name: "async-effects", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.EffectAsyncLabeled("two", func() cordis.AsyncEffect {
			return cordis.AsyncEffect{asyncCleanupOf("async-first", tr), asyncCleanupOf("async-second", tr)}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}

	if err := fiber.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("DisposeAsync: %v", err)
	}
	assertOrder(t, tr.snapshot(), []string{"async-second", "async-first"})
	if err := fiber.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("second DisposeAsync: %v", err)
	}
	assertOrder(t, tr.snapshot(), []string{"async-second", "async-first"})
}

// TestCore007_AttachEffectOwnsExternalDisposer: ctx.AttachEffect makes an
// externally created disposer a labeled, fiber-owned effect; unload disposes it
// exactly once, and late registration on the disposed context is rejected with
// InactiveEffect.
func TestCore007_AttachEffectOwnsExternalDisposer(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	plugin := &pluginFunc{name: "attach", activate: func(cordis.Context, cordis.Value) error { return nil }}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	ctx, ok := fiber.Context()
	if !ok {
		t.Fatal("fiber context missing while active")
	}

	attached := cordis.NewDisposer([]cordis.Cleanup{cleanupOf("attached", tr)})
	if err := ctx.AttachEffect(attached, "api.attached"); err != nil {
		t.Fatalf("AttachEffect: %v", err)
	}
	if got := effectLabels(fiber.Effects()); len(got) != 1 || got[0] != "api.attached" {
		t.Fatalf("effects = %v, want [api.attached]", got)
	}

	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	assertOrder(t, tr.snapshot(), []string{"attached"})
	if !attached.IsDisposed() {
		t.Fatal("attached disposer was not drained by the fiber unload")
	}
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("second dispose: %v", err)
	}
	assertOrder(t, tr.snapshot(), []string{"attached"})

	// A context that belongs to a disposed fiber must reject new effects.
	late := cordis.NewDisposer([]cordis.Cleanup{cleanupOf("late", tr)})
	err = ctx.AttachEffect(late, "late.attached")
	assertCode(t, err, cordis.CodeInactiveEffect)
	if _, effectErr := ctx.Effect(func() cordis.Effect { return nil }); effectErr == nil {
		t.Fatal("Effect on a disposed fiber succeeded, want InactiveEffect")
	} else {
		assertCode(t, effectErr, cordis.CodeInactiveEffect)
	}
}

// TestCore007_EffectFutureRegistersAsyncCleanups: EffectFuture is the Go
// counterpart of effect_future (the body has already been awaited): the
// disposer is visible in the diagnostics tree before it runs and single-shot.
func TestCore007_EffectFutureRegistersAsyncCleanups(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	plugin := &pluginFunc{name: "future", activate: func(cordis.Context, cordis.Value) error { return nil }}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	ctx, _ := fiber.Context()

	var runs int
	disposer, err := ctx.EffectFuture(cordis.AsyncEffect{
		cordis.AsyncCleanupOf(func(context.Context) error {
			runs++
			tr.add("future")
			return nil
		}),
	}, "future-body")
	if err != nil {
		t.Fatalf("EffectFuture: %v", err)
	}
	if !hasLabel(fiber.Effects(), "future-body") {
		t.Fatalf("effects = %v, want a future-body node", effectLabels(fiber.Effects()))
	}
	if runs != 0 {
		t.Fatalf("cleanup ran before disposal (runs = %d)", runs)
	}
	if err := disposer.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("dispose future effect: %v", err)
	}
	if runs != 1 {
		t.Fatalf("cleanup runs = %d, want 1", runs)
	}
	if got := fiber.Effects(); len(got) != 0 {
		t.Fatalf("effects after effect disposal = %v, want empty", effectLabels(got))
	}
	// Disposing the fiber must not run it again.
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("fiber dispose: %v", err)
	}
	if runs != 1 {
		t.Fatalf("cleanup runs after fiber dispose = %d, want 1", runs)
	}
	assertOrder(t, tr.snapshot(), []string{"future"})
}

// TestCore007_InactiveFiberRejectsNewEffects covers the boundary assertion:
// registering an effect while Unloading/Disposed must return InactiveEffect.
func TestCore007_InactiveFiberRejectsNewEffects(t *testing.T) {
	root := newRoot(t)
	cleanupErr := make(chan error, 1)
	attachErr := make(chan error, 1)
	plugin := &pluginFunc{name: "inactive-effects", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {
				// The cleanup runs while the fiber is Unloading.
				_, effectErr := c.Effect(func() cordis.Effect { return nil })
				cleanupErr <- effectErr
				attachErr <- c.AttachEffect(cordis.NewDisposer(nil), "late")
			})}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	assertCode(t, awaitChan(t, "effect registration during unload", cleanupErr), cordis.CodeInactiveEffect)
	assertCode(t, awaitChan(t, "attach during unload", attachErr), cordis.CodeInactiveEffect)
}

// TestCore007_AsyncDisposerSingleShot is the AsyncDisposer layer of CORE-007/008:
// reverse order and exactly-once across repeated calls.
func TestCore007_AsyncDisposerSingleShot(t *testing.T) {
	tr := &trace{}
	d := cordis.NewAsyncDisposer([]cordis.AsyncCleanup{
		asyncCleanupOf("first", tr),
		asyncCleanupOf("second", tr),
	})
	if err := d.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("DisposeAsync: %v", err)
	}
	if !d.IsDisposed() {
		t.Fatal("disposer not marked disposed after Drain")
	}
	assertOrder(t, tr.snapshot(), []string{"second", "first"})
	if err := d.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("second DisposeAsync: %v", err)
	}
	assertOrder(t, tr.snapshot(), []string{"second", "first"})
}

// ---------------------------------------------------------------------------
// CORE-008: disposers are single-shot
// (async_conformance.rs::async_disposer_is_single_shot)
// ---------------------------------------------------------------------------

func TestCore008_SyncDisposerSingleShot(t *testing.T) {
	tr := &trace{}
	d := cordis.NewDisposer([]cordis.Cleanup{
		cleanupOf("first", tr),
		cleanupOf("second", tr),
	})
	if d.IsDisposed() {
		t.Fatal("fresh disposer reports disposed")
	}
	d.Dispose()
	if !d.IsDisposed() {
		t.Fatal("disposer not marked disposed")
	}
	assertOrder(t, tr.snapshot(), []string{"second", "first"})
	d.Dispose()
	d.Dispose()
	assertOrder(t, tr.snapshot(), []string{"second", "first"})
}

// ---------------------------------------------------------------------------
// CORE-009: sync dispose awaits async cleanups before returning
// (async_conformance.rs::sync_dispose_awaits_async_cleanups_before_returning)
// ---------------------------------------------------------------------------

func TestCore009_SyncDisposeAwaitsAsyncCleanups(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	plugin := &pluginFunc{name: "sync-awaits-async", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.EffectAsyncLabeled("awaited", func() cordis.AsyncEffect {
			return cordis.AsyncEffect{
				asyncCleanupOf("async-first", tr),
				asyncCleanupOf("async-second", tr),
			}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}

	// The sync path must have completed both cleanups by the time it returns:
	// no fire-and-forget.
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("sync dispose: %v", err)
	}
	assertOrder(t, tr.snapshot(), []string{"async-second", "async-first"})
	assertState(t, fiber, cordis.Disposed)
}

// ---------------------------------------------------------------------------
// CORE-011: an async cleanup panic does not skip the remaining cleanups; the
// first panic is re-raised after the whole drain (payload preserved).
// (async_conformance.rs::async_cleanup_panic_does_not_skip_remaining_cleanups)
// ---------------------------------------------------------------------------

type panicPayload struct {
	message string
}

func TestCore011_AsyncCleanupPanicDoesNotSkipRemaining(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	boom := &panicPayload{message: "cleanup boom"}
	plugin := &pluginFunc{name: "async-cleanup-panic", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.EffectAsyncLabeled("panicking", func() cordis.AsyncEffect {
			return cordis.AsyncEffect{
				// Registered first => disposed last, i.e. after the surviving one.
				cordis.AsyncCleanupOf(func(context.Context) error {
					tr.add("panicking")
					panic(boom)
				}),
				asyncCleanupOf("surviving", tr),
			}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}

	recovered := catchPanic(func() { _ = fiber.Dispose() })
	if recovered != boom {
		t.Fatalf("recovered panic = %v, want the original payload %p", recovered, boom)
	}
	// Every cleanup ran, in reverse registration order, before the re-raise.
	assertOrder(t, tr.snapshot(), []string{"surviving", "panicking"})
	assertState(t, fiber, cordis.Disposed)
}

// ---------------------------------------------------------------------------
// CORE-012: the diagnostics tree only reports live registrations; labels and
// nesting are exact; a disposer removes its node (and nested ghosts disappear).
// (fiber.rs::fiber_reports_effects_and_awaits_settlement,
//  ::effect_disposer_removes_its_meta_node,
//  regression_core.rs::c3_disposed_nested_effect_is_not_reported_in_the_tree)
// ---------------------------------------------------------------------------

func TestCore012_EffectTreeReflectsLiveRegistrations(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	plugin := &pluginFunc{name: "effects-tree", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.EffectLabeled("outer", func() cordis.Effect {
			// Nested registration while the outer body runs: parent ownership.
			if _, nestedErr := c.EffectLabeled("inner", func() cordis.Effect {
				return cordis.Effect{cleanupOf("inner", tr)}
			}); nestedErr != nil {
				return nil
			}
			return cordis.Effect{cleanupOf("outer", tr)}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if err := fiber.AwaitSettled(); err != nil {
		t.Fatalf("AwaitSettled: %v", err)
	}
	if got := effectTree(fiber.Effects()); got != "[outer[inner[]]]" {
		t.Fatalf("effect tree = %s, want [outer[inner[]]]", got)
	}

	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	if got := fiber.Effects(); len(got) != 0 {
		t.Fatalf("effects after dispose = %s, want empty", effectTree(got))
	}
	if err := fiber.AwaitSettled(); err != nil {
		t.Fatalf("AwaitSettled after dispose: %v", err)
	}
	assertState(t, fiber, cordis.Disposed)
}

func TestCore012_EffectDisposerRemovesItsNode(t *testing.T) {
	root := newRoot(t)
	plugin := &pluginFunc{name: "one-effect", activate: func(c cordis.Context, _ cordis.Value) error { return nil }}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	ctx, _ := fiber.Context()

	owned, err := ctx.EffectLabeled("owned", func() cordis.Effect {
		return cordis.Effect{cordis.CleanupOf(func() {})}
	})
	if err != nil {
		t.Fatalf("effect: %v", err)
	}
	if got := effectLabels(fiber.Effects()); len(got) != 1 || got[0] != "owned" {
		t.Fatalf("effects = %v, want [owned]", got)
	}
	owned.Dispose()
	if got := fiber.Effects(); len(got) != 0 {
		t.Fatalf("effects after disposer = %s, want empty", effectTree(got))
	}
}

// TestCore012_DisposedNestedEffectIsNotAReportedGhost is the C3 regression: a
// nested effect released by the user disposer must disappear from its parent's
// children instead of lingering as a ghost node.
func TestCore012_DisposedNestedEffectIsNotAReportedGhost(t *testing.T) {
	root := newRoot(t)
	plugin := &pluginFunc{name: "c3-tree", activate: func(c cordis.Context, _ cordis.Value) error { return nil }}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	ctx, _ := fiber.Context()

	var childDisposer *cordis.Disposer
	if _, err := ctx.EffectLabeled("parent", func() cordis.Effect {
		d, nestedErr := ctx.EffectLabeled("c3-child", func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() {})}
		})
		if nestedErr == nil {
			childDisposer = d
		}
		return cordis.Effect{cordis.CleanupOf(func() {})}
	}); err != nil {
		t.Fatalf("parent effect: %v", err)
	}
	if got := effectTree(fiber.Effects()); got != "[parent[c3-child[]]]" {
		t.Fatalf("effect tree = %s, want [parent[c3-child[]]]", got)
	}
	if childDisposer == nil {
		t.Fatal("nested disposer missing")
	}
	childDisposer.Dispose()
	if got := effectTree(fiber.Effects()); got != "[parent[]]" {
		t.Fatalf("effect tree after child disposal = %s, want [parent[]] (no ghost node)", got)
	}
}

// ---------------------------------------------------------------------------
// CORE-030: a sync cleanup panic does not skip the remaining cleanups; the
// first panic is re-raised after the full drain
// (hardening.rs::cleanup_panic_does_not_skip_remaining_cleanups)
// ---------------------------------------------------------------------------

func TestCore030_CleanupPanicDoesNotSkipRemaining(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	boom := &panicPayload{message: "cleanup boom"}
	plugin := &pluginFunc{name: "cleanup-panic", activate: func(c cordis.Context, _ cordis.Value) error {
		_, err := c.Effect(func() cordis.Effect {
			return cordis.Effect{
				cleanupOf("first", tr),
				cleanupOf("second", tr),
				cordis.CleanupOf(func() { panic(boom) }),
			}
		})
		return err
	}}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}

	recovered := catchPanic(func() { _ = fiber.Dispose() })
	if recovered != boom {
		t.Fatalf("recovered panic = %v, want the original payload %p", recovered, boom)
	}
	// Registration order first, second, panic => disposal panic, second, first.
	assertOrder(t, tr.snapshot(), []string{"second", "first"})
	// The panic is re-raised only after the unload finalized the fiber.
	assertState(t, fiber, cordis.Disposed)
}

// TestCore030_RefreshDrivenUnloadContainsCleanupPanic covers the boundary from
// the CORE-030 row: only a direct Fiber.Dispose re-raises; refresh/retire-driven
// unloads contain the panic and keep the tree consistent (fix01/fix02).
func TestCore030_RefreshDrivenUnloadContainsCleanupPanic(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("core030.gate")
	gate, err := cordis.Provide(root, key, "on")
	if err != nil {
		t.Fatalf("provide: %v", err)
	}
	boom := &panicPayload{message: "dependent cleanup boom"}
	consumer := &pluginFunc{name: "core030-consumer", deps: []string{key.Name()}, activate: func(c cordis.Context, _ cordis.Value) error {
		_, effectErr := c.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() { panic(boom) })}
		})
		return effectErr
	}}
	fiber, err := root.Plugin(consumer, nil)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	assertState(t, fiber, cordis.Active)

	// Removing the provider drives a refresh unload of the dependent; the
	// dependent's panicking cleanup must be contained, not escape this call.
	if got := catchPanic(func() { gate.Dispose() }); got != nil {
		t.Fatalf("provider disposal leaked a cleanup panic: %v", got)
	}
	assertState(t, fiber, cordis.Pending)
	if got := fiber.Effects(); len(got) != 0 {
		t.Fatalf("contained unload left effects behind: %s", effectTree(got))
	}
}
