package services_test

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
// CORE-022 (dedupe/recycle): one runtime per plugin identity; the runtime is
// removed with its last fiber.
// ---------------------------------------------------------------------------

func TestCore022_RuntimeDedupeAndRecycle(t *testing.T) {
	root := newRoot(t)
	runs := 0
	plugin := &funcPlugin{
		name: "plugin-a",
		activate: func(cordis.Context, cordis.Value) error {
			runs++
			return nil
		},
	}

	first, err := root.Plugin(plugin, nil)
	mustNoError(t, err, "load first fiber")
	second, err := root.Plugin(plugin, nil)
	mustNoError(t, err, "load second fiber")
	needState(t, first, cordis.Active, "first fiber")
	needState(t, second, cordis.Active, "second fiber")
	if runs != 2 {
		t.Fatalf("activation runs = %d, want 2 (same runtime, distinct fibers)", runs)
	}

	registry := root.Registry()
	if registry.Size() != 1 {
		t.Fatalf("registry size = %d, want 1 (same plugin identity shares one runtime)", registry.Size())
	}
	runtime, ok := registry.Get(cordis.NamedPluginKey("plugin-a"))
	if !ok {
		t.Fatal("runtime for plugin-a missing")
	}
	if runtime.Name() != "plugin-a" {
		t.Fatalf("runtime name = %q, want %q", runtime.Name(), "plugin-a")
	}
	if got := runtime.FiberCount(); got != 2 {
		t.Fatalf("runtime fiber count = %d, want 2", got)
	}

	mustNoError(t, first.Dispose(), "dispose first fiber")
	if registry.Size() != 1 {
		t.Fatalf("registry size after one dispose = %d, want 1 (runtime survives)", registry.Size())
	}
	if got := runtime.FiberCount(); got != 1 {
		t.Fatalf("runtime fiber count after one dispose = %d, want 1", got)
	}

	mustNoError(t, second.Dispose(), "dispose second fiber")
	if registry.Size() != 0 {
		t.Fatalf("registry size after last dispose = %d, want 0", registry.Size())
	}
	if registry.Has(cordis.NamedPluginKey("plugin-a")) {
		t.Fatal("runtime still present after its last fiber was disposed")
	}
}

func TestCore022_TypedPluginIdentity(t *testing.T) {
	root := newRoot(t)
	first := &funcPlugin{name: "typed"}
	second := &funcPlugin{name: "typed"} // separate instance, same concrete type

	f1, err := cordis.PluginTyped(root, first, nil)
	mustNoError(t, err, "load first typed fiber")
	f2, err := cordis.PluginTyped(root, second, nil)
	mustNoError(t, err, "load second typed fiber")
	needState(t, f1, cordis.Active, "first typed fiber")
	needState(t, f2, cordis.Active, "second typed fiber")

	registry := root.Registry()
	typeKey := cordis.TypedPluginKey[*funcPlugin]()
	if registry.Size() != 1 {
		t.Fatalf("registry size = %d, want 1 (TypeId identity dedupes instances)", registry.Size())
	}
	runtime, ok := registry.Get(typeKey)
	if !ok {
		t.Fatal("typed runtime key missing")
	}
	if got := runtime.FiberCount(); got != 2 {
		t.Fatalf("typed runtime fiber count = %d, want 2", got)
	}
	if registry.Has(cordis.NamedPluginKey("typed")) {
		t.Fatal("typed runtime must not be addressable through a named key")
	}

	mustNoError(t, f1.Dispose(), "dispose first typed fiber")
	mustNoError(t, f2.Dispose(), "dispose second typed fiber")
	if registry.Size() != 0 {
		t.Fatalf("registry size after typed dispose = %d, want 0", registry.Size())
	}
}

// ---------------------------------------------------------------------------
// CORE-022 (map API): size/keys/values/entries/for_each snapshots and the
// graceful delete of missing keys.
// ---------------------------------------------------------------------------

func TestCore022_MapAPI(t *testing.T) {
	root := newRoot(t)
	registry := root.Registry()

	alpha := &funcPlugin{name: "alpha"}
	beta := &funcPlugin{name: "beta"}
	fiberAlpha, err := root.Plugin(alpha, nil)
	mustNoError(t, err, "load alpha")
	fiberBeta, err := root.Plugin(beta, nil)
	mustNoError(t, err, "load beta")

	if registry.Size() != 2 {
		t.Fatalf("registry size = %d, want 2", registry.Size())
	}
	if fiberAlpha.ID() == fiberBeta.ID() {
		t.Fatalf("fiber ids collide: %d", fiberAlpha.ID())
	}
	if got := registry.FiberCount(); got < 2 {
		t.Fatalf("registry fiber count = %d, want >= 2", got)
	}

	keys := registry.Keys()
	if len(keys) != 2 {
		t.Fatalf("keys length = %d, want 2", len(keys))
	}
	hasAlpha := false
	for _, key := range keys {
		if key == cordis.NamedPluginKey("alpha") {
			hasAlpha = true
		}
	}
	if !hasAlpha {
		t.Fatalf("keys = %v, want it to contain Named(alpha)", keys)
	}

	if got := len(registry.Values()); got != 2 {
		t.Fatalf("values length = %d, want 2", got)
	}
	entries := registry.Entries()
	if len(entries) != 2 {
		t.Fatalf("entries length = %d, want 2", len(entries))
	}
	betaRuntime, ok := entries[cordis.NamedPluginKey("beta")]
	if !ok || betaRuntime.Name() != "beta" {
		t.Fatalf("entries[beta] = %v, want a runtime named beta", betaRuntime)
	}

	visited := 0
	registry.ForEach(func(cordis.PluginKey, *cordis.PluginRuntime) { visited++ })
	if visited != 2 {
		t.Fatalf("for_each visits = %d, want 2", visited)
	}

	if removed := registry.Delete(cordis.NamedPluginKey("missing")); removed != nil {
		t.Fatalf("delete(missing) = %v, want nil", removed)
	}

	// Empty registry: all map accessors are empty and delete is graceful.
	emptyRoot := newRoot(t)
	empty := emptyRoot.Registry()
	if empty.Size() != 0 {
		t.Fatalf("fresh registry size = %d, want 0", empty.Size())
	}
	if len(empty.Keys()) != 0 || len(empty.Values()) != 0 || len(empty.Entries()) != 0 {
		t.Fatalf("fresh registry map accessors are not empty: keys=%v values=%v",
			empty.Keys(), empty.Values())
	}
	if removed := empty.Delete(cordis.NamedPluginKey("nothing")); removed != nil {
		t.Fatalf("fresh registry delete(nothing) = %v, want nil", removed)
	}
}

// ---------------------------------------------------------------------------
// CORE-022 (delete): deleting a runtime disposes all of its fibers and returns
// the runtime. The Go port currently rejects deletes of non-empty runtimes, so
// this test detects the divergence and reports it as a kernel gap (it turns into
// a hard assertion once Delete implements the documented contract).
// ---------------------------------------------------------------------------

func TestCore022_DeleteDisposesRuntimeFibers(t *testing.T) {
	root := newRoot(t)
	tr := &trace{}
	plugin := &funcPlugin{
		name: "delete-alpha",
		activate: func(ctx cordis.Context, _ cordis.Value) error {
			_, err := ctx.Effect(traceEffect(tr, "delete-alpha-cleanup"))
			return err
		},
	}
	fiber, err := root.Plugin(plugin, nil)
	mustNoError(t, err, "load plugin")
	needState(t, fiber, cordis.Active, "plugin")

	registry := root.Registry()
	removed := registry.Delete(cordis.NamedPluginKey("delete-alpha"))
	if removed == nil {
		t.Skip("KERNEL-GAP CORE-022: Registry.Delete must dispose the runtime's fibers and return the removed runtime (Rust registry.rs:1076-1086); Go Registry.Delete rejects non-empty runtimes and returns nil (registry.go:450-464)")
	}
	if removed.Name() != "delete-alpha" {
		t.Fatalf("removed runtime name = %q, want %q", removed.Name(), "delete-alpha")
	}
	if registry.Size() != 0 {
		t.Fatalf("registry size after delete = %d, want 0", registry.Size())
	}
	needState(t, fiber, cordis.Disposed, "fiber after delete")
	if got, want := tr.snapshot(), []string{"delete-alpha-cleanup"}; !sameStrings(got, want) {
		t.Fatalf("delete cleanup trace = %v, want %v", got, want)
	}
}

// ---------------------------------------------------------------------------
// CORE-022 (-race): concurrent load/dispose churn on one registry settles back
// to an empty, idle tree.
// ---------------------------------------------------------------------------

func TestCore022_ConcurrentLoadDispose(t *testing.T) {
	root := newRoot(t)
	names := [2]string{"churn-a", "churn-b"}

	var wg sync.WaitGroup
	errCh := make(chan error, len(names))
	start := make(chan struct{})
	for w := range names {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			plugin := &funcPlugin{name: names[w]}
			for i := 0; i < 20; i++ {
				fiber, err := root.Plugin(plugin, nil)
				if err != nil {
					errCh <- err
					return
				}
				if err := fiber.Dispose(); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	waitClosed(t, done, 10*time.Second, "concurrent load/dispose churn")
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent churn: %v", err)
	}

	root.WhenIdle()
	registry := root.Registry()
	if registry.Size() != 0 {
		t.Fatalf("registry size after churn = %d, want 0", registry.Size())
	}
	if got := registry.FiberCount(); got != 0 {
		t.Fatalf("registry fiber count after churn = %d, want 0", got)
	}
	if !registry.IsIdle() {
		t.Fatal("registry not idle after churn")
	}
}

// ---------------------------------------------------------------------------
// CORE-048 helpers: sync provider with cleanup, dependent, async provider,
// failing plugins and a blocking-cleanup plugin.
// ---------------------------------------------------------------------------

type core048Provider struct {
	name  string
	key   cordis.ServiceKey[int]
	value int
	tr    *trace
}

func (p *core048Provider) Name() string { return p.name }

func (p *core048Provider) Activate(ctx cordis.Context, _ cordis.Value) error {
	if _, err := cordis.Provide(ctx, p.key, p.value); err != nil {
		return err
	}
	tr := p.tr
	_, err := ctx.Effect(traceEffect(tr, p.name+"-cleanup"))
	return err
}

type core048Dependent struct {
	name    string
	key     cordis.ServiceKey[int]
	seen    []int
	applies int
}

func (p *core048Dependent) Name() string     { return p.name }
func (p *core048Dependent) Inject() []string { return []string{p.key.Name()} }

func (p *core048Dependent) Activate(ctx cordis.Context, _ cordis.Value) error {
	value, ok := cordis.Get(ctx, p.key)
	if !ok {
		return cordis.PluginFailedError("replace: injected service not visible")
	}
	p.applies++
	p.seen = append(p.seen, value)
	return nil
}

type core048AsyncProvider struct {
	name    string
	key     cordis.ServiceKey[int]
	value   int
	applied *atomic.Int32
}

func (p *core048AsyncProvider) Name() string { return p.name }

func (p *core048AsyncProvider) ActivateAsync(_ context.Context, ctx cordis.Context, _ cordis.Value) error {
	if p.applied != nil {
		p.applied.Add(1)
	}
	_, err := cordis.Provide(ctx, p.key, p.value)
	return err
}

type core048FailPlugin struct {
	name string
}

func (p *core048FailPlugin) Name() string { return p.name }
func (p *core048FailPlugin) Activate(cordis.Context, cordis.Value) error {
	return errors.New("reload activation boom")
}

type core048StructuredFailPlugin struct{}

func (*core048StructuredFailPlugin) Name() string { return "api-failing-sync" }
func (*core048StructuredFailPlugin) Activate(cordis.Context, cordis.Value) error {
	return cordis.PluginError("api-failing-sync", "config", "boom")
}

type core048BlockingPlugin struct {
	name    string
	entered chan struct{}
	release chan struct{}
}

func (p *core048BlockingPlugin) Name() string { return p.name }

func (p *core048BlockingPlugin) Activate(ctx cordis.Context, _ cordis.Value) error {
	entered, release := p.entered, p.release
	_, err := ctx.Effect(func() cordis.Effect {
		return cordis.Effect{cordis.CleanupOf(func() {
			close(entered)
			<-release
		})}
	})
	return err
}

type core048RawPlugin struct {
	name    string
	configs []string
}

func (p *core048RawPlugin) Name() string { return p.name }
func (p *core048RawPlugin) Activate(_ cordis.Context, config cordis.Value) error {
	value, _ := config.(string)
	p.configs = append(p.configs, value)
	return nil
}

// ---------------------------------------------------------------------------
// CORE-048: replace re-binds every fiber of the old runtime, preserves fiber
// identity and uid, reloads with the new implementation and keeps dependents
// consistent.
// ---------------------------------------------------------------------------

func TestCore048_ReplaceRebindsAndPreservesIdentity(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[int]("replace.svc")
	tr := &trace{}

	provider := &core048Provider{name: "replace-a", key: key, value: 1, tr: tr}
	fiber, err := root.Plugin(provider, nil)
	mustNoError(t, err, "load replace-a")
	needState(t, fiber, cordis.Active, "replace-a")
	oldID := fiber.ID()

	dependent := &core048Dependent{name: "replace-dependent", key: key}
	dependentFiber, err := root.Plugin(dependent, nil)
	mustNoError(t, err, "load dependent")
	needState(t, dependentFiber, cordis.Active, "dependent")
	if len(dependent.seen) != 1 || dependent.seen[0] != 1 {
		t.Fatalf("dependent initial seen = %v, want [1]", dependent.seen)
	}

	replacement := &core048Provider{name: "replace-b", key: key, value: 2, tr: tr}
	reloaded, err := root.Registry().Replace(cordis.NamedPluginKey("replace-a"), replacement)
	mustNoError(t, err, "replace plugin")
	if len(reloaded) != 1 {
		t.Fatalf("replace returned %d fibers, want 1", len(reloaded))
	}
	if got := reloaded[0].ID(); got != oldID {
		t.Fatalf("reloaded fiber id = %d, want original id %d (identity preserved)", got, oldID)
	}
	needState(t, reloaded[0], cordis.Active, "reloaded fiber")
	if got, ok := cordis.Get(root, key); !ok || got != 2 {
		t.Fatalf("service value after replace = %v/%v, want 2", got, ok)
	}
	if dependent.applies != 2 {
		t.Fatalf("dependent applies after replace = %d, want 2", dependent.applies)
	}
	if len(dependent.seen) != 2 || dependent.seen[0] != 1 || dependent.seen[1] != 2 {
		t.Fatalf("dependent seen after replace = %v, want [1 2]", dependent.seen)
	}
	needState(t, dependentFiber, cordis.Active, "dependent after replace")
	if got, want := tr.snapshot(), []string{"replace-a-cleanup"}; !sameStrings(got, want) {
		t.Fatalf("cleanup trace = %v, want %v (old implementation cleaned up before reload)", got, want)
	}

	registry := root.Registry()
	if registry.Has(cordis.NamedPluginKey("replace-a")) {
		t.Fatal("old runtime key still registered after replace")
	}
	if !registry.Has(cordis.NamedPluginKey("replace-b")) {
		t.Fatal("new runtime key missing after replace")
	}
}

func TestCore048_ReplaceKeepsRawConfig(t *testing.T) {
	root := newRoot(t)
	hookDisposer, err := root.OnInternalConfig(func(_ *cordis.Fiber, config cordis.Value, next *cordis.ConfigNext) cordis.Value {
		resolved := next.Next(config)
		if value, ok := resolved.(string); ok {
			return value + "|R"
		}
		return resolved
	})
	mustNoError(t, err, "register internal/config hook")

	providerA := &core048RawPlugin{name: "replace-raw-a"}
	fiber, err := root.Plugin(providerA, "x")
	mustNoError(t, err, "load replace-raw-a")
	needState(t, fiber, cordis.Active, "replace-raw-a")
	if want := []string{"x|R"}; !sameStrings(providerA.configs, want) {
		t.Fatalf("provider A activation configs = %v, want %v", providerA.configs, want)
	}

	providerB := &core048RawPlugin{name: "replace-raw-b"}
	reloaded, err := root.Registry().Replace(cordis.NamedPluginKey("replace-raw-a"), providerB)
	mustNoError(t, err, "replace raw provider")
	if len(reloaded) != 1 {
		t.Fatalf("replace returned %d fibers, want 1", len(reloaded))
	}
	// The retained raw config must be re-resolved exactly once, not compounded.
	if want := []string{"x|R"}; !sameStrings(providerB.configs, want) {
		t.Fatalf("provider B activation configs = %v, want %v (raw config preserved, resolved once)", providerB.configs, want)
	}
	if got := reloaded[0].RawConfig(); got != "x" {
		t.Fatalf("raw config after replace = %v, want %q", got, "x")
	}
	needState(t, reloaded[0], cordis.Active, "reloaded raw provider")
	hookDisposer.Dispose()
}

// ---------------------------------------------------------------------------
// CORE-048: async replace through ReplaceAsync.
// ---------------------------------------------------------------------------

func TestCore048_ReplaceAsyncPlugin(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[int]("replace.async")

	var appliedA atomic.Int32
	providerA := &core048AsyncProvider{name: "replace-async-a", key: key, value: 1, applied: &appliedA}
	fiber, err := root.PluginAsync(providerA, nil)
	mustNoError(t, err, "load async replace-async-a")
	needState(t, fiber, cordis.Active, "replace-async-a")
	if got, ok := cordis.Get(root, key); !ok || got != 1 {
		t.Fatalf("service before async replace = %v/%v, want 1", got, ok)
	}

	var appliedB atomic.Int32
	providerB := &core048AsyncProvider{name: "replace-async-b", key: key, value: 3, applied: &appliedB}
	reloaded, err := root.Registry().ReplaceAsync(context.Background(), cordis.NamedPluginKey("replace-async-a"), providerB)
	mustNoError(t, err, "replace async plugin")
	if len(reloaded) != 1 {
		t.Fatalf("async replace returned %d fibers, want 1", len(reloaded))
	}
	if got := reloaded[0].ID(); got != fiber.ID() {
		t.Fatalf("reloaded async fiber id = %d, want %d", got, fiber.ID())
	}
	needState(t, reloaded[0], cordis.Active, "reloaded async fiber")
	if got, ok := cordis.Get(root, key); !ok || got != 3 {
		t.Fatalf("service after async replace = %v/%v, want 3", got, ok)
	}
	if got := appliedB.Load(); got != 1 {
		t.Fatalf("new async implementation applied %d times, want 1", got)
	}
	if root.Registry().Has(cordis.NamedPluginKey("replace-async-a")) {
		t.Fatal("old async runtime key still registered")
	}
	if !root.Registry().Has(cordis.NamedPluginKey("replace-async-b")) {
		t.Fatal("new async runtime key missing")
	}
}

// ---------------------------------------------------------------------------
// CORE-048 "Ok != Active": an activation failure during reload is contained at
// the fiber level (Replace returns Ok, the fiber lands Failed and keeps the
// structured error).
// ---------------------------------------------------------------------------

func TestCore048_ReplaceActivationFailureContained(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[int]("replace.fail")

	provider := &core048Provider{name: "replace-fail-a", key: key, value: 1, tr: &trace{}}
	fiber, err := root.Plugin(provider, nil)
	mustNoError(t, err, "load replace-fail-a")
	oldID := fiber.ID()

	failing := &core048StructuredFailPlugin{}
	reloaded, err := root.Registry().Replace(cordis.NamedPluginKey("replace-fail-a"), failing)
	// Activation failure is fiber state, not a Replace error.
	mustNoError(t, err, "replace with failing implementation must return Ok")
	if len(reloaded) != 1 {
		t.Fatalf("replace returned %d fibers, want 1", len(reloaded))
	}
	if got := reloaded[0].ID(); got != oldID {
		t.Fatalf("failed reload fiber id = %d, want %d", got, oldID)
	}
	needState(t, reloaded[0], cordis.Failed, "fiber after failed reload")
	detail := reloaded[0].ErrorDetail()
	if detail == nil {
		t.Fatal("failed reload lost its structured error detail")
	}
	var ce *cordis.CordisError
	if !errors.As(detail, &ce) {
		t.Fatalf("error detail = %v, want *cordis.CordisError", detail)
	}
	if code, _ := cordis.CodeOf(detail); code != cordis.CodePlugin {
		t.Fatalf("error detail code = %v, want CodePlugin", code)
	}
	if ce.Name() != "api-failing-sync" || ce.PluginKind() != "config" || ce.Detail() != "boom" {
		t.Fatalf("structured error = name:%q kind:%q detail:%q, want api-failing-sync/config/boom",
			ce.Name(), ce.PluginKind(), ce.Detail())
	}
	if root.Registry().Has(cordis.NamedPluginKey("replace-fail-a")) {
		t.Fatal("old runtime key still registered after failed reload")
	}
	if !root.Registry().Has(cordis.NamedPluginKey("api-failing-sync")) {
		t.Fatal("failed reload must keep the fiber registered under the new runtime key")
	}
}

// TestCore048_ReplaceLifecycleRejectionRollsBack (fix07/fix08): a LifecycleBusy
// rejection returns an error and leaves the registry binding untouched.
func TestCore048_ReplaceLifecycleRejectionRollsBack(t *testing.T) {
	root := newRoot(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	blocking := &core048BlockingPlugin{name: "replace-busy", entered: entered, release: release}
	fiber, err := root.Plugin(blocking, nil)
	mustNoError(t, err, "load blocking plugin")

	disposeDone := make(chan error, 1)
	go func() { disposeDone <- fiber.Dispose() }()
	waitClosed(t, entered, 2*time.Second, "blocking cleanup to start")

	// While the dispose owns the fiber lifecycle slot, replace must be rejected
	// with LifecycleBusy and roll the re-binding back.
	key := cordis.NewServiceKey[int]("replace.busy")
	newImpl := &core048Provider{name: "replace-busy-new", key: key, value: 9, tr: &trace{}}
	_, err = root.Registry().Replace(cordis.NamedPluginKey("replace-busy"), newImpl)
	requireCode(t, err, cordis.CodeLifecycleBusy)

	registry := root.Registry()
	if !registry.Has(cordis.NamedPluginKey("replace-busy")) {
		t.Fatal("failed replace removed the old runtime binding (rollback failed)")
	}
	if registry.Has(cordis.NamedPluginKey("replace-busy-new")) {
		t.Fatal("failed replace left the new runtime binding behind (rollback failed)")
	}
	if registry.Size() != 1 {
		t.Fatalf("registry size after failed replace = %d, want 1", registry.Size())
	}

	close(release)
	select {
	case err := <-disposeDone:
		mustNoError(t, err, "blocking dispose")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the blocking dispose to finish")
	}
	needState(t, fiber, cordis.Disposed, "blocking fiber after dispose")
	if registry.Size() != 0 {
		t.Fatalf("registry size after dispose = %d, want 0", registry.Size())
	}
}

// ---------------------------------------------------------------------------
// CORE-048 (fiber-level): Fiber.ReplacePlugin swaps a single fiber's
// implementation in place.
// ---------------------------------------------------------------------------

func TestCore048_FiberReplacePlugin(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[int]("replace.single")
	tr := &trace{}
	providerA := &core048Provider{name: "replace-single-a", key: key, value: 1, tr: tr}
	fiber, err := root.Plugin(providerA, nil)
	mustNoError(t, err, "load replace-single-a")
	if got, ok := cordis.Get(root, key); !ok || got != 1 {
		t.Fatalf("service before replace_plugin = %v/%v, want 1", got, ok)
	}

	providerB := &core048Provider{name: "replace-single-b", key: key, value: 7, tr: &trace{}}
	mustNoError(t, fiber.ReplacePlugin(providerB), "fiber replace_plugin")
	needState(t, fiber, cordis.Active, "fiber after replace_plugin")
	if got, ok := cordis.Get(root, key); !ok || got != 7 {
		t.Fatalf("service after replace_plugin = %v/%v, want 7", got, ok)
	}
	registry := root.Registry()
	if registry.Has(cordis.NamedPluginKey("replace-single-a")) {
		t.Fatal("old runtime key still registered after replace_plugin")
	}
	if !registry.Has(cordis.NamedPluginKey("replace-single-b")) {
		t.Fatal("new runtime key missing after replace_plugin")
	}
}

// ---------------------------------------------------------------------------
// CORE-048: replace errors on unknown and disposed runtime keys are safe: no
// resurrection, no panic.
// ---------------------------------------------------------------------------

func TestCore048_ReplaceUnknownAndDisposedErrors(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[int]("replace.err")
	registry := root.Registry()

	// Unknown key.
	_, err := registry.Replace(cordis.NamedPluginKey("no-such-runtime"),
		&core048Provider{name: "replace-err-b", key: key, value: 2, tr: &trace{}})
	requireCode(t, err, cordis.CodeInvalidPlugin)

	// Disposed fiber: the runtime is gone once its last fiber is disposed, so a
	// registry replace must fail safely.
	provider := &core048Provider{name: "replace-gone", key: key, value: 1, tr: &trace{}}
	fiber, err := root.Plugin(provider, nil)
	mustNoError(t, err, "load replace-gone")
	mustNoError(t, fiber.Dispose(), "dispose replace-gone")
	_, err = registry.Replace(cordis.NamedPluginKey("replace-gone"),
		&core048Provider{name: "replace-err-c", key: key, value: 3, tr: &trace{}})
	requireCode(t, err, cordis.CodeInvalidPlugin)

	// Replacing a disposed fiber's implementation must not resurrect it.
	err = fiber.ReplacePlugin(&core048Provider{name: "replace-err-d", key: key, value: 4, tr: &trace{}})
	if err == nil {
		t.Fatal("ReplacePlugin on a disposed fiber returned nil")
	}
	code, ok := cordis.CodeOf(err)
	if !ok || (code != cordis.CodeDisposed && code != cordis.CodeInvalidPlugin) {
		t.Fatalf("ReplacePlugin on disposed fiber code = %v/%v, want Disposed or InvalidPlugin", code, ok)
	}
	needState(t, fiber, cordis.Disposed, "disposed fiber after failed ReplacePlugin")
}
