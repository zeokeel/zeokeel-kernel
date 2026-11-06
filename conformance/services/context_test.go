package services_test

import (
	"strings"
	"sync"
	"testing"

	cordis "github.com/zeokeel/cordis-go"
)

// ---------------------------------------------------------------------------
// CORE-003: isolate only shadows the named service for the derived subtree; it
// never falls back to the ancestor provider and two isolate() calls are always
// fresh scopes.
// ---------------------------------------------------------------------------

func TestCore003_IsolateShadowsParentService(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("greeting")

	rootDisposer, err := cordis.Provide(root, key, "root")
	mustNoError(t, err, "root provide")

	isolated := root.Isolate("greeting")
	childDisposer, err := cordis.Provide(isolated, key, "child")
	mustNoError(t, err, "isolated provide")

	if got, ok := cordis.Get(isolated, key); !ok || got != "child" {
		t.Fatalf("isolated.Get = %q/%v, want %q", got, ok, "child")
	}
	if got, ok := cordis.Get(root, key); !ok || got != "root" {
		t.Fatalf("root.Get = %q/%v, want %q", got, ok, "root")
	}

	// Removing the shadowing provider must not fall back to the ancestor.
	childDisposer.Dispose()
	if got, ok := cordis.Get(isolated, key); ok {
		t.Fatalf("isolated.Get after child dispose = %q; must not fall back to the root provider", got)
	}
	if got, ok := cordis.Get(root, key); !ok || got != "root" {
		t.Fatalf("root.Get after child dispose = %q/%v, want %q", got, ok, "root")
	}
	rootDisposer.Dispose()
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("root service still visible after its disposer ran")
	}

	// Each unlabeled isolate() call creates a fresh scope: same name does not
	// share the binding.
	scopedKey := cordis.NewServiceKey[string]("scoped")
	first := root.Isolate("scoped")
	second := root.Isolate("scoped")
	if _, err := cordis.Provide(first, scopedKey, "only-first"); err != nil {
		t.Fatalf("provide in first isolate: %v", err)
	}
	if got, ok := cordis.Get(first, scopedKey); !ok || got != "only-first" {
		t.Fatalf("first isolate.Get = %q/%v, want %q", got, ok, "only-first")
	}
	if _, ok := cordis.Get(second, scopedKey); ok {
		t.Fatal("two isolate() calls share a scope; want fresh scopes")
	}
	if _, ok := cordis.Get(root, scopedKey); ok {
		t.Fatal("unlabeled isolate leaked its service into the default scope")
	}
}

// ---------------------------------------------------------------------------
// CORE-025 (accessor): computed properties are read before services, are
// fiber-scoped and owned by their fiber, and their writes are classified.
// ---------------------------------------------------------------------------

func TestCore025_AccessorResolvedBeforeService(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("computed")
	if _, err := cordis.Provide(root, key, "service-value"); err != nil {
		t.Fatalf("provide service: %v", err)
	}
	accessorDisposer, err := root.Accessor("computed", func(cordis.Context) cordis.ServiceValue {
		return "from-accessor"
	})
	mustNoError(t, err, "register accessor")

	// The accessor shadows the service of the same name on both lanes.
	if got, ok := root.GetNamed("computed"); !ok || got != "from-accessor" {
		t.Fatalf("GetNamed = %q/%v, want %q", got, ok, "from-accessor")
	}
	if got, ok := cordis.Get(root, key); !ok || got != "from-accessor" {
		t.Fatalf("typed Get = %q/%v, want the accessor value", got, ok)
	}

	// A read-only accessor rejects writes with the dedicated variant.
	err = root.SetNamed("computed", "written")
	ce := requireCode(t, err, cordis.CodeAccessorReadOnly)
	if ce.Name() != "computed" {
		t.Fatalf("AccessorReadOnly name = %q, want %q", ce.Name(), "computed")
	}
	if !strings.Contains(err.Error(), "accessor") {
		t.Fatalf("AccessorReadOnly message = %q, want it to mention accessor", err.Error())
	}

	// Disposing the accessor uncovers the service again.
	accessorDisposer.Dispose()
	if got, ok := root.GetNamed("computed"); !ok || got != "service-value" {
		t.Fatalf("GetNamed after accessor dispose = %q/%v, want %q", got, ok, "service-value")
	}
}

func TestCore025_AccessorWithSetterWrites(t *testing.T) {
	root := newRoot(t)
	var mu sync.Mutex
	stored := cordis.ServiceValue("initial")
	writes := 0
	disposer, err := root.AccessorWithSetter("sink",
		func(cordis.Context) cordis.ServiceValue {
			mu.Lock()
			defer mu.Unlock()
			return stored
		},
		func(_ cordis.Context, value cordis.ServiceValue) bool {
			mu.Lock()
			defer mu.Unlock()
			stored = value
			writes++
			return true
		})
	mustNoError(t, err, "register writable accessor")

	if got, ok := root.GetNamed("sink"); !ok || got != "initial" {
		t.Fatalf("GetNamed before write = %q/%v", got, ok)
	}
	mustNoError(t, root.SetNamed("sink", "written"), "write through accessor")
	if got, ok := root.GetNamed("sink"); !ok || got != "written" {
		t.Fatalf("GetNamed after write = %q/%v, want %q", got, ok, "written")
	}
	mu.Lock()
	gotWrites := writes
	mu.Unlock()
	if gotWrites != 1 {
		t.Fatalf("setter writes = %d, want 1", gotWrites)
	}

	disposer.Dispose()
	if _, ok := root.GetNamed("sink"); ok {
		t.Fatal("accessor still resolvable after its disposer ran")
	}
}

func TestCore025_AccessorSetterRejectionUsesDedicatedError(t *testing.T) {
	root := newRoot(t)
	disposer, err := root.AccessorWithSetter("svc.rejected",
		func(cordis.Context) cordis.ServiceValue { return "visible" },
		func(cordis.Context, cordis.ServiceValue) bool { return false })
	mustNoError(t, err, "register rejecting accessor")

	err = root.SetNamed("svc.rejected", "attempted")
	ce := requireCode(t, err, cordis.CodeAccessorWriteRejected)
	if ce.Name() != "svc.rejected" {
		t.Fatalf("AccessorWriteRejected name = %q, want %q", ce.Name(), "svc.rejected")
	}
	if !strings.Contains(err.Error(), "accessor") {
		t.Fatalf("AccessorWriteRejected message = %q, want it to mention accessor", err.Error())
	}
	if got, ok := root.GetNamed("svc.rejected"); !ok || got != "visible" {
		t.Fatalf("GetNamed after rejected write = %q/%v, want %q", got, ok, "visible")
	}
	_ = disposer
}

func TestCore025_AccessorGetterPanicIsAbsent(t *testing.T) {
	root := newRoot(t)
	if _, err := root.Accessor("panicky", func(cordis.Context) cordis.ServiceValue {
		panic("accessor getter boom")
	}); err != nil {
		t.Fatalf("register panicking accessor: %v", err)
	}
	// A panicking getter is contained and treated as an absent property.
	if value, ok := root.GetNamed("panicky"); ok {
		t.Fatalf("GetNamed = %v, want absent for a panicking getter", value)
	}
	// The kernel stays usable afterwards.
	if _, ok := root.GetNamed("logger"); !ok {
		t.Fatal("kernel unusable after contained accessor getter panic")
	}
}

func TestCore025_AccessorIsScopedToItsFiber(t *testing.T) {
	root := newRoot(t)
	var ownerCtx cordis.Context
	var disposer *cordis.Disposer
	fiber, err := root.Plugin(&funcPlugin{
		name: "core025-accessor-owner",
		activate: func(ctx cordis.Context, _ cordis.Value) error {
			ownerCtx = ctx
			d, err := ctx.Accessor("fiber.computed", func(cordis.Context) cordis.ServiceValue {
				return "fiber-value"
			})
			disposer = d
			return err
		},
	}, nil)
	mustNoError(t, err, "load accessor owner")
	needState(t, fiber, cordis.Active, "accessor owner")

	// Accessors live on the node that registered them: the root cannot see a
	// plugin-local computed property, but the owning fiber's context can.
	if _, ok := root.GetNamed("fiber.computed"); ok {
		t.Fatal("root sees a plugin-local accessor")
	}
	if got, ok := ownerCtx.GetNamed("fiber.computed"); !ok || got != "fiber-value" {
		t.Fatalf("owner ctx GetNamed = %q/%v, want %q", got, ok, "fiber-value")
	}

	disposer.Dispose()
	if _, ok := ownerCtx.GetNamed("fiber.computed"); ok {
		t.Fatal("accessor still resolvable after its disposer ran")
	}

	// Re-registered accessor is removed by the owning fiber's unload.
	if _, err := ownerCtx.Accessor("fiber.computed", func(cordis.Context) cordis.ServiceValue {
		return "re-registered"
	}); err != nil {
		t.Fatalf("re-register accessor: %v", err)
	}
	if _, ok := ownerCtx.GetNamed("fiber.computed"); !ok {
		t.Fatal("re-registered accessor not resolvable")
	}
	mustNoError(t, fiber.Dispose(), "dispose accessor owner")
	if _, ok := ownerCtx.GetNamed("fiber.computed"); ok {
		t.Fatal("accessor outlived its owning fiber")
	}
}

// ---------------------------------------------------------------------------
// CORE-025 (meta): extend_meta attaches diagnostics metadata that derived
// contexts inherit; the root does not see it and typed mismatches are absent.
// ---------------------------------------------------------------------------

func TestCore025_MetaAttachedInheritedAndTyped(t *testing.T) {
	root := newRoot(t)
	meta := cordis.NewContextMeta()
	meta.Insert("dir", "/tmp/project")
	meta.Insert("version", uint32(3))
	meta.Insert("version", uint32(7)) // last write wins

	if meta.IsEmpty() {
		t.Fatal("metadata bag reported empty after inserts")
	}
	if !meta.Contains("dir") {
		t.Fatal("metadata bag lost key dir")
	}
	if meta.Len() != 2 {
		t.Fatalf("metadata Len = %d, want 2", meta.Len())
	}

	child := root.ExtendMeta(*meta)
	grandchild := child.Extend()

	if got, ok := cordis.Meta[string](child, "dir"); !ok || got != "/tmp/project" {
		t.Fatalf("child Meta[dir] = %q/%v, want %q", got, ok, "/tmp/project")
	}
	if got, ok := cordis.Meta[string](grandchild, "dir"); !ok || got != "/tmp/project" {
		t.Fatalf("grandchild Meta[dir] = %q/%v (must inherit)", got, ok)
	}
	if _, ok := cordis.Meta[string](root, "dir"); ok {
		t.Fatal("root sees metadata attached to a derived context")
	}
	// Typed mismatch is reported as absent, not coerced.
	if _, ok := cordis.Meta[uint32](grandchild, "dir"); ok {
		t.Fatal("Meta[uint32](dir) matched a string entry")
	}
	if got, ok := cordis.Meta[uint32](child, "version"); !ok || got != 7 {
		t.Fatalf("Meta[version] = %d/%v, want 7 (last write wins)", got, ok)
	}
	if value, ok := grandchild.MetaAny("version"); !ok || value != any(uint32(7)) {
		t.Fatalf("MetaAny(version) = %v/%v, want uint32(7)", value, ok)
	}
}

// ---------------------------------------------------------------------------
// CORE-025 (isolate_labeled): contexts sharing a label share the named scope;
// other labels and the default scope stay isolated.
// ---------------------------------------------------------------------------

func TestCore025_IsolateLabeledSharesScope(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("scoped")

	first := root.IsolateLabeled("scoped", "shared")
	second := root.IsolateLabeled("scoped", "shared")
	other := root.IsolateLabeled("scoped", "other")

	disposer, err := cordis.Provide(first, key, "shared-value")
	mustNoError(t, err, "provide in labeled scope")

	if got, ok := cordis.Get(second, key); !ok || got != "shared-value" {
		t.Fatalf("same-label Get = %q/%v, want %q", got, ok, "shared-value")
	}
	if _, ok := cordis.Get(other, key); ok {
		t.Fatal("different label sees the shared scope binding")
	}
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("labeled isolate leaked into the default scope")
	}

	// Same label => same (scope, name) key => duplicate provide is rejected.
	_, err = cordis.Provide(second, key, "duplicate")
	ce := requireCode(t, err, cordis.CodeDuplicateService)
	if ce.Name() != "scoped" {
		t.Fatalf("DuplicateService name = %q, want %q", ce.Name(), "scoped")
	}

	disposer.Dispose()
	if _, ok := cordis.Get(second, key); ok {
		t.Fatal("labeled scope binding still visible after disposer")
	}
}

// ---------------------------------------------------------------------------
// CORE-040: raw config is re-resolved from raw on every activation; resolved
// values never compound back into the raw config. A plugin fiber reload (restart)
// resolves the same raw input twice.
// ---------------------------------------------------------------------------

type core040Plugin struct {
	seen []string
}

func (p *core040Plugin) Name() string { return "core040-plugin" }

func (p *core040Plugin) Activate(_ cordis.Context, config cordis.Value) error {
	value, _ := config.(string)
	p.seen = append(p.seen, value)
	return nil
}

func TestCore040_RawConfigReResolvedOnEveryActivation(t *testing.T) {
	root := newRoot(t)
	hookDisposer, err := root.OnInternalConfig(func(_ *cordis.Fiber, config cordis.Value, next *cordis.ConfigNext) cordis.Value {
		resolved := next.Next(config)
		if value, ok := resolved.(string); ok {
			return value + "|R"
		}
		return resolved
	})
	mustNoError(t, err, "register internal/config hook")

	plugin := &core040Plugin{}
	fiber, err := root.Plugin(plugin, "x")
	mustNoError(t, err, "load plugin")
	needState(t, fiber, cordis.Active, "plugin")

	if got := fiber.Config(); got != "x|R" {
		t.Fatalf("resolved config = %v, want %q", got, "x|R")
	}
	if got := fiber.RawConfig(); got != "x" {
		t.Fatalf("raw config = %v, want %q (resolved value must not be written back)", got, "x")
	}

	mustNoError(t, fiber.Restart(), "restart plugin")
	needState(t, fiber, cordis.Active, "plugin after restart")
	if got := fiber.RawConfig(); got != "x" {
		t.Fatalf("raw config after restart = %v, want %q", got, "x")
	}
	if got := fiber.Config(); got != "x|R" {
		t.Fatalf("resolved config after restart = %v, want %q", got, "x|R")
	}
	if want := []string{"x|R", "x|R"}; !sameStrings(plugin.seen, want) {
		t.Fatalf("activation configs = %v, want %v (raw re-resolved, no compounding)", plugin.seen, want)
	}
	hookDisposer.Dispose()
}
