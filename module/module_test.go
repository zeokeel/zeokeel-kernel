package module_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	cordis "github.com/zeokeel/cordis-go"
	"github.com/zeokeel/cordis-go/module"
)

// ---------------------------------------------------------------------------
// Test module definitions (a host assembly registered at compile time)
// ---------------------------------------------------------------------------

type dbConfig struct {
	DSN  string `json:"dsn"`
	Pool int    `json:"pool"`
}

type db struct {
	dsn  string
	pool int
}

type api struct {
	db *db
}

type optService struct{}

var (
	dbKey        = cordis.NewServiceKey[*db]("test/db")
	apiKey       = cordis.NewServiceKey[*api]("test/api")
	optKey       = cordis.NewServiceKey[*optService]("test/optional")
	cycleAKey    = cordis.NewServiceKey[*cycleA]("test/cycle-a")
	cycleBKey    = cordis.NewServiceKey[*cycleB]("test/cycle-b")
	otherDBKey   = cordis.NewServiceKey[*otherDB]("test/db")
	optionalSeen bool
)

type cycleA struct{}
type cycleB struct{}
type otherDB struct{}

// Stateful hot-update/reload fixtures.
type counterConfig struct {
	Step int `json:"step"`
}

type counter struct {
	value int
	step  int
}

var counterKey = cordis.NewServiceKey[*counter]("test/counter")

var stateful struct {
	activates    int
	updates      int
	snapshots    int
	restores     int
	failSnapshot bool
	current      *counter
}

var dependentState struct {
	activates int
	restores  int
	value     int
}

func resetStateful() {
	stateful = struct {
		activates    int
		updates      int
		snapshots    int
		restores     int
		failSnapshot bool
		current      *counter
	}{}
	dependentState = struct {
		activates int
		restores  int
		value     int
	}{}
}

func statefulModule() *module.Descriptor[counterConfig] {
	return module.Define("test/stateful", func(ctx cordis.Context, cfg counterConfig) error {
		stateful.activates++
		service := &counter{step: cfg.Step}
		stateful.current = service
		_, err := ctx.Provide(counterKey, service)
		return err
	}).AddProvides(module.Provides(counterKey)).
		WithValidate(func(cfg counterConfig) error {
			if cfg.Step <= 0 {
				return fmt.Errorf("step must be positive")
			}
			return nil
		}).
		WithUpdate(func(_ cordis.Context, _ counterConfig, next counterConfig) error {
			stateful.updates++
			// In-place update mutates behind the stable service handle: dependents
			// keep the same *counter and are not re-activated.
			stateful.current.step = next.Step
			return nil
		}).
		WithSnapshot(func(cordis.Context) (any, error) {
			if stateful.failSnapshot {
				return nil, fmt.Errorf("snapshot intentionally failed")
			}
			stateful.snapshots++
			return stateful.current.value, nil
		}).
		WithRestore(func(_ cordis.Context, snapshot any) error {
			stateful.restores++
			value, ok := snapshot.(int)
			if !ok {
				return fmt.Errorf("unexpected snapshot type %T", snapshot)
			}
			stateful.current.value = value
			return nil
		})
}

func statefulDependentModule() *module.Descriptor[struct{}] {
	return module.Define("test/stateful-dependent", func(ctx cordis.Context, _ struct{}) error {
		if _, err := ctx.Require(counterKey); err != nil {
			return err
		}
		dependentState.activates++
		return nil
	}).AddRequires(module.Requires(counterKey)).
		WithSnapshot(func(cordis.Context) (any, error) { return dependentState.value, nil }).
		WithRestore(func(_ cordis.Context, snapshot any) error {
			dependentState.restores++
			value, ok := snapshot.(int)
			if !ok {
				return fmt.Errorf("unexpected dependent snapshot type %T", snapshot)
			}
			dependentState.value = value
			return nil
		})
}

// plainDependentModule requires the counter but cannot preserve state.
func plainDependentModule() *module.Descriptor[struct{}] {
	return module.Define("test/plain-dependent", func(ctx cordis.Context, _ struct{}) error {
		_, err := ctx.Require(counterKey)
		return err
	}).AddRequires(module.Requires(counterKey))
}

func newStatefulRegistry(t *testing.T) *module.Registry {
	t.Helper()
	registry := module.NewRegistry()
	for _, register := range []func() error{
		func() error { return registry.Register(statefulModule()) },
		func() error { return registry.Register(statefulDependentModule()) },
		func() error { return registry.Register(plainDependentModule()) },
	} {
		if err := register(); err != nil {
			t.Fatalf("register stateful module: %v", err)
		}
	}
	return registry
}

func dbModule() *module.Descriptor[dbConfig] {
	return module.Define("test/db", func(ctx cordis.Context, cfg dbConfig) error {
		if cfg.DSN == "" {
			return cordis.InvalidPluginError("test/db: dsn is empty")
		}
		_, err := ctx.Provide(dbKey, &db{dsn: cfg.DSN, pool: cfg.Pool})
		return err
	}).AddProvides(module.Provides(dbKey)).
		AddRequires(module.Requires(cordis.LoggerKey)).
		WithValidate(func(cfg dbConfig) error {
			if cfg.DSN == "" {
				return fmt.Errorf("dsn is required")
			}
			return nil
		})
}

func apiModule() *module.Descriptor[struct{}] {
	return module.Define("test/api", func(ctx cordis.Context, _ struct{}) error {
		database, err := ctx.Require(dbKey)
		if err != nil {
			return err
		}
		_, err = ctx.Provide(apiKey, &api{db: database})
		return err
	}).AddProvides(module.Provides(apiKey)).AddRequires(module.Requires(dbKey))
}

func parentModule() *module.Descriptor[struct{}] {
	return module.Define("test/parent", func(cordis.Context, struct{}) error { return nil })
}

func optionalConsumerModule() *module.Descriptor[struct{}] {
	return module.Define("test/optional-consumer", func(ctx cordis.Context, _ struct{}) error {
		_, optionalSeen = ctx.Get(optKey)
		return nil
	}).AddRequires(module.RequiresOptional(optKey))
}

// Providing optionalService makes optionalSeen true for the consumer.
func optionalProviderModule() *module.Descriptor[struct{}] {
	return module.Define("test/optional-provider", func(ctx cordis.Context, _ struct{}) error {
		_, err := ctx.Provide(optKey, &optService{})
		return err
	}).AddProvides(module.Provides(optKey))
}

func failingModule(id string) *module.Descriptor[struct{}] {
	return module.Define(id, func(cordis.Context, struct{}) error {
		return cordis.PluginError(id, "startup", "exploded on purpose")
	})
}

func cycleAModule() *module.Descriptor[struct{}] {
	return module.Define("test/cycle-a", func(ctx cordis.Context, _ struct{}) error {
		_, err := ctx.Provide(cycleAKey, &cycleA{})
		return err
	}).AddProvides(module.Provides(cycleAKey)).AddRequires(module.Requires(cycleBKey))
}

func cycleBModule() *module.Descriptor[struct{}] {
	return module.Define("test/cycle-b", func(ctx cordis.Context, _ struct{}) error {
		_, err := ctx.Provide(cycleBKey, &cycleB{})
		return err
	}).AddProvides(module.Provides(cycleBKey)).AddRequires(module.Requires(cycleAKey))
}

func duplicateProviderModule(id string) *module.Descriptor[struct{}] {
	return module.Define(id, func(ctx cordis.Context, _ struct{}) error {
		_, err := ctx.Provide(dbKey, &db{dsn: id})
		return err
	}).AddProvides(module.Provides(dbKey))
}

func wrongTypeProviderModule() *module.Descriptor[struct{}] {
	return module.Define("test/wrong-type-db", func(cordis.Context, struct{}) error { return nil }).
		AddProvides(module.Provides(otherDBKey))
}

// lyingModule declares a provided service in its manifest but never provides it.
func lyingModule() *module.Descriptor[struct{}] {
	return module.Define("test/lying", func(cordis.Context, struct{}) error { return nil }).
		AddProvides(module.Provides(dbKey))
}

func newRegistry(t *testing.T) *module.Registry {
	t.Helper()
	registry := module.NewRegistry()
	register := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	register(registry.Register(dbModule()))
	register(registry.Register(apiModule()))
	register(registry.Register(parentModule()))
	register(registry.Register(optionalConsumerModule()))
	register(registry.Register(optionalProviderModule()))
	register(registry.Register(failingModule("test/failing")))
	register(registry.Register(cycleAModule()))
	register(registry.Register(cycleBModule()))
	register(registry.Register(duplicateProviderModule("test/dup-a")))
	register(registry.Register(duplicateProviderModule("test/dup-b")))
	register(registry.Register(wrongTypeProviderModule()))
	register(registry.Register(lyingModule()))
	return registry
}

func newRoot(t *testing.T) cordis.Context {
	t.Helper()
	root := cordis.Root()
	t.Cleanup(func() {
		defer func() { _ = recover() }()
		_ = root.DisposeAll()
	})
	return root
}

func load(t *testing.T, root cordis.Context, registry *module.Registry, entries []module.Entry) *module.Loader {
	t.Helper()
	loader, err := module.Load(root, registry, entries)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	t.Cleanup(func() { _ = loader.Close() })
	return loader
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

func TestRegistryRegisterInspectAndDuplicates(t *testing.T) {
	registry := newRegistry(t)
	if !registry.Has("test/db") {
		t.Fatal("registered module missing")
	}
	info, ok := registry.Module("test/db")
	if !ok || info.Name == "" || len(info.Provides) != 1 || len(info.Requires) != 1 {
		t.Fatalf("module info = %+v/%v", info, ok)
	}
	if got := len(registry.Modules()); got != 12 {
		t.Fatalf("module count = %d, want 12", got)
	}
	// Duplicate registration is an ordinary error, not a panic.
	if err := registry.Register(dbModule()); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("duplicate register err = %v", err)
	}
	// Invalid descriptors are rejected at registration time.
	bad := module.Define("", func(cordis.Context, struct{}) error { return nil })
	if err := registry.Register(bad); err == nil {
		t.Fatal("empty-ID descriptor should be rejected")
	}
	noFactory := module.Define[struct{}]("test/no-factory", nil)
	if err := registry.Register(noFactory); err == nil {
		t.Fatal("nil factory should be rejected")
	}
	// Go 1.27 generic method: T is inferred from the key.
	registry.AddBuiltin(cordis.NewServiceKey[*db]("test/db-builtin"))
	if got := len(registry.Builtins()); got != 2 {
		t.Fatalf("builtin count = %d, want 2 (logger + added)", got)
	}
}

// ---------------------------------------------------------------------------
// Happy path: typed config, nested tree, typed services
// ---------------------------------------------------------------------------

func TestLoadAssembly(t *testing.T) {
	root := newRoot(t)
	registry := newRegistry(t)
	loader := load(t, root, registry, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "mem://", "pool": 3}},
		{ID: "app", Module: "test/parent", Children: []module.Entry{
			{ID: "http-api", Module: "test/api"},
		}},
	})

	for _, id := range []string{"database", "app", "http-api"} {
		instance, ok := loader.Instance(id)
		if !ok {
			t.Fatalf("instance %q missing", id)
		}
		if instance.State() != cordis.Active {
			t.Fatalf("instance %q state = %v, want Active", id, instance.State())
		}
	}
	if loader.Warnings() == nil {
		t.Fatal("warnings should be an empty slice, not nil")
	}
	database, ok := root.Get(dbKey)
	if !ok || database.dsn != "mem://" || database.pool != 3 {
		t.Fatalf("typed config was not decoded into the module: %+v/%v", database, ok)
	}
	apiService, ok := root.Get(apiKey)
	if !ok || apiService.db != database {
		t.Fatalf("injected service identity mismatch: %+v/%v", apiService, ok)
	}
	// Nested instance's parent link and locate work.
	apiInstance, _ := loader.Instance("http-api")
	parent, _ := loader.Instance("app")
	if apiInstance.Parent() != parent {
		t.Fatal("nested instance parent link is wrong")
	}
	if located, ok := loader.Locate(apiInstance.Fiber()); !ok || located != apiInstance {
		t.Fatal("Locate did not find the instance by fiber")
	}
	if ok := loader.Closed(); ok {
		t.Fatal("loader should be open")
	}
	if err := loader.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if apiInstance.State() != cordis.Disposed {
		t.Fatalf("nested instance after close = %v, want Disposed", apiInstance.State())
	}
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("fiber count after close = %d, want 0", got)
	}
}

func TestLoadIsOrderIndependent(t *testing.T) {
	root := newRoot(t)
	registry := newRegistry(t)
	// The dependent loads before its provider: the kernel leaves it Pending and
	// activates it when the provider appears (reactive inject gate).
	loader := load(t, root, registry, []module.Entry{
		{ID: "http-api", Module: "test/api"},
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "late://"}},
	})
	for _, id := range []string{"http-api", "database"} {
		instance, _ := loader.Instance(id)
		if instance.State() != cordis.Active {
			t.Fatalf("instance %q state = %v, want Active", id, instance.State())
		}
	}
}

// ---------------------------------------------------------------------------
// Pre-flight validation
// ---------------------------------------------------------------------------

func validateErr(t *testing.T, registry *module.Registry, entries []module.Entry) string {
	t.Helper()
	_, err := registry.Validate(entries)
	if err == nil {
		t.Fatal("expected a validation error")
	}
	return err.Error()
}

func TestValidateMissingProvider(t *testing.T) {
	registry := newRegistry(t)
	message := validateErr(t, registry, []module.Entry{{ID: "http-api", Module: "test/api"}})
	for _, want := range []string{"requires service", "test/db", "http-api"} {
		if !strings.Contains(message, want) {
			t.Fatalf("validation message %q does not mention %q", message, want)
		}
	}
}

func TestValidateUnknownModuleAndDuplicateID(t *testing.T) {
	registry := newRegistry(t)
	message := validateErr(t, registry, []module.Entry{{ID: "x", Module: "not/registered"}})
	if !strings.Contains(message, "unknown module") {
		t.Fatalf("message = %q", message)
	}
	message = validateErr(t, registry, []module.Entry{
		{ID: "same", Module: "test/parent"},
		{ID: "same", Module: "test/parent"},
	})
	if !strings.Contains(message, "duplicate instance ID") {
		t.Fatalf("message = %q", message)
	}
}

func TestValidateDisabledProviderIsMissing(t *testing.T) {
	registry := newRegistry(t)
	_, err := registry.Validate([]module.Entry{
		{ID: "database", Module: "test/db", Disabled: true},
		{ID: "http-api", Module: "test/api"},
	})
	if err == nil || !strings.Contains(err.Error(), "test/db") {
		t.Fatalf("err = %v", err)
	}
}

func TestValidateDuplicateProviders(t *testing.T) {
	registry := newRegistry(t)
	message := validateErr(t, registry, []module.Entry{
		{ID: "a", Module: "test/dup-a"},
		{ID: "b", Module: "test/dup-b"},
	})
	if !strings.Contains(message, "provided both by") {
		t.Fatalf("message = %q", message)
	}
}

func TestValidateTypeMismatch(t *testing.T) {
	registry := newRegistry(t)
	message := validateErr(t, registry, []module.Entry{
		{ID: "wrong", Module: "test/wrong-type-db"},
		{ID: "http-api", Module: "test/api"},
	})
	if !strings.Contains(message, "incompatible contracts") {
		t.Fatalf("message = %q", message)
	}
}

func TestValidateCycle(t *testing.T) {
	registry := newRegistry(t)
	message := validateErr(t, registry, []module.Entry{
		{ID: "a", Module: "test/cycle-a"},
		{ID: "b", Module: "test/cycle-b"},
	})
	if !strings.Contains(message, "cycle") {
		t.Fatalf("message = %q", message)
	}
}

func TestValidateConfigDecodeAndHook(t *testing.T) {
	registry := newRegistry(t)
	message := validateErr(t, registry, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": 42}},
	})
	if !strings.Contains(message, "invalid config") {
		t.Fatalf("decode message = %q", message)
	}
	message = validateErr(t, registry, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{}},
	})
	if !strings.Contains(message, "dsn is required") {
		t.Fatalf("validate message = %q", message)
	}
}

// ---------------------------------------------------------------------------
// Optional requirements and optional entries
// ---------------------------------------------------------------------------

func TestOptionalRequirementMissingAndPresent(t *testing.T) {
	root := newRoot(t)
	registry := newRegistry(t)
	optionalSeen = false
	load(t, root, registry, []module.Entry{{ID: "optional", Module: "test/optional-consumer"}})
	if optionalSeen {
		t.Fatal("optional service should be absent")
	}

	root2 := newRoot(t)
	optionalSeen = false
	load(t, root2, registry, []module.Entry{
		{ID: "optional", Module: "test/optional-consumer"},
		{ID: "provider", Module: "test/optional-provider"},
	})
	if !optionalSeen {
		t.Fatal("optional service should be present when a provider is loaded")
	}
}

func TestOptionalEntryFailureIsKept(t *testing.T) {
	root := newRoot(t)
	registry := newRegistry(t)
	loader := load(t, root, registry, []module.Entry{
		{ID: "optional-failure", Module: "test/failing", Optional: true},
	})
	instance, ok := loader.Instance("optional-failure")
	if !ok {
		t.Fatal("optional failed instance should be kept")
	}
	if instance.State() != cordis.Failed {
		t.Fatalf("state = %v, want Failed", instance.State())
	}
	if len(loader.Warnings()) == 0 {
		t.Fatal("optional failure should produce a warning")
	}
}

func TestRequiredEntryFailureRollsBack(t *testing.T) {
	root := newRoot(t)
	registry := newRegistry(t)
	loader, err := module.Load(root, registry, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "mem://"}},
		{ID: "failure", Module: "test/failing"},
	})
	if err == nil {
		_ = loader.Close()
		t.Fatal("required failure should fail the load")
	}
	var loadErr *module.LoadError
	if !errors.As(err, &loadErr) {
		t.Fatalf("err = %T %v, want *module.LoadError", err, err)
	}
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("fiber count after rollback = %d, want 0", got)
	}
	if !root.Registry().IsIdle() {
		t.Fatal("root should be idle after rollback")
	}
}

func TestManifestLiesAreDetected(t *testing.T) {
	root := newRoot(t)
	registry := newRegistry(t)
	_, err := module.Load(root, registry, []module.Entry{{ID: "lying", Module: "test/lying"}})
	if err == nil || !strings.Contains(err.Error(), "declares service") {
		t.Fatalf("err = %v", err)
	}
	if got := root.Registry().FiberCount(); got != 0 {
		t.Fatalf("fiber count after rollback = %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// Reload, reactive unload and introspection
// ---------------------------------------------------------------------------

func TestRecreateKeepsEntryID(t *testing.T) {
	root := newRoot(t)
	registry := newRegistry(t)
	loader := load(t, root, registry, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "mem://"}},
		{ID: "http-api", Module: "test/api"},
	})
	before, _ := loader.Instance("http-api")
	oldFiberID := before.Fiber().ID()
	if before.Fiber().State() != cordis.Active {
		t.Fatalf("state before reload = %v", before.Fiber().State())
	}
	after, err := loader.Recreate("http-api")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if after.ID() != "http-api" {
		t.Fatalf("entry ID changed: %q", after.ID())
	}
	if after.Fiber().ID() == oldFiberID {
		t.Fatal("reload should create a new fiber")
	}
	if after.State() != cordis.Active {
		t.Fatalf("state after reload = %v", after.State())
	}
	if before.Fiber().State() != cordis.Disposed {
		t.Fatalf("old fiber state = %v, want Disposed", before.Fiber().State())
	}
}

func TestReactiveProviderLossAndMissingServices(t *testing.T) {
	root := newRoot(t)
	registry := newRegistry(t)
	loader := load(t, root, registry, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "mem://"}},
		{ID: "http-api", Module: "test/api"},
	})
	database, _ := loader.Instance("database")
	api, _ := loader.Instance("http-api")
	// Stopping the provider at runtime unloads the dependent through the kernel's
	// space-composability rule instead of leaving a broken service binding.
	acceptStopped := func() bool {
		return database.Fiber().State() == cordis.Disposed && api.State() == cordis.Pending
	}
	if err := database.Fiber().Dispose(); err != nil {
		t.Fatalf("dispose provider: %v", err)
	}
	waitFor(t, acceptStopped, "dependent unloaded after provider disposal")
	missing := loader.MissingServices("http-api")
	if len(missing) != 1 || missing[0] != "test/db" {
		t.Fatalf("missing services = %v", missing)
	}
}

// ---------------------------------------------------------------------------
// Builtins and isolation between hosts
// ---------------------------------------------------------------------------

func TestUndeclaredBuiltinFailsFast(t *testing.T) {
	root := newRoot(t)
	registry := module.NewRegistry()
	customProviderKey := cordis.NewServiceKey[*db]("test/custom-builtin")
	registry.AddBuiltin(customProviderKey)
	if _, err := module.Load(root, registry, nil); err == nil || !strings.Contains(err.Error(), "builtin") {
		t.Fatalf("err = %v, want missing builtin failure", err)
	}
}

func TestTwoHostsAreIndependent(t *testing.T) {
	rootA := newRoot(t)
	rootB := newRoot(t)
	registryA := newRegistry(t)
	registryB := newRegistry(t)
	loaderA := load(t, rootA, registryA, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "a://"}},
	})
	loaderB := load(t, rootB, registryB, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "b://"}},
	})
	dbA, _ := rootA.Get(dbKey)
	dbB, _ := rootB.Get(dbKey)
	if dbA.dsn != "a://" || dbB.dsn != "b://" {
		t.Fatalf("independent hosts leaked state: %q vs %q", dbA.dsn, dbB.dsn)
	}
	if len(loaderA.Instances()) != 1 || len(loaderB.Instances()) != 1 {
		t.Fatal("instance bookkeeping is not isolated")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// State-preserving config update and reload
// ---------------------------------------------------------------------------

func TestUpdateInPlacePreservesState(t *testing.T) {
	resetStateful()
	root := newRoot(t)
	registry := newStatefulRegistry(t)
	loader := load(t, root, registry, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 1}},
		{ID: "dependent", Module: "test/stateful-dependent"},
	})
	service, ok := root.Get(counterKey)
	if !ok || service.step != 1 {
		t.Fatalf("service = %+v/%v", service, ok)
	}
	service.value = 7
	counterInstance, _ := loader.Instance("counter")
	dependentInstance, _ := loader.Instance("dependent")
	counterFiberID := counterInstance.Fiber().ID()
	dependentFiberID := dependentInstance.Fiber().ID()
	if stateful.activates != 1 || dependentState.activates != 1 {
		t.Fatalf("activation counts = %d/%d", stateful.activates, dependentState.activates)
	}

	if err := loader.Update("counter", map[string]cordis.Value{"step": 5}); err != nil {
		t.Fatalf("update: %v", err)
	}
	updated, _ := root.Get(counterKey)
	if updated != service {
		t.Fatal("in-place update must keep the same service handle")
	}
	if updated.step != 5 || updated.value != 7 {
		t.Fatalf("counter after update = %+v, want step 5 and state value 7", updated)
	}
	if stateful.updates != 1 {
		t.Fatalf("update hook calls = %d", stateful.updates)
	}
	if stateful.activates != 1 || dependentState.activates != 1 {
		t.Fatalf("in-place update re-activated a fiber: %d/%d", stateful.activates, dependentState.activates)
	}
	if counterInstance.Fiber().ID() != counterFiberID || dependentInstance.Fiber().ID() != dependentFiberID {
		t.Fatal("in-place update must not recreate fibers")
	}
	if counterInstance.State() != cordis.Active || dependentInstance.State() != cordis.Active {
		t.Fatal("in-place update must keep instances active")
	}
}

func TestUpdateUnsupportedAndInvalidConfig(t *testing.T) {
	resetStateful()
	root := newRoot(t)
	registry := newStatefulRegistry(t)
	loader := load(t, root, registry, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 2}},
		{ID: "dependent", Module: "test/stateful-dependent"},
	})
	// The dependent declares no Update hook: the loader must not silently restart.
	err := loader.Update("dependent", nil)
	if !errors.Is(err, module.ErrUpdateUnsupported) {
		t.Fatalf("unsupported update err = %v, want ErrUpdateUnsupported", err)
	}
	dependent, _ := loader.Instance("dependent")
	if dependent.State() != cordis.Active || dependentState.activates != 1 {
		t.Fatal("unsupported update must leave the instance untouched")
	}
	// Schema/validation failure keeps the old config and running state.
	err = loader.Update("counter", map[string]cordis.Value{"step": 0})
	if err == nil || !strings.Contains(err.Error(), "step must be positive") {
		t.Fatalf("invalid update err = %v", err)
	}
	service, _ := root.Get(counterKey)
	if service.step != 2 || stateful.updates != 0 {
		t.Fatalf("invalid update mutated state: step=%d updates=%d", service.step, stateful.updates)
	}
}

func TestReloadPreservesStateAcrossDependentClosure(t *testing.T) {
	resetStateful()
	root := newRoot(t)
	registry := newStatefulRegistry(t)
	loader := load(t, root, registry, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 1}},
		{ID: "dependent", Module: "test/stateful-dependent"},
	})
	counterService, _ := root.Get(counterKey)
	counterService.value = 42
	dependentState.value = 99
	counterInstance, _ := loader.Instance("counter")
	dependentInstance, _ := loader.Instance("dependent")
	oldCounterFiber := counterInstance.Fiber().ID()
	oldDependentFiber := dependentInstance.Fiber().ID()

	reloaded, err := loader.Reload("counter")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.ID() != "counter" || reloaded.State() != cordis.Active {
		t.Fatalf("reloaded instance = %q/%v", reloaded.ID(), reloaded.State())
	}
	if reloaded.Fiber().ID() == oldCounterFiber {
		t.Fatal("reload should create a new fiber")
	}
	if old := counterInstance.State(); old != cordis.Disposed {
		t.Fatalf("old provider fiber = %v, want Disposed", old)
	}
	if stateful.snapshots != 1 || stateful.restores != 1 {
		t.Fatalf("provider snapshot/restore counts = %d/%d, want 1/1", stateful.snapshots, stateful.restores)
	}
	if dependentState.restores != 1 {
		t.Fatalf("dependent restore count = %d, want 1 (closure must include the dependent)", dependentState.restores)
	}
	newCounter, ok := root.Get(counterKey)
	if !ok || newCounter.value != 42 {
		t.Fatalf("provider state not restored: %+v/%v", newCounter, ok)
	}
	newDependent, _ := loader.Instance("dependent")
	if newDependent.State() != cordis.Active {
		t.Fatalf("dependent state after reload = %v", newDependent.State())
	}
	if newDependent.Fiber().ID() == oldDependentFiber {
		t.Fatal("dependent in the reload closure should also be recreated")
	}
	if dependentState.restores != 1 || dependentState.value != 99 {
		t.Fatalf("dependent state not restored: restores=%d value=%d", dependentState.restores, dependentState.value)
	}
	if dependentState.activates != 2 {
		t.Fatalf("dependent activations = %d, want 2 (initial + closure reload)", dependentState.activates)
	}
}

func TestReloadRequiresStateHooks(t *testing.T) {
	resetStateful()
	root := newRoot(t)
	registry := newStatefulRegistry(t)
	loader := load(t, root, registry, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 1}},
		{ID: "dependent", Module: "test/plain-dependent"},
	})
	counterInstance, _ := loader.Instance("counter")
	dependentInstance, _ := loader.Instance("dependent")
	oldCounterFiber := counterInstance.Fiber().ID()
	oldDependentFiber := dependentInstance.Fiber().ID()
	_, err := loader.Reload("counter")
	if !errors.Is(err, module.ErrReloadNotStateful) {
		t.Fatalf("reload err = %v, want ErrReloadNotStateful", err)
	}
	// A rejected reload must be a no-op: old tree untouched.
	if counterInstance.State() != cordis.Active || dependentInstance.State() != cordis.Active {
		t.Fatal("rejected reload must leave instances active")
	}
	if counterInstance.Fiber().ID() != oldCounterFiber || dependentInstance.Fiber().ID() != oldDependentFiber {
		t.Fatal("rejected reload must not recreate fibers")
	}
}

func TestReloadSnapshotFailureLeavesTreeIntact(t *testing.T) {
	resetStateful()
	root := newRoot(t)
	registry := newStatefulRegistry(t)
	loader := load(t, root, registry, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 1}},
		{ID: "dependent", Module: "test/stateful-dependent"},
	})
	counterInstance, _ := loader.Instance("counter")
	oldFiber := counterInstance.Fiber().ID()
	stateful.failSnapshot = true
	_, err := loader.Reload("counter")
	if err == nil || !strings.Contains(err.Error(), "snapshot") {
		t.Fatalf("reload err = %v, want snapshot failure", err)
	}
	if counterInstance.State() != cordis.Active || counterInstance.Fiber().ID() != oldFiber {
		t.Fatal("snapshot failure must leave the old tree untouched")
	}
}

func waitFor(t *testing.T, condition func() bool, what string) {
	t.Helper()
	for i := 0; i < 300; i++ {
		if condition() {
			return
		}
		cordis.YieldNow()
	}
	t.Fatalf("condition not reached: %s", what)
}
