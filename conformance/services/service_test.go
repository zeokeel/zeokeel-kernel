package services_test

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cordis "github.com/zeokeel/cordis-go"
)

// ---------------------------------------------------------------------------
// CORE-002: provider registrations are visible across sibling fibers; removing
// the provider sends the dependent back to Pending.
// ---------------------------------------------------------------------------

func TestCore002_ServiceVisibleAcrossSiblingFibers(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("sibling-service")
	consumerHits := 0

	consumer, err := root.Plugin(&funcPlugin{
		name:   "core002-consumer",
		inject: []string{key.Name()},
		activate: func(ctx cordis.Context, _ cordis.Value) error {
			consumerHits++
			if got, ok := cordis.Get(ctx, key); !ok || got != "from-sibling" {
				return cordis.PluginFailedError("injected service value mismatch")
			}
			return nil
		},
	}, nil)
	mustNoError(t, err, "load consumer")
	needState(t, consumer, cordis.Pending, "consumer before provider")
	if consumerHits != 0 {
		t.Fatalf("consumer apply ran while Pending: hits = %d", consumerHits)
	}

	provider, err := root.Plugin(&funcPlugin{
		name: "core002-provider",
		activate: func(ctx cordis.Context, _ cordis.Value) error {
			_, err := cordis.Provide(ctx, key, "from-sibling")
			return err
		},
	}, nil)
	mustNoError(t, err, "load provider")

	needState(t, consumer, cordis.Active, "consumer after provider activated")
	if consumerHits != 1 {
		t.Fatalf("consumer apply hits = %d, want 1", consumerHits)
	}
	if got, ok := cordis.Get(root, key); !ok || got != "from-sibling" {
		t.Fatalf("root.Get = %q/%v, want %q", got, ok, "from-sibling")
	}

	// Unloading the provider must withdraw the binding and send the dependent
	// back to Pending (not Failed/Disposed).
	mustNoError(t, provider.Dispose(), "dispose provider")
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("service still visible after provider dispose")
	}
	needState(t, consumer, cordis.Pending, "consumer after provider dispose")
}

// ---------------------------------------------------------------------------
// CORE-004: duplicate provide in one scope is rejected with DuplicateService
// carrying the exact name; an isolate has an independent namespace.
// ---------------------------------------------------------------------------

func TestCore004_DuplicateProvideRejectedIsolateShadows(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("greeter")

	if _, err := cordis.Provide(root, key, "first"); err != nil {
		t.Fatalf("first provide: %v", err)
	}
	_, err := cordis.Provide(root, key, "second")
	if !errors.Is(err, cordis.ErrDuplicateService) {
		t.Fatalf("duplicate provide error = %v, want ErrDuplicateService", err)
	}
	ce := requireCode(t, err, cordis.CodeDuplicateService)
	if ce.Name() != "greeter" {
		t.Fatalf("duplicate service error name = %q, want %q", ce.Name(), "greeter")
	}

	// A fresh isolate scope is an independent namespace: same name is legal and
	// shadows the default scope.
	scoped := root.Isolate("greeter")
	scopedDisposer, err := cordis.Provide(scoped, key, "scoped-value")
	mustNoError(t, err, "provide in isolate")
	if got, ok := cordis.Get(scoped, key); !ok || got != "scoped-value" {
		t.Fatalf("isolated Get = %q/%v, want %q", got, ok, "scoped-value")
	}
	if got, ok := cordis.Get(root, key); !ok || got != "first" {
		t.Fatalf("root Get = %q/%v, want %q", got, ok, "first")
	}
	scopedDisposer.Dispose()
}

// ---------------------------------------------------------------------------
// CORE-005: a missing inject keeps the fiber Pending and never runs apply; the
// service appearing activates it exactly once.
// ---------------------------------------------------------------------------

func TestCore005_InjectGatePendingNeverApplies(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("greeting")
	hits := 0

	fiber, err := root.Plugin(&funcPlugin{
		name:   "core005-consumer",
		inject: []string{key.Name()},
		activate: func(ctx cordis.Context, _ cordis.Value) error {
			hits++
			return nil
		},
	}, nil)
	mustNoError(t, err, "load consumer")
	needState(t, fiber, cordis.Pending, "consumer without its dependency")
	if hits != 0 {
		t.Fatalf("Pending fiber ran apply: hits = %d", hits)
	}

	disposer, err := cordis.Provide(root, key, "hello")
	mustNoError(t, err, "provide greeting")
	needState(t, fiber, cordis.Active, "consumer after provide")
	if hits != 1 {
		t.Fatalf("apply hits = %d, want 1", hits)
	}
	if got, err := cordis.Require(root, key); err != nil || got != "hello" {
		t.Fatalf("Require = %q/%v, want %q/nil", got, err, "hello")
	}

	// The service is provided by the root, so disposing the consumer leaves it.
	mustNoError(t, fiber.Dispose(), "dispose consumer")
	needState(t, fiber, cordis.Disposed, "consumer after dispose")
	if got, ok := cordis.Get(root, key); !ok || got != "hello" {
		t.Fatalf("root.Get after consumer dispose = %q/%v", got, ok)
	}

	disposer.Dispose()
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("service still visible after provider disposer")
	}
}

// ---------------------------------------------------------------------------
// CORE-006: availability-check flips gate dependents (activate/unload), each
// reactivation re-runs apply.
// ---------------------------------------------------------------------------

func TestCore006_ServiceGateFlipsDependents(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("gated")
	gate := cordis.NewServiceGate(false)
	if _, err := cordis.ProvideGated(root, key, "gated-value", gate); err != nil {
		t.Fatalf("provide gated: %v", err)
	}

	hits := 0
	dependent, err := root.Plugin(&funcPlugin{
		name:   "core006-dependent",
		inject: []string{key.Name()},
		activate: func(ctx cordis.Context, _ cordis.Value) error {
			hits++
			return nil
		},
	}, nil)
	mustNoError(t, err, "load dependent")
	needState(t, dependent, cordis.Pending, "dependent while gate closed")
	if hits != 0 {
		t.Fatalf("dependent applied while gate closed: hits = %d", hits)
	}

	if !gate.Set(true) {
		t.Fatal("gate.Set(true) reported no change")
	}
	needState(t, dependent, cordis.Active, "dependent after gate opened")
	if hits != 1 {
		t.Fatalf("apply hits after open = %d, want 1", hits)
	}

	if !gate.Set(false) {
		t.Fatal("gate.Set(false) reported no change")
	}
	needState(t, dependent, cordis.Pending, "dependent after gate closed")

	if !gate.Set(true) {
		t.Fatal("gate.Set(true) reported no change on reopen")
	}
	needState(t, dependent, cordis.Active, "dependent after gate reopened")
	if hits != 2 {
		t.Fatalf("apply hits after reopen = %d, want 2 (re-apply per activation)", hits)
	}

	// Closing an already-closed gate is not a change.
	if gate.Set(true) {
		t.Fatal("gate.Set(true) on an open gate reported a change")
	}
}

// ---------------------------------------------------------------------------
// CORE-010: provider unload auto-unregisters; an explicit disposer removes the
// service and is idempotent.
// ---------------------------------------------------------------------------

func TestCore010_ProviderUnloadAutoUnregistersService(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("owned-service")
	var providerCtx cordis.Context

	provider, err := root.Plugin(&funcPlugin{
		name: "core010-provider",
		activate: func(ctx cordis.Context, _ cordis.Value) error {
			providerCtx = ctx
			_, err := cordis.Provide(ctx, key, "owned")
			return err
		},
	}, nil)
	mustNoError(t, err, "load provider")
	needState(t, provider, cordis.Active, "provider")
	if got, ok := cordis.Get(root, key); !ok || got != "owned" {
		t.Fatalf("root.Get before dispose = %q/%v", got, ok)
	}

	mustNoError(t, provider.Dispose(), "dispose provider")
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("service still visible after provider fiber dispose")
	}
	// A context captured by the unloaded plugin no longer resolves the service.
	if _, ok := cordis.Get(providerCtx, key); ok {
		t.Fatal("unloaded plugin context still resolves its own service")
	}

	// Explicit disposer path on the root.
	disposer, err := cordis.Provide(root, key, "explicit")
	mustNoError(t, err, "explicit provide")
	if got, ok := cordis.Get(root, key); !ok || got != "explicit" {
		t.Fatalf("root.Get after explicit provide = %q/%v", got, ok)
	}
	disposer.Dispose()
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("service still visible after explicit disposer")
	}
	disposer.Dispose() // single-shot: no panic, no second removal
	if !disposer.IsDisposed() {
		t.Fatal("disposer not marked disposed")
	}
}

// ---------------------------------------------------------------------------
// CORE-013: an availability predicate gates both lookups and injection; toggling
// it only takes effect after an explicit refresh.
// ---------------------------------------------------------------------------

func TestCore013_AvailabilityCheckGatesLookupsAndDependents(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("greeting")
	var open atomic.Bool

	disposer, err := cordis.ProvideChecked(root, key, "hi", func() bool { return open.Load() })
	mustNoError(t, err, "provide checked")

	// Closed predicate: reads require unknown-service errors and injection fails.
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("Get returned the service while its availability check was false")
	}
	if root.ServiceAvailable("greeting") {
		t.Fatal("ServiceAvailable = true while the check was false")
	}
	_, requireErr := cordis.Require(root, key)
	ce := requireCode(t, requireErr, cordis.CodeUnknownService)
	if ce.Name() != "greeting" {
		t.Fatalf("UnknownService name = %q, want %q", ce.Name(), "greeting")
	}

	hits := 0
	dependent, err := root.Plugin(&funcPlugin{
		name:   "core013-dependent",
		inject: []string{key.Name()},
		activate: func(cordis.Context, cordis.Value) error {
			hits++
			return nil
		},
	}, nil)
	mustNoError(t, err, "load dependent")
	needState(t, dependent, cordis.Pending, "dependent with the check closed")

	open.Store(true)
	root.Refresh()
	needState(t, dependent, cordis.Active, "dependent after check opened + refresh")
	if hits != 1 {
		t.Fatalf("dependent apply hits = %d, want 1", hits)
	}
	if got, ok := cordis.Get(root, key); !ok || got != "hi" {
		t.Fatalf("Get after check opened = %q/%v, want %q", got, ok, "hi")
	}
	if !root.ServiceAvailable("greeting") {
		t.Fatal("ServiceAvailable = false after the check opened")
	}

	disposer.Dispose()
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("service still visible after disposer")
	}
	needState(t, dependent, cordis.Pending, "dependent after the provider was removed")
}

// ---------------------------------------------------------------------------
// CORE-014: a closed gate in an isolate masks the ancestor provider instead of
// falling back to it.
// ---------------------------------------------------------------------------

func TestCore014_ClosedGateDoesNotFallBackToAncestor(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("greeting")

	if _, err := cordis.Provide(root, key, "root"); err != nil {
		t.Fatalf("root provide: %v", err)
	}
	scoped := root.Isolate("greeting")
	gate := cordis.NewServiceGate(false)
	scopedDisposer, err := cordis.ProvideGated(scoped, key, "scoped", gate)
	mustNoError(t, err, "provide gated in isolate")

	// The isolate-local unavailable provider must shadow the ancestor provider:
	// no fallback to the default scope.
	if got, ok := cordis.Get(scoped, key); ok {
		t.Fatalf("isolated Get = %q while its gate is closed; must not fall back", got)
	}
	// The default scope keeps resolving its own provider.
	if got, ok := cordis.Get(root, key); !ok || got != "root" {
		t.Fatalf("root Get = %q/%v, want %q", got, ok, "root")
	}

	if !gate.Set(true) {
		t.Fatal("gate.Set(true) reported no change")
	}
	if got, ok := cordis.Get(scoped, key); !ok || got != "scoped" {
		t.Fatalf("isolated Get after opening = %q/%v, want %q", got, ok, "scoped")
	}
	if got, ok := cordis.Get(root, key); !ok || got != "root" {
		t.Fatalf("root Get must stay %q, got %q/%v", "root", got, ok)
	}
	scopedDisposer.Dispose()
}

// ---------------------------------------------------------------------------
// CORE-015: the availability predicate runs outside the service-store lock and
// may re-enter service lookups. A concurrent lookup must not deadlock.
// ---------------------------------------------------------------------------

func TestCore015_AvailabilityCheckMayReadOtherServices(t *testing.T) {
	root := newRoot(t)
	innerKey := cordis.NewServiceKey[string]("probe.inner")
	outerKey := cordis.NewServiceKey[string]("probe.outer")

	if _, err := cordis.Provide(root, innerKey, "inner"); err != nil {
		t.Fatalf("provide inner: %v", err)
	}
	var checks atomic.Int64
	if _, err := cordis.ProvideChecked(root, outerKey, "outer", func() bool {
		checks.Add(1)
		// Re-entrant read of another service while the check is running.
		if _, ok := cordis.Get(root, innerKey); !ok {
			return false
		}
		return root.ServiceAvailable(innerKey.Name())
	}); err != nil {
		t.Fatalf("provide checked outer: %v", err)
	}

	const workers = 2
	const iterations = 50
	errCh := make(chan error, workers)
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					value, ok := cordis.Get(root, outerKey)
					if !ok || value != "outer" {
						select {
						case errCh <- errors.New("concurrent lookup failed"):
						default:
						}
						return
					}
				}
			}()
		}
		wg.Wait()
		close(done)
	}()

	waitClosed(t, done, 2*time.Second, "concurrent availability-check lookups (deadlock guard)")
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent lookup: %v", err)
	}
	if checks.Load() == 0 {
		t.Fatal("availability check never ran")
	}
}

// ---------------------------------------------------------------------------
// CORE-032: interface-typed service keys (the trait-object lane) round-trip a
// dynamic implementation and disappear with their provider.
// ---------------------------------------------------------------------------

type core032Greeter interface {
	Greet() string
}

type core032English struct{}

func (core032English) Greet() string { return "hello" }

func TestCore032_InterfaceTypedServiceKey(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[core032Greeter]("trait.greeter")

	provider, err := root.Plugin(&funcPlugin{
		name: "core032-provider",
		activate: func(ctx cordis.Context, _ cordis.Value) error {
			_, err := cordis.Provide(ctx, key, core032Greeter(core032English{}))
			return err
		},
	}, nil)
	mustNoError(t, err, "load provider")
	needState(t, provider, cordis.Active, "provider")

	greeter, err := cordis.Require(root, key)
	mustNoError(t, err, "require interface service")
	if got := greeter.Greet(); got != "hello" {
		t.Fatalf("greet = %q, want %q", got, "hello")
	}
	if got, ok := cordis.Get(root, key); !ok || got.Greet() != "hello" {
		t.Fatalf("Get interface service = %v/%v", got, ok)
	}

	mustNoError(t, provider.Dispose(), "dispose provider")
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("interface service still visible after provider dispose")
	}
}

// ---------------------------------------------------------------------------
// CORE-033: Go analogy of "the root context/fiber graph has no strong cycle" -
// dropping the root after DisposeAll releases the whole plugin/fiber graph (Go
// GC collects reference cycles; the observable contract is release, not refcount).
// See docs/analysis/05-core-cases.md §5.1.
// ---------------------------------------------------------------------------

type core033Plugin struct{}

func (*core033Plugin) Name() string { return "core033-plugin" }
func (*core033Plugin) Activate(cordis.Context, cordis.Value) error {
	return nil
}

// core033Probe loads one plugin into a fresh root, disposes the tree and returns
// only a channel closed by the plugin's finalizer; every strong reference to the
// graph must be gone once the probe returns.
func core033Probe() <-chan struct{} {
	finalized := make(chan struct{})
	root := cordis.Root()
	plugin := &core033Plugin{}
	runtime.SetFinalizer(plugin, func(*core033Plugin) { close(finalized) })
	if _, err := root.Plugin(plugin, nil); err != nil {
		panic("core033: load plugin: " + err.Error())
	}
	if err := root.DisposeAll(); err != nil {
		panic("core033: dispose all: " + err.Error())
	}
	return finalized
}

func TestCore033_RootGraphReleasedWhenDropped(t *testing.T) {
	released := core033Probe()

	deadline := time.Now().Add(10 * time.Second)
	for {
		runtime.GC()
		select {
		case <-released:
			return // the graph was collectable: no strong references leak
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("CORE-033: the disposed root graph still strongly references its plugin after GC; graph is not releasable")
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// CORE-041 (part 1): ctx.set is a classified, versioned write. Changing a
// service value re-evaluates dependents, which reactivate once per version.
// ---------------------------------------------------------------------------

type core041Provider struct {
	name    string
	key     cordis.ServiceKey[int]
	initial int
	setTo   int
	ctx     cordis.Context
}

func (p *core041Provider) Name() string { return p.name }
func (p *core041Provider) Activate(ctx cordis.Context, _ cordis.Value) error {
	p.ctx = ctx
	if _, err := cordis.Provide(ctx, p.key, p.initial); err != nil {
		return err
	}
	return ctx.SetNamed(p.key.Name(), p.setTo)
}

type core041Dependent struct {
	name    string
	key     cordis.ServiceKey[int]
	seen    []int
	applies int
}

func (p *core041Dependent) Name() string          { return p.name }
func (p *core041Dependent) Inject() []string      { return []string{p.key.Name()} }
func (p *core041Dependent) Activate(ctx cordis.Context, _ cordis.Value) error {
	value, ok := cordis.Get(ctx, p.key)
	if !ok {
		return cordis.PluginFailedError("injected service not visible")
	}
	p.applies++
	p.seen = append(p.seen, value)
	return nil
}

func TestCore041_SetVersionReactivatesDependents(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[int]("set.version")

	provider := &core041Provider{name: "core041-provider", key: key, initial: 1, setTo: 2}
	if _, err := root.Plugin(provider, nil); err != nil {
		t.Fatalf("load provider: %v", err)
	}

	dependent := &core041Dependent{name: "core041-dependent", key: key}
	dependentFiber, err := root.Plugin(dependent, nil)
	mustNoError(t, err, "load dependent")
	needState(t, dependentFiber, cordis.Active, "dependent")
	if dependent.applies != 1 {
		t.Fatalf("dependent applies = %d, want 1", dependent.applies)
	}
	// The first activation must observe the value written by the provider's own
	// ctx.set (1 -> 2 during apply), not the pre-set value.
	if len(dependent.seen) != 1 || dependent.seen[0] != 2 {
		t.Fatalf("dependent seen = %v, want [2]", dependent.seen)
	}

	// A version change from the provider context re-evaluates dependents.
	mustNoError(t, provider.ctx.SetNamed(key.Name(), 3), "set service version 3")
	needState(t, dependentFiber, cordis.Active, "dependent after set")
	if dependent.applies != 2 {
		t.Fatalf("dependent applies = %d, want 2", dependent.applies)
	}
	if len(dependent.seen) != 2 || dependent.seen[0] != 2 || dependent.seen[1] != 3 {
		t.Fatalf("dependent seen = %v, want [2 3]", dependent.seen)
	}
	if got, ok := cordis.Get(root, key); !ok || got != 3 {
		t.Fatalf("service value = %v/%v, want 3", got, ok)
	}
}

// ---------------------------------------------------------------------------
// CORE-041 (part 2): ctx.set classification errors keep the original value.
// ---------------------------------------------------------------------------

func TestCore041_SetClassifiedErrorsKeepOriginalValue(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[string]("set.svc")
	if _, err := cordis.Provide(root, key, "one"); err != nil {
		t.Fatalf("provide: %v", err)
	}

	// Type mismatch: same-name write with a different concrete type.
	err := root.SetNamed("set.svc", 42)
	ce := requireCode(t, err, cordis.CodeServiceTypeMismatch)
	if ce.Name() != "set.svc" {
		t.Fatalf("mismatch error name = %q, want %q", ce.Name(), "set.svc")
	}
	if got, ok := cordis.Get(root, key); !ok || got != "one" {
		t.Fatalf("value after rejected set = %q/%v, want %q", got, ok, "one")
	}

	// Unknown service name.
	requireCode(t, root.SetNamed("set.missing", "x"), cordis.CodeUnknownService)

	// A service owned by another fiber is not writable from the root context.
	foreignKey := cordis.NewServiceKey[string]("set.foreign")
	if _, err := root.Plugin(&funcPlugin{
		name: "core041-foreign-provider",
		activate: func(ctx cordis.Context, _ cordis.Value) error {
			_, err := cordis.Provide(ctx, foreignKey, "two")
			return err
		},
	}, nil); err != nil {
		t.Fatalf("load foreign provider: %v", err)
	}
	ce = requireCode(t, root.SetNamed("set.foreign", "hijack"), cordis.CodeServiceWriteForbidden)
	if ce.Name() != "set.foreign" {
		t.Fatalf("forbidden error name = %q, want %q", ce.Name(), "set.foreign")
	}
	if got, ok := cordis.Get(root, foreignKey); !ok || got != "two" {
		t.Fatalf("foreign service value after rejected set = %q/%v, want %q", got, ok, "two")
	}
}

// ---------------------------------------------------------------------------
// CORE-041 (part 3): revertible ctx.set. The Rust contract is
// `set_revertible` returning a Disposer whose disposal restores the previous
// entry; the Go port only exposes SetNamed (the inverse effect is registered
// internally but its handle is discarded), so the observable revert contract
// cannot be exercised through the public API. Reported as a kernel gap.
// ---------------------------------------------------------------------------

func TestCore041_SetRevertibleRestoresPreviousEntry(t *testing.T) {
	t.Skip("KERNEL-GAP CORE-041: set_revertible (semantics.rs:184 set_is_revertible_and_classified) returns a Disposer that restores the previous entry; Go exposes only Context.SetNamed, which drops the inverse Disposer handle, so disposal/restore is not observable via the public API")
}

// ---------------------------------------------------------------------------
// CORE-042: provider withdrawal happens before binding removal; the dependent's
// cleanup observes the service as withdrawn and runs before the provider's own
// cleanup.
// ---------------------------------------------------------------------------

func TestCore042_ProviderWithdrawalPrecedesBindingRemoval(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[int]("withdraw.svc")
	tr := &trace{}

	provider, err := root.Plugin(&funcPlugin{
		name: "core042-provider",
		activate: func(ctx cordis.Context, _ cordis.Value) error {
			if _, err := cordis.Provide(ctx, key, 1); err != nil {
				return err
			}
			_, err := ctx.Effect(traceEffect(tr, "provider-cleanup"))
			return err
		},
	}, nil)
	mustNoError(t, err, "load provider")

	dependent, err := root.Plugin(&funcPlugin{
		name:   "core042-dependent",
		inject: []string{key.Name()},
		activate: func(ctx cordis.Context, _ cordis.Value) error {
			_, err := ctx.Effect(func() cordis.Effect {
				return cordis.Effect{cordis.CleanupOf(func() {
					// During dependent cleanup the service must be withdrawn
					// (absent) even though its binding still exists.
					if _, ok := cordis.Get(ctx, key); ok {
						tr.add("dep-cleanup-live")
					} else {
						tr.add("dep-cleanup-withdrawn")
					}
				})}
			})
			return err
		},
	}, nil)
	mustNoError(t, err, "load dependent")
	needState(t, dependent, cordis.Active, "dependent before provider dispose")

	mustNoError(t, provider.Dispose(), "dispose provider")

	if got, want := tr.snapshot(), []string{"dep-cleanup-withdrawn", "provider-cleanup"}; !sameStrings(got, want) {
		t.Fatalf("cleanup trace = %v, want %v (dependents first, service already withdrawn)", got, want)
	}
	needState(t, dependent, cordis.Pending, "dependent after provider withdrawal")
	if _, ok := cordis.Get(root, key); ok {
		t.Fatal("withdrawn service still visible after provider dispose")
	}
}
