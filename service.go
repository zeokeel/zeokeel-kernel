package cordis

import (
	"reflect"
	"sync"
	"sync/atomic"
	"weak"
)

// ServiceValue is the type-erased value stored in the service store.
//
// In the Rust reference this is `Arc<dyn Any + Send + Sync>`; in Go the value
// is stored directly (usually a pointer or an interface value) and recovered
// with a type assertion. Provide pointer values for shared mutable state.
type ServiceValue = any

// ServiceKey is a typed handle to a service name.
//
// The name is what plugins declare in Inject; the Go type parameter makes
// Get/Provide type-safe at compile time (the kernel still verifies the stored
// value's dynamic type for Set writes, DESIGN I5).
//
// Because Go does not allow type parameters on methods, service access is
// provided by package-level generic functions:
//
//	key := cordis.NewServiceKey[*Config]("config")
//	cordis.Provide(ctx, key, cfg)
//	got, ok := cordis.Get(ctx, key)
type ServiceKey[T any] struct {
	name string
}

// NewServiceKey creates a typed service key.
func NewServiceKey[T any](name string) ServiceKey[T] {
	return ServiceKey[T]{name: name}
}

// Name returns the registered service name.
func (k ServiceKey[T]) Name() string { return k.name }

// ServiceType returns the dynamic type identity stored for T.
func ServiceType[T any]() reflect.Type { return reflect.TypeFor[T]() }

// AvailabilityCheck is an optional predicate gating a provided service
// (Cordis Service.check). Dependents only see the service while it returns
// true; reads honor it too. A panicking predicate is treated as false, so a
// broken check can never break dispatch. The predicate runs outside the service
// store lock and may re-enter the context.
type AvailabilityCheck func() bool

// serviceEntry is one provided service binding.
type serviceEntry struct {
	value  ServiceValue
	typ    reflect.Type
	check  AvailabilityCheck
	schema ConfigSchema
	// owner is the fiber id that provided the service (ctx.set ownership rule).
	owner uint64
	// version is a monotonic entry version; dependents reactivate when it
	// changes even if the service stays available.
	version uint64
	// withdrawn is set while the providing fiber is unloading so dependents
	// deactivate before the binding is actually removed. Guarded by the store
	// lock; entries are copied out before their availability is evaluated.
	withdrawn bool
	// provider is a weak handle to the providing fiber, used by the injection
	// gate (the provider must be Active). A nil/dropped provider counts as
	// available (root-only/hand-built stores keep working).
	provider weak.Pointer[fiberData]
}

// isAvailable reports non-strict availability (the read lane).
func (e *serviceEntry) isAvailable() bool {
	if e.withdrawn {
		return false
	}
	if e.check == nil {
		return true
	}
	return contained("cordis::service", "AvailabilityCheck", false, e.check)
}

// availableForInjection is the strict query used by the inject gate: stored,
// available, and the providing fiber is Active.
func (e *serviceEntry) availableForInjection() bool {
	if !e.isAvailable() {
		return false
	}
	return e.providerActive()
}

func (e *serviceEntry) providerActive() bool {
	provider := e.provider.Value()
	if provider == nil {
		return true
	}
	return provider.stateSnapshot() == Active
}

// serviceStoreKey identifies a service binding: (isolation scope id, name).
type serviceStoreKey struct {
	scope uint64
	name  string
}

// serviceStore is the shared service map of a context tree.
type serviceStore struct {
	mu      sync.Mutex
	entries map[serviceStoreKey]*serviceEntry
	// nextVersion allocates monotonic entry versions tree-wide.
	nextVersion atomic.Uint64
}

func newServiceStore() *serviceStore {
	return &serviceStore{entries: make(map[serviceStoreKey]*serviceEntry)}
}

func (s *serviceStore) newVersion() uint64 { return s.nextVersion.Add(1) }

// mangle keeps the scope/name pair in one comparable key.
func storeKey(scope uint64, name string) serviceStoreKey { return serviceStoreKey{scope: scope, name: name} }

// get returns the entry for key (nil when absent), without evaluating the
// availability check (callers must not hold the store lock while running user
// code).
func (s *serviceStore) get(key serviceStoreKey) (*serviceEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[key]
	return entry, ok
}

// ServiceGate is a shared availability toggle (Cordis Service.check).
//
// ProvideGated registers the gate as the service's availability check and
// remembers the providing context; flipping the gate re-evaluates that
// context's dependents, so plugins unload while the gate is closed and activate
// again when it reopens.
type ServiceGate struct {
	available atomic.Bool
	mu        sync.Mutex
	watchers  []weak.Pointer[contextInner]
}

// NewServiceGate creates a gate with an initial availability.
func NewServiceGate(available bool) *ServiceGate {
	gate := &ServiceGate{}
	gate.available.Store(available)
	return gate
}

// IsAvailable reports the current gate state.
func (g *ServiceGate) IsAvailable() bool { return g.available.Load() }

// Set flips availability. It returns true when the value changed, after
// re-evaluating every context that provided a service through this gate.
func (g *ServiceGate) Set(available bool) bool {
	if g.available.Swap(available) == available {
		return false
	}
	g.mu.Lock()
	watchers := g.watchers[:0]
	live := make([]Context, 0, len(g.watchers))
	for _, watcher := range g.watchers {
		inner := watcher.Value()
		if inner == nil {
			continue
		}
		watchers = append(watchers, watcher)
		live = append(live, Context{inner: inner})
	}
	g.watchers = watchers
	g.mu.Unlock()
	for _, c := range live {
		c.Refresh()
	}
	return true
}

// Check returns the predicate to hand to ProvideChecked.
func (g *ServiceGate) Check() AvailabilityCheck {
	return func() bool { return g.IsAvailable() }
}

func (g *ServiceGate) watch(c Context) {
	if c.inner == nil {
		return
	}
	g.mu.Lock()
	g.watchers = append(g.watchers, weak.Make(c.inner))
	g.mu.Unlock()
}

// Accessor is a computed context property (Cordis ctx.accessor).
//
// Its read is consulted before services by Get/GetNamed; it never satisfies an
// Inject dependency (accessors are read-lane only).
type Accessor struct {
	// Get computes the property value for a context.
	Get func(Context) ServiceValue
	// Set optionally writes the property; returning false rejects the write
	// with AccessorWriteRejected.
	Set func(Context, ServiceValue) bool
}

// NewAccessor creates a read-only computed property.
func NewAccessor(get func(Context) ServiceValue) Accessor { return Accessor{Get: get} }

// NewAccessorWithSetter creates a read/write computed property.
func NewAccessorWithSetter(get func(Context) ServiceValue, set func(Context, ServiceValue) bool) Accessor {
	return Accessor{Get: get, Set: set}
}

// read evaluates the accessor with panic containment: a panicking getter is
// treated as an absent property.
func (a Accessor) read(c Context) (ServiceValue, bool) {
	type result struct {
		value ServiceValue
		ok    bool
	}
	r := contained("cordis::reflect", "accessor get()", result{}, func() result {
		if a.Get == nil {
			return result{}
		}
		return result{value: a.Get(c), ok: true}
	})
	return r.value, r.ok
}

// write invokes the accessor setter with panic containment.
func (a Accessor) write(c Context, value ServiceValue) bool {
	if a.Set == nil {
		return false
	}
	return contained("cordis::reflect", "accessor set()", false, func() bool {
		return a.Set(c, value)
	})
}

// ContextMeta is diagnostic/extension metadata attached to a context child
// (Cordis ctx.extend(meta)). Entries never participate in service lookup.
type ContextMeta struct {
	entries map[string]any
}

// NewContextMeta creates an empty metadata bag.
func NewContextMeta() *ContextMeta { return &ContextMeta{entries: make(map[string]any)} }

// Insert stores a metadata entry.
func (m *ContextMeta) Insert(key string, value any) {
	if m.entries == nil {
		m.entries = make(map[string]any)
	}
	m.entries[key] = value
}

// Contains reports whether a key is present.
func (m *ContextMeta) Contains(key string) bool {
	if m == nil {
		return false
	}
	_, ok := m.entries[key]
	return ok
}

// Len returns the number of entries.
func (m *ContextMeta) Len() int {
	if m == nil {
		return 0
	}
	return len(m.entries)
}

// IsEmpty reports whether no entry was attached.
func (m *ContextMeta) IsEmpty() bool { return m.Len() == 0 }

func (m *ContextMeta) lookup(key string) (any, bool) {
	if m == nil {
		return nil, false
	}
	value, ok := m.entries[key]
	return value, ok
}

// Service is the optional interface for a value registered with ProvideService
// (Cordis `class X extends Service`).
//
// Rust services also expose optional check/schema/resolveConfig hooks; Go
// exposes them through optional interfaces so a minimal service only needs Name.
type Service interface {
	Name() string
}

// ServiceChecker is implemented by services that expose an availability check.
type ServiceChecker interface {
	Check() AvailabilityCheck
}

// ServiceSchemaProvider is implemented by services that declare a config schema.
type ServiceSchemaProvider interface {
	ConfigSchema() ConfigSchema
}

// ProvideService registers a Service instance in c under its Name.
//
// The returned disposer removes the registration; the registration is owned by
// the current fiber, so it unloads with the plugin that provided it. It returns
// a DuplicateService error when the name is already provided in the same
// isolation scope.
func ProvideService[S Service](c Context, service S) (*Disposer, error) {
	name := contained("cordis::service", "Service.Name()", "<panicked service>", service.Name)
	var check AvailabilityCheck
	if checker, ok := any(service).(ServiceChecker); ok {
		check = contained[AvailabilityCheck]("cordis::service", "Service.Check()", nil, checker.Check)
	}
	var schema ConfigSchema
	if provider, ok := any(service).(ServiceSchemaProvider); ok {
		schema = contained[ConfigSchema]("cordis::service", "Service.ConfigSchema()", nil, provider.ConfigSchema)
	}
	return c.provideUntyped(name, any(service), check, schema)
}
