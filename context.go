package cordis

import (
	"context"
	"errors"
	"reflect"
	"sync"
)

// Context is a scoped dependency container and plugin context (Cordis Context).
//
// Context values are cheap handles: copying a Context is safe (the Go
// counterpart of the Rust `Clone` on Arc<ContextInner>). Deriving a child with
// Extend/Isolate/Intercept/ExtendMeta never mutates the parent.
type Context struct {
	inner *contextInner
}

// contextInner is the shared state behind a Context handle.
type contextInner struct {
	parent   *contextInner
	fiber    *fiberData
	isolate  map[string]uint64
	intercepts map[string]Value
	events   *Events
	registry *Registry
	logger   *LoggerService
	store    *serviceStore
	meta     ContextMeta
	clock    *clockSlot

	accessorMu sync.Mutex
	accessors  map[string]Accessor
}

// clockSlot is the per-tree injectable clock (shared by every context derived
// from the same root).
type clockSlot struct {
	mu    sync.Mutex
	clock Clock
}

func (slot *clockSlot) get() Clock {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.clock == nil {
		return SystemClock{}
	}
	return slot.clock
}

func (slot *clockSlot) set(clock Clock) {
	slot.mu.Lock()
	slot.clock = clock
	slot.mu.Unlock()
}

// Root creates the root context with the root fiber (id 0) and the built-in
// logger service.
func Root() Context {
	events := NewEvents()
	registry := NewRegistry()
	logger := NewLoggerService(LogInfo)
	slot := &clockSlot{clock: SystemClock{}}
	rootFiber := &fiberData{
		id:         0,
		uid:        0,
		state:      Active,
		plugin:     pluginKind{sync: rootPlugin{}},
		pluginName: "root",
		registry:   registry,
		clock:      SystemClock{},
		hasRuntime: false,
		settleCh:   nil,
	}
	inner := &contextInner{
		fiber:    rootFiber,
		isolate:  map[string]uint64{},
		events:   events,
		registry: registry,
		logger:   logger,
		store:    newServiceStore(),
		accessors: map[string]Accessor{},
		clock:    slot,
	}
	rootFiber.ctx = inner
	// The root fiber's parent is itself (matches the Rust weak self-link; the Go
	// cycle is collected by the GC and no strong-cycle contract is needed).
	rootFiber.parent = inner
	registry.setRootFiber(rootFiber)
	ctx := Context{inner: inner}
	// Built-in logger service, owned by the root fiber (internal effect).
	if _, err := ctx.provideUntyped("logger", logger, nil, nil); err != nil {
		// The root context has just been constructed; a duplicate is impossible.
		panic("cordis: installing the root logger failed: " + err.Error())
	}
	return ctx
}

func (c Context) valid() bool { return c.inner != nil && c.inner.fiber != nil }

// Inner helpers (unexported; shared with the fiber machinery).

func (c Context) fiberData() *fiberData {
	if c.inner == nil {
		return nil
	}
	return c.inner.fiber
}

func (c Context) childWith(fiber *fiberData) Context {
	return Context{inner: &contextInner{
		parent:    c.inner,
		fiber:     fiber,
		isolate:   c.inner.isolate,
		events:    c.inner.events,
		registry:  c.inner.registry,
		logger:    c.inner.logger,
		store:     c.inner.store,
		accessors: map[string]Accessor{},
		clock:     c.inner.clock,
	}}
}

// Fiber returns the fiber handle owning this context.
func (c Context) Fiber() *Fiber {
	if !c.valid() {
		return nil
	}
	return &Fiber{data: c.inner.fiber}
}

// FiberID returns the id of the fiber owning this context (0 for the root).
func (c Context) FiberID() uint64 {
	if !c.valid() {
		return 0
	}
	return c.inner.fiber.id
}

// PluginName returns the display name of the plugin fiber owning this context.
func (c Context) PluginName() string {
	if !c.valid() {
		return ""
	}
	return c.inner.fiber.nameSnapshot()
}

// Registry returns the registry of this context tree.
func (c Context) Registry() *Registry {
	if !c.valid() {
		return nil
	}
	return c.inner.registry
}

// Events returns the event bus of this context tree.
func (c Context) Events() *Events {
	if !c.valid() {
		return nil
	}
	return c.inner.events
}

// Extend creates a child context that inherits services and the event bus.
func (c Context) Extend() Context {
	fiber := c.inner.fiber
	return c.childWith(fiber)
}

// Isolate creates a child whose service name is scoped independently. Every call
// creates a fresh scope; use IsolateLabeled to join a named scope.
func (c Context) Isolate(name string) Context {
	isolated := cloneScope(c.inner.isolate)
	isolated[name] = c.inner.registry.NextScopeID()
	return c.childWithIsolate(isolated)
}

// IsolateLabeled creates a child whose service name joins the scope of label.
func (c Context) IsolateLabeled(name, label string) Context {
	isolated := cloneScope(c.inner.isolate)
	isolated[name] = c.inner.registry.LabeledScopeID(label)
	return c.childWithIsolate(isolated)
}

func cloneScope(scope map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(scope)+1)
	for key, value := range scope {
		out[key] = value
	}
	return out
}

func (c Context) childWithIsolate(isolated map[string]uint64) Context {
	return Context{inner: &contextInner{
		parent:    c.inner,
		fiber:     c.inner.fiber,
		isolate:   isolated,
		events:    c.inner.events,
		registry:  c.inner.registry,
		logger:    c.inner.logger,
		store:     c.inner.store,
		accessors: map[string]Accessor{},
		clock:     c.inner.clock,
	}}
}

// Intercept adds config for name, used by ResolveConfig (Cordis ctx.intercept).
//
// Intercepts affect service configuration (the logger and Service implementations
// of ResolveConfig); plugin config goes through the internal/config waterfall and
// is not affected.
func (c Context) Intercept(name string, config Value) Context {
	return Context{inner: &contextInner{
		parent:    c.inner,
		fiber:     c.inner.fiber,
		isolate:   c.inner.isolate,
		intercepts: map[string]Value{name: config},
		events:    c.inner.events,
		registry:  c.inner.registry,
		logger:    c.inner.logger,
		store:     c.inner.store,
		accessors: map[string]Accessor{},
		clock:     c.inner.clock,
	}}
}

// ExtendMeta creates a child context carrying metadata (Cordis ctx.extend(meta)).
func (c Context) ExtendMeta(meta ContextMeta) Context {
	return Context{inner: &contextInner{
		parent:    c.inner,
		fiber:     c.inner.fiber,
		isolate:   c.inner.isolate,
		events:    c.inner.events,
		registry:  c.inner.registry,
		logger:    c.inner.logger,
		store:     c.inner.store,
		meta:      meta,
		accessors: map[string]Accessor{},
		clock:     c.inner.clock,
	}}
}

// serviceScope returns the isolation scope id for name (the nearest ancestor
// mapping wins; no fallback to an outer scope when a closer scope defines it).
func (c Context) serviceScope(name string) uint64 {
	for cursor := c.inner; cursor != nil; cursor = cursor.parent {
		if scope, ok := cursor.isolate[name]; ok {
			return scope
		}
	}
	return 0
}

// --- clock ---

// Clock returns the time source of this context tree.
func (c Context) Clock() Clock {
	if c.inner == nil || c.inner.clock == nil {
		return SystemClock{}
	}
	return c.inner.clock.get()
}

// SetClock installs the time source for this context tree.
//
// Only contexts owned by the root fiber (FiberID() == 0) may install it: every
// context derived from the root shares the same slot, so installing from any of
// them changes the whole tree. Install it once, right after Root, before loading
// plugins.
func (c Context) SetClock(clock Clock) error {
	if !c.valid() {
		return ErrDisposed
	}
	if c.FiberID() != 0 {
		return InvalidPluginError("clock must be set on the root context")
	}
	if clock == nil {
		clock = SystemClock{}
	}
	c.inner.clock.set(clock)
	c.inner.fiber.mu.Lock()
	c.inner.fiber.clock = clock
	c.inner.fiber.mu.Unlock()
	for _, data := range c.inner.registry.liveFibers() {
		data.mu.Lock()
		data.clock = clock
		data.mu.Unlock()
	}
	return nil
}

// --- services ---

// Provide registers a typed service. The returned disposer removes it; removing
// a service re-evaluates the fibers that injected its name.
func Provide[T any](c Context, key ServiceKey[T], value T) (*Disposer, error) {
	return c.provideUntyped(key.Name(), value, nil, nil)
}

// ProvideChecked registers a typed service together with an availability
// predicate (Cordis Service.check). Dependents only activate while it returns
// true.
func ProvideChecked[T any](c Context, key ServiceKey[T], value T, check func() bool) (*Disposer, error) {
	return c.provideUntyped(key.Name(), value, AvailabilityCheck(check), nil)
}

// ProvideGated registers a typed service gated by a ServiceGate. Flipping the
// gate re-evaluates the context's dependents automatically.
func ProvideGated[T any](c Context, key ServiceKey[T], value T, gate *ServiceGate) (*Disposer, error) {
	gate.watch(c)
	return c.provideUntyped(key.Name(), value, gate.Check(), nil)
}

// ProvideWithSchema registers a typed service with a config schema (normally
// done through ProvideService).
func ProvideWithSchema[T any](c Context, key ServiceKey[T], value T, schema ConfigSchema) (*Disposer, error) {
	return c.provideUntyped(key.Name(), value, nil, schema)
}

// provideUntyped is the shared registration path (sized and unsized lanes are
// the same in Go: values are stored as ServiceValue and recovered by assertion).
func (c Context) provideUntyped(name string, value ServiceValue, check AvailabilityCheck, schema ConfigSchema) (*Disposer, error) {
	fiber := c.fiberData()
	if fiber == nil {
		return nil, ErrDisposed
	}
	if state := fiber.stateSnapshot(); state == Unloading || state == Disposed {
		return nil, ErrInactiveEffect
	}
	scope := c.serviceScope(name)
	key := storeKey(scope, name)
	entry := &serviceEntry{
		value:    value,
		typ:      reflect.TypeOf(value),
		check:    check,
		schema:   schema,
		owner:    fiber.id,
		version:  c.inner.store.newVersion(),
		provider: makeWeakFiber(fiber),
	}
	store := c.inner.store
	store.mu.Lock()
	if _, exists := store.entries[key]; exists {
		store.mu.Unlock()
		return nil, DuplicateServiceError(name)
	}
	store.entries[key] = entry
	store.mu.Unlock()
	c.inner.events.Internal().emitService(c, name, value, true)

	storeRef := store
	registry := c.inner.registry
	ctxRef := c
	disposer := NewDisposer([]Cleanup{func() {
		storeRef.mu.Lock()
		delete(storeRef.entries, key)
		storeRef.mu.Unlock()
		ctxRef.inner.events.Internal().emitService(ctxRef, name, nil, false)
		registry.Refresh()
	}})
	if err := fiber.registerSyncEffect(disposer, "ctx.provide", true); err != nil {
		disposer.Dispose()
		return nil, err
	}
	registry.Refresh()
	return disposer, nil
}

// Get performs a typed service lookup (non-strict read lane: accessors are
// resolved first and a service provided by a still-loading fiber is visible).
func Get[T any](c Context, key ServiceKey[T]) (T, bool) {
	var zero T
	if c.inner == nil {
		return zero, false
	}
	name := key.Name()
	if accessor, ok := c.findAccessor(name); ok {
		value, ok := accessor.read(c)
		if !ok {
			return zero, false
		}
		typed, ok := value.(T)
		return typed, ok
	}
	value, ok := c.inner.events.Internal().waterfallGet(c, name, func() (ServiceValue, bool) {
		entry, found := c.lookupNonStrict(name)
		if !found {
			return nil, false
		}
		return entry.value, true
	})
	if !ok {
		return zero, false
	}
	typed, ok := value.(T)
	return typed, ok
}

// Require is Get with an UnknownService error.
func Require[T any](c Context, key ServiceKey[T]) (T, error) {
	value, ok := Get(c, key)
	if !ok {
		var zero T
		return zero, UnknownServiceError(key.Name())
	}
	return value, nil
}

// GetNamed performs an untyped service lookup (accessor → internal/get hook →
// store).
func (c Context) GetNamed(name string) (ServiceValue, bool) {
	if c.inner == nil {
		return nil, false
	}
	if accessor, ok := c.findAccessor(name); ok {
		return accessor.read(c)
	}
	return c.inner.events.Internal().waterfallGet(c, name, func() (ServiceValue, bool) {
		entry, found := c.lookupNonStrict(name)
		if !found {
			return nil, false
		}
		return entry.value, true
	})
}

// SetNamed overwrites a provided service value (Cordis ctx.set).
//
// Only the fiber that provided the service may set it; the replacement must have
// the same concrete type. A computed accessor read/write is honored first.
func (c Context) SetNamed(name string, value ServiceValue) error {
	_, err := c.setRevertible(name, value)
	return err
}

// setRevertible overwrites a service as a revertible, classified mutation: the
// previous entry is captured and restored by an effect owned by the providing
// fiber, and the version bump re-evaluates dependents (CORE-041).
func (c Context) setRevertible(name string, value ServiceValue) (*Disposer, error) {
	if accessor, ok := c.findAccessor(name); ok {
		if accessor.Set == nil {
			return nil, AccessorReadOnlyError(name)
		}
		if !accessor.write(c, value) {
			return nil, AccessorWriteRejectedError(name)
		}
		return NewDisposer(nil), nil
	}
	fiber := c.fiberData()
	if fiber == nil {
		return nil, ErrDisposed
	}
	scope := c.serviceScope(name)
	key := storeKey(scope, name)
	store := c.inner.store
	store.mu.Lock()
	current, exists := store.entries[key]
	if !exists {
		store.mu.Unlock()
		return nil, UnknownServiceError(name)
	}
	if current.owner != fiber.id {
		store.mu.Unlock()
		return nil, ServiceWriteForbiddenError(name)
	}
	if reflect.TypeOf(value) != current.typ {
		store.mu.Unlock()
		return nil, ServiceTypeMismatchError(name)
	}
	previous := *current
	store.mu.Unlock()
	if state := fiber.stateSnapshot(); state == Unloading || state == Disposed {
		return nil, ErrInactiveEffect
	}
	written := c.inner.events.Internal().waterfallSet(c, name, value, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		entry, ok := store.entries[key]
		if !ok {
			return false
		}
		if reflect.TypeOf(value) != entry.typ {
			return false
		}
		entry.value = value
		entry.version = store.newVersion()
		return true
	})
	if !written {
		return NewDisposer(nil), nil
	}
	registry := c.inner.registry
	ctxRef := c
	inverse := NewDisposer([]Cleanup{func() {
		store.mu.Lock()
		if previous.withdrawn || fiber.stateSnapshot() == Unloading || fiber.stateSnapshot() == Disposed {
			previous.withdrawn = true
		}
		store.entries[key] = &previous
		store.mu.Unlock()
		registry.Refresh()
	}})
	if err := fiber.registerSyncEffect(inverse, "ctx.set", true); err != nil {
		inverse.Dispose()
		return nil, err
	}
	registry.Refresh()
	_ = ctxRef
	return inverse, nil
}

// ServiceAvailable reports whether name currently resolves to a service usable
// for injection (strict query: provider fiber Active).
func (c Context) ServiceAvailable(name string) bool {
	_, ok := c.lookupForInjection(name)
	return ok
}

// lookupNonStrict is the read-lane lookup: honors availability checks and
// withdrawal but ignores the provider fiber's lifecycle state.
func (c Context) lookupNonStrict(name string) (*serviceEntry, bool) {
	if c.inner == nil {
		return nil, false
	}
	entry, ok := c.inner.store.get(storeKey(c.serviceScope(name), name))
	if !ok || !entry.isAvailable() {
		return nil, false
	}
	return entry, true
}

// lookupForInjection is the strict query used by the inject gate.
func (c Context) lookupForInjection(name string) (*serviceEntry, bool) {
	if c.inner == nil {
		return nil, false
	}
	entry, ok := c.inner.store.get(storeKey(c.serviceScope(name), name))
	if !ok || !entry.availableForInjection() {
		return nil, false
	}
	return entry, true
}

func (c Context) allServicesAvailable(names []string) bool {
	for _, name := range names {
		if _, ok := c.lookupForInjection(name); !ok {
			return false
		}
	}
	return true
}

// withdrawOwnerServices marks every service provided by owner as withdrawn and
// returns their names (provider-withdrawal order, CORE-042).
func (c Context) withdrawOwnerServices(owner uint64) []string {
	if c.inner == nil {
		return nil
	}
	var withdrawn []string
	store := c.inner.store
	store.mu.Lock()
	for key, entry := range store.entries {
		if entry.owner == owner {
			entry.withdrawn = true
			withdrawn = append(withdrawn, key.name)
		}
	}
	store.mu.Unlock()
	return withdrawn
}

// ResolveConfig merges intercept configs from root to leaf with base/head
// overrides (Cordis Service.resolveConfig).
func (c Context) ResolveConfig(name string, base, head Value) Value {
	var intercepts []Value
	for cursor := c.inner; cursor != nil; cursor = cursor.parent {
		if config, ok := cursor.intercepts[name]; ok {
			intercepts = append([]Value{config}, intercepts...)
		}
	}
	if head != nil {
		intercepts = append(intercepts, head)
	}
	if schema, ok := c.FindSchema(name); ok && schema != nil {
		return containedMerge(schema, baseOrNil(base), intercepts)
	}
	merged := map[string]Value{}
	mergeObject(merged, base)
	for _, layer := range intercepts {
		mergeObject(merged, layer)
	}
	return merged
}

func baseOrNil(base Value) Value {
	if base == nil {
		return nil
	}
	return base
}

func mergeObject(target map[string]Value, source Value) {
	object, ok := source.(map[string]Value)
	if !ok {
		return
	}
	for key, value := range object {
		target[key] = value
	}
}

// FindSchema returns the nearest config schema registered for name.
func (c Context) FindSchema(name string) (ConfigSchema, bool) {
	if c.inner == nil {
		return nil, false
	}
	entry, ok := c.inner.store.get(storeKey(c.serviceScope(name), name))
	if !ok || entry.schema == nil {
		return nil, false
	}
	return entry.schema, true
}

// findAccessor returns the nearest computed accessor for name.
func (c Context) findAccessor(name string) (Accessor, bool) {
	for cursor := c.inner; cursor != nil; cursor = cursor.parent {
		cursor.accessorMu.Lock()
		accessor, ok := cursor.accessors[name]
		cursor.accessorMu.Unlock()
		if ok {
			return accessor, true
		}
	}
	return Accessor{}, false
}

// Accessor defines a read-only computed context property (Cordis ctx.accessor).
// It is owned by the current fiber and resolved before services.
func (c Context) Accessor(name string, get func(Context) ServiceValue) (*Disposer, error) {
	return c.registerAccessor(name, NewAccessor(get))
}

// AccessorWithSetter defines a computed context property with a setter.
func (c Context) AccessorWithSetter(name string, get func(Context) ServiceValue, set func(Context, ServiceValue) bool) (*Disposer, error) {
	return c.registerAccessor(name, NewAccessorWithSetter(get, set))
}

func (c Context) registerAccessor(name string, accessor Accessor) (*Disposer, error) {
	fiber := c.fiberData()
	if fiber == nil {
		return nil, ErrDisposed
	}
	inner := c.inner
	inner.accessorMu.Lock()
	if _, exists := inner.accessors[name]; exists {
		inner.accessorMu.Unlock()
		return nil, InvalidPluginError("property `" + name + "` is already declared as an accessor")
	}
	inner.accessors[name] = accessor
	inner.accessorMu.Unlock()
	disposer := NewDisposer([]Cleanup{func() {
		inner.accessorMu.Lock()
		delete(inner.accessors, name)
		inner.accessorMu.Unlock()
	}})
	if err := fiber.registerSyncEffect(disposer, "ctx.accessor", true); err != nil {
		disposer.Dispose()
		return nil, err
	}
	return disposer, nil
}

// Meta reads typed metadata from this context and its ancestors.
func Meta[T any](c Context, key string) (T, bool) {
	var zero T
	value, ok := c.MetaAny(key)
	if !ok {
		return zero, false
	}
	typed, ok := value.(T)
	return typed, ok
}

// MetaAny reads type-erased metadata from this context and its ancestors.
func (c Context) MetaAny(key string) (any, bool) {
	for cursor := c.inner; cursor != nil; cursor = cursor.parent {
		if value, ok := cursor.meta.lookup(key); ok {
			return value, true
		}
	}
	return nil, false
}

// --- effects ---

// Effect runs body immediately and owns its returned cleanups (Cordis ctx.effect).
//
// The cleanups run in reverse registration order exactly once when the disposer
// is called or the fiber unloads. Sync and async effects share one ordered list.
func (c Context) Effect(body func() Effect) (*Disposer, error) {
	return c.EffectLabeled("ctx.effect", body)
}

// EffectLabeled is Effect with an explicit diagnostics label.
func (c Context) EffectLabeled(label string, body func() Effect) (*Disposer, error) {
	fiber := c.fiberData()
	if fiber == nil {
		return nil, ErrDisposed
	}
	if state := fiber.stateSnapshot(); state == Unloading || state == Disposed {
		return nil, ErrInactiveEffect
	}
	node := newEffectNode(label)
	fiber.attachEffectNode(node)
	fiber.pushEffectStack(node)
	cleanups, bodyPanic := runEffectBody(body)
	fiber.popEffectStack(node)
	if bodyPanic != nil {
		// An effect-body panic inside apply fails the activation (CORE-029): the
		// panic is re-raised so the activation path handles the rollback.
		panic(bodyPanic)
	}
	disposer := NewDisposer(cleanups)
	if err := fiber.pushEffect(&fiberEffect{syncDisp: disposer, meta: node, internal: false}); err != nil {
		disposer.Dispose()
		return nil, err
	}
	return disposer, nil
}

func runEffectBody(body func() Effect) (cleanups Effect, panicValue any) {
	panicValue = callContained(func() {
		if body != nil {
			cleanups = body()
		}
	})
	return cleanups, panicValue
}

// EffectAsync runs body immediately and owns its returned async cleanups. The
// cleanups are awaited in reverse registration order by DisposeAsync or the
// fiber's async unload.
func (c Context) EffectAsync(body func() AsyncEffect) (*AsyncDisposer, error) {
	return c.EffectAsyncLabeled("ctx.effect_async", body)
}

// EffectAsyncLabeled is EffectAsync with an explicit diagnostics label.
func (c Context) EffectAsyncLabeled(label string, body func() AsyncEffect) (*AsyncDisposer, error) {
	fiber := c.fiberData()
	if fiber == nil {
		return nil, ErrDisposed
	}
	if state := fiber.stateSnapshot(); state == Unloading || state == Disposed {
		return nil, ErrInactiveEffect
	}
	node := newEffectNode(label)
	fiber.attachEffectNode(node)
	fiber.pushEffectStack(node)
	var cleanups AsyncEffect
	panicValue := callContained(func() {
		if body != nil {
			cleanups = body()
		}
	})
	fiber.popEffectStack(node)
	if panicValue != nil {
		panic(panicValue)
	}
	disposer := NewAsyncDisposer(cleanups)
	if err := fiber.pushEffect(&fiberEffect{asyncDisp: disposer, meta: node, internal: false}); err != nil {
		return nil, err
	}
	return disposer, nil
}

// EffectFuture registers an async effect from an already-computed AsyncEffect
// (the Go counterpart of effect_future: the body has already been awaited).
func (c Context) EffectFuture(cleanups AsyncEffect, label string) (*AsyncDisposer, error) {
	if label == "" {
		label = "ctx.effect_future"
	}
	return c.EffectAsyncLabeled(label, func() AsyncEffect { return cleanups })
}

// EffectStream registers an async effect fed by a stream of Effects. The stream
// is consumed on its own goroutine; each yielded effect is registered as a nested
// effect and disposed when the stream ends or the disposer runs.
func (c Context) EffectStream(stream func(ctx context.Context) <-chan Effect, label string) (*AsyncDisposer, error) {
	if label == "" {
		label = "ctx.effect_stream"
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	nestedMu := &sync.Mutex{}
	var nested []*Disposer
	done := make(chan struct{})
	if stream != nil {
		go func() {
			defer close(done)
			defer func() {
				if r := recover(); r != nil {
					containedVoid("cordis::context", "effect stream consumer", func() { panic(r) })
				}
			}()
			for effect := range stream(streamCtx) {
				disposer, err := c.EffectLabeled("ctx.effect_stream.item", func() Effect { return effect })
				if err != nil {
					continue
				}
				nestedMu.Lock()
				nested = append(nested, disposer)
				nestedMu.Unlock()
			}
		}()
	} else {
		close(done)
	}
	cleanup := func(context.Context) error {
		cancel()
		<-done
		nestedMu.Lock()
		items := nested
		nested = nil
		nestedMu.Unlock()
		for i := len(items) - 1; i >= 0; i-- {
			items[i].Dispose()
		}
		return nil
	}
	return c.EffectAsyncLabeled(label, func() AsyncEffect { return AsyncEffect{cleanup} })
}

// AttachEffect attaches an already-created disposer to the current fiber as a
// labeled effect.
func (c Context) AttachEffect(disposer *Disposer, label string) error {
	fiber := c.fiberData()
	if fiber == nil {
		return ErrDisposed
	}
	if label == "" {
		label = "ctx.attach_effect"
	}
	return fiber.registerSyncEffect(disposer, label, false)
}

// --- plugins ---

// Plugin loads a synchronous plugin. The fiber starts Pending until every Inject
// service is available; it is returned immediately (like Cordis ctx.plugin).
func (c Context) Plugin(plugin Plugin, config Value) (*Fiber, error) {
	kind := pluginKind{sync: plugin}
	return c.pluginWithKey(kind, config, kind.key())
}

// PluginTyped loads a plugin whose registry identity is its concrete type.
func PluginTyped[P Plugin](c Context, plugin P, config Value) (*Fiber, error) {
	kind := pluginKind{sync: plugin}
	return c.pluginWithKey(kind, config, TypedPluginKey[P]())
}

// PluginAsync loads an asynchronous plugin; activation is awaited here when its
// inject dependencies are already available, otherwise the fiber stays Pending
// until RefreshAsync/RestartAsync.
func (c Context) PluginAsync(plugin AsyncPlugin, config Value) (*Fiber, error) {
	kind := pluginKind{async: plugin}
	return c.pluginWithKey(kind, config, kind.key())
}

// PluginAsyncTyped loads an async plugin identified by its concrete type.
func PluginAsyncTyped[P AsyncPlugin](c Context, plugin P, config Value) (*Fiber, error) {
	kind := pluginKind{async: plugin}
	return c.pluginWithKey(kind, config, TypedPluginKey[P]())
}

func (c Context) pluginWithKey(kind pluginKind, config Value, key PluginKey) (*Fiber, error) {
	if !c.valid() {
		return nil, ErrDisposed
	}
	guard := c.inner.registry.BeginRegistration()
	defer guard.End()
	fiber, err := c.createFiber(kind, config, key)
	if err != nil {
		return nil, err
	}
	endOp := fiber.data.beginOperation()
	defer endOp()
	actx := fiber.data.contextRef()
	if actx != nil && fiber.data.dependenciesReady(*actx) {
		_ = fiber.data.activateByKind(*actx, kind.async != nil, context.Background())
	}
	return fiber, nil
}

// createFiber builds and registers one fiber (including the parent ownership
// effect and the internal/plugin broadcast).
func (c Context) createFiber(kind pluginKind, config Value, key PluginKey) (*Fiber, error) {
	parentFiber := c.inner.fiber
	switch parentFiber.stateSnapshot() {
	case Disposed, Failed, Unloading:
		return nil, ErrDisposed
	}
	// Loading-depth guard counted along the fiber parent chain.
	depth := 0
	for cursor := parentFiber; cursor != nil; cursor = cursor.parentFiberData() {
		depth++
		if depth >= MaxPluginDepth {
			return nil, InvalidPluginError("plugin loading depth limit (" +
				itoa(MaxPluginDepth) + ") exceeded at " + kind.name())
		}
		if cursor.parent == nil || cursor.parent.fiber == cursor {
			break
		}
	}
	if limit := c.inner.registry.quotaLimits().MaxChildFibers; limit > 0 {
		if parentFiber.liveChildCount() >= limit {
			return nil, QuotaExceededError("child_fibers")
		}
	}
	id := c.inner.registry.nextFiberID()
	name := kind.name()
	metas := replacementMetadata{kind: kind, name: name, key: key, inject: kind.inject(), schema: kind.schema()}
	clock := c.Clock()
	data := &fiberData{
		id:          id,
		uid:         id,
		state:       Pending,
		plugin:      metas.kind,
		pluginName:  metas.name,
		inject:      metas.inject,
		schema:      metas.schema,
		config:      config,
		rawConfig:   config,
		registry:    c.inner.registry,
		clock:       clock,
		runtimeKey:  key,
		hasRuntime:  true,
		createEpoch: parentFiber.activatingEpoch.Load(),
	}
	fiber := &Fiber{data: data}
	childCtx := c.childWith(data)
	data.ctx = childCtx.inner
	data.parent = c.inner
	parentFiber.mu.Lock()
	parentFiber.children = append(parentFiber.children, data)
	parentFiber.mu.Unlock()
	c.inner.registry.registerFiber(data)
	c.inner.registry.registerRuntimeFiber(key, metas.name, data)
	if err := parentFiber.registerChildOwnershipEffect(fiber, id); err != nil {
		_ = fiber.Dispose()
		return nil, err
	}
	c.inner.events.Internal().emitPlugin(fiber)
	return fiber, nil
}

// parentFiberData resolves the fiber of the parent context (the root fiber
// returns itself, matching the Rust weak self-link).
func (d *fiberData) parentFiberData() *fiberData {
	d.mu.Lock()
	parent := d.parent
	d.mu.Unlock()
	if parent == nil || parent.fiber == nil {
		return nil
	}
	return parent.fiber
}

func (d *fiberData) liveChildCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := 0
	for _, child := range d.children {
		if child.stateSnapshot() != Disposed {
			count++
		}
	}
	return count
}

// Inject is the Cordis ctx.inject convenience: one closure plugin per call,
// keyed by the closure type.
func (c Context) Inject(deps []string, callback func(Context) error) (*Fiber, error) {
	plugin := injectedPlugin{deps: deps, callback: callback}
	key := PluginKey{kind: pluginKeyTyped, typ: reflect.TypeOf(callback)}
	return c.pluginWithKey(pluginKind{sync: plugin}, nil, key)
}

type injectedPlugin struct {
	deps     []string
	callback func(Context) error
}

func (p injectedPlugin) Name() string   { return "injected" }
func (p injectedPlugin) Inject() []string { return p.deps }
func (p injectedPlugin) Activate(ctx Context, _ Value) error {
	if p.callback == nil {
		return nil
	}
	return p.callback(ctx)
}

// --- events ---

// On registers a synchronous event listener owned by the current fiber.
func (c Context) On(name string, listener ListenerFunc) (*Disposer, error) {
	return c.OnWithOptions(name, EventOptions{}, listener)
}

// OnWith registers a listener with positional prepend/global flags.
func (c Context) OnWith(name string, prepend, global bool, listener ListenerFunc) (*Disposer, error) {
	return c.OnWithOptions(name, EventOptions{Prepend: prepend, Global: global}, listener)
}

// OnWithOptions registers a listener with explicit options.
func (c Context) OnWithOptions(name string, options EventOptions, listener ListenerFunc) (*Disposer, error) {
	fiber := c.fiberData()
	if fiber == nil {
		return nil, ErrDisposed
	}
	request := ListenerRequest{Name: name, Prepend: options.Prepend, Global: options.Global, Once: options.Once, Owner: fiber.id}
	if override := c.inner.events.Internal().interceptListener(request); override != nil {
		listener = override.Callback
		options.Prepend = override.Prepend
		options.Global = override.Global
		options.Once = override.Once
	}
	if limit := c.inner.registry.quotaLimits().MaxListeners; limit > 0 && fiber.liveListenerCount() >= limit {
		return nil, QuotaExceededError("listeners")
	}
	disposer := c.inner.events.addListener(name, fiber.id, options, c.inner.isolate, listener)
	if err := fiber.registerSyncEffect(disposer, "ctx.on", true); err != nil {
		disposer.Dispose()
		return nil, err
	}
	return disposer, nil
}

// Once registers a self-removing listener.
func (c Context) Once(name string, listener ListenerFunc) (*Disposer, error) {
	return c.OnWithOptions(name, EventOptions{Once: true}, listener)
}

// OnAsync registers an asynchronous listener (async lane only).
func (c Context) OnAsync(name string, listener AsyncListenerFunc) (*Disposer, error) {
	return c.onAsyncWithOptions(name, EventOptions{}, listener)
}

// OnAsyncWith registers an async listener with positional prepend/global flags.
func (c Context) OnAsyncWith(name string, prepend, global bool, listener AsyncListenerFunc) (*Disposer, error) {
	return c.onAsyncWithOptions(name, EventOptions{Prepend: prepend, Global: global}, listener)
}

func (c Context) onAsyncWithOptions(name string, options EventOptions, listener AsyncListenerFunc) (*Disposer, error) {
	fiber := c.fiberData()
	if fiber == nil {
		return nil, ErrDisposed
	}
	disposer := c.inner.events.addAsyncListener(name, fiber.id, options, c.inner.isolate, listener)
	if err := fiber.registerSyncEffect(disposer, "ctx.on_async", true); err != nil {
		disposer.Dispose()
		return nil, err
	}
	return disposer, nil
}

// OnWaterfall registers a synchronous waterfall middleware.
func (c Context) OnWaterfall(name string, listener WaterfallListenerFunc) (*Disposer, error) {
	fiber := c.fiberData()
	if fiber == nil {
		return nil, ErrDisposed
	}
	disposer := c.inner.events.addWaterfallListener(name, fiber.id, EventOptions{}, c.inner.isolate, listener)
	if err := fiber.registerSyncEffect(disposer, "ctx.on_waterfall", true); err != nil {
		disposer.Dispose()
		return nil, err
	}
	return disposer, nil
}

// OnWaterfallAsync registers an asynchronous waterfall middleware.
func (c Context) OnWaterfallAsync(name string, listener AsyncWaterfallListenerFunc) (*Disposer, error) {
	fiber := c.fiberData()
	if fiber == nil {
		return nil, ErrDisposed
	}
	disposer := c.inner.events.addAsyncWaterfallListener(name, fiber.id, EventOptions{}, c.inner.isolate, listener)
	if err := fiber.registerSyncEffect(disposer, "ctx.on_waterfall_async", true); err != nil {
		disposer.Dispose()
		return nil, err
	}
	return disposer, nil
}

// Emit dispatches an event synchronously, ignoring listener return values.
func (c Context) Emit(name string, args ...Value) {
	if c.inner == nil {
		return
	}
	c.inner.events.emitScoped(name, args, c.inner.isolate)
}

// Bail dispatches synchronously until a listener bails.
func (c Context) Bail(name string, args ...Value) (Value, bool) {
	if c.inner == nil {
		return nil, false
	}
	return c.inner.events.bailScoped(name, args, c.inner.isolate)
}

// ParallelAsync awaits every listener concurrently (sync listeners run inline
// first; async listeners run on their own goroutines).
func (c Context) ParallelAsync(name string, args ...Value) {
	c.ParallelAsyncContext(context.Background(), name, args...)
}

// ParallelAsyncContext is ParallelAsync with cancellation.
func (c Context) ParallelAsyncContext(ctx context.Context, name string, args ...Value) {
	if c.inner == nil {
		return
	}
	c.inner.events.parallelAsyncScoped(ctx, name, args, c.inner.isolate)
}

// SerialAsync awaits listeners in registration order until one bails.
func (c Context) SerialAsync(name string, args ...Value) (Value, bool) {
	return c.SerialAsyncContext(context.Background(), name, args...)
}

// SerialAsyncContext is SerialAsync with cancellation.
func (c Context) SerialAsyncContext(ctx context.Context, name string, args ...Value) (Value, bool) {
	if c.inner == nil {
		return nil, false
	}
	return c.inner.events.serialAsyncScoped(ctx, name, args, c.inner.isolate)
}

// Waterfall composes the synchronous waterfall chain for name.
func (c Context) Waterfall(name string, args []Value, terminal WaterfallTerminal) Value {
	if c.inner == nil {
		return nil
	}
	return c.inner.events.waterfallScoped(name, args, terminal, c.inner.isolate)
}

// WaterfallAsync composes and runs the async waterfall chain for name.
func (c Context) WaterfallAsync(ctx context.Context, name string, args []Value, terminal AsyncWaterfallTerminal) Value {
	if c.inner == nil {
		return nil
	}
	return c.inner.events.waterfallAsyncScoped(ctx, name, args, terminal, c.inner.isolate)
}

// --- lifecycle / quiescence ---

// Refresh re-evaluates every fiber (bounded fixed point, at most 64 passes).
func (c Context) Refresh() {
	if c.inner != nil {
		c.inner.registry.Refresh()
	}
}

// RefreshAsync re-evaluates every fiber, awaiting async activation/unload.
func (c Context) RefreshAsync(ctx context.Context) {
	if c.inner != nil {
		c.inner.registry.RefreshAsync(ctx)
	}
}

// DisposeAll disposes every fiber in the tree and the root fiber (shutdown path).
func (c Context) DisposeAll() error {
	if c.inner == nil {
		return nil
	}
	return c.inner.registry.DisposeAll()
}

// WhenIdle blocks until the tree is quiescent.
func (c Context) WhenIdle() {
	if c.inner != nil {
		c.inner.registry.WhenIdle()
	}
}

// WhenIdleContext is WhenIdle with cancellation.
func (c Context) WhenIdleContext(ctx context.Context) error {
	if c.inner == nil {
		return nil
	}
	return c.inner.registry.WhenIdleContext(ctx)
}

// Checkpoint returns ErrCancelled once the owning fiber was cancelled, so
// long-running plugin work can unwind through the normal failure path (CORE-050).
func (c Context) Checkpoint() error {
	if !c.valid() {
		return ErrDisposed
	}
	if c.inner.fiber.cancelRequested.Load() {
		return ErrCancelled
	}
	return nil
}

// OnLifecycle subscribes to fiber state transitions of the whole context tree.
//
// The subscription is not fiber-owned: the returned disposer is its single owner
// and must be disposed explicitly (Go has no Drop safety net). Panics are
// contained; other subscribers still run.
func (c Context) OnLifecycle(listener LifecycleListener) (*Disposer, error) {
	if c.inner == nil || listener == nil {
		return nil, ErrDisposed
	}
	hook := func(f *Fiber, from, to FiberState, seq uint64) {
		containedVoid("cordis::internal", "lifecycle listener", func() {
			listener(&LifecycleEvent{FiberID: f.ID(), Name: f.Name(), From: from, To: to, Seq: seq})
		})
	}
	return c.inner.events.Internal().onLifecycleHook(hook), nil
}

// --- internal/* hooks (fiber-owned wrappers) ---

func (c Context) ownInternal(label string, disposer *Disposer) (*Disposer, error) {
	fiber := c.fiberData()
	if fiber == nil {
		disposer.Dispose()
		return nil, ErrDisposed
	}
	if err := fiber.registerSyncEffect(disposer, label, true); err != nil {
		disposer.Dispose()
		return nil, err
	}
	return disposer, nil
}

// OnInternalPlugin registers an internal/plugin hook owned by this fiber.
func (c Context) OnInternalPlugin(hook PluginHook) (*Disposer, error) {
	return c.ownInternal("ctx.on_internal_plugin", c.inner.events.Internal().OnPlugin(hook))
}

// OnInternalStatus registers an internal/status hook owned by this fiber.
func (c Context) OnInternalStatus(hook StatusHook) (*Disposer, error) {
	return c.ownInternal("ctx.on_internal_status", c.inner.events.Internal().OnStatus(hook))
}

// OnInternalService registers an internal/service hook owned by this fiber.
func (c Context) OnInternalService(hook ServiceHook) (*Disposer, error) {
	return c.ownInternal("ctx.on_internal_service", c.inner.events.Internal().OnService(hook))
}

// OnInternalConfig registers an internal/config waterfall hook owned by this
// fiber.
func (c Context) OnInternalConfig(hook ConfigHook) (*Disposer, error) {
	return c.ownInternal("ctx.on_internal_config", c.inner.events.Internal().OnConfig(hook))
}

// OnInternalUpdate registers an internal/update waterfall hook owned by this
// fiber.
func (c Context) OnInternalUpdate(hook UpdateHook) (*Disposer, error) {
	return c.ownInternal("ctx.on_internal_update", c.inner.events.Internal().OnUpdate(hook))
}

// OnInternalGet registers an internal/get waterfall hook owned by this fiber.
func (c Context) OnInternalGet(hook GetHook) (*Disposer, error) {
	return c.ownInternal("ctx.on_internal_get", c.inner.events.Internal().OnGet(hook))
}

// OnInternalSet registers an internal/set waterfall hook owned by this fiber.
func (c Context) OnInternalSet(hook SetHook) (*Disposer, error) {
	return c.ownInternal("ctx.on_internal_set", c.inner.events.Internal().OnSet(hook))
}

// OnInternalListener registers an internal/listener hook owned by this fiber.
func (c Context) OnInternalListener(hook ListenerHook) (*Disposer, error) {
	return c.ownInternal("ctx.on_internal_listener", c.inner.events.Internal().OnListener(hook))
}

// OnInternalDispatch registers an internal/dispatch observer owned by this
// fiber.
func (c Context) OnInternalDispatch(hook DispatchHook) (*Disposer, error) {
	return c.ownInternal("ctx.on_internal_dispatch", c.inner.events.Internal().OnDispatch(hook))
}

// --- logger / config ---

// Logger returns a facade named after the current fiber/plugin, honoring
// ctx.intercept("logger", ...) configuration.
func (c Context) Logger() *Logger {
	config, _ := c.ResolveConfig("logger", nil, nil).(map[string]Value)
	name := c.PluginName()
	var level *LogLevel
	if config != nil {
		if override, ok := config["name"].(string); ok {
			name = override
		}
		level = configLevel(config)
	}
	return c.inner.logger.loggerWithClock(name, level, c.Clock())
}

// LoggerNamed returns a named logger facade.
func (c Context) LoggerNamed(name string) *Logger {
	config, _ := c.ResolveConfig("logger", nil, nil).(map[string]Value)
	var level *LogLevel
	if config != nil {
		level = configLevel(config)
	}
	return c.inner.logger.loggerWithClock(name, level, c.Clock())
}

// LoggerService returns the shared logging service.
func (c Context) LoggerService() *LoggerService { return c.inner.logger }

// LogExporter registers a log exporter owned by the current fiber.
func (c Context) LogExporter(exporter LogExporter) (*Disposer, error) {
	if exporter == nil {
		return nil, InvalidPluginError("nil log exporter")
	}
	disposer := c.inner.logger.Exporter(exporter)
	if err := c.fiberData().registerSyncEffect(disposer, "ctx.log_exporter", true); err != nil {
		disposer.Dispose()
		return nil, err
	}
	return disposer, nil
}

func configLevel(config map[string]Value) *LogLevel {
	level, ok := config["level"]
	if !ok {
		return nil
	}
	var parsed LogLevel
	switch value := level.(type) {
	case string:
		parsed, ok = ParseLogLevel(value)
		if !ok {
			return nil
		}
	case int:
		parsed = clampLevel(uint8(value))
	case int64:
		parsed = clampLevel(uint8(value))
	case float64:
		parsed = clampLevel(uint8(value))
	case uint8:
		parsed = clampLevel(value)
	default:
		return nil
	}
	return &parsed
}

func clampLevel(code uint8) LogLevel {
	if code <= 3 {
		return LogLevelFromU8(code)
	}
	// Unknown numeric levels clamp to the safe end (Error), never widening
	// logging to the most verbose level (S2/XT-09).
	return LogError
}

// --- fiber helpers ---

func (d *fiberData) liveListenerCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := 0
	for _, effect := range d.effects {
		if effect.meta != nil && !effect.disposed() &&
			(effect.meta.label == "ctx.on" || effect.meta.label == "ctx.on_async" ||
				effect.meta.label == "ctx.on_waterfall" || effect.meta.label == "ctx.on_waterfall_async") {
			count++
		}
	}
	return count
}

var _ = errors.Is
