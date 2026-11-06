package cordis

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"weak"
)

// PluginKey is the registry identity of a plugin.
//
// A plugin is identified by its name (NamedPluginKey, the default) or by its
// concrete Go type (TypedPluginKey[T], the counterpart of the Rust
// PluginKey::Type TypeId). Fibers of the same runtime share one PluginRuntime
// and are deduplicated: loading the same plugin again returns the existing
// runtime.
type PluginKey struct {
	kind pluginKeyKind
	name string
	typ  reflect.Type
}

type pluginKeyKind uint8

const (
	pluginKeyNamed pluginKeyKind = iota + 1
	pluginKeyTyped
)

// NamedPluginKey builds a name-based plugin identity.
func NamedPluginKey(name string) PluginKey { return PluginKey{kind: pluginKeyNamed, name: name} }

// TypedPluginKey builds a type-based plugin identity (the Go counterpart of
// Rust's PluginKey::Type). Because it is a package-level generic function it can
// be used where the Rust reference uses `plugin_typed`/`plugin_async_typed`.
func TypedPluginKey[T any]() PluginKey {
	return PluginKey{kind: pluginKeyTyped, typ: reflect.TypeFor[T]()}
}

// Kind reports how the key identifies the plugin.
func (k PluginKey) Kind() pluginKeyKind { return k.kind }

// Name returns the plugin display name (empty for type keys).
func (k PluginKey) Name() string { return k.name }

// Type returns the concrete plugin type (nil for name keys).
func (k PluginKey) Type() reflect.Type { return k.typ }

// String renders the key for diagnostics.
func (k PluginKey) String() string {
	switch k.kind {
	case pluginKeyTyped:
		if k.typ == nil {
			return "type:<nil>"
		}
		return "type:" + k.typ.String()
	default:
		return "named:" + k.name
	}
}

// Plugin is a synchronous plugin: activation is a blocking Go function.
type Plugin interface {
	// Name is the plugin display name.
	Name() string
	// Activate loads the plugin into ctx. It runs as a transaction: every
	// registration made through ctx belongs to the fiber and is rolled back
	// when Activate returns an error or panics.
	Activate(ctx Context, config Value) error
}

// AsyncPlugin is an asynchronous plugin: activation is awaited through the
// *Async entry points. In Go it is a blocking function that receives a
// context.Context; cancellation of that context is the Go analogue of dropping
// the activation future.
type AsyncPlugin interface {
	// Name is the plugin display name.
	Name() string
	// ActivateAsync loads the plugin. The kernel runs it only through async
	// entry points; a synchronous API used on an async fiber returns
	// AsyncPluginNeedsAsync.
	ActivateAsync(ctx context.Context, c Context, config Value) error
}

// PluginKeyer optionally overrides the registry identity of a plugin.
type PluginKeyer interface {
	PluginKey() PluginKey
}

// PluginInjector optionally declares the service dependencies that gate
// activation (Cordis Plugin.inject).
type PluginInjector interface {
	Inject() []string
}

// PluginConfigSchema optionally declares a config schema.
type PluginConfigSchema interface {
	ConfigSchema() ConfigSchema
}

// pluginKind is the dynamic plugin implementation hosted by a fiber.
type pluginKind struct {
	sync  Plugin
	async AsyncPlugin
}

func (k pluginKind) name() string {
	if k.async != nil {
		return metadataCall("Plugin.Name()", func() string { return k.async.Name() })
	}
	if k.sync != nil {
		return metadataCall("Plugin.Name()", func() string { return k.sync.Name() })
	}
	return "<nil>"
}

func (k pluginKind) key() PluginKey {
	var keyer PluginKeyer
	if k.async != nil {
		if v, ok := k.async.(PluginKeyer); ok {
			keyer = v
		}
	} else if k.sync != nil {
		if v, ok := k.sync.(PluginKeyer); ok {
			keyer = v
		}
	}
	if keyer != nil {
		key := contained("cordis::registry", "Plugin.PluginKey()", PluginKey{kind: pluginKeyNamed, name: "<panicked plugin>"}, keyer.PluginKey)
		if key.kind == 0 {
			return NamedPluginKey(k.name())
		}
		return key
	}
	return NamedPluginKey(k.name())
}

func (k pluginKind) inject() []string {
	if k.async != nil {
		if v, ok := k.async.(PluginInjector); ok {
			return contained("cordis::registry", "AsyncPlugin.Inject()", nil, v.Inject)
		}
		return nil
	}
	if k.sync != nil {
		if v, ok := k.sync.(PluginInjector); ok {
			return contained("cordis::registry", "Plugin.Inject()", nil, v.Inject)
		}
		return nil
	}
	return nil
}

func (k pluginKind) schema() ConfigSchema {
	if k.async != nil {
		if v, ok := k.async.(PluginConfigSchema); ok {
			return contained[ConfigSchema]("cordis::registry", "AsyncPlugin.ConfigSchema()", nil, v.ConfigSchema)
		}
		return nil
	}
	if k.sync != nil {
		if v, ok := k.sync.(PluginConfigSchema); ok {
			return contained[ConfigSchema]("cordis::registry", "Plugin.ConfigSchema()", nil, v.ConfigSchema)
		}
	}
	return nil
}

// metadataCall runs plugin metadata code with panic containment and a
// deterministic degraded value (C2): name() panics degrade to
// "<panicked plugin>".
func metadataCall(callback string, body func() string) string {
	return contained("cordis::registry", callback, "<panicked plugin>", body)
}

// PluginRuntime is the registry entry of one plugin identity: every fiber
// loaded under the same PluginKey shares it.
type PluginRuntime struct {
	name string
	key  PluginKey

	mu     sync.Mutex
	fibers []*fiberData
}

func newPluginRuntime(name string, key PluginKey) *PluginRuntime {
	return &PluginRuntime{name: name, key: key}
}

// Name returns the plugin display name.
func (r *PluginRuntime) Name() string { return r.name }

// Key returns the runtime's plugin identity.
func (r *PluginRuntime) Key() PluginKey { return r.key }

// Fibers returns a snapshot of the runtime's live fibers.
func (r *PluginRuntime) Fibers() []*Fiber {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Fiber, len(r.fibers))
	for i, data := range r.fibers {
		out[i] = &Fiber{data: data}
	}
	return out
}

// FiberCount returns the number of live fibers attached to the runtime.
func (r *PluginRuntime) FiberCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.fibers)
}

// IsEmpty reports whether the runtime has no live fibers.
func (r *PluginRuntime) IsEmpty() bool { return r.FiberCount() == 0 }

func (r *PluginRuntime) push(f *fiberData) {
	r.mu.Lock()
	r.fibers = append(r.fibers, f)
	r.mu.Unlock()
}

func (r *PluginRuntime) remove(id uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, data := range r.fibers {
		if data.id == id {
			r.fibers = append(r.fibers[:i], r.fibers[i+1:]...)
			return true
		}
	}
	return false
}

func (r *PluginRuntime) snapshot() []*fiberData {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*fiberData(nil), r.fibers...)
}

// Registry maps plugin identities to runtime entries and owns the fiber list of
// a context tree.
//
// All map/list accessors are per-structure snapshots: during concurrent
// lifecycle operations different calls can be transiently inconsistent. Use
// IsIdle/WhenIdle when a consistent view is required.
type Registry struct {
	mu       sync.Mutex
	runtimes map[PluginKey]*PluginRuntime
	// root is a weak handle: a disposed root fiber must be collectable without
	// the registry pinning it (CORE-033; the Rust reference uses weak self-links
	// for the same reason).
	root weak.Pointer[fiberData]

	fibersMu sync.Mutex
	fibers   []*fiberData

	nextID    atomic.Uint64
	nextScope atomic.Uint64

	scopeMu     sync.Mutex
	labelScopes map[string]uint64

	// pendingOps counts registry-level in-flight work (registration windows).
	pendingOps atomic.Int64
	regMu      sync.Mutex
	regOwners  map[uint64]int

	idleMu sync.Mutex
	idleCh chan struct{}

	// refresh coalescing: one refresh pass owns refreshMu; nested calls mark
	// refreshDirty and return, the outer loop drains it (bounded).
	refreshMu    sync.Mutex
	refreshDirty atomic.Bool

	quotaMu sync.Mutex
	quota   FiberQuota

	// Reverse dependency index (inject name → fiber ids): keeps the unload
	// cascade proportional to the affected dependents instead of rescanning
	// every fiber (the Rust reference's S1–S4 index, simplified).
	depMu     sync.Mutex
	depIndex  map[string]map[uint64]struct{}
	fiberDeps map[uint64][]string
	byID      map[uint64]*fiberData
	// dirtyServices are service names whose availability/version changed since
	// the last indexed dependents refresh (the Rust S4 fast path).
	dirtyServices map[string]struct{}
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	r := &Registry{
		runtimes:      make(map[PluginKey]*PluginRuntime),
		labelScopes:   make(map[string]uint64),
		regOwners:     make(map[uint64]int),
		depIndex:      make(map[string]map[uint64]struct{}),
		fiberDeps:     make(map[uint64][]string),
		byID:          make(map[uint64]*fiberData),
		dirtyServices: make(map[string]struct{}),
		idleCh:        make(chan struct{}),
		nextID:        atomic.Uint64{},
		nextScope:     atomic.Uint64{},
		quota:         FiberQuota{},
	}
	r.nextScope.Store(1) // scope 0 is the default (unisolated) scope
	return r
}

func (r *Registry) setRootFiber(f *fiberData) {
	r.mu.Lock()
	r.root = weak.Make(f)
	r.mu.Unlock()
}

func (r *Registry) rootFiber() *fiberData {
	r.mu.Lock()
	root := r.root
	r.mu.Unlock()
	return root.Value()
}

func (r *Registry) nextFiberID() uint64 { return r.nextID.Add(1) }

// NextScopeID allocates a fresh isolation scope id.
func (r *Registry) NextScopeID() uint64 { return r.nextScope.Add(1) }

// LabeledScopeID returns the scope id of label, allocating it on first use.
func (r *Registry) LabeledScopeID(label string) uint64 {
	r.scopeMu.Lock()
	defer r.scopeMu.Unlock()
	if id, ok := r.labelScopes[label]; ok {
		return id
	}
	id := r.nextScope.Add(1)
	r.labelScopes[label] = id
	return id
}

// --- fiber registration ---

func (r *Registry) registerFiber(f *fiberData) {
	r.fibersMu.Lock()
	r.fibers = append(r.fibers, f)
	r.fibersMu.Unlock()
	r.mu.Lock()
	r.byID[f.id] = f
	registry := r
	r.mu.Unlock()
	registry.reindexFiberDeps(f.id, f.injectSnapshot())
}

func (r *Registry) removeFiber(id uint64) {
	r.fibersMu.Lock()
	for i, data := range r.fibers {
		if data.id == id {
			r.fibers = append(r.fibers[:i], r.fibers[i+1:]...)
			break
		}
	}
	r.fibersMu.Unlock()
	r.mu.Lock()
	delete(r.byID, id)
	r.mu.Unlock()
	r.unindexFiberDeps(id)
}

func (r *Registry) liveFibers() []*fiberData {
	r.fibersMu.Lock()
	defer r.fibersMu.Unlock()
	return append([]*fiberData(nil), r.fibers...)
}

// FiberByID returns the live fiber handle with the given id.
func (r *Registry) FiberByID(id uint64) (*Fiber, bool) {
	for _, data := range r.liveFibers() {
		if data.id == id {
			return &Fiber{data: data}, true
		}
	}
	return nil, false
}

// reindexFiberDeps replaces the inject-name edges of one fiber.
func (r *Registry) reindexFiberDeps(id uint64, inject []string) {
	r.depMu.Lock()
	defer r.depMu.Unlock()
	if old, ok := r.fiberDeps[id]; ok {
		for _, name := range old {
			if set, ok := r.depIndex[name]; ok {
				delete(set, id)
				if len(set) == 0 {
					delete(r.depIndex, name)
				}
			}
		}
		delete(r.fiberDeps, id)
	}
	if len(inject) == 0 {
		return
	}
	r.fiberDeps[id] = append([]string(nil), inject...)
	for _, name := range inject {
		set := r.depIndex[name]
		if set == nil {
			set = make(map[uint64]struct{})
			r.depIndex[name] = set
		}
		set[id] = struct{}{}
	}
}

// unindexFiberDeps removes the inject-name edges of a fiber.
func (r *Registry) unindexFiberDeps(id uint64) {
	r.reindexFiberDeps(id, nil)
}

// dependentsOfNames returns the live fibers that inject any of names.
func (r *Registry) dependentsOfNames(names []string) []*fiberData {
	r.depMu.Lock()
	ids := make(map[uint64]struct{})
	for _, name := range names {
		for id := range r.depIndex[name] {
			ids[id] = struct{}{}
		}
	}
	r.depMu.Unlock()
	if len(ids) == 0 {
		return nil
	}
	r.mu.Lock()
	out := make([]*fiberData, 0, len(ids))
	for id := range ids {
		if f, ok := r.byID[id]; ok {
			out = append(out, f)
		}
	}
	r.mu.Unlock()
	return out
}

// noteServiceChanged marks a service name dirty for the next indexed dependents
// refresh (called by provide/set/withdraw paths).
func (r *Registry) noteServiceChanged(name string) {
	r.depMu.Lock()
	r.dirtyServices[name] = struct{}{}
	r.depMu.Unlock()
}

// takeDirtyServices drains the dirty service-name set.
func (r *Registry) takeDirtyServices() []string {
	r.depMu.Lock()
	defer r.depMu.Unlock()
	if len(r.dirtyServices) == 0 {
		return nil
	}
	names := make([]string, 0, len(r.dirtyServices))
	for name := range r.dirtyServices {
		names = append(names, name)
	}
	r.dirtyServices = make(map[string]struct{})
	return names
}

// refreshCandidates re-evaluates one fiber (used by the indexed dependents
// refresh). It reports whether a state change was attempted.
func (r *Registry) refreshCandidate(data *fiberData) bool {
	actx := data.contextRef()
	if actx == nil {
		return false
	}
	// Sync dependents refresh skips async plugins (use RefreshAsync).
	if data.isAsyncPlugin() {
		return false
	}
	switch data.stateSnapshot() {
	case Active:
		if data.dependenciesSatisfied(*actx) {
			return false
		}
		unchanged := data.unloadForRefresh(*actx, Pending, false, nil)
		if !unchanged {
			return false
		}
		// A version change (ctx.set / provider swap) leaves the dependency
		// available: reactivate the dependent in the same pass instead of
		// leaving it Pending until an unrelated refresh (CORE-041).
		if reactx := data.contextRef(); reactx != nil &&
			data.stateSnapshot() == Pending && data.dependenciesReady(*reactx) {
			data.activateByKind(*reactx, false, nil)
		}
		return true
	case Pending:
		if !data.dependenciesReady(*actx) {
			return false
		}
		return data.activateByKind(*actx, false, nil)
	}
	return false
}

// RefreshDependents re-evaluates only the fibers affected by service changes
// (indexed fast path). It coalesces re-entrant calls through a dirty flag and
// keeps going until no candidate changed, so a dependency chain settles in one
// caller even when it is long.
func (r *Registry) RefreshDependents() {
	if !r.refreshMu.TryLock() {
		r.refreshDirty.Store(true)
		return
	}
	defer r.refreshMu.Unlock()
	limit := r.FiberCount()*2 + 64
	for pass := 0; pass < limit; pass++ {
		r.refreshDirty.Store(false)
		names := r.takeDirtyServices()
		if len(names) == 0 {
			if r.refreshDirty.Load() {
				continue
			}
			return
		}
		changed := false
		for _, candidate := range r.dependentsOfNames(names) {
			if candidate.isAsyncPlugin() {
				// Leave async dependents to RefreshAsync: re-mark their inject
				// names so an async refresh still sees the change.
				for _, name := range candidate.injectSnapshot() {
					r.noteServiceChanged(name)
				}
				continue
			}
			if r.refreshCandidate(candidate) {
				changed = true
			}
		}
		if !changed && !r.refreshDirty.Load() {
			return
		}
	}
}

// refreshDependentsAsync is the async counterpart used by async activation and
// unload paths.
func (r *Registry) refreshDependentsAsync(ctx context.Context) {
	if !r.refreshMu.TryLock() {
		r.refreshDirty.Store(true)
		return
	}
	defer r.refreshMu.Unlock()
	limit := r.FiberCount()*2 + 64
	for pass := 0; pass < limit; pass++ {
		r.refreshDirty.Store(false)
		names := r.takeDirtyServices()
		if len(names) == 0 {
			if r.refreshDirty.Load() {
				continue
			}
			return
		}
		changed := false
		for _, candidate := range r.dependentsOfNames(names) {
			actx := candidate.contextRef()
			if actx == nil {
				continue
			}
			switch candidate.stateSnapshot() {
			case Active:
				if !candidate.dependenciesSatisfied(*actx) {
					if candidate.unloadForRefresh(*actx, Pending, true, ctx) {
						changed = true
						if reactx := candidate.contextRef(); reactx != nil &&
							candidate.stateSnapshot() == Pending && candidate.dependenciesReady(*reactx) {
							candidate.activateByKind(*reactx, true, ctx)
						}
					}
				}
			case Pending:
				if candidate.dependenciesReady(*actx) {
					candidate.activateByKind(*actx, true, ctx)
					changed = true
				}
			}
		}
		if !changed && !r.refreshDirty.Load() {
			return
		}
	}
}

// --- runtime registration ---

func (r *Registry) registerRuntimeFiber(key PluginKey, name string, f *fiberData) {
	r.mu.Lock()
	runtime, ok := r.runtimes[key]
	if !ok {
		runtime = newPluginRuntime(name, key)
		r.runtimes[key] = runtime
	}
	r.mu.Unlock()
	runtime.push(f)
}

func (r *Registry) unregisterRuntimeFiber(id uint64) {
	r.mu.Lock()
	runtimes := make([]*PluginRuntime, 0, len(r.runtimes))
	for _, runtime := range r.runtimes {
		runtimes = append(runtimes, runtime)
	}
	r.mu.Unlock()
	for _, runtime := range runtimes {
		runtime.remove(id)
	}
	r.pruneEmptyRuntimes()
}

// unregisterRuntimeFiberFromKey detaches a fiber from one specific runtime key
// (used when a replacement moves a fiber between runtimes).
func (r *Registry) unregisterRuntimeFiberFromKey(id uint64, key PluginKey) {
	r.mu.Lock()
	runtime, ok := r.runtimes[key]
	r.mu.Unlock()
	if !ok {
		return
	}
	runtime.remove(id)
	if runtime.IsEmpty() {
		r.mu.Lock()
		if current, still := r.runtimes[key]; still && current == runtime && runtime.IsEmpty() {
			delete(r.runtimes, key)
		}
		r.mu.Unlock()
	}
}

func (r *Registry) pruneEmptyRuntimes() {
	r.mu.Lock()
	for key, runtime := range r.runtimes {
		if runtime.IsEmpty() {
			delete(r.runtimes, key)
		}
	}
	r.mu.Unlock()
}

func (r *Registry) rebindFiber(f *fiberData, newKey PluginKey, name string) {
	r.mu.Lock()
	runtime, ok := r.runtimes[newKey]
	if !ok {
		runtime = newPluginRuntime(name, newKey)
		r.runtimes[newKey] = runtime
	}
	r.mu.Unlock()
	runtime.push(f)
}

// --- map API ---

// Size returns the number of live plugin runtimes.
func (r *Registry) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.runtimes)
}

// Get returns the runtime for key.
func (r *Registry) Get(key PluginKey) (*PluginRuntime, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	runtime, ok := r.runtimes[key]
	return runtime, ok
}

// Has reports whether key has a runtime.
func (r *Registry) Has(key PluginKey) bool {
	_, ok := r.Get(key)
	return ok
}

// fmtCache formats diagnostics without a package-level fmt import dependency in
// the hot paths (it is only used on rollback error construction).
func fmtCache(format string, args ...any) string { return fmt.Sprintf(format, args...) }

// Delete removes the runtime for key and disposes every fiber it hosts,
// returning the (now-detached) runtime. It returns nil when the key is absent.
//
// Cleanup panics are contained per fiber so one broken fiber cannot stop the
// removal (the Rust reference disposes all fibers, then returns the runtime).
func (r *Registry) Delete(key PluginKey) *PluginRuntime {
	r.mu.Lock()
	runtime, ok := r.runtimes[key]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	delete(r.runtimes, key)
	r.mu.Unlock()
	for _, data := range runtime.snapshot() {
		containedVoid("cordis::registry", "delete runtime fiber dispose", func() {
			_ = (&Fiber{data: data}).Dispose()
		})
	}
	r.pruneEmptyRuntimes()
	return runtime
}

// Keys returns a snapshot of the live runtime keys (sorted for determinism).
func (r *Registry) Keys() []PluginKey {
	r.mu.Lock()
	keys := make([]PluginKey, 0, len(r.runtimes))
	for key := range r.runtimes {
		keys = append(keys, key)
	}
	r.mu.Unlock()
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	return keys
}

// Values returns a snapshot of the live runtimes.
func (r *Registry) Values() []*PluginRuntime {
	r.mu.Lock()
	out := make([]*PluginRuntime, 0, len(r.runtimes))
	for _, runtime := range r.runtimes {
		out = append(out, runtime)
	}
	r.mu.Unlock()
	return out
}

// Entries returns a snapshot of the key→runtime map.
func (r *Registry) Entries() map[PluginKey]*PluginRuntime {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[PluginKey]*PluginRuntime, len(r.runtimes))
	for key, runtime := range r.runtimes {
		out[key] = runtime
	}
	return out
}

// ForEach calls fn for a snapshot of every runtime.
func (r *Registry) ForEach(fn func(PluginKey, *PluginRuntime)) {
	for key, runtime := range r.Entries() {
		fn(key, runtime)
	}
}

// Fibers returns a snapshot of every registered fiber (the root fiber is not
// included).
func (r *Registry) Fibers() []*Fiber {
	fibers := r.liveFibers()
	out := make([]*Fiber, 0, len(fibers))
	for _, data := range fibers {
		out = append(out, &Fiber{data: data})
	}
	return out
}

// FiberCount returns the number of registered fibers.
func (r *Registry) FiberCount() int { return len(r.liveFibers()) }

// --- quiescence ---

// IsIdle reports whether no lifecycle work is in flight anywhere in the tree.
// It is strict: the caller's own in-flight registrations are not excluded.
func (r *Registry) IsIdle() bool { return r.isIdleExcluding(0) }

func (r *Registry) isIdleExcluding(own int64) bool {
	if r.pendingOps.Load()-own > 0 {
		return false
	}
	for _, data := range r.liveFibers() {
		state := data.stateSnapshot()
		if state == Loading || state == Unloading {
			return false
		}
		if data.pendingOps.Load() > 0 {
			return false
		}
	}
	return true
}

// notifyIdle wakes every WhenIdle waiter: the current channel is closed and a
// fresh one installed.
func (r *Registry) notifyIdle() {
	r.idleMu.Lock()
	close(r.idleCh)
	r.idleCh = make(chan struct{})
	r.idleMu.Unlock()
}

// WhenIdle blocks until the tree is quiescent.
//
// It is recovery-aware in the Rust reference; the Go port completes teardown
// inline instead of parking frames, so waiting for idle is sufficient.
func (r *Registry) WhenIdle() {
	_ = r.WhenIdleContext(context.Background())
}

// WhenIdleContext is WhenIdle with cancellation.
func (r *Registry) WhenIdleContext(ctx context.Context) error {
	goid := currentGoroutineID()
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		// Capture the wake-up channel *before* checking the condition, so a
		// notify that happens between the check and the wait is either visible in
		// the state or wakes the captured (already closed) channel. Checking
		// first and capturing afterwards loses wakeups.
		r.idleMu.Lock()
		ch := r.idleCh
		r.idleMu.Unlock()
		if r.isIdleExcluding(r.registrationCountFor(goid)) {
			return nil
		}
		if ctx == nil {
			<-ch
			continue
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// --- registration windows ---

type registrationGuard struct {
	r    *Registry
	goid uint64
	once sync.Once
}

// BeginRegistration marks the start of a plugin registration→activation window
// so WhenIdle does not return early. The caller must End() it (the guard is a
// token; use defer guard.End()).
func (r *Registry) BeginRegistration() *registrationGuard {
	goid := currentGoroutineID()
	r.pendingOps.Add(1)
	r.regMu.Lock()
	r.regOwners[goid]++
	r.regMu.Unlock()
	r.notifyIdle()
	return &registrationGuard{r: r, goid: goid}
}

// End closes the registration window (idempotent).
func (g *registrationGuard) End() {
	if g == nil {
		return
	}
	g.once.Do(func() {
		g.r.regMu.Lock()
		if g.r.regOwners[g.goid] > 0 {
			g.r.regOwners[g.goid]--
			if g.r.regOwners[g.goid] == 0 {
				delete(g.r.regOwners, g.goid)
			}
		}
		g.r.regMu.Unlock()
		g.r.pendingOps.Add(-1)
		g.r.notifyIdle()
	})
}

func (r *Registry) registrationCountFor(goid uint64) int64 {
	r.regMu.Lock()
	defer r.regMu.Unlock()
	return int64(r.regOwners[goid])
}

// --- refresh ---

// Refresh re-evaluates every fiber once (bounded fixed point). It is the
// implementation of Context.Refresh and of the dependents refresh after service
// changes; re-entrant calls are coalesced through a dirty flag.
func (r *Registry) Refresh() { r.refreshAll(false) }

// RefreshAsync is the awaiting counterpart of Refresh: async plugins activate
// and unload through their async entry points.
func (r *Registry) RefreshAsync(ctx context.Context) { r.refreshAllAsync(ctx) }

func (r *Registry) refreshAll(asyncMode bool) {
	if !r.refreshMu.TryLock() {
		r.refreshDirty.Store(true)
		return
	}
	defer r.refreshMu.Unlock()
	for pass := 0; pass < 64; pass++ {
		r.refreshDirty.Store(false)
		changed := r.refreshPass(asyncMode)
		if !changed && !r.refreshDirty.Load() {
			return
		}
	}
}

func (r *Registry) refreshAllAsync(ctx context.Context) {
	if !r.refreshMu.TryLock() {
		r.refreshDirty.Store(true)
		return
	}
	defer r.refreshMu.Unlock()
	for pass := 0; pass < 64; pass++ {
		r.refreshDirty.Store(false)
		changed := r.refreshPassAsync(ctx)
		if !changed && !r.refreshDirty.Load() {
			return
		}
	}
}

func (r *Registry) refreshPass(asyncMode bool) bool {
	changed := false
	for _, data := range r.liveFibers() {
		ctxOpt := data.contextRef()
		if ctxOpt == nil {
			continue
		}
		// A synchronous refresh only handles sync plugins: async activation and
		// async unload require an await point (Rust: use RefreshAsync).
		if !asyncMode && data.isAsyncPlugin() {
			continue
		}
		actx := *ctxOpt
		switch data.stateSnapshot() {
		case Active:
			if !data.dependenciesSatisfied(actx) {
				if data.unloadForRefresh(actx, Pending, asyncMode, context.Background()) {
					changed = true
				}
			}
		case Pending:
			if data.dependenciesReady(actx) {
				if data.activateByKind(actx, asyncMode, context.Background()) {
					changed = true
				}
			}
		}
	}
	return changed
}

func (r *Registry) refreshPassAsync(ctx context.Context) bool {
	changed := false
	for _, data := range r.liveFibers() {
		ctxOpt := data.contextRef()
		if ctxOpt == nil {
			continue
		}
		actx := *ctxOpt
		switch data.stateSnapshot() {
		case Active:
			if !data.dependenciesSatisfied(actx) {
				if data.unloadForRefreshAsync(ctx, actx, Pending) {
					changed = true
				}
			}
		case Pending:
			if data.dependenciesReady(actx) {
				if data.activateByKindAsync(ctx, actx) {
					changed = true
				}
			}
		}
	}
	return changed
}

// --- replace ---

// Replace re-binds every fiber of the old runtime to a new plugin
// implementation, keeps fiber identity/uid and the retained raw config, moves
// the runtime to the new plugin's key and reloads each fiber through the normal
// activation pipeline.
//
// The returned fibers are the reloaded ones. A lifecycle rejection rolls the
// re-binding and already-applied reloads back (Rollback error); an activation
// failure after the swap is contained at the fiber level (the fiber lands in
// Failed and Ok is returned) — check Fiber.State()/ErrorDetail().
func (r *Registry) Replace(old PluginKey, newPlugin Plugin) ([]*Fiber, error) {
	metadata := r.resolveMetadata(pluginKind{sync: newPlugin})
	oldRuntime, ok := r.Get(old)
	if !ok || oldRuntime.FiberCount() == 0 {
		return nil, InvalidPluginError("replace: no runtime for key " + old.String())
	}
	newKey := metadata.key
	fibers := oldRuntime.snapshot()

	// Re-bind: move the runtime to the new key and swap every fiber's plugin.
	undoBind := r.bindReplacement(old, newKey, metadata, fibers)
	reloaded := make([]*Fiber, 0, len(fibers))
	for _, data := range fibers {
		handle := &Fiber{data: data}
		if err := handle.Restart(); err != nil {
			// Roll the binding back first, then reload every fiber that had
			// already been switched so the previous implementation is running
			// again (CORE-048 atomic rollback; XT-05 lifecycle rejection).
			undoBind()
			var rollbackErr error
			// Only fibers that were already reloaded with the new implementation
			// need a reload back; the rejecting fiber never started its reload
			// and its metadata was restored by undoBind.
			for _, done := range reloaded {
				if rbErr := done.Restart(); rbErr != nil && rollbackErr == nil {
					rollbackErr = rbErr
				}
			}
			if rollbackErr != nil {
				return nil, RollbackError(fmtCache("original: %v; rollback: %v", err, rollbackErr))
			}
			return nil, err
		}
		reloaded = append(reloaded, handle)
	}
	return reloaded, nil
}

// ReplaceAsync is the awaiting counterpart of Replace.
func (r *Registry) ReplaceAsync(ctx context.Context, old PluginKey, newPlugin AsyncPlugin) ([]*Fiber, error) {
	metadata := r.resolveMetadata(pluginKind{async: newPlugin})
	oldRuntime, ok := r.Get(old)
	if !ok || oldRuntime.FiberCount() == 0 {
		return nil, InvalidPluginError("replace: no runtime for key " + old.String())
	}
	newKey := metadata.key
	fibers := oldRuntime.snapshot()
	undoBind := r.bindReplacement(old, newKey, metadata, fibers)
	reloaded := make([]*Fiber, 0, len(fibers))
	for _, data := range fibers {
		handle := &Fiber{data: data}
		if err := handle.RestartAsync(ctx); err != nil {
			undoBind()
			var rollbackErr error
			for _, done := range reloaded {
				if rbErr := done.RestartAsync(ctx); rbErr != nil && rollbackErr == nil {
					rollbackErr = rbErr
				}
			}
			if rollbackErr != nil {
				return nil, RollbackError(fmtCache("original: %v; rollback: %v", err, rollbackErr))
			}
			return nil, err
		}
		reloaded = append(reloaded, handle)
	}
	return reloaded, nil
}

type replacementMetadata struct {
	kind   pluginKind
	name   string
	key    PluginKey
	inject []string
	schema ConfigSchema
}

func (r *Registry) resolveMetadata(kind pluginKind) replacementMetadata {
	return replacementMetadata{
		kind:   kind,
		name:   kind.name(),
		key:    kind.key(),
		inject: kind.inject(),
		schema: kind.schema(),
	}
}

// bindReplacement switches the runtime key and every fiber to the new plugin;
// the returned undo reverts the switch.
func (r *Registry) bindReplacement(old, newKey PluginKey, metadata replacementMetadata, fibers []*fiberData) func() {
	r.mu.Lock()
	oldRuntime := r.runtimes[old]
	delete(r.runtimes, old)
	oldName, oldKey := "", PluginKey{}
	if oldRuntime != nil {
		oldName, oldKey = oldRuntime.name, oldRuntime.key
	}
	target, exists := r.runtimes[newKey]
	if !exists {
		target = newPluginRuntime(metadata.name, newKey)
		r.runtimes[newKey] = target
	}
	r.mu.Unlock()
	// swapPluginMetadata moves each fiber to the new runtime entry.
	oldPlugins := make([]pluginKind, len(fibers))
	for i, data := range fibers {
		oldPlugins[i], _, _ = data.swapPluginMetadata(metadata, newKey)
	}
	return func() {
		for i, data := range fibers {
			data.restorePluginMetadata(oldPlugins[i], oldKey, oldName, oldKey)
		}
		r.pruneEmptyRuntimes()
	}
}

func removeFiberData(list []*fiberData, id uint64) []*fiberData {
	for i, data := range list {
		if data.id == id {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

// --- dispose all ---

// DisposeAll disposes every registered fiber and the root fiber, collecting the
// first cleanup error after attempting all of them.
//
// On a nil return the registry is empty and IsIdle() is true. A busy fiber stops
// that fiber with the first error and may leave the registry non-empty; the
// caller must retry after its lifecycle operation finishes. When a cleanup
// panics the first panic value wins over collected errors: it is re-raised after
// every fiber was attempted.
func (r *Registry) DisposeAll() error {
	var firstErr error
	var firstPanic any
	fibers := r.liveFibers()
	if root := r.rootFiber(); root != nil {
		fibers = append(fibers, root)
	}
	for _, data := range fibers {
		func() {
			defer func() {
				if p := recover(); p != nil {
					if firstPanic == nil {
						firstPanic = p
					} else {
						slog.Error("cleanup panicked during DisposeAll; contained",
							"fiber", data.id, "panic", panicText(p))
					}
				}
			}()
			if err := (&Fiber{data: data}).Dispose(); err != nil && firstErr == nil {
				firstErr = err
			}
		}()
	}
	if firstPanic != nil {
		if firstErr != nil {
			slog.Error("DisposeAll: cleanup panic wins over collected errors",
				"error", firstErr, "panic", panicText(firstPanic))
		}
		panic(firstPanic)
	}
	return firstErr
}

// --- quota (L2 supervision) ---

// SetQuota installs the per-fiber resource quota applied to the whole tree.
func (r *Registry) SetQuota(quota FiberQuota) {
	r.quotaMu.Lock()
	r.quota = quota
	r.quotaMu.Unlock()
}

// Quota returns the current per-fiber resource quota.
func (r *Registry) Quota() FiberQuota {
	r.quotaMu.Lock()
	defer r.quotaMu.Unlock()
	return r.quota
}

func (r *Registry) quotaLimits() FiberQuota { return r.Quota() }

var _ = fmtCache
var _ = weak.Make[contextInner]
