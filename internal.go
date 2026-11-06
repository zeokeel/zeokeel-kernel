package cordis

import (
	"sync"
	"sync/atomic"
)

// DispatchMode names a public event dispatch strategy (the payload of the
// internal/dispatch hook).
type DispatchMode uint8

const (
	DispatchEmit DispatchMode = iota + 1
	DispatchParallel
	DispatchSerial
	DispatchBail
	DispatchWaterfall
)

// String returns the lower-case name used in diagnostics.
func (m DispatchMode) String() string {
	switch m {
	case DispatchEmit:
		return "emit"
	case DispatchParallel:
		return "parallel"
	case DispatchSerial:
		return "serial"
	case DispatchBail:
		return "bail"
	case DispatchWaterfall:
		return "waterfall"
	default:
		return "unknown"
	}
}

// DispatchInfo is the internal/dispatch payload: a non-internal event is about
// to be delivered.
type DispatchInfo struct {
	Mode DispatchMode
	Name string
	Args []Value
}

// ListenerRequest is the internal/listener request: a listener is about to be
// registered.
type ListenerRequest struct {
	Name    string
	Prepend bool
	Global  bool
	Once    bool
	Owner   uint64
}

// ListenerOverride lets an internal/listener hook substitute a registration.
type ListenerOverride struct {
	Callback ListenerFunc
	Prepend  bool
	Global   bool
	Once     bool
}

// LifecycleEvent is one fiber lifecycle transition delivered to a
// LifecycleListener.
//
// All fields are a snapshot taken at transition time under the fiber state
// lock: later transitions never rewrite an already delivered event. Delivery
// can interleave across goroutines, so sort by (FiberID, Seq) when exact
// ordering matters.
type LifecycleEvent struct {
	// FiberID is the id of the fiber whose state changed (0 is the root fiber).
	FiberID uint64
	// Name is the display name of the fiber's plugin ("root" for the root).
	Name string
	// From is the state before the transition.
	From FiberState
	// To is the state after the transition.
	To FiberState
	// Seq is the monotonic per-fiber transition sequence.
	Seq uint64
}

// LifecycleListener subscribes to lifecycle broadcasts (Context.OnLifecycle).
//
// The subscription is not fiber-owned: it keeps receiving events until its
// Disposer is disposed, even after the installing fiber unloads. Panics are
// contained and logged; other subscribers still run.
type LifecycleListener func(*LifecycleEvent)

// Hook types for the typed internal/* surface. See the package documentation of
// the Rust reference's internal.rs for the event-to-hook mapping.
type (
	// PluginHook observes fiber creation (internal/plugin).
	PluginHook func(f *Fiber)
	// StatusHook observes a fiber state change; old is the previous state.
	StatusHook func(f *Fiber, old FiberState)
	// LifecycleHook is the internal broadcast hook (snapshot form).
	LifecycleHook func(f *Fiber, from, to FiberState, seq uint64)
	// ServiceHook observes service provision (present=true) or removal
	// (present=false, value nil).
	ServiceHook func(c Context, name string, value ServiceValue, present bool)
	// ConfigHook is one internal/config waterfall middleware.
	ConfigHook func(f *Fiber, config Value, next *ConfigNext) Value
	// UpdateHook is one internal/update waterfall middleware. A hook that
	// returns without calling next vetoes the restart.
	UpdateHook func(f *Fiber, config Value, noSave bool, next *UpdateNext)
	// GetHook is one internal/get waterfall middleware. Returning ok=false
	// means "service absent/invisible", not "skip this hook": call next.Next()
	// to continue the chain.
	GetHook func(c Context, name string, next *GetNext) (ServiceValue, bool)
	// SetHook is one internal/set waterfall middleware; it returns whether the
	// write was applied.
	SetHook func(c Context, name string, value ServiceValue, next *SetNext) bool
	// ListenerHook is the internal/listener bail hook: the first non-nil
	// override replaces the registration.
	ListenerHook func(req ListenerRequest) *ListenerOverride
	// DispatchHook observes public event dispatch.
	DispatchHook func(info DispatchInfo)
)

// ConfigNext continues the internal/config waterfall.
type ConfigNext struct {
	hooks []ConfigHook
	fiber *Fiber
}

// Next continues the chain with config; the terminal returns the final config.
func (n *ConfigNext) Next(config Value) Value {
	if len(n.hooks) == 0 {
		return config
	}
	hook, rest := n.hooks[0], n.hooks[1:]
	next := &ConfigNext{hooks: rest, fiber: n.fiber}
	return contained("cordis::internal", "internal/config", config, func() Value {
		return hook(n.fiber, config, next)
	})
}

// UpdateNext continues the internal/update waterfall.
//
// The terminal (raw-config store + restart) runs at most once; a hook that
// never calls Next vetoes the update.
type UpdateNext struct {
	hooks    []UpdateHook
	fiber    *Fiber
	terminal func(config Value, noSave bool) bool
	ran      *atomic.Bool
}

// Next continues the chain; it returns whether the terminal has run.
func (n *UpdateNext) Next(config Value, noSave bool) bool {
	if len(n.hooks) == 0 {
		if n.ran.Swap(true) {
			return true
		}
		n.terminal(config, noSave)
		return true
	}
	hook, rest := n.hooks[0], n.hooks[1:]
	next := &UpdateNext{hooks: rest, fiber: n.fiber, terminal: n.terminal, ran: n.ran}
	result := contained("cordis::internal", "internal/update", true, func() bool {
		hook(n.fiber, config, noSave, next)
		return true
	})
	if result {
		// A panicking hook is contained and falls through with the incoming
		// value so the terminal still commits (panic = allow, by contract).
		if !next.ran.Load() {
			return next.Next(config, noSave)
		}
	}
	return n.ran.Load()
}

// GetNext continues the internal/get waterfall.
type GetNext struct {
	hooks    []GetHook
	ctx      Context
	name     string
	terminal func() (ServiceValue, bool)
}

// Next resolves the service, running the remaining hooks.
func (n *GetNext) Next() (ServiceValue, bool) {
	if len(n.hooks) == 0 {
		return n.terminal()
	}
	hook, rest := n.hooks[0], n.hooks[1:]
	next := &GetNext{hooks: rest, ctx: n.ctx, name: n.name, terminal: n.terminal}
	type getOutcome struct {
		value ServiceValue
		ok    bool
	}
	outcome := contained("cordis::internal", "internal/get", getOutcome{}, func() getOutcome {
		value, ok := hook(n.ctx, n.name, next)
		return getOutcome{value: value, ok: ok}
	})
	if outcome.ok {
		return outcome.value, true
	}
	return next.Next()
}

// SetNext continues the internal/set waterfall.
type SetNext struct {
	hooks    []SetHook
	ctx      Context
	name     string
	value    ServiceValue
	terminal func() bool
}

// Next performs the write, running the remaining hooks.
func (n *SetNext) Next() bool {
	if len(n.hooks) == 0 {
		return n.terminal()
	}
	hook, rest := n.hooks[0], n.hooks[1:]
	next := &SetNext{hooks: rest, ctx: n.ctx, name: n.name, value: n.value, terminal: n.terminal}
	written := contained("cordis::internal", "internal/set", true, func() bool {
		return hook(n.ctx, n.name, n.value, next)
	})
	if !written {
		return false
	}
	return next.Next()
}

// hookList is an ordered hook list with id-based removal.
type hookList[T any] struct {
	next atomic.Uint64
	mu   sync.Mutex
	list []hookEntry[T]
}

type hookEntry[T any] struct {
	id   uint64
	hook T
}

func (h *hookList[T]) register(hook T) *Disposer {
	id := h.next.Add(1)
	h.mu.Lock()
	h.list = append(h.list, hookEntry[T]{id: id, hook: hook})
	h.mu.Unlock()
	bus := h
	return NewDisposer([]Cleanup{func() {
		bus.mu.Lock()
		for i, entry := range bus.list {
			if entry.id == id {
				bus.list = append(bus.list[:i], bus.list[i+1:]...)
				break
			}
		}
		bus.mu.Unlock()
	}})
}

func (h *hookList[T]) snapshot() []T {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]T, len(h.list))
	for i, entry := range h.list {
		out[i] = entry.hook
	}
	return out
}

func (h *hookList[T]) isEmpty() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.list) == 0
}

// InternalBus holds the typed internal/* hooks of one context tree.
//
// There are two registration entry points, and they differ in ownership:
// Context.OnInternal* registrations are owned by the calling fiber and removed
// when it unloads; InternalBus methods are caller-owned and must be disposed
// explicitly (advanced surface for embedder tooling). Both feed the same
// ordered hook list, which is shared by the whole context tree.
type InternalBus struct {
	plugin    hookList[PluginHook]
	status    hookList[StatusHook]
	lifecycle hookList[LifecycleHook]
	service   hookList[ServiceHook]
	config    hookList[ConfigHook]
	update    hookList[UpdateHook]
	get       hookList[GetHook]
	set       hookList[SetHook]
	listener  hookList[ListenerHook]
	dispatch  hookList[DispatchHook]

	getDepth      atomic.Int64
	dispatchDepth atomic.Int64
}

// NewInternalBus creates an empty hook bus.
func NewInternalBus() *InternalBus { return &InternalBus{} }

// OnPlugin registers an internal/plugin hook (caller-owned).
func (b *InternalBus) OnPlugin(h PluginHook) *Disposer { return b.plugin.register(h) }

// OnStatus registers an internal/status hook (caller-owned).
func (b *InternalBus) OnStatus(h StatusHook) *Disposer { return b.status.register(h) }

// OnService registers an internal/service hook (caller-owned).
func (b *InternalBus) OnService(h ServiceHook) *Disposer { return b.service.register(h) }

// OnConfig registers an internal/config waterfall hook (caller-owned).
func (b *InternalBus) OnConfig(h ConfigHook) *Disposer { return b.config.register(h) }

// OnUpdate registers an internal/update waterfall hook (caller-owned).
func (b *InternalBus) OnUpdate(h UpdateHook) *Disposer { return b.update.register(h) }

// OnGet registers an internal/get waterfall hook (caller-owned).
func (b *InternalBus) OnGet(h GetHook) *Disposer { return b.get.register(h) }

// OnSet registers an internal/set waterfall hook (caller-owned).
func (b *InternalBus) OnSet(h SetHook) *Disposer { return b.set.register(h) }

// OnListener registers an internal/listener hook (caller-owned).
func (b *InternalBus) OnListener(h ListenerHook) *Disposer { return b.listener.register(h) }

// OnDispatch registers an internal/dispatch observer (caller-owned).
func (b *InternalBus) OnDispatch(h DispatchHook) *Disposer { return b.dispatch.register(h) }

// Empty reports whether any hook of any kind is registered (fast-path guard).
func (b *InternalBus) Empty() bool {
	return b.plugin.isEmpty() && b.status.isEmpty() && b.lifecycle.isEmpty() &&
		b.service.isEmpty() && b.config.isEmpty() && b.update.isEmpty() &&
		b.get.isEmpty() && b.set.isEmpty() && b.listener.isEmpty() && b.dispatch.isEmpty()
}

func (b *InternalBus) onLifecycleHook(h LifecycleHook) *Disposer { return b.lifecycle.register(h) }

func (b *InternalBus) emitPlugin(f *Fiber) {
	for _, hook := range b.plugin.snapshot() {
		containedVoid("cordis::internal", "internal/plugin", func() { hook(f) })
	}
}

func (b *InternalBus) emitStatus(f *Fiber, old FiberState) {
	for _, hook := range b.status.snapshot() {
		hook := hook
		containedVoid("cordis::internal", "internal/status", func() { hook(f, old) })
	}
}

func (b *InternalBus) emitLifecycle(f *Fiber, from, to FiberState, seq uint64) {
	for _, hook := range b.lifecycle.snapshot() {
		hook := hook
		containedVoid("cordis::internal", "lifecycle broadcast", func() { hook(f, from, to, seq) })
	}
}

func (b *InternalBus) emitService(c Context, name string, value ServiceValue, present bool) {
	for _, hook := range b.service.snapshot() {
		hook := hook
		containedVoid("cordis::internal", "internal/service", func() { hook(c, name, value, present) })
	}
}

// emitDispatch reports a public dispatch (never for internal/ names, which would
// recurse into the bus itself).
func (b *InternalBus) emitDispatch(mode DispatchMode, name string, args []Value) {
	if len(name) >= 9 && name[:9] == "internal/" {
		return
	}
	if b.dispatch.isEmpty() {
		return
	}
	if b.dispatchDepth.Add(1) > 1 {
		b.dispatchDepth.Add(-1)
		return
	}
	info := DispatchInfo{Mode: mode, Name: name, Args: append([]Value(nil), args...)}
	for _, hook := range b.dispatch.snapshot() {
		hook := hook
		containedVoid("cordis::internal", "internal/dispatch", func() { hook(info) })
	}
	b.dispatchDepth.Add(-1)
}

func (b *InternalBus) waterfallConfig(f *Fiber, config Value) Value {
	if b.config.isEmpty() {
		return config
	}
	return (&ConfigNext{hooks: b.config.snapshot(), fiber: f}).Next(config)
}

func (b *InternalBus) waterfallUpdate(f *Fiber, config Value, noSave bool, terminal func(Value, bool) bool) bool {
	if b.update.isEmpty() {
		return terminal(config, noSave)
	}
	ran := &atomic.Bool{}
	return (&UpdateNext{hooks: b.update.snapshot(), fiber: f, terminal: terminal, ran: ran}).Next(config, noSave)
}

func (b *InternalBus) waterfallGet(c Context, name string, terminal func() (ServiceValue, bool)) (ServiceValue, bool) {
	if b.get.isEmpty() {
		return terminal()
	}
	// Global re-entrancy latch: while a get hook runs, further reads bypass all
	// get hooks and hit the store directly (prevents unbounded recursion).
	if b.getDepth.Add(1) > 1 {
		b.getDepth.Add(-1)
		return terminal()
	}
	value, ok := (&GetNext{hooks: b.get.snapshot(), ctx: c, name: name, terminal: terminal}).Next()
	b.getDepth.Add(-1)
	return value, ok
}

func (b *InternalBus) waterfallSet(c Context, name string, value ServiceValue, terminal func() bool) bool {
	if b.set.isEmpty() {
		return terminal()
	}
	return (&SetNext{hooks: b.set.snapshot(), ctx: c, name: name, value: value, terminal: terminal}).Next()
}

func (b *InternalBus) interceptListener(req ListenerRequest) *ListenerOverride {
	for _, hook := range b.listener.snapshot() {
		hook := hook
		override := contained[*ListenerOverride]("cordis::internal", "internal/listener", nil, func() *ListenerOverride {
			return hook(req)
		})
		if override != nil {
			return override
		}
	}
	return nil
}
