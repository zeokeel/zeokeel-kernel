package cordis

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// EventResult is what a synchronous event listener reports.
//
// Continue keeps the dispatch going; Bail(value) stops a bail/serial dispatch.
// A Bail of nil or false is treated as "keep going" (Cordis isBailed semantics).
type EventResult struct {
	bail  bool
	value Value
}

// Continue reports that the listener did not bail.
func Continue() EventResult { return EventResult{} }

// Bail reports a value that stops a bail/serial dispatch. Bailing with nil or
// false is ignored (isBailed semantics).
func Bail(value Value) EventResult { return EventResult{bail: true, value: value} }

// IsBailed reports whether the result carries a value that actually stops the
// chain (non-nil and not false).
func (r EventResult) IsBailed() bool {
	if !r.bail {
		return false
	}
	if r.value == nil {
		return false
	}
	if b, ok := r.value.(bool); ok && !b {
		return false
	}
	return true
}

// Value returns the bail value, if any.
func (r EventResult) Value() (Value, bool) {
	if r.IsBailed() {
		return r.value, true
	}
	return nil, false
}

// EventOptions configures an event listener registration.
type EventOptions struct {
	// Prepend inserts the listener before existing listeners of the same event.
	Prepend bool
	// Global receives the event regardless of context-scope filtering.
	Global bool
	// Once disposes the listener after its first invocation.
	Once bool
}

// ListenerFunc is a synchronous event listener. args are the dispatch payload.
type ListenerFunc func(args []Value) EventResult

// AsyncListenerFunc is an asynchronous event listener. In Go it is a blocking
// function; the async lane runs it on its own goroutine for parallel dispatch.
type AsyncListenerFunc func(ctx context.Context, args []Value) EventResult

// contextScope is the isolation map snapshot of a context
// (service/event name → isolation label id). Two contexts see each other's
// events only when their maps are equal (or the listener is global).
type contextScope map[string]uint64

func (s contextScope) clone() contextScope {
	if len(s) == 0 {
		return nil
	}
	out := make(contextScope, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}

func scopesEqual(a, b contextScope) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

type listenerBody struct {
	sync  ListenerFunc
	async AsyncListenerFunc
}

type listenerEntry struct {
	id     uint64
	owner  uint64
	body   listenerBody
	scope  contextScope
	global bool
	once   bool
	called atomic.Bool
}

type waterfallEntry[T any] struct {
	id     uint64
	owner  uint64
	cb     T
	scope  contextScope
	global bool
}

// Events is the event bus of a context tree. Listeners are owned by the fiber
// that registered them and are removed when that fiber unloads.
//
// There are two listener lanes because Go function types (like Rust closures)
// have one fixed signature: the sync lane (dispatched by Emit/Bail/Parallel and
// the synchronous Waterfall) and the async lane (dispatched by ParallelAsync/
// SerialAsync). Sync dispatch never runs async listeners. Both lanes share one
// registration order per event name, so SerialAsync interleaves sync and async
// listeners exactly as they were registered.
type Events struct {
	mu             sync.Mutex
	listeners      map[string][]*listenerEntry
	waterfall      map[string][]*waterfallEntry[WaterfallListenerFunc]
	asyncWaterfall map[string][]*waterfallEntry[AsyncWaterfallListenerFunc]
	nextID         atomic.Uint64
	internal       *InternalBus
}

// NewEvents creates an empty event bus with its own internal hook bus.
func NewEvents() *Events {
	return &Events{
		listeners:      make(map[string][]*listenerEntry),
		waterfall:      make(map[string][]*waterfallEntry[WaterfallListenerFunc]),
		asyncWaterfall: make(map[string][]*waterfallEntry[AsyncWaterfallListenerFunc]),
		internal:       NewInternalBus(),
	}
}

// Internal returns the typed internal/* hook bus attached to this event bus.
//
// Hooks registered here are not fiber-owned: nothing removes them on unload,
// the caller must dispose the returned Disposer explicitly. Prefer the
// Context.OnInternal* wrappers unless the hook must outlive the fiber.
func (e *Events) Internal() *InternalBus { return e.internal }

func passes(global bool, listenerScope, dispatchScope contextScope) bool {
	if global || dispatchScope == nil {
		return true
	}
	return scopesEqual(listenerScope, dispatchScope)
}

func (e *Events) listenerSnapshot(name string) []*listenerEntry {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]*listenerEntry(nil), e.listeners[name]...)
}

func (e *Events) removeListener(name string, id uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	list := e.listeners[name]
	for i, entry := range list {
		if entry.id == id {
			e.listeners[name] = append(list[:i], list[i+1:]...)
			break
		}
	}
}

func (e *Events) removeOwner(owner uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for name, list := range e.listeners {
		out := list[:0]
		for _, entry := range list {
			if entry.owner != owner {
				out = append(out, entry)
			}
		}
		if len(out) == 0 {
			delete(e.listeners, name)
		} else {
			e.listeners[name] = out
		}
	}
	for name, list := range e.waterfall {
		out := list[:0]
		for _, entry := range list {
			if entry.owner != owner {
				out = append(out, entry)
			}
		}
		if len(out) == 0 {
			delete(e.waterfall, name)
		} else {
			e.waterfall[name] = out
		}
	}
	for name, list := range e.asyncWaterfall {
		out := list[:0]
		for _, entry := range list {
			if entry.owner != owner {
				out = append(out, entry)
			}
		}
		if len(out) == 0 {
			delete(e.asyncWaterfall, name)
		} else {
			e.asyncWaterfall[name] = out
		}
	}
}

func callSyncListener(name string, callback ListenerFunc, args []Value) (result EventResult) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("event listener panicked; dispatch continues",
				"event", name, "panic", panicText(r))
			result = Continue()
		}
	}()
	return callback(args)
}

func callAsyncListener(ctx context.Context, name string, callback AsyncListenerFunc, args []Value) (result EventResult) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("async event listener panicked; dispatch continues",
				"event", name, "panic", panicText(r))
			result = Continue()
		}
	}()
	return callback(ctx, args)
}

// dispatchScope is the isolation map of the dispatching context (nil means
// unfiltered dispatch).
func (e *Events) emitScoped(name string, args []Value, scope contextScope) {
	e.internal.emitDispatch(DispatchEmit, name, args)
	for _, listener := range e.listenerSnapshot(name) {
		if listener.body.sync == nil || !passes(listener.global, listener.scope, scope) {
			continue
		}
		if listener.once && listener.called.Swap(true) {
			continue
		}
		_ = callSyncListener(name, listener.body.sync, args)
		if listener.once {
			e.removeListener(name, listener.id)
		}
	}
}

func (e *Events) bailScoped(name string, args []Value, scope contextScope) (Value, bool) {
	e.internal.emitDispatch(DispatchBail, name, args)
	for _, listener := range e.listenerSnapshot(name) {
		if listener.body.sync == nil || !passes(listener.global, listener.scope, scope) {
			continue
		}
		if listener.once && listener.called.Swap(true) {
			continue
		}
		result := callSyncListener(name, listener.body.sync, args)
		if listener.once {
			e.removeListener(name, listener.id)
		}
		if value, ok := result.Value(); ok {
			return value, true
		}
	}
	return nil, false
}

func (e *Events) parallelAsyncScoped(ctx context.Context, name string, args []Value, scope contextScope) {
	e.internal.emitDispatch(DispatchParallel, name, args)
	if ctx == nil {
		ctx = context.Background()
	}
	var pending []AsyncListenerFunc
	for _, listener := range e.listenerSnapshot(name) {
		if !passes(listener.global, listener.scope, scope) {
			continue
		}
		if listener.body.sync != nil {
			if listener.once && listener.called.Swap(true) {
				continue
			}
			_ = callSyncListener(name, listener.body.sync, args)
			if listener.once {
				e.removeListener(name, listener.id)
			}
			continue
		}
		if listener.body.async == nil {
			continue
		}
		if listener.once && listener.called.Swap(true) {
			continue
		}
		pending = append(pending, listener.body.async)
		if listener.once {
			e.removeListener(name, listener.id)
		}
	}
	if len(pending) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, callback := range pending {
		wg.Go(func() {
			_ = callAsyncListener(ctx, name, callback, args)
		})
	}
	wg.Wait()
}

func (e *Events) serialAsyncScoped(ctx context.Context, name string, args []Value, scope contextScope) (Value, bool) {
	e.internal.emitDispatch(DispatchSerial, name, args)
	if ctx == nil {
		ctx = context.Background()
	}
	for _, listener := range e.listenerSnapshot(name) {
		if !passes(listener.global, listener.scope, scope) {
			continue
		}
		var result EventResult
		switch {
		case listener.body.sync != nil:
			if listener.once && listener.called.Swap(true) {
				continue
			}
			result = callSyncListener(name, listener.body.sync, args)
		case listener.body.async != nil:
			if listener.once && listener.called.Swap(true) {
				continue
			}
			result = callAsyncListener(ctx, name, listener.body.async, args)
		default:
			continue
		}
		if listener.once {
			e.removeListener(name, listener.id)
		}
		if value, ok := result.Value(); ok {
			return value, true
		}
	}
	return nil, false
}

func (e *Events) waterfallScoped(name string, args []Value, terminal WaterfallTerminal, scope contextScope) Value {
	e.internal.emitDispatch(DispatchWaterfall, name, args)
	if terminal == nil {
		terminal = NullWaterfallTerminal()
	}
	return (&WaterfallNext{remaining: e.waterfallSnapshot(name, scope), args: args, terminal: terminal}).Next()
}

func (e *Events) waterfallSnapshot(name string, scope contextScope) []WaterfallListenerFunc {
	e.mu.Lock()
	defer e.mu.Unlock()
	list := e.waterfall[name]
	out := make([]WaterfallListenerFunc, 0, len(list))
	for _, entry := range list {
		if passes(entry.global, entry.scope, scope) {
			out = append(out, entry.cb)
		}
	}
	return out
}

func (e *Events) waterfallAsyncScoped(ctx context.Context, name string, args []Value, terminal AsyncWaterfallTerminal, scope contextScope) Value {
	e.internal.emitDispatch(DispatchWaterfall, name, args)
	if ctx == nil {
		ctx = context.Background()
	}
	if terminal == nil {
		terminal = NullAsyncWaterfallTerminal()
	}
	e.mu.Lock()
	list := e.asyncWaterfall[name]
	callbacks := make([]AsyncWaterfallListenerFunc, 0, len(list))
	for _, entry := range list {
		if passes(entry.global, entry.scope, scope) {
			callbacks = append(callbacks, entry.cb)
		}
	}
	e.mu.Unlock()
	return (&AsyncWaterfallNext{ctx: ctx, remaining: callbacks, args: args, terminal: terminal}).Next()
}

// ListenerCount returns the number of live listeners across both lanes and both
// waterfall chains.
func (e *Events) ListenerCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	total := 0
	for _, list := range e.listeners {
		total += len(list)
	}
	for _, list := range e.waterfall {
		total += len(list)
	}
	for _, list := range e.asyncWaterfall {
		total += len(list)
	}
	return total
}

// AsyncListenerCount returns the number of live async-lane listeners.
func (e *Events) AsyncListenerCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	total := 0
	for _, list := range e.listeners {
		for _, entry := range list {
			if entry.body.async != nil {
				total++
			}
		}
	}
	return total
}

// WaterfallListenerCount returns the number of live waterfall middlewares, sync
// and async.
func (e *Events) WaterfallListenerCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	total := 0
	for _, list := range e.waterfall {
		total += len(list)
	}
	for _, list := range e.asyncWaterfall {
		total += len(list)
	}
	return total
}

// --- internal registration helpers (used by Context) ---

func (e *Events) addListener(name string, owner uint64, options EventOptions, scope contextScope, callback ListenerFunc) *Disposer {
	return e.addListenerBody(name, owner, options, scope, listenerBody{sync: callback})
}

func (e *Events) addAsyncListener(name string, owner uint64, options EventOptions, scope contextScope, callback AsyncListenerFunc) *Disposer {
	return e.addListenerBody(name, owner, options, scope, listenerBody{async: callback})
}

func (e *Events) addListenerBody(name string, owner uint64, options EventOptions, scope contextScope, body listenerBody) *Disposer {
	entry := &listenerEntry{
		id:     e.nextID.Add(1),
		owner:  owner,
		body:   body,
		scope:  scope.clone(),
		global: options.Global,
		once:   options.Once,
	}
	e.mu.Lock()
	if options.Prepend {
		e.listeners[name] = append([]*listenerEntry{entry}, e.listeners[name]...)
	} else {
		e.listeners[name] = append(e.listeners[name], entry)
	}
	e.mu.Unlock()
	return NewDisposer([]Cleanup{func() { e.removeListener(name, entry.id) }})
}

func (e *Events) addWaterfallListener(name string, owner uint64, options EventOptions, scope contextScope, callback WaterfallListenerFunc) *Disposer {
	entry := &waterfallEntry[WaterfallListenerFunc]{
		id: e.nextID.Add(1), owner: owner, cb: callback, scope: scope.clone(), global: options.Global,
	}
	e.mu.Lock()
	if options.Prepend {
		e.waterfall[name] = append([]*waterfallEntry[WaterfallListenerFunc]{entry}, e.waterfall[name]...)
	} else {
		e.waterfall[name] = append(e.waterfall[name], entry)
	}
	e.mu.Unlock()
	return NewDisposer([]Cleanup{func() { e.removeWaterfallListener(name, entry.id) }})
}

func (e *Events) addAsyncWaterfallListener(name string, owner uint64, options EventOptions, scope contextScope, callback AsyncWaterfallListenerFunc) *Disposer {
	entry := &waterfallEntry[AsyncWaterfallListenerFunc]{
		id: e.nextID.Add(1), owner: owner, cb: callback, scope: scope.clone(), global: options.Global,
	}
	e.mu.Lock()
	if options.Prepend {
		e.asyncWaterfall[name] = append([]*waterfallEntry[AsyncWaterfallListenerFunc]{entry}, e.asyncWaterfall[name]...)
	} else {
		e.asyncWaterfall[name] = append(e.asyncWaterfall[name], entry)
	}
	e.mu.Unlock()
	return NewDisposer([]Cleanup{func() { e.removeAsyncWaterfallListener(name, entry.id) }})
}

func (e *Events) removeWaterfallListener(name string, id uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	list := e.waterfall[name]
	for i, entry := range list {
		if entry.id == id {
			e.waterfall[name] = append(list[:i], list[i+1:]...)
			break
		}
	}
}

func (e *Events) removeAsyncWaterfallListener(name string, id uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	list := e.asyncWaterfall[name]
	for i, entry := range list {
		if entry.id == id {
			e.asyncWaterfall[name] = append(list[:i], list[i+1:]...)
			break
		}
	}
}
