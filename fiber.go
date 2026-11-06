package cordis

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// MaxPluginDepth bounds recursive plugin loading along the fiber parent chain
// (Cordis MAX_PLUGIN_DEPTH). Context-only nesting (Extend/Intercept) consumes no
// budget.
const MaxPluginDepth = 256

// FiberState is the lifecycle state of one plugin activation.
type FiberState uint8

const (
	// Pending: waiting for the services declared in Inject.
	Pending FiberState = iota + 1
	// Loading: activation is running.
	Loading
	// Active: activation succeeded and the plugin is live.
	Active
	// Failed: activation failed; the fiber stays inert.
	Failed
	// Unloading: teardown is running.
	Unloading
	// Disposed: terminal state; a disposed fiber cannot be restarted.
	Disposed
)

// String returns the lower-case state name.
func (s FiberState) String() string {
	switch s {
	case Pending:
		return "pending"
	case Loading:
		return "loading"
	case Active:
		return "active"
	case Failed:
		return "failed"
	case Unloading:
		return "unloading"
	case Disposed:
		return "disposed"
	default:
		return "unknown"
	}
}

// EffectMeta is a diagnostics snapshot of one live effect.
type EffectMeta struct {
	// Label is the human-readable registration label (e.g. `ctx.on("event")`).
	Label string
	// Children are the effects registered while this effect's body ran.
	Children []EffectMeta
}

// effectNode is the mutable diagnostics node stored on a fiber.
type effectNode struct {
	mu       sync.Mutex
	label    string
	children []*effectNode
	parent   *effectNode
	// ownerChild is the owned child fiber id for `ctx.plugin` ownership effects
	// (0 for all other effects).
	ownerChild uint64
}

func newEffectNode(label string) *effectNode { return &effectNode{label: label} }

// fiberEffect is one effect owned by a fiber: its cleanup handle plus its
// diagnostics node. Sync and async effects share one ordered list so unloading
// keeps a single deterministic reverse-registration order.
type fiberEffect struct {
	syncDisp   *Disposer
	asyncDisp  *AsyncDisposer
	meta       *effectNode
	activation uint64
	internal   bool
	ownerChild uint64
}

func (e *fiberEffect) disposed() bool {
	if e.syncDisp != nil {
		return e.syncDisp.IsDisposed()
	}
	if e.asyncDisp != nil {
		return e.asyncDisp.IsDisposed()
	}
	return true
}

func (e *fiberEffect) disposeContained() (panicValue any) {
	if e.syncDisp != nil {
		return e.syncDisp.disposeContained()
	}
	if e.asyncDisp != nil {
		p, _ := e.asyncDisp.disposeBlockingContained(context.Background())
		return p
	}
	return nil
}

// fiberData is the shared core of a fiber. A Fiber handle is a thin pointer
// wrapper; like the Rust `Arc<Mutex<FiberData>>`, the data lives as long as any
// handle, its context, or the registry references it (Go GC does the lifetime
// management).
type fiberData struct {
	mu     sync.Mutex
	id     uint64
	uid    uint64
	state  FiberState
	plugin pluginKind
	// pluginName caches the contained plugin display name.
	pluginName string
	inject     []string
	schema     ConfigSchema

	config    Value
	rawConfig Value
	lastError error

	ctx    *contextInner
	parent *contextInner
	// createEpoch is the parent activation generation this fiber was created in
	// (stale-activation child rollback).
	createEpoch uint64
	registry    *Registry
	runtimeKey  PluginKey
	hasRuntime  bool

	effects       []*fiberEffect
	deferredAsync []*AsyncDisposer
	effectStack   []*effectNode
	depVersions   []depVersion
	children      []*fiberData
	provided      []string
	listenerCount int

	clock Clock

	cancelRequested  atomic.Bool
	disposeRequested atomic.Bool
	lastActivityMs   atomic.Int64
	activationEpoch  atomic.Uint64
	activatingEpoch  atomic.Uint64
	statusSeq        atomic.Uint64
	pendingOps       atomic.Int64

	lifecycleMu    sync.Mutex
	lifecycleOwner uint64
	lifecycleToken uint64

	settleMu sync.Mutex
	settleCh chan struct{}
}

type depVersion struct {
	name    string
	version uint64
}

// rootPlugin is the plugin of the root fiber: it has no apply body.
type rootPlugin struct{}

// Name implements Plugin.
func (rootPlugin) Name() string { return "root" }

// Activate implements Plugin.
func (rootPlugin) Activate(Context, Value) error { return nil }

// --- basic fiberData helpers ---

// injectSnapshot returns a copy of the fiber's inject declarations.
func (d *fiberData) injectSnapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.inject...)
}

// addProvided records a service name provided by this fiber (used to re-mark
// dependents dirty once the provider becomes Active).
func (d *fiberData) addProvided(name string) {
	d.mu.Lock()
	d.provided = append(d.provided, name)
	d.mu.Unlock()
}

// removeProvided forgets a provided service name.
func (d *fiberData) removeProvided(name string) {
	d.mu.Lock()
	for i, existing := range d.provided {
		if existing == name {
			d.provided = append(d.provided[:i], d.provided[i+1:]...)
			break
		}
	}
	d.mu.Unlock()
}

// isAsyncPlugin reports whether the fiber hosts an AsyncPlugin.
func (d *fiberData) isAsyncPlugin() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.plugin.async != nil
}

// providedNamesSnapshot returns a copy of the service names this fiber provides.
func (d *fiberData) providedNamesSnapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.provided...)
}

func (d *fiberData) nameSnapshot() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.pluginName
}

func (d *fiberData) stateSnapshot() FiberState {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state
}

func (d *fiberData) contextRef() *Context {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ctx == nil {
		return nil
	}
	return &Context{inner: d.ctx}
}

func (d *fiberData) parentRef() *Context {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.parent == nil {
		return nil
	}
	return &Context{inner: d.parent}
}

func (d *fiberData) lastErrorSnapshot() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastError
}

func (d *fiberData) setError(err error) {
	d.mu.Lock()
	d.lastError = err
	d.mu.Unlock()
}

func (d *fiberData) clockSnapshot() Clock {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.clock == nil {
		return SystemClock{}
	}
	return d.clock
}

func (d *fiberData) touchActivity() {
	d.lastActivityMs.Store(d.clockSnapshot().NowMonotonicMs())
}

// beginOperation marks a fiber-level in-flight lifecycle operation (activation,
// unload, restart, update, replace). The returned func ends it.
func (d *fiberData) beginOperation() func() {
	d.mu.Lock()
	registry := d.registry
	d.mu.Unlock()
	d.pendingOps.Add(1)
	return func() {
		d.pendingOps.Add(-1)
		d.notifySettle()
		// Quiescence waiters observe pending_ops dropping to zero; without this
		// notification a waiter that woke on the last state transition but
		// re-checked before the operation counter hit zero could park forever
		// (CORE-049 lost wakeup).
		if registry != nil {
			registry.notifyIdle()
		}
	}
}

// notifySettle wakes every AwaitSettled waiter.
func (d *fiberData) notifySettle() {
	d.settleMu.Lock()
	if d.settleCh != nil {
		close(d.settleCh)
		d.settleCh = nil
	}
	d.settleMu.Unlock()
}

func (d *fiberData) settleChan() <-chan struct{} {
	d.settleMu.Lock()
	defer d.settleMu.Unlock()
	if d.settleCh == nil {
		d.settleCh = make(chan struct{})
	}
	return d.settleCh
}

// transition sets the fiber state and publishes the status/lifecycle
// notifications. It is a no-op when the state is unchanged, and a disposed fiber
// is final (a reentrant dispose/restart during activation wins over a late
// transition, CORE-044).
func (d *fiberData) transition(next FiberState) {
	d.mu.Lock()
	if d.state == next || d.state == Disposed {
		d.mu.Unlock()
		return
	}
	old := d.state
	d.state = next
	seq := d.statusSeq.Add(1) - 1
	ctx := d.ctx
	d.lastActivityMs.Store(d.clockTick())
	d.mu.Unlock()

	if ctx != nil {
		handle := &Fiber{data: d}
		ctx.events.Internal().emitStatus(handle, old)
		ctx.events.Internal().emitLifecycle(handle, old, next, seq)
	}
	if d.registry != nil {
		d.registry.notifyIdle()
	}
}

// clockTick reads the fiber clock; callers hold the fiber lock.
func (d *fiberData) clockTick() int64 {
	clock := d.clock
	if clock == nil {
		return 0
	}
	return clock.NowMonotonicMs()
}

// transitionIfEpoch atomically transitions only when the fiber is still in
// expected state and its activation epoch is still epoch.
func (d *fiberData) transitionIfEpoch(expected, next FiberState, epoch uint64) bool {
	d.mu.Lock()
	if d.state != expected || d.state == Disposed || d.activationEpoch.Load() != epoch {
		d.mu.Unlock()
		return false
	}
	old := d.state
	d.state = next
	seq := d.statusSeq.Add(1) - 1
	ctx := d.ctx
	d.mu.Unlock()
	if ctx != nil {
		handle := &Fiber{data: d}
		ctx.events.Internal().emitStatus(handle, old)
		ctx.events.Internal().emitLifecycle(handle, old, next, seq)
	}
	if d.registry != nil {
		d.registry.notifyIdle()
	}
	return true
}

// --- lifecycle slot (operation-level token, CORE-045) ---

// beginLifecycleOp claims the fiber's lifecycle slot.
//
// It returns the operation token and whether the call was reentrant (the caller
// already owns an operation on this fiber on the current goroutine). A
// concurrent non-reentrant caller gets ErrLifecycleBusy instead of racing.
func (d *fiberData) beginLifecycleOp() (token uint64, reentrant bool, err error) {
	goid := currentGoroutineID()
	if current, ok := currentOpToken(goid); ok {
		d.lifecycleMu.Lock()
		owner := d.lifecycleOwner
		d.lifecycleMu.Unlock()
		if owner != 0 && owner == current {
			return current, true, nil
		}
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	if d.lifecycleOwner != 0 {
		return 0, false, ErrLifecycleBusy
	}
	d.lifecycleToken = nextOperationToken()
	token = d.lifecycleToken
	d.lifecycleOwner = token
	return token, false, nil
}

// endLifecycleOp releases the lifecycle slot.
func (d *fiberData) endLifecycleOp(token uint64, reentrant bool) {
	if reentrant {
		return
	}
	d.lifecycleMu.Lock()
	if d.lifecycleOwner == token {
		d.lifecycleOwner = 0
		d.lifecycleToken = 0
	}
	d.lifecycleMu.Unlock()
}

// withLifecycle runs fn while owning the fiber lifecycle slot (installing the
// operation token on the calling goroutine for reentrancy detection).
func (d *fiberData) withLifecycle(fn func(token uint64, reentrant bool) error) error {
	token, reentrant, err := d.beginLifecycleOp()
	if err != nil {
		return err
	}
	restore := pushCurrentOpToken(token)
	defer restore()
	defer d.endLifecycleOp(token, reentrant)
	d.touchActivity()
	return fn(token, reentrant)
}

// --- dependencies (space composability) ---

// dependenciesReady reports whether every injected service is strictly available
// (stored, check true, provider fiber Active) and unchanged since activation.
func (d *fiberData) dependenciesReady(c Context) bool {
	d.mu.Lock()
	inject := append([]string(nil), d.inject...)
	d.mu.Unlock()
	return c.allServicesAvailable(inject)
}

// dependenciesSatisfied reports whether every injected service is still
// available *and* has not changed version (provider swap / ctx.set).
func (d *fiberData) dependenciesSatisfied(c Context) bool {
	d.mu.Lock()
	inject := append([]string(nil), d.inject...)
	recorded := append([]depVersion(nil), d.depVersions...)
	d.mu.Unlock()
	for _, name := range inject {
		entry, ok := c.lookupForInjection(name)
		if !ok {
			return false
		}
		matched := false
		for _, dep := range recorded {
			if dep.name == name && dep.version == entry.version {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func (d *fiberData) recordDepVersions(c Context) {
	d.mu.Lock()
	inject := append([]string(nil), d.inject...)
	d.mu.Unlock()
	recorded := make([]depVersion, 0, len(inject))
	for _, name := range inject {
		if entry, ok := c.lookupNonStrict(name); ok {
			recorded = append(recorded, depVersion{name: name, version: entry.version})
		}
	}
	d.mu.Lock()
	d.depVersions = recorded
	d.mu.Unlock()
}

// --- effects ---

// pushEffect appends one effect to the fiber after checking that the fiber can
// still accept registrations.
func (d *fiberData) pushEffect(effect *fiberEffect) error {
	d.mu.Lock()
	if d.state == Disposed || d.state == Unloading {
		d.mu.Unlock()
		return ErrInactiveEffect
	}
	// Drop disposed entries so the list cannot grow unboundedly on churn.
	live := d.effects[:0]
	userEffects := 0
	for _, existing := range d.effects {
		if !existing.disposed() {
			live = append(live, existing)
			if !existing.internal {
				userEffects++
			}
		}
	}
	d.effects = live
	if !effect.internal {
		if limit := d.registry.quotaLimits().MaxEffects; limit > 0 && userEffects >= limit {
			d.mu.Unlock()
			return QuotaExceededError("effects")
		}
	}
	epoch := d.activatingEpoch.Load()
	if epoch == 0 {
		epoch = d.activationEpoch.Load()
	}
	effect.activation = epoch
	d.effects = append(d.effects, effect)
	d.mu.Unlock()
	return nil
}

// attachEffectNode wires a new effect node under the currently running effect
// (if any), so Fiber.Effects reports the nested tree.
func (d *fiberData) attachEffectNode(node *effectNode) {
	d.mu.Lock()
	var parent *effectNode
	if len(d.effectStack) > 0 {
		parent = d.effectStack[len(d.effectStack)-1]
	}
	d.mu.Unlock()
	if parent != nil {
		parent.mu.Lock()
		parent.children = append(parent.children, node)
		parent.mu.Unlock()
		node.mu.Lock()
		node.parent = parent
		node.mu.Unlock()
	}
}

func (d *fiberData) pushEffectStack(node *effectNode) {
	d.mu.Lock()
	d.effectStack = append(d.effectStack, node)
	d.mu.Unlock()
}

func (d *fiberData) popEffectStack(node *effectNode) {
	d.mu.Lock()
	if n := len(d.effectStack); n > 0 && d.effectStack[n-1] == node {
		d.effectStack = d.effectStack[:n-1]
	} else {
		for i := len(d.effectStack) - 1; i >= 0; i-- {
			if d.effectStack[i] == node {
				d.effectStack = append(d.effectStack[:i], d.effectStack[i+1:]...)
				break
			}
		}
	}
	d.mu.Unlock()
}

// registerSyncEffect owns a sync disposer with a fresh diagnostics node.
func (d *fiberData) registerSyncEffect(disposer *Disposer, label string, internal bool) error {
	node := newEffectNode(label)
	d.attachEffectNode(node)
	return d.pushEffect(&fiberEffect{syncDisp: disposer, meta: node, internal: internal})
}

// registerAsyncEffect owns an async disposer with a fresh diagnostics node.
func (d *fiberData) registerAsyncEffect(disposer *AsyncDisposer, label string, internal bool) error {
	node := newEffectNode(label)
	d.attachEffectNode(node)
	return d.pushEffect(&fiberEffect{asyncDisp: disposer, meta: node, internal: internal})
}

// registerChildOwnershipEffect registers the parent-owned effect that disposes a
// child fiber, tagged with the child id so a self-disposed child can detach it.
func (d *fiberData) registerChildOwnershipEffect(child *Fiber, childID uint64) error {
	node := newEffectNode("ctx.plugin")
	node.ownerChild = childID
	d.attachEffectNode(node)
	disposer := NewDisposer([]Cleanup{func() {
		containedVoid("cordis::fiber", "child ownership cleanup", func() {
			_ = child.Dispose()
		})
	}})
	return d.pushEffect(&fiberEffect{syncDisp: disposer, meta: node, internal: true, ownerChild: childID})
}

// detachChildOwnership removes the parent ownership effect and children entry of
// a child that disposed itself (bounded bookkeeping, R3-A4).
func (d *fiberData) detachChildOwnership(childID uint64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	var removed []*fiberEffect
	kept := d.effects[:0]
	for _, effect := range d.effects {
		if effect.ownerChild == childID {
			removed = append(removed, effect)
			continue
		}
		kept = append(kept, effect)
	}
	d.effects = kept
	children := d.children[:0]
	for _, child := range d.children {
		if child.id != childID {
			children = append(children, child)
		}
	}
	d.children = children
	d.mu.Unlock()
	for _, effect := range removed {
		// Discard: the child is already disposed, so the ownership cleanup is a
		// no-op; dropping the entry without running it keeps the deferred drain
		// (if any) owned by the user's retry.
		if effect.syncDisp != nil {
			// Discard without running: the child is already disposed.
			effect.syncDisp.discard()
		}
	}
}

// effectsSnapshot returns the live effect diagnostics tree.
func (d *fiberData) effectsSnapshot() []EffectMeta {
	d.mu.Lock()
	defer d.mu.Unlock()
	live := make(map[*effectNode]bool, len(d.effects))
	for _, effect := range d.effects {
		if !effect.disposed() {
			live[effect.meta] = true
		}
	}
	var roots []*effectNode
	for _, effect := range d.effects {
		if effect.disposed() {
			continue
		}
		if effect.meta.parent == nil {
			roots = append(roots, effect.meta)
		}
	}
	out := make([]EffectMeta, 0, len(roots))
	for _, node := range roots {
		out = append(out, snapshotNode(node, live))
	}
	return out
}

func snapshotNode(node *effectNode, live map[*effectNode]bool) EffectMeta {
	node.mu.Lock()
	defer node.mu.Unlock()
	meta := EffectMeta{Label: node.label}
	for _, child := range node.children {
		if live[child] {
			meta.Children = append(meta.Children, snapshotNode(child, live))
		}
	}
	return meta
}

func (d *fiberData) takeEffects() ([]*fiberEffect, []*AsyncDisposer) {
	d.mu.Lock()
	defer d.mu.Unlock()
	effects := d.effects
	deferred := d.deferredAsync
	d.effects = nil
	d.deferredAsync = nil
	return effects, deferred
}

// takeEffectsForEpoch removes and returns the effects of one activation attempt.
func (d *fiberData) takeEffectsForEpoch(epoch uint64) ([]*fiberEffect, []*AsyncDisposer) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var taken []*fiberEffect
	kept := d.effects[:0]
	for _, effect := range d.effects {
		if effect.activation == epoch {
			taken = append(taken, effect)
		} else {
			kept = append(kept, effect)
		}
	}
	d.effects = kept
	deferred := d.deferredAsync
	d.deferredAsync = nil
	return taken, deferred
}

func (d *fiberData) userEffectCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := 0
	for _, effect := range d.effects {
		if !effect.internal && !effect.disposed() {
			count++
		}
	}
	return count
}

// sweepChildrenContained disposes every child fiber of d, containing panics.
func (d *fiberData) sweepChildrenContained() {
	d.mu.Lock()
	children := append([]*fiberData(nil), d.children...)
	d.children = nil
	d.mu.Unlock()
	for _, child := range children {
		containedVoid("cordis::fiber", "child sweep", func() {
			_ = (&Fiber{data: child}).Dispose()
		})
	}
}

// sweepChildrenEpoch disposes children created during one activation attempt.
func (d *fiberData) sweepChildrenEpoch(epoch uint64) {
	d.mu.Lock()
	var sweep []*fiberData
	kept := d.children[:0]
	for _, child := range d.children {
		if child.createEpoch == epoch {
			sweep = append(sweep, child)
		} else {
			kept = append(kept, child)
		}
	}
	d.children = kept
	d.mu.Unlock()
	for _, child := range sweep {
		containedVoid("cordis::fiber", "stale child sweep", func() {
			_ = (&Fiber{data: child}).Dispose()
		})
	}
}

// --- drain helpers ---

// drainEffectsBlocking drains effects in reverse registration order, running
// async cleanups through the (optional) blocking executor. Every panic and error
// is isolated so one bad cleanup cannot skip the rest.
func drainEffectsBlocking(effects []*fiberEffect, deferred []*AsyncDisposer) (firstPanic any, firstErr error) {
	for i := len(effects) - 1; i >= 0; i-- {
		effect := effects[i]
		if effect.syncDisp != nil {
			if p := effect.syncDisp.disposeContained(); p != nil && firstPanic == nil {
				firstPanic = p
			}
			continue
		}
		if effect.asyncDisp != nil {
			var p any
			var err error
			runBlocking(context.Background(), func() {
				p, err = effect.asyncDisp.disposeBlockingContained(context.Background())
			})
			if p != nil && firstPanic == nil {
				firstPanic = p
			}
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	for i := len(deferred) - 1; i >= 0; i-- {
		disposer := deferred[i]
		var p any
		var err error
		runBlocking(context.Background(), func() {
			p, err = disposer.disposeBlockingContained(context.Background())
		})
		if p != nil && firstPanic == nil {
			firstPanic = p
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstPanic, firstErr
}

// drainEffectsAsync drains effects in reverse registration order, awaiting async
// cleanups. On ctx cancellation the cleanups that had not started are parked in
// the returned deferred slice (A-9), sync cleanups still run inline, and the
// caller finishes the unload in the target state.
func drainEffectsAsync(ctx context.Context, effects []*fiberEffect, deferred []*AsyncDisposer) (firstPanic any, firstErr error, cancelled bool, parked []*AsyncDisposer) {
	if ctx == nil {
		ctx = context.Background()
	}
	for i := len(effects) - 1; i >= 0; i-- {
		effect := effects[i]
		if effect.syncDisp != nil {
			if p := effect.syncDisp.disposeContained(); p != nil && firstPanic == nil {
				firstPanic = p
			}
			continue
		}
		if effect.asyncDisp == nil {
			continue
		}
		var p any
		var err error
		p = callContained(func() { err = effect.asyncDisp.DisposeAsync(ctx) })
		if p != nil && firstPanic == nil {
			firstPanic = p
		}
		if ctx.Err() != nil {
			// Cancelled: the current async effect restored its unstarted
			// cleanups; park it plus every remaining async effect and run the
			// remaining sync cleanups inline before finishing the teardown.
			cancelled = true
			parked = append(parked, effect.asyncDisp)
			for j := i - 1; j >= 0; j-- {
				remaining := effects[j]
				if remaining.syncDisp != nil {
					if rp := remaining.syncDisp.disposeContained(); rp != nil && firstPanic == nil {
						firstPanic = rp
					}
				} else if remaining.asyncDisp != nil {
					parked = append(parked, remaining.asyncDisp)
				}
			}
			parked = append(parked, deferred...)
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			return firstPanic, firstErr, true, parked
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// The deferred tail (parked by an earlier cancelled teardown) is drained by
	// this retry, in registration order.
	for i, disposer := range deferred {
		var err error
		p := callContained(func() { err = disposer.DisposeAsync(ctx) })
		if p != nil && firstPanic == nil {
			firstPanic = p
		}
		if ctx.Err() != nil {
			parked = append(parked, disposer)
			parked = append(parked, deferred[i+1:]...)
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			return firstPanic, firstErr, true, parked
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstPanic, firstErr, false, parked
}

// --- transitions for activation/unload ---

// beginActivationEpoch starts a new activation attempt generation.
func (d *fiberData) beginActivationEpoch() uint64 {
	return d.activationEpoch.Add(1)
}

func (d *fiberData) activationStale(epoch uint64) bool {
	return d.activationEpoch.Load() != epoch
}

func (d *fiberData) activationInvalidated() bool {
	state := d.stateSnapshot()
	return state == Disposed || state == Unloading
}

// --- unload ---

// unloadBatch is one fiber's teardown work taken off the fiber.
type unloadBatch struct {
	effects  []*fiberEffect
	deferred []*AsyncDisposer
	ctx      *contextInner
	withdrew []string
}

// beginUnload marks the fiber Unloading and takes its effect list. It returns
// nil when there is nothing left to do (an already fully disposed fiber).
func (d *fiberData) beginUnload() *unloadBatch {
	d.mu.Lock()
	if d.state == Disposed {
		if len(d.effects) == 0 && len(d.deferredAsync) == 0 {
			d.mu.Unlock()
			return nil
		}
		// Retry path: a cancelled async dispose parked cleanups for this call.
		batch := &unloadBatch{effects: d.effects, deferred: d.deferredAsync, ctx: d.ctx}
		d.effects = nil
		d.deferredAsync = nil
		d.mu.Unlock()
		return batch
	}
	batch := &unloadBatch{effects: d.effects, deferred: d.deferredAsync, ctx: d.ctx}
	d.effects = nil
	d.deferredAsync = nil
	state := d.state
	ctx := d.ctx
	d.mu.Unlock()
	if state != Unloading {
		d.transition(Unloading)
	}
	// Withdraw before draining: dependents deactivate while the binding is
	// still present, then the provider's own disposers remove it.
	if ctx != nil {
		batch.withdrew = (&Context{inner: ctx}).withdrawOwnerServices(d.id)
	}
	return batch
}

// finishUnload releases listeners/registrations and lands the fiber in target.
func (d *fiberData) finishUnload(batch *unloadBatch, target FiberState) {
	if batch.ctx != nil {
		batch.ctx.events.removeOwner(d.id)
	}
	d.mu.Lock()
	registry := d.registry
	d.mu.Unlock()
	d.transition(target)
	if target == Disposed {
		// A self-disposed child must detach its ownership effect from the parent
		// so dynamic load/unload churn cannot grow the parent's effect list.
		if parent := d.parentRef(); parent != nil {
			if pdata := parent.fiberData(); pdata != nil {
				pdata.detachChildOwnership(d.id)
			}
		}
		d.mu.Lock()
		d.ctx = nil
		d.parent = nil
		// Break the fiber→registry→root-fiber cycle so a disposed tree is
		// collectable (Go finalizers in cyclic garbage are not guaranteed to
		// run; CORE-033).
		d.registry = nil
		d.mu.Unlock()
		if registry != nil {
			registry.unregisterRuntimeFiber(d.id)
			registry.removeFiber(d.id)
		}
	}
	d.notifySettle()
	// Always wake quiescence waiters through the captured handle: the fiber's
	// own registry field may have been cleared above (CORE-049 lost wakeup).
	if registry != nil {
		registry.notifyIdle()
	}
}

// cascadeFrame is one dependent fiber whose unload was begun by the cascade.
type cascadeFrame struct {
	data  *fiberData
	batch *unloadBatch
}

// beginCascade begins the unload of every active fiber whose injected services
// became unavailable, transitively, in provider-before-dependent order. The
// caller drains the returned frames in reverse begin order (dependents before
// providers), matching the Rust two-phase cascade (CORE-042/CORE-047) while
// keeping the walk iterative and proportional to the affected dependents via the
// reverse dependency index.
func (d *fiberData) beginCascade(frontier []string) []cascadeFrame {
	registry := d.registry
	if registry == nil || len(frontier) == 0 {
		return nil
	}
	var frames []cascadeFrame
	visited := map[uint64]struct{}{d.id: {}}
	for len(frontier) > 0 {
		candidates := registry.dependentsOfNames(frontier)
		var next []string
		for _, other := range candidates {
			if _, seen := visited[other.id]; seen {
				continue
			}
			visited[other.id] = struct{}{}
			if other.stateSnapshot() != Active {
				continue
			}
			octx := other.contextRef()
			if octx == nil || other.dependenciesSatisfied(*octx) {
				continue
			}
			batch := other.beginUnload()
			if batch == nil {
				continue
			}
			frames = append(frames, cascadeFrame{data: other, batch: batch})
			next = append(next, batch.withdrew...)
		}
		frontier = next
	}
	return frames
}

// drainBatch runs one unload batch (sync or async) and returns the first panic,
// first error, whether the async drain was cancelled, and the deferred tail.
func drainBatch(asyncMode bool, ctx context.Context, effects []*fiberEffect, deferred []*AsyncDisposer) (firstPanic any, firstErr error, cancelled bool, parked []*AsyncDisposer) {
	if asyncMode {
		return drainEffectsAsync(ctx, effects, deferred)
	}
	firstPanic, firstErr = drainEffectsBlocking(effects, deferred)
	return firstPanic, firstErr, false, nil
}

// unload tears the fiber down to target, cascading transitively-unready
// dependents first. It returns the first captured cleanup panic (the caller
// decides whether to re-raise it) and whether the teardown was cancelled.
func (d *fiberData) unload(target FiberState, asyncMode bool, ctx context.Context) (firstPanic any, err error) {
	batch := d.beginUnload()
	if batch == nil {
		return nil, nil
	}
	// The withdrawal invalidates the dependency edges: mark the names dirty so
	// the indexed dependents refresh retries busy/skipped dependents later.
	for _, name := range batch.withdrew {
		if d.registry != nil {
			d.registry.noteServiceChanged(name)
		}
	}
	// Two-phase cascade: begin the provider, begin every transitively-unready
	// dependent (services withdrawn in provider-before-dependent order), then
	// drain the frames in reverse begin order (dependents before providers) and
	// finally this fiber. Cleanup panics of dependents are contained.
	frames := d.beginCascade(batch.withdrew)
	for i := len(frames) - 1; i >= 0; i-- {
		frame := frames[i]
		fp, ferr, _, parked := drainBatch(asyncMode, ctx, frame.batch.effects, frame.batch.deferred)
		if len(parked) > 0 {
			frame.data.mu.Lock()
			frame.data.deferredAsync = append(frame.data.deferredAsync, parked...)
			frame.data.mu.Unlock()
		}
		frame.data.finishUnload(frame.batch, Pending)
		if fp != nil {
			slog.Error("dependent cleanup panicked during cascade; contained",
				"fiber", frame.data.id, "panic", panicText(fp))
		}
		if ferr != nil {
			slog.Error("dependent cleanup failed during cascade; contained",
				"fiber", frame.data.id, "error", ferr)
		}
	}
	var parked []*AsyncDisposer
	if asyncMode {
		firstPanic, err, _, parked = drainEffectsAsync(ctx, batch.effects, batch.deferred)
	} else {
		firstPanic, err = drainEffectsBlocking(batch.effects, batch.deferred)
	}
	if len(parked) > 0 {
		d.mu.Lock()
		d.deferredAsync = append(d.deferredAsync, parked...)
		d.mu.Unlock()
	}
	d.finishUnload(batch, target)
	if d.registry != nil {
		d.registry.RefreshDependents()
	}
	return firstPanic, err
}

// unloadContained is unload with panic containment (used by refresh-driven
// unloads and dependent cascades: a cleanup panic must not abort the sweep).
func (d *fiberData) unloadContained(target FiberState, asyncMode bool, ctx context.Context) bool {
	panicValue := callContained(func() {
		_, _ = d.unload(target, asyncMode, ctx)
	})
	if panicValue != nil {
		slog.Error("cleanup panicked during contained unload; contained",
			"fiber", d.id, "panic", panicText(panicValue))
	}
	return true
}

// unloadForRefresh is the refresh-driven unload: it claims the lifecycle slot
// (v0.6 semantics) and skips the fiber when busy, leaving it to a later pass.
func (d *fiberData) unloadForRefresh(_ Context, target FiberState, asyncMode bool, ctx context.Context) bool {
	token, reentrant, err := d.beginLifecycleOp()
	if err != nil {
		return false
	}
	defer d.endLifecycleOp(token, reentrant)
	restore := pushCurrentOpToken(token)
	defer restore()
	endOp := d.beginOperation()
	defer endOp()
	return d.unloadContained(target, asyncMode, ctx)
}

func (d *fiberData) unloadForRefreshAsync(ctx context.Context, _ Context, target FiberState) bool {
	return d.unloadForRefresh(Context{}, target, true, ctx)
}

// --- activation ---

// activateByKind activates the fiber using its plugin kind when the fiber is
// Pending. It reports whether an activation was attempted.
func (d *fiberData) activateByKind(c Context, asyncMode bool, ctx context.Context) bool {
	d.mu.Lock()
	kind := d.plugin
	d.mu.Unlock()
	if kind.async != nil {
		if !asyncMode {
			return false
		}
		_ = d.activateAsync(c, ctx)
		return true
	}
	if kind.sync == nil {
		return false
	}
	_ = d.activateSync(c)
	return true
}

func (d *fiberData) activateByKindAsync(ctx context.Context, c Context) bool {
	return d.activateByKind(c, true, ctx)
}

// activateSync runs one synchronous activation attempt. It never returns an
// error: activation failure is fiber state (Failed) and is reported through
// Fiber.Error.
func (d *fiberData) activateSync(actx Context) (err error) {
	d.mu.Lock()
	if d.state == Disposed || d.state == Unloading {
		d.mu.Unlock()
		return nil
	}
	kind := d.plugin
	rawConfig := d.rawConfig
	d.lastError = nil
	d.mu.Unlock()

	endOp := d.beginOperation()
	defer endOp()
	epoch := d.beginActivationEpoch()
	d.transition(Loading)

	handle := &Fiber{data: d}
	resolved := actx.inner.events.Internal().waterfallConfig(handle, rawConfig)
	config, verr := validateConfig(kind, resolved)
	if verr != nil {
		if d.activationStale(epoch) {
			return nil
		}
		d.setError(verr)
		d.transition(Failed)
		actx.inner.registry.RefreshDependents()
		return nil
	}
	d.mu.Lock()
	d.config = config
	d.mu.Unlock()

	d.activatingEpoch.Store(epoch)
	var panicValue any
	var activateErr error
	panicValue = callContained(func() {
		activateErr = kind.sync.Activate(actx, config)
	})
	d.activatingEpoch.Store(0)

	switch {
	case panicValue != nil:
		if d.activationStale(epoch) {
			d.discardActivationEpoch(epoch)
			return nil
		}
		d.failActivation(actx, PluginFailedError("panic: "+panicText(panicValue)))
	case activateErr != nil:
		if d.activationStale(epoch) {
			d.discardActivationEpoch(epoch)
			return nil
		}
		d.failActivation(actx, normalizePluginError(activateErr))
	default:
		if d.activationStale(epoch) {
			d.discardActivationEpoch(epoch)
			return nil
		}
		if d.activationInvalidated() {
			d.discardActivationEffects()
			return nil
		}
		// XT-02: never publish Active when an injected service disappeared while
		// apply ran; roll this attempt back and stay Pending.
		if !d.dependenciesReady(actx) {
			d.rollbackActivationToPending(epoch)
			return nil
		}
		d.recordDepVersions(actx)
		d.transition(Active)
		// The provider is Active now: re-mark the services it provides dirty so
		// dependents that were Pending during apply activate in this pass.
		registry := actx.inner.registry
		for _, name := range d.providedNamesSnapshot() {
			registry.noteServiceChanged(name)
		}
		registry.RefreshDependents()
	}
	return nil
}

// activateAsync runs one asynchronous activation attempt.
func (d *fiberData) activateAsync(actx Context, ctx context.Context) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	d.mu.Lock()
	if d.state == Disposed || d.state == Unloading {
		d.mu.Unlock()
		return nil
	}
	kind := d.plugin
	rawConfig := d.rawConfig
	d.lastError = nil
	d.mu.Unlock()

	endOp := d.beginOperation()
	defer endOp()
	epoch := d.beginActivationEpoch()
	d.transition(Loading)
	// A cancelled activation must not stay Loading with live registrations.
	cancelled := false
	defer func() {
		if cancelled || (ctx.Err() != nil && d.stateSnapshot() == Loading) {
			d.discardActivationEpoch(epoch)
			if !d.activationInvalidated() {
				d.transitionIfEpoch(Loading, Pending, epoch)
			}
			actx.Refresh()
		}
	}()

	handle := &Fiber{data: d}
	resolved := actx.inner.events.Internal().waterfallConfig(handle, rawConfig)
	config, verr := validateConfig(kind, resolved)
	if verr != nil {
		if d.activationStale(epoch) {
			return nil
		}
		d.setError(verr)
		d.transition(Failed)
		actx.inner.registry.RefreshDependents()
		return nil
	}
	d.mu.Lock()
	d.config = config
	d.mu.Unlock()

	d.activatingEpoch.Store(epoch)
	var panicValue any
	var activateErr error
	panicValue = callContained(func() {
		if kind.async != nil {
			activateErr = kind.async.ActivateAsync(ctx, actx, config)
		} else if kind.sync != nil {
			activateErr = kind.sync.Activate(actx, config)
		}
	})
	d.activatingEpoch.Store(0)

	if ctx.Err() != nil && activateErr != nil {
		// Cancelled while awaiting: roll back and stay Pending (activate's
		// guard below finishes the rollback).
		cancelled = true
		return nil
	}
	switch {
	case panicValue != nil:
		if d.activationStale(epoch) {
			d.discardActivationEpoch(epoch)
			return nil
		}
		d.failActivation(actx, PluginFailedError("panic: "+panicText(panicValue)))
	case activateErr != nil:
		if d.activationStale(epoch) {
			d.discardActivationEpoch(epoch)
			return nil
		}
		d.failActivation(actx, normalizePluginError(activateErr))
	default:
		if d.activationStale(epoch) {
			d.discardActivationEpoch(epoch)
			return nil
		}
		if d.activationInvalidated() {
			d.discardActivationEffects()
			return nil
		}
		if !d.dependenciesReady(actx) {
			d.rollbackActivationToPending(epoch)
			return nil
		}
		d.recordDepVersions(actx)
		d.transition(Active)
		// The provider is Active now: re-mark the services it provides dirty so
		// dependents that were Pending during apply activate in this pass.
		registry := actx.inner.registry
		for _, name := range d.providedNamesSnapshot() {
			registry.noteServiceChanged(name)
		}
		registry.RefreshDependents()
	}
	return nil
}

func normalizePluginError(err error) error {
	var cordisErr *CordisError
	if errors.As(err, &cordisErr) {
		return err
	}
	return PluginFailedError(err.Error())
}

// failActivation rolls back everything registered by the failed attempt and
// leaves the fiber Failed (CORE-028/029).
func (d *fiberData) failActivation(actx Context, failure error) {
	effects, deferred := d.takeEffects()
	firstPanic, _ := drainEffectsBlocking(effects, deferred)
	actx.inner.events.removeOwner(d.id)
	d.sweepChildrenContained()
	d.setError(failure)
	if !d.activationInvalidated() {
		d.transition(Failed)
	}
	actx.inner.registry.RefreshDependents()
	if firstPanic != nil {
		slog.Error("cleanup panicked during failed activation rollback; contained",
			"panic", panicText(firstPanic))
	}
}

// rollbackActivationToPending rolls back a successful apply whose injected
// services disappeared mid-activation; the fiber stays Pending so a later refresh
// can reactivate it (XT-02).
func (d *fiberData) rollbackActivationToPending(epoch uint64) {
	effects, deferred := d.takeEffectsForEpoch(epoch)
	firstPanic, _ := drainEffectsBlocking(effects, deferred)
	d.sweepChildrenEpoch(epoch)
	if d.activationStale(epoch) {
		return
	}
	d.transitionIfEpoch(Loading, Pending, epoch)
	if firstPanic != nil {
		slog.Error("cleanup panicked during activation rollback; contained",
			"panic", panicText(firstPanic))
	}
	if ctx := d.contextRef(); ctx != nil {
		ctx.inner.registry.RefreshDependents()
	}
}

// discardActivationEffects drops registrations of an activation that lost the
// race against a concurrent dispose/restart.
func (d *fiberData) discardActivationEffects() {
	effects, deferred := d.takeEffects()
	drainEffectsBlocking(effects, deferred)
	d.sweepChildrenContained()
	if ctx := d.contextRef(); ctx != nil {
		ctx.inner.events.removeOwner(d.id)
	}
}

// discardActivationEpoch drops only the registrations of one stale attempt.
func (d *fiberData) discardActivationEpoch(epoch uint64) {
	effects, deferred := d.takeEffectsForEpoch(epoch)
	drainEffectsBlocking(effects, deferred)
	d.sweepChildrenEpoch(epoch)
}

// validateConfig validates config with the plugin schema, containing panics.
func validateConfig(kind pluginKind, config Value) (Value, error) {
	schema := kind.schema()
	if schema == nil {
		return config, nil
	}
	value, err := containedValidate(schema, config)
	if err != nil {
		var validation *ValidationError
		if errors.As(err, &validation) {
			return nil, ValidationErrorOf(validation)
		}
		return nil, ValidationErrorOf(NewValidationError([]string{"invalid config"}))
	}
	return value, nil
}

// --- Fiber handle ---

// Fiber is a handle to one plugin activation.
type Fiber struct {
	data *fiberData
}

func (f *Fiber) fiberData() *fiberData {
	if f == nil {
		return nil
	}
	return f.data
}

// Name returns the plugin display name ("root" for the root fiber).
func (f *Fiber) Name() string {
	if d := f.fiberData(); d != nil {
		return d.nameSnapshot()
	}
	return ""
}

// ID returns the fiber id (the root fiber is 0).
func (f *Fiber) ID() uint64 {
	if d := f.fiberData(); d != nil {
		return d.id
	}
	return 0
}

// UID returns the registry uid assigned at creation (same as ID in the Go port).
//
// A disposed fiber reports no uid, matching the Rust reference's
// `Fiber::uid() -> Option<u64>` after disposal (CORE-026).
func (f *Fiber) UID() (uint64, bool) {
	d := f.fiberData()
	if d == nil {
		return 0, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == Disposed {
		return 0, false
	}
	return d.uid, true
}

// Parent returns the fiber's parent context, if any.
func (f *Fiber) Parent() (Context, bool) {
	if d := f.fiberData(); d != nil {
		if parent := d.parentRef(); parent != nil {
			return *parent, true
		}
	}
	return Context{}, false
}

// State returns the current lifecycle state.
func (f *Fiber) State() FiberState {
	if d := f.fiberData(); d != nil {
		return d.stateSnapshot()
	}
	return Disposed
}

// IsActive reports whether the fiber is Active.
func (f *Fiber) IsActive() bool { return f.State() == Active }

// AssertActive returns ErrInactiveEffect when the fiber is not Active
// (Cordis `fiber.assert_active`).
func (f *Fiber) AssertActive() error {
	if f.IsActive() {
		return nil
	}
	return ErrInactiveEffect
}

// Context returns the fiber's own context (nil after dispose).
func (f *Fiber) Context() (Context, bool) {
	if d := f.fiberData(); d != nil {
		if ctx := d.contextRef(); ctx != nil {
			return *ctx, true
		}
	}
	return Context{}, false
}

// Config returns the resolved (validated/normalized) config in effect.
func (f *Fiber) Config() Value {
	if d := f.fiberData(); d != nil {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.config
	}
	return nil
}

// RawConfig returns the caller-supplied raw config retained for re-resolution.
func (f *Fiber) RawConfig() Value {
	if d := f.fiberData(); d != nil {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.rawConfig
	}
	return nil
}

// Error returns the last failed activation's error message (nil when not
// failed).
func (f *Fiber) Error() error {
	if d := f.fiberData(); d != nil {
		if d.stateSnapshot() == Failed {
			return d.lastErrorSnapshot()
		}
	}
	return nil
}

// ErrorDetail returns the structured error of the last failed activation.
func (f *Fiber) ErrorDetail() error {
	if d := f.fiberData(); d != nil {
		return d.lastErrorSnapshot()
	}
	return nil
}

// Effects returns the live effect diagnostics tree.
func (f *Fiber) Effects() []EffectMeta {
	if d := f.fiberData(); d != nil {
		return d.effectsSnapshot()
	}
	return nil
}

// Cancel requests cooperative cancellation; long-running plugin work should
// observe it through Context.Checkpoint.
func (f *Fiber) Cancel() {
	if d := f.fiberData(); d != nil {
		d.cancelRequested.Store(true)
	}
}

// IsCancelled reports whether Cancel was requested.
func (f *Fiber) IsCancelled() bool {
	if d := f.fiberData(); d != nil {
		return d.cancelRequested.Load()
	}
	return false
}

// Heartbeat refreshes the fiber's watchdog activity timestamp.
func (f *Fiber) Heartbeat() {
	if d := f.fiberData(); d != nil {
		d.lastActivityMs.Store(d.clockSnapshot().NowMonotonicMs())
	}
}

// LastActivityMs returns the monotonic milliseconds of the last observed
// activity.
func (f *Fiber) LastActivityMs() int64 {
	if d := f.fiberData(); d != nil {
		return d.lastActivityMs.Load()
	}
	return 0
}

// IdleFor returns how long the fiber has been quiet.
func (f *Fiber) IdleFor() time.Duration {
	if d := f.fiberData(); d != nil {
		now := d.clockSnapshot().NowMonotonicMs()
		idle := now - d.lastActivityMs.Load()
		if idle < 0 {
			idle = 0
		}
		return time.Duration(idle) * time.Millisecond
	}
	return 0
}

// AwaitSettled blocks until no fiber-level lifecycle operation is in flight and
// returns the structured activation error for a Failed fiber.
func (f *Fiber) AwaitSettled() error {
	d := f.fiberData()
	if d == nil {
		return nil
	}
	for {
		// Capture the settle channel before reading the counter (see the
		// quiescence loop for the lost-wakeup rationale).
		ch := d.settleChan()
		d.mu.Lock()
		ops := d.pendingOps.Load()
		state := d.state
		err := d.lastError
		d.mu.Unlock()
		if ops == 0 {
			if state == Failed && err != nil {
				return err
			}
			return nil
		}
		<-ch
	}
}

// AwaitSettledContext is AwaitSettled with cancellation.
func (f *Fiber) AwaitSettledContext(ctx context.Context) error {
	d := f.fiberData()
	if d == nil {
		return nil
	}
	for {
		ch := d.settleChan()
		d.mu.Lock()
		ops := d.pendingOps.Load()
		state := d.state
		err := d.lastError
		d.mu.Unlock()
		if ops == 0 {
			if state == Failed && err != nil {
				return err
			}
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Dispose unloads the fiber: effects run in reverse registration order, services
// and listeners are released, and the fiber becomes Disposed. Idempotent.
//
// It returns LifecycleBusy when another lifecycle operation owns the fiber. The
// first cleanup panic is re-raised after the drain completes.
func (f *Fiber) Dispose() error {
	d := f.fiberData()
	if d == nil {
		return nil
	}
	if d.stateSnapshot() == Disposed {
		d.mu.Lock()
		leftover := len(d.effects) > 0 || len(d.deferredAsync) > 0
		d.mu.Unlock()
		if !leftover {
			return nil
		}
	}
	return d.withLifecycle(func(_ uint64, _ bool) error {
		endOp := d.beginOperation()
		defer endOp()
		firstPanic, err := d.unload(Disposed, false, context.Background())
		if firstPanic != nil {
			panic(firstPanic)
		}
		return err
	})
}

// DisposeAsync unloads the fiber awaiting every async cleanup.
//
// Cancelling ctx mid-teardown completes the teardown instead of restoring the old
// state: sync cleanups run inline, async cleanups are parked for the next
// dispose, and the fiber lands Disposed (A-9). The returned error is ctx.Err()
// in that case.
func (f *Fiber) DisposeAsync(ctx context.Context) error {
	d := f.fiberData()
	if d == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if d.stateSnapshot() == Disposed {
		d.mu.Lock()
		leftover := len(d.effects) > 0 || len(d.deferredAsync) > 0
		d.mu.Unlock()
		if !leftover {
			return nil
		}
	}
	return d.withLifecycle(func(_ uint64, _ bool) error {
		endOp := d.beginOperation()
		defer endOp()
		firstPanic, err := d.unload(Disposed, true, ctx)
		if firstPanic != nil {
			panic(firstPanic)
		}
		return err
	})
}

// Abandon is the explicit Go analogue of dropping the last Rust Fiber handle
// without disposing: it runs sync cleanups, and either runs async cleanups
// through an installed BlockingExecutor or hands them to the orphan queue
// (observable through OrphanAsyncCleanupsPending and recoverable with
// DrainOrphanAsyncCleanups). Go has no deterministic destructor, so dropping a
// handle does nothing; callers that cannot dispose explicitly should call
// Abandon.
func (f *Fiber) Abandon() {
	d := f.fiberData()
	if d == nil {
		return
	}
	token, reentrant, err := d.beginLifecycleOp()
	if err != nil {
		slog.Error("Abandon skipped: fiber lifecycle is busy; dispose it explicitly",
			"fiber", d.id)
		return
	}
	restore := pushCurrentOpToken(token)
	defer restore()
	defer d.endLifecycleOp(token, reentrant)

	batch := d.beginUnload()
	if batch == nil {
		return
	}
	effects := batch.effects
	deferred := batch.deferred
	for i := len(effects) - 1; i >= 0; i-- {
		effect := effects[i]
		switch {
		case effect.syncDisp != nil:
			effect.syncDisp.Dispose()
		case effect.asyncDisp != nil:
			if HasBlockingExecutor() {
				containedVoid("cordis::fiber", "abandon async cleanup", func() {
					_ = effect.asyncDisp.DisposeBlocking(context.Background())
				})
			} else {
				orphanAsyncDisposer(effect.asyncDisp)
			}
		}
	}
	for _, disposer := range deferred {
		if HasBlockingExecutor() {
			containedVoid("cordis::fiber", "abandon deferred async cleanup", func() {
				_ = disposer.DisposeBlocking(context.Background())
			})
		} else {
			orphanAsyncDisposer(disposer)
		}
	}
	d.finishUnload(batch, Disposed)
}

// Restart unloads and reactivates the fiber with its current config (hot reload).
//
// Ok does not mean Active: an activation failure is fiber state, reported through
// State/ErrorDetail.
func (f *Fiber) Restart() error {
	return f.restart(false, context.Background())
}

// RestartAsync is the awaiting counterpart of Restart.
func (f *Fiber) RestartAsync(ctx context.Context) error {
	return f.restart(true, ctx)
}

func (f *Fiber) restart(allowAsync bool, ctx context.Context) error {
	d := f.fiberData()
	if d == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return d.withLifecycle(func(_ uint64, _ bool) error {
		endOp := d.beginOperation()
		defer endOp()
		actx, kind, raw, err := d.beginRestart(allowAsync)
		if err != nil {
			return err
		}
		if actx == nil {
			return nil
		}
		firstPanic, err := d.unload(Pending, allowAsync, ctx)
		if firstPanic != nil {
			panic(firstPanic)
		}
		if err != nil {
			return err
		}
		d.mu.Lock()
		d.config = raw
		d.rawConfig = raw
		d.mu.Unlock()
		if d.dependenciesReady(*actx) {
			if allowAsync && kind.async != nil {
				_ = d.activateAsync(*actx, ctx)
			} else {
				_ = d.activateSync(*actx)
			}
		}
		return nil
	})
}

// beginRestart validates restartability and captures the activation inputs.
func (d *fiberData) beginRestart(allowAsync bool) (*Context, pluginKind, Value, error) {
	d.mu.Lock()
	state := d.state
	kind := d.plugin
	raw := d.rawConfig
	actx := d.ctx
	d.mu.Unlock()
	if state == Disposed {
		return nil, pluginKind{}, nil, ErrDisposed
	}
	if state == Unloading {
		return nil, pluginKind{}, nil, ErrInactiveEffect
	}
	if !allowAsync && kind.async != nil {
		return nil, pluginKind{}, nil, AsyncPluginNeedsAsyncError(kind.name())
	}
	if actx == nil {
		return nil, pluginKind{}, nil, ErrDisposed
	}
	return &Context{inner: actx}, kind, raw, nil
}

// Update replaces the raw config and restarts the fiber (Cordis fiber.update).
//
// Order: internal/config waterfall → schema validation → internal/update
// waterfall; only the terminal commits the raw config and restarts. A validation
// failure returns before touching the fiber; a hook that never calls Next vetoes
// the update (Ok, no restart).
func (f *Fiber) Update(config Value) error { return f.UpdateWithNoSave(config, false) }

// UpdateWithNoSave is Update with the Cordis update(config, noSave) hint.
func (f *Fiber) UpdateWithNoSave(config Value, noSave bool) error {
	return f.update(false, config, noSave, context.Background())
}

// UpdateAsync is the awaiting counterpart of Update.
func (f *Fiber) UpdateAsync(config Value) error {
	return f.UpdateAsyncWithNoSave(config, false)
}

// UpdateAsyncWithNoSave is UpdateWithNoSave through the async lifecycle entry
// point.
func (f *Fiber) UpdateAsyncWithNoSave(config Value, noSave bool) error {
	return f.update(true, config, noSave, context.Background())
}

func (f *Fiber) update(allowAsync bool, config Value, noSave bool, ctx context.Context) error {
	d := f.fiberData()
	if d == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return d.withLifecycle(func(_ uint64, _ bool) error {
		endOp := d.beginOperation()
		defer endOp()
		actx, kind, err := d.resolveUpdate(allowAsync, config, noSave)
		if err != nil {
			return err
		}
		if actx == nil || kind.sync == nil && kind.async == nil {
			return nil
		}
		if !allowAsync && kind.async != nil {
			return AsyncPluginNeedsAsyncError(kind.name())
		}
		// Reload: unload to Pending, then activate with the new raw config.
		firstPanic, unloadErr := d.unload(Pending, allowAsync, ctx)
		if firstPanic != nil {
			panic(firstPanic)
		}
		if unloadErr != nil {
			return unloadErr
		}
		if d.dependenciesReady(*actx) {
			if allowAsync && kind.async != nil {
				_ = d.activateAsync(*actx, ctx)
			} else {
				_ = d.activateSync(*actx)
			}
		}
		return nil
	})
}

// resolveUpdate runs the config pipeline and commits the new raw config when the
// update is not vetoed. It returns the context for the reload.
func (d *fiberData) resolveUpdate(allowAsync bool, config Value, noSave bool) (*Context, pluginKind, error) {
	d.mu.Lock()
	state := d.state
	kind := d.plugin
	actx := d.ctx
	d.mu.Unlock()
	if state == Disposed {
		return nil, pluginKind{}, ErrDisposed
	}
	if state == Unloading {
		return nil, pluginKind{}, ErrInactiveEffect
	}
	if !allowAsync && kind.async != nil {
		return nil, pluginKind{}, AsyncPluginNeedsAsyncError(kind.name())
	}
	if actx == nil {
		return nil, pluginKind{}, ErrDisposed
	}
	actxValue := Context{inner: actx}
	handle := &Fiber{data: d}
	// 1. internal/config waterfall on the caller config.
	resolved := actx.events.Internal().waterfallConfig(handle, config)
	// 2. schema validation: a rejected update keeps the old raw config and does
	//    not restart.
	validated, verr := validateConfig(kind, resolved)
	if verr != nil {
		return nil, pluginKind{}, verr
	}
	// 3. internal/update waterfall: only the terminal commits the raw config.
	rawForTerminal := config
	rejected := (*ValidationError)(nil)
	proceeded := actx.events.Internal().waterfallUpdate(handle, validated, noSave, func(replaced Value, gotNoSave bool) bool {
		if !valueEqual(replaced, validated) {
			// A hook replaced the config: that replacement becomes the new raw
			// config and must itself be schema-valid.
			if _, replaceErr := validateConfig(kind, replaced); replaceErr != nil {
				var validation *ValidationError
				if errors.As(replaceErr, &validation) {
					rejected = validation
				} else {
					rejected = NewValidationError([]string{"invalid config"})
				}
				return false
			}
			d.storeRawConfig(replaced)
			return true
		}
		// Pass-through: store the caller's *raw* config so derived values do not
		// compound on the next activation (CORE-040).
		d.storeRawConfig(rawForTerminal)
		return true
	})
	if rejected != nil {
		return nil, pluginKind{}, ValidationErrorOf(rejected)
	}
	if !proceeded {
		// Vetoed: Ok, no restart, retained config unchanged.
		return nil, pluginKind{}, nil
	}
	_ = actxValue
	return &actxValue, kind, nil
}

func (d *fiberData) storeRawConfig(config Value) {
	d.mu.Lock()
	d.rawConfig = config
	d.mu.Unlock()
}

func valueEqual(a, b Value) bool {
	return reflect.DeepEqual(a, b)
}

// ReplacePlugin swaps the fiber's implementation in place, keeping fiber
// identity/uid and the retained raw config, then reloads it (CORE-048).
func (f *Fiber) ReplacePlugin(plugin Plugin) error {
	return f.replacePlugin(pluginKind{sync: plugin}, false, context.Background())
}

// ReplacePluginAsync is the awaiting counterpart of ReplacePlugin.
func (f *Fiber) ReplacePluginAsync(ctx context.Context, plugin AsyncPlugin) error {
	return f.replacePlugin(pluginKind{async: plugin}, true, ctx)
}

func (f *Fiber) replacePlugin(kind pluginKind, allowAsync bool, ctx context.Context) error {
	d := f.fiberData()
	if d == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return d.withLifecycle(func(_ uint64, _ bool) error {
		endOp := d.beginOperation()
		defer endOp()
		oldKind, oldKey, oldName := d.swapPluginMetadata(replacementMetadata{
			kind: kind, name: kind.name(), key: kind.key(), inject: kind.inject(), schema: kind.schema(),
		}, kind.key())
		newMetadata := replacementMetadata{kind: kind, name: kind.name(), key: kind.key(), inject: kind.inject(), schema: kind.schema()}
		// Reload through the normal pipeline.
		actx := d.contextRef()
		if actx == nil {
			d.restorePluginMetadata(oldKind, oldKey, oldName, oldKey)
			return ErrDisposed
		}
		firstPanic, err := d.unload(Pending, allowAsync, ctx)
		if firstPanic != nil {
			// A reload panic is contained and reported as a plugin failure, then
			// the old implementation is restored.
			d.restorePluginMetadata(oldKind, oldKey, oldName, oldKey)
			return PluginFailedError("replace: reload panicked: " + panicText(firstPanic))
		}
		if err != nil {
			d.restorePluginMetadata(oldKind, oldKey, oldName, oldKey)
			_ = d.reloadAfterRestore(actx, allowAsync, ctx)
			return err
		}
		if d.dependenciesReady(*actx) {
			if allowAsync { //nolint:staticcheck // explicit branch clarity
				_ = d.activateAsync(*actx, ctx)
			} else if newMetadata.kind.async != nil && !allowAsync {
				_ = d.activateAsync(*actx, ctx)
			} else {
				_ = d.activateSync(*actx)
			}
		}
		return nil
	})
}

func (d *fiberData) reloadAfterRestore(actx *Context, allowAsync bool, ctx context.Context) error {
	firstPanic, err := d.unload(Pending, allowAsync, ctx)
	if firstPanic != nil {
		return PluginFailedError("replace rollback: reload panicked: " + panicText(firstPanic))
	}
	if err != nil {
		return err
	}
	if d.dependenciesReady(*actx) {
		if allowAsync {
			_ = d.activateAsync(*actx, ctx)
		} else {
			_ = d.activateSync(*actx)
		}
	}
	return nil
}

// swapPluginMetadata replaces the fiber's plugin implementation and registry
// identity, returning the previous values for rollback.
func (d *fiberData) swapPluginMetadata(metadata replacementMetadata, key PluginKey) (pluginKind, PluginKey, string) {
	d.mu.Lock()
	oldKind := d.plugin
	oldKey := d.runtimeKey
	oldName := d.pluginName
	oldHasRuntime := d.hasRuntime
	d.plugin = metadata.kind
	d.pluginName = metadata.name
	d.inject = metadata.inject
	d.schema = metadata.schema
	d.runtimeKey = key
	d.hasRuntime = true
	d.mu.Unlock()
	if d.registry != nil {
		if oldHasRuntime {
			d.registry.unregisterRuntimeFiberFromKey(d.id, oldKey)
		}
		d.registry.rebindFiber(d, key, metadata.name)
		// The new implementation may declare a different Inject list: rebuild the
		// reverse dependency edges so cascade/refresh see the new dependencies.
		d.registry.reindexFiberDeps(d.id, metadata.inject)
	}
	return oldKind, oldKey, oldName
}

// restorePluginMetadata restores a previous plugin implementation.
//
// The fiber is detached from whatever runtime key it currently holds before
// being re-bound to the restored key, so a failed Registry.Replace leaves no
// stranded runtime behind (CORE-048: rollback must restore the registry
// exactly).
func (d *fiberData) restorePluginMetadata(oldKind pluginKind, oldKey PluginKey, oldName string, restoreKey PluginKey) {
	_ = restoreKey
	d.mu.Lock()
	currentKey := d.runtimeKey
	d.plugin = oldKind
	d.pluginName = oldName
	d.inject = oldKind.inject()
	d.schema = oldKind.schema()
	d.runtimeKey = oldKey
	d.hasRuntime = true
	d.mu.Unlock()
	if d.registry != nil {
		d.registry.unregisterRuntimeFiberFromKey(d.id, currentKey)
		d.registry.rebindFiber(d, oldKey, oldName)
		d.registry.reindexFiberDeps(d.id, d.injectSnapshot())
	}
}
