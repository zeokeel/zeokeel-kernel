package cordis

import "sync"

// Cleanup is a synchronous cleanup callback owned by a fiber.
//
// Cleanups run exactly once, in reverse registration order, when the owning
// effect is disposed or the owning fiber unloads. A panicking cleanup does not
// skip the remaining cleanups: the first panic is captured and re-raised after
// the whole effect has drained (CORE-030).
type Cleanup func()

// Effect is the result of an effect body: zero or more synchronous cleanup
// callbacks. An empty effect means the registration owns nothing to clean up.
type Effect []Cleanup

// CleanupOf wraps a closure as a Cleanup (ergonomic helper mirroring the Cordis
// cleanup() function).
func CleanupOf(fn func()) Cleanup { return fn }

// Disposer is a single-shot handle to one effect's synchronous cleanups.
//
// Dispose runs the cleanups in reverse order; repeated calls are no-ops. The
// zero value is not usable; construct one with NewDisposer, or receive one from
// a kernel registration (Context.Effect, Context.Provide, Context.On, ...).
type Disposer struct {
	state *disposerState
}

type disposerState struct {
	mu       sync.Mutex
	cleanups []Cleanup
	disposed bool
}

// NewDisposer creates a disposer owning the given cleanups.
func NewDisposer(cleanups []Cleanup) *Disposer {
	return &Disposer{state: &disposerState{cleanups: cleanups}}
}

// Dispose runs the cleanups in reverse order, exactly once.
//
// The first panic raised by a cleanup is re-raised after every cleanup of this
// disposer ran, so callers can observe it while the drain still completes.
func (d *Disposer) Dispose() {
	if d == nil || d.state == nil {
		return
	}
	cleanups, ok := d.takeAll()
	if !ok {
		return
	}
	var first any
	for i := len(cleanups) - 1; i >= 0; i-- {
		if p, panicked := callCleanup(cleanups[i]); panicked && first == nil {
			first = p
		}
	}
	if first != nil {
		panic(first)
	}
}

// disposeContained drains the disposer and returns the first captured panic
// value instead of re-raising it. The fiber unload path uses it so one effect's
// panic cannot skip the remaining effects (CORE-030).
func (d *Disposer) disposeContained() (first any) {
	if d == nil || d.state == nil {
		return nil
	}
	cleanups, ok := d.takeAll()
	if !ok {
		return nil
	}
	for i := len(cleanups) - 1; i >= 0; i-- {
		if p, panicked := callCleanup(cleanups[i]); panicked && first == nil {
			first = p
		}
	}
	return first
}

// takeAll removes and returns the cleanup list; the second result is false when
// another caller already drained it.
func (d *Disposer) takeAll() ([]Cleanup, bool) {
	d.state.mu.Lock()
	defer d.state.mu.Unlock()
	if d.state.disposed {
		return nil, false
	}
	d.state.disposed = true
	cleanups := d.state.cleanups
	d.state.cleanups = nil
	return cleanups, true
}

// IsDisposed reports whether the disposer has been drained (or was never
// armed).
func (d *Disposer) IsDisposed() bool {
	if d == nil || d.state == nil {
		return true
	}
	d.state.mu.Lock()
	defer d.state.mu.Unlock()
	return d.state.disposed
}

// discard drops the cleanups without running them and marks the disposer
// settled (used when a child disposed itself and the parent detaches the
// ownership effect, R3-A4).
func (d *Disposer) discard() {
	if d == nil || d.state == nil {
		return
	}
	d.state.mu.Lock()
	d.state.cleanups = nil
	d.state.disposed = true
	d.state.mu.Unlock()
}

// callCleanup runs one cleanup with panic containment.
func callCleanup(c Cleanup) (panicValue any, panicked bool) {
	if c == nil {
		return nil, false
	}
	defer func() {
		if r := recover(); r != nil {
			panicValue, panicked = r, true
		}
	}()
	c()
	return nil, false
}
