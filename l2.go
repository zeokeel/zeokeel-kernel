package cordis

import (
	"fmt"
	"strconv"
	"time"
	"weak"
)

// L2 runtime defenses: capability introspection, per-fiber quotas and
// watchdog/heartbeat support.
//
// The Go kernel uses the same model as the Rust reference but ships the
// supervision surface unconditionally: the bookkeeping is a couple of atomics
// per fiber, so the Rust `supervision` cargo feature (compile-time size
// optimization) has no Go equivalent worth a build tag.

// IsolationCapabilities reports what this build can and cannot contain.
//
// It is intentionally honest: panic containment is best-effort process-local
// recovery; process/WASM isolation are embedder (L3/L4) layers and are always
// false here.
type IsolationCapabilities struct {
	// PanicStrategy is "recover" for Go builds.
	PanicStrategy string
	// PanicContainment reports whether panics from plugin code are contained.
	PanicContainment bool
	// UnsafeForbidden reports whether the kernel itself avoids unsafe (Go code
	// cannot express Rust's unsafe; the kernel uses no unsafe package).
	UnsafeForbidden bool
	// ProcessIsolation is an embedder (L3) layer, never provided by the kernel.
	ProcessIsolation bool
	// WASMIsolation is an embedder (L4) layer, never provided by the kernel.
	WASMIsolation bool
}

// DetectIsolationCapabilities detects the capabilities of the current build.
func DetectIsolationCapabilities() IsolationCapabilities {
	return IsolationCapabilities{
		PanicStrategy:    "recover",
		PanicContainment: true,
		UnsafeForbidden:  true,
		ProcessIsolation: false,
		WASMIsolation:    false,
	}
}

// Summary renders a human-readable summary for logs/startup banners.
func (c IsolationCapabilities) Summary() string {
	return fmt.Sprintf("isolation: panic=%s containment=%t unsafe_forbidden=%t process=%t wasm=%t",
		c.PanicStrategy, c.PanicContainment, c.UnsafeForbidden, c.ProcessIsolation, c.WASMIsolation)
}

// FiberQuota is the per-fiber resource limit set. The zero value means
// unlimited (idiomatic Go zero value; the Rust reference uses Option<usize>).
//
// Quotas are checked when a resource is registered, so a plugin that exceeds
// them gets a QuotaExceeded error and its activation rolls back through the
// normal failure path.
type FiberQuota struct {
	// MaxEffects limits live user effects (ctx.Effect*/ctx.On* etc.); kernel
	// internal bookkeeping effects never consume a slot.
	MaxEffects int
	// MaxListeners limits live event listeners registered on one fiber's behalf.
	MaxListeners int
	// MaxChildFibers limits live child fibers loaded with ctx.Plugin.
	MaxChildFibers int
}

// MaxEffectsOf returns a quota limited to n live user effects.
func (q FiberQuota) WithMaxEffects(n int) FiberQuota {
	q.MaxEffects = n
	return q
}

// WithMaxListeners returns a quota limited to n live listeners.
func (q FiberQuota) WithMaxListeners(n int) FiberQuota {
	q.MaxListeners = n
	return q
}

// WithMaxChildFibers returns a quota limited to n live child fibers.
func (q FiberQuota) WithMaxChildFibers(n int) FiberQuota {
	q.MaxChildFibers = n
	return q
}

// StalledFiber is one watchdog observation: a fiber with an in-flight operation
// that has not reported activity for at least the configured timeout.
type StalledFiber struct {
	// Fiber is the stalled fiber handle.
	Fiber *Fiber
	// IdleFor is how long it has been quiet.
	IdleFor time.Duration
}

// ID returns the fiber id, for correlation with logs/metrics.
func (s StalledFiber) ID() uint64 { return s.Fiber.ID() }

// Name returns the plugin name of the stalled fiber.
func (s StalledFiber) Name() string { return s.Fiber.Name() }

// StalledFibers returns the fibers with work in flight that have been quiet for
// at least timeout (cooperative watchdog; the kernel never force-kills).
func (r *Registry) StalledFibers(timeout time.Duration) []StalledFiber {
	var out []StalledFiber
	for _, data := range r.liveFibers() {
		if data.pendingOps.Load() == 0 {
			continue
		}
		fiber := &Fiber{data: data}
		if idle := fiber.IdleFor(); idle >= timeout {
			out = append(out, StalledFiber{Fiber: fiber, IdleFor: idle})
		}
	}
	return out
}

// makeWeakFiber builds the weak provider handle used by service entries.
func makeWeakFiber(f *fiberData) weak.Pointer[fiberData] { return weak.Make(f) }

// itoa is a tiny helper to keep error strings allocation-light.
func itoa(value int) string { return strconv.Itoa(value) }
