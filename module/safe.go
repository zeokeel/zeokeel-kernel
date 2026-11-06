package module

import (
	"log/slog"
	"runtime/debug"
	"sync"
)

// PanicReport describes a panic contained by a module-managed goroutine or
// callback wrapper.
type PanicReport struct {
	// Name identifies the module/goroutine/callback that panicked.
	Name string
	// Value is the recovered panic value.
	Value any
	// Stack is the goroutine stack captured at recovery time.
	Stack []byte
}

// PanicHandler receives contained module panics. Hosts should install one to
// turn a contained panic into an alert/metric and decide whether to recreate,
// reload or quarantine the module.
type PanicHandler func(PanicReport)

var panicHandlers struct {
	mu      sync.RWMutex
	handler PanicHandler
}

// SetPanicHandler installs the process-wide contained-panic handler used by
// SafeGo and SafeCall. Passing nil restores the default (log through
// slog.Default). It is a host-layer convenience, not a kernel guarantee: panics
// in goroutines a module spawns with a raw `go` statement still crash the
// process — the module contract is to use SafeGo instead.
func SetPanicHandler(handler PanicHandler) {
	panicHandlers.mu.Lock()
	panicHandlers.handler = handler
	panicHandlers.mu.Unlock()
}

func currentPanicHandler() PanicHandler {
	panicHandlers.mu.RLock()
	defer panicHandlers.mu.RUnlock()
	return panicHandlers.handler
}

func reportPanic(name string, value any) {
	report := PanicReport{Name: name, Value: value, Stack: debug.Stack()}
	if handler := currentPanicHandler(); handler != nil {
		func() {
			defer func() { _ = recover() }()
			handler(report)
		}()
		return
	}
	slog.Error("module goroutine panicked; contained", "module", name, "panic", value)
}

// SafeGo starts fn on a new goroutine and contains a panic instead of letting it
// crash the process. Modules must use it for every goroutine they own:
//
//	go func() { ... }()          // ✗ panic here kills the whole host
//	module.SafeGo("app/ticker", func() { ... }) // ✓ panic is reported and contained
//
// Use SafeCall for synchronous callbacks (timers, third-party callbacks) where
// the caller wants to observe the recovered value.
func SafeGo(name string, fn func()) {
	go func() {
		defer func() {
			if value := recover(); value != nil {
				reportPanic(name, value)
			}
		}()
		fn()
	}()
}

// SafeCall runs fn synchronously with panic containment and returns the
// recovered panic value (nil when fn returned normally). It is useful for
// callbacks invoked by libraries that cannot be expected to recover panics.
func SafeCall(name string, fn func()) (panicValue any) {
	defer func() {
		if value := recover(); value != nil {
			panicValue = value
			reportPanic(name, value)
		}
	}()
	fn()
	return nil
}
