package cordis

import (
	"fmt"
	"log/slog"
)

// contained runs one embedder-supplied callback with panic containment.
//
// Hook/cleanup/accessor panics are reported through slog and never unwind out
// of the kernel: the caller continues with its fallback value (or with the
// remaining hooks). This is the single containment point for every boundary
// where embedder code is called outside of activation/dispatch, mirroring the
// Rust reference's catch_unwind policy.
func contained[T any](component, callback string, fallback T, body func() T) (result T) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("embedder callback panicked; contained",
				"component", component, "callback", callback, "panic", panicText(r))
			result = fallback
		}
	}()
	return body()
}

// containedVoid is the no-result form of contained.
func containedVoid(component, callback string, body func()) {
	contained(component, callback, struct{}{}, func() struct{} {
		body()
		return struct{}{}
	})
}

// panicText renders a recovered panic value as text for diagnostics.
func panicText(r any) string {
	if r == nil {
		return "panic"
	}
	if s, ok := r.(string); ok {
		return s
	}
	if e, ok := r.(error); ok {
		return e.Error()
	}
	return fmt.Sprint(r)
}

// callContained invokes fn and returns its panic value (nil when it returned
// normally). It is the building block for "run every callback, remember the
// first panic, re-raise after the drain".
func callContained(fn func()) (panicValue any) {
	defer func() {
		if r := recover(); r != nil {
			panicValue = r
		}
	}()
	fn()
	return nil
}

// rethrow re-raises a captured panic value (used after a drain completes).
func rethrow(panicValue any) {
	if panicValue != nil {
		panic(panicValue)
	}
}
