package cordis

import (
	"bytes"
	"runtime"
	"strconv"
)

// runtimeGosched yields the processor, allowing other goroutines to run.
func runtimeGosched() { runtime.Gosched() }

// currentGoroutineID returns the numeric id of the calling goroutine.
//
// Go deliberately exposes no goroutine-local storage. The kernel needs one
// piece of per-goroutine state to reproduce the reference implementation's
// reentrancy contract: a lifecycle operation may call back into its own fiber
// (a cleanup disposing its own fiber, an update hook restarting it), while a
// concurrent call from another goroutine must return LifecycleBusy instead of
// deadlocking. The Rust reference tracks this with an operation token plus a
// thread/task-local frame; Go's goroutines are the equivalent execution unit,
// so the token registry is keyed by goroutine id.
//
// This is the only place the kernel inspects the runtime stack. It is called on
// the (rare) lifecycle entry paths, never on hot paths, and never to transfer
// ownership — only to answer "is this the same in-flight operation?".
func currentGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// The stack header looks like "goroutine 123 [...]:
	fields := bytes.Fields(buf[:n])
	if len(fields) < 2 {
		return 0
	}
	id, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		return 0
	}
	return id
}
