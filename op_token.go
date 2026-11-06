package cordis

import (
	"sync"
	"sync/atomic"
)

// globalOpToken allocates process-wide unique lifecycle operation tokens.
var globalOpToken atomic.Uint64

// nextOperationToken allocates one lifecycle operation token.
func nextOperationToken() uint64 { return globalOpToken.Add(1) }

// goroutineOps is the stack of operation tokens currently installed on one
// goroutine.
//
// Go has no goroutine-local storage. The kernel needs exactly one piece of it to
// reproduce the reference implementation's lifecycle reentrancy contract: a
// lifecycle operation may call back into its own fiber (a cleanup disposing its
// own fiber, an update hook restarting it) on the same execution unit, while a
// concurrent call from another goroutine must return LifecycleBusy instead of
// racing or deadlocking. Cordis tracks this with an operation token
// plus a thread/task-local frame; a Go goroutine is the equivalent execution
// unit, so the token stack is keyed by goroutine id.
type goroutineOps struct {
	mu    sync.Mutex
	stack []uint64
}

var goroutineOpStacks sync.Map // uint64 -> *goroutineOps

// pushCurrentOpToken installs token as the innermost current operation of the
// calling goroutine and returns a restore function.
func pushCurrentOpToken(token uint64) func() {
	goid := currentGoroutineID()
	value, _ := goroutineOpStacks.LoadOrStore(goid, &goroutineOps{})
	ops := value.(*goroutineOps)
	ops.mu.Lock()
	ops.stack = append(ops.stack, token)
	ops.mu.Unlock()
	return func() {
		ops.mu.Lock()
		if len(ops.stack) > 0 {
			ops.stack = ops.stack[:len(ops.stack)-1]
		}
		empty := len(ops.stack) == 0
		ops.mu.Unlock()
		if empty {
			goroutineOpStacks.Delete(goid)
		}
	}
}

// currentOpToken returns the innermost operation token of the given goroutine.
func currentOpToken(goid uint64) (uint64, bool) {
	value, ok := goroutineOpStacks.Load(goid)
	if !ok {
		return 0, false
	}
	ops := value.(*goroutineOps)
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if len(ops.stack) == 0 {
		return 0, false
	}
	return ops.stack[len(ops.stack)-1], true
}
