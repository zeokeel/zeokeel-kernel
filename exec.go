package cordis

import (
	"context"
	"sync"
)

// BlockingExecutor lets an embedder control how synchronous disposal edges run
// asynchronous cleanups.
//
// The Go kernel can simply call an async cleanup function and block on it — Go
// has no reactor that a thread-park executor would be unable to drive, so the
// default behavior is to run the cleanup inline on the calling goroutine. The
// hook is kept for hosts that want to route blocking disposal through their own
// runtime (or observe/reject it), mirroring the Rust reference's
// BlockingExecutor contract (CORE-034).
type BlockingExecutor interface {
	// BlockOn runs fn to completion on the calling goroutine or dispatches it
	// to a runtime-managed thread and blocks until it finishes. It must not
	// return before fn finished, and must not be called from a goroutine that
	// the kernel itself is running user code in.
	BlockOn(ctx context.Context, fn func())
}

var (
	executorMu sync.RWMutex
	executor   BlockingExecutor
)

// SetBlockingExecutor installs the executor used by synchronous disposal paths.
//
// It returns false when an executor was already installed: the first one wins,
// so library code cannot silently replace the host's choice.
func SetBlockingExecutor(e BlockingExecutor) bool {
	executorMu.Lock()
	defer executorMu.Unlock()
	if executor != nil {
		return false
	}
	executor = e
	return true
}

// HasBlockingExecutor reports whether the embedder installed a
// BlockingExecutor.
func HasBlockingExecutor() bool {
	executorMu.RLock()
	defer executorMu.RUnlock()
	return executor != nil
}

// runBlocking runs fn on the embedder executor when one is installed, otherwise
// inline on the calling goroutine.
func runBlocking(ctx context.Context, fn func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	executorMu.RLock()
	e := executor
	executorMu.RUnlock()
	if e == nil {
		fn()
		return
	}
	e.BlockOn(ctx, fn)
}

// YieldNow is the Go analogue of the Rust kernel's yield_now(): it gives other
// goroutines a chance to run. Conformance tests use it to observe whether
// asynchronous listeners are dispatched concurrently or serially.
func YieldNow() { runtimeGosched() }
