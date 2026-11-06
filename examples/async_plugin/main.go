// Command async_plugin demonstrates the async plugin interface and async
// effects.
//
// Go has no futures: an async plugin is a blocking ActivateAsync that receives a
// context.Context, and async cleanups are functions that also receive a context.
// Cancelling that context is the Go analogue of cancelling an async operation; the
// kernel completes the teardown and parks the cleanups it could not await for
// the next dispose (the Cordis A-9 contract).
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	cordis "github.com/zeokeel/zeokeel-kernel"
)

// AsyncWorker is an asynchronous plugin.
type AsyncWorker struct{}

func (AsyncWorker) Name() string { return "async-worker" }

func (AsyncWorker) ActivateAsync(ctx context.Context, c cordis.Context, _ cordis.Value) error {
	fmt.Println("async-worker: activation started")
	// A real plugin would await I/O here; blocking is fine in Go, and the
	// context makes the work cancellable.
	select {
	case <-time.After(10 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	if _, err := c.EffectAsync(func() cordis.AsyncEffect {
		return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(cleanupCtx context.Context) error {
			fmt.Println("async-worker: async cleanup ran (connection released)")
			return nil
		})}
	}); err != nil {
		return err
	}
	fmt.Println("async-worker: activation finished")
	return nil
}

// CancellableCleanup blocks until its context is cancelled, showing how
// DisposeAsync cancellation behaves.
type CancellableCleanup struct{}

func (CancellableCleanup) Name() string { return "cancellable" }

func (CancellableCleanup) ActivateAsync(_ context.Context, c cordis.Context, _ cordis.Value) error {
	_, err := c.EffectAsync(func() cordis.AsyncEffect {
		return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(cleanupCtx context.Context) error {
			fmt.Println("cancellable: cleanup waiting for cancellation")
			<-cleanupCtx.Done()
			fmt.Println("cancellable: cleanup observed cancellation")
			return cleanupCtx.Err()
		})}
	})
	return err
}

func main() {
	root := cordis.Root()

	worker, err := root.PluginAsync(AsyncWorker{}, nil)
	if err != nil {
		fail(err)
	}
	fmt.Println("worker state:", worker.State())

	// DisposeAsync awaits every async cleanup before returning.
	if err := worker.DisposeAsync(context.Background()); err != nil {
		fail(err)
	}
	fmt.Println("worker state after dispose:", worker.State())

	// Cancel a disposal mid-cleanup: the teardown still completes (A-9) and the
	// call reports context.Canceled for the interrupted cleanup.
	cancellable, err := root.PluginAsync(CancellableCleanup{}, nil)
	if err != nil {
		fail(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err = cancellable.DisposeAsync(ctx)
	fmt.Println("cancelled dispose returned:", err)
	fmt.Println("cancellable state:", cancellable.State())

	// A retry drains anything that was parked by the cancelled teardown.
	_ = cancellable.Dispose()
	if err := root.DisposeAll(); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
