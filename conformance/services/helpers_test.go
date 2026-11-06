package services_test

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	cordis "github.com/zeokeel/cordis-go"
)

// trace records cleanup/event order. The kernel may dispatch some work on
// goroutines, so every recorder shared with a concurrent test is lock-protected.
type trace struct {
	mu    sync.Mutex
	order []string
}

func (tr *trace) add(label string) {
	tr.mu.Lock()
	tr.order = append(tr.order, label)
	tr.mu.Unlock()
}

func (tr *trace) snapshot() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]string(nil), tr.order...)
}

func (tr *trace) length() int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return len(tr.order)
}

// newRoot creates an isolated root context and registers a cleanup that disposes
// the whole tree, containing cleanup panics so one failing test cannot cascade.
func newRoot(t *testing.T) cordis.Context {
	t.Helper()
	root := cordis.Root()
	t.Cleanup(func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("cleanup: DisposeAll panicked (contained): %v", r)
			}
		}()
		if err := root.DisposeAll(); err != nil {
			t.Logf("cleanup: DisposeAll reported: %v", err)
		}
	})
	return root
}

// funcPlugin is the shared table-driven plugin helper: everything the tests need
// (name, inject gate, activation body) is supplied per case.
type funcPlugin struct {
	name     string
	inject   []string
	activate func(ctx cordis.Context, config cordis.Value) error
}

// Name implements cordis.Plugin.
func (p *funcPlugin) Name() string {
	if p.name == "" {
		return "func-plugin"
	}
	return p.name
}

// Inject implements cordis.PluginInjector (nil means "no dependencies").
func (p *funcPlugin) Inject() []string { return p.inject }

// Activate implements cordis.Plugin.
func (p *funcPlugin) Activate(ctx cordis.Context, config cordis.Value) error {
	if p.activate == nil {
		return nil
	}
	return p.activate(ctx, config)
}

// traceEffect builds an effect body registering one ordered cleanup label.
func traceEffect(tr *trace, label string) func() cordis.Effect {
	return func() cordis.Effect {
		return cordis.Effect{cordis.CleanupOf(func() { tr.add(label) })}
	}
}

// requireCode asserts that err carries the given CordisError classification and
// returns the concrete error for variant-specific checks (name, detail, ...).
func requireCode(t *testing.T, err error, code cordis.ErrCode) *cordis.CordisError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %v, got nil", code)
	}
	got, ok := cordis.CodeOf(err)
	if !ok || got != code {
		t.Fatalf("error = %v; code = %v (ok=%v), want code %v", err, got, ok, code)
	}
	var ce *cordis.CordisError
	if !errors.As(err, &ce) {
		t.Fatalf("error %v is not a *cordis.CordisError", err)
	}
	return ce
}

// mustNoError fails the test when err is not nil.
func mustNoError(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", what, err)
	}
}

// needState asserts the exact fiber lifecycle state.
func needState(t *testing.T, fiber *cordis.Fiber, want cordis.FiberState, what string) {
	t.Helper()
	if fiber == nil {
		t.Fatalf("%s: nil fiber", what)
	}
	if got := fiber.State(); got != want {
		t.Fatalf("%s: state = %v, want %v", what, got, want)
	}
}

// waitClosed waits for ch with a bounded timeout (deadlock watchdog).
func waitClosed(t *testing.T, ch <-chan struct{}, timeout time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timeout after %v waiting for %s", timeout, what)
	}
}

// waitUntil polls predicate with short yields (no long sleeps) and a bounded
// timeout: the kernel's async work must settle within the watchdog window.
func waitUntil(t *testing.T, timeout time.Duration, predicate func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout after %v waiting for %s", timeout, what)
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
}

// sameStrings reports slice equality for order assertions.
func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
