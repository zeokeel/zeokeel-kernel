// Package runtime_test holds Go conformance tests for the CORE runtime surface:
// events/waterfall/internal hooks (CORE-017..021, 031), logger/config/clock
// (CORE-023, 024) and quiescence (CORE-049).
//
// These tests are derived from the Rust reference test suite; see
// docs/analysis/05-core-cases.md (CORE main table), 03-registry-events.md
// (events/waterfall) and 04-runtime-config-logger.md (config/logger/clock).
//
// Every test builds its own Root and registers a cleanup that disposes the
// whole tree with recover protection, so a failing assertion elsewhere cannot
// leave background lifecycle work behind. Concurrency assertions use channels
// and short "must not have happened yet" windows instead of long sleeps.
package runtime_test

import (
	"reflect"
	"sync"
	"testing"
	"time"

	cordis "github.com/zeokeel/zeokeel-kernel"
)

// newRoot creates an isolated context tree for one test and disposes it on
// cleanup. The recover protects the test run from a kernel cleanup panic that
// an assertion failure may have caused (for example a deliberately panicking
// listener/waterfall middleware left registered on the root).
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
			t.Logf("cleanup: DisposeAll returned %v", err)
		}
	})
	return root
}

// trace is a goroutine-safe ordered event log (listeners and hooks may run on
// kernel goroutines).
type trace struct {
	mu    sync.Mutex
	items []string
}

func (tr *trace) add(label string) {
	tr.mu.Lock()
	tr.items = append(tr.items, label)
	tr.mu.Unlock()
}

func (tr *trace) snapshot() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]string(nil), tr.items...)
}

func (tr *trace) len() int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return len(tr.items)
}

// assertTrace fails unless got equals want exactly (order included).
func assertTrace(t *testing.T, label string, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s:\n  got  %#v\n  want %#v", label, got, want)
	}
}

// assertOrderedPrefix fails unless got starts with want.
func assertOrderedPrefix(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) < len(want) || !reflect.DeepEqual(got[:len(want)], want) {
		t.Fatalf("%s:\n  got  %#v\n  want prefix %#v", label, got, want)
	}
}

// assertDeepEqual is a small deep-equality helper (Value is any, so the
// arguments stay untyped to avoid inference conflicts between named and
// anonymous map/slice types).
func assertDeepEqual(t *testing.T, label string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: got %#v, want %#v", label, got, want)
	}
}

// waitClosed waits until ch is closed, failing the test on timeout.
func waitClosed(t *testing.T, ch <-chan struct{}, timeout time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("%s: did not complete within %v", what, timeout)
	}
}

// waitErr waits until a result is delivered, failing the test on timeout.
func waitErr(t *testing.T, ch <-chan error, timeout time.Duration, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(timeout):
		t.Fatalf("%s: no result within %v", what, timeout)
		return nil
	}
}

// assertBlocked fails if ch becomes ready within d. It is the "did not return
// early" half of the quiescence assertions.
func assertBlocked(t *testing.T, ch <-chan struct{}, d time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s: completed while it should still be blocked", what)
	case <-time.After(d):
	}
}

// setupPlugin runs an arbitrary registration body during activation. Most
// ownership tests need a plugin that owns listeners/waterfall middleware so
// their recycling on unload can be observed.
type setupPlugin struct {
	name  string
	setup func(cordis.Context) error
}

func (p setupPlugin) Name() string { return p.name }

func (p setupPlugin) Activate(c cordis.Context, _ cordis.Value) error {
	if p.setup == nil {
		return nil
	}
	return p.setup(c)
}
