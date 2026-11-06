// Package lifecycle_test is the Go conformance suite for the fiber /
// effect / async-cancellation CORE cases of the Rust reference
// (upstream zeokeel-cordis v0.6.0, spec/CORDIS-CORE.md §4).
//
// Every test builds its own cordis.Root() and registers a t.Cleanup that
// disposes the tree, recovering from a re-raised cleanup panic so one failing
// test cannot leak fibers into the next one.
package lifecycle_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	cordis "github.com/zeokeel/zeokeel-kernel"
)

// childEnv marks a re-executed child test binary. Some CORE cases depend on
// process-global state (the BlockingExecutor is install-once; the orphan queue
// only receives async cleanups while no executor exists), mirroring the Rust
// reference's separate integration-test binaries. Running those tests in a
// fresh child process makes them independent of test order and of -count
// iterations.
const childEnv = "CORDIS_LIFECYCLE_CHILD"

// runInFreshProcess re-executes the named test in a clean child test binary.
func runInFreshProcess(t *testing.T, name string) {
	t.Helper()
	cmd := exec.Command(os.Args[0],
		"-test.run=^"+name+"$",
		"-test.count=1",
		"-test.timeout=120s",
	)
	cmd.Env = append(os.Environ(), childEnv+"="+name)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s child process failed: %v\n%s", name, err, output)
	}
}

// inFreshProcess reports whether the current process is that test's child.
func inFreshProcess(name string) bool { return os.Getenv(childEnv) == name }

// awaitTimeout is the upper bound used by every channel wait / poll loop in
// this package. The kernel does the work on goroutines, so the suite waits on
// signals and bounded polls instead of sleeping.
const awaitTimeout = 2 * time.Second

// trace records cleanup / lifecycle order. The kernel may dispatch teardown on
// the calling goroutine, but a few paths (EffectStream, async dispatch) use
// goroutines, so every access is lock-protected.
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

func (tr *trace) count() int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return len(tr.order)
}

// newRoot creates a fresh tree and guarantees disposal even when a test fails
// early. DisposeAll re-raises a cleanup panic after attempting every fiber, so
// the cleanup recovers: tests that assert the panic inspect it themselves.
func newRoot(t *testing.T) cordis.Context {
	t.Helper()
	root := cordis.Root()
	t.Cleanup(func() {
		defer func() { _ = recover() }()
		_ = root.DisposeAll()
	})
	return root
}

// waitFor polls cond with a 1ms backoff until awaitTimeout expires.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(awaitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// awaitChan receives from ch or fails the test after awaitTimeout.
func awaitChan[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(awaitTimeout):
		var zero T
		t.Fatalf("timed out waiting for %s", what)
		return zero
	}
}

// awaitSignal waits until ch is closed or a value is available.
func awaitSignal(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(awaitTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// expectNoSignal asserts that ch does not fire within d. It is used to prove
// that a joiner/waiter is really blocked (small bounded window, never a sleep
// used as synchronization).
func expectNoSignal(t *testing.T, what string, ch <-chan error, d time.Duration) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("%s returned early: %v", what, err)
	case <-time.After(d):
	}
}

func assertOrder(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

func assertCode(t *testing.T, err error, code cordis.ErrCode) {
	t.Helper()
	got, ok := cordis.CodeOf(err)
	if !ok {
		t.Fatalf("err = %v (%T), want CordisError code %v", err, err, code)
	}
	if got != code {
		t.Fatalf("err code = %v (%v), want %v (err: %v)", got, got.String(), code, err)
	}
}

func assertState(t *testing.T, f *cordis.Fiber, want cordis.FiberState) {
	t.Helper()
	if f == nil {
		t.Fatalf("nil fiber, want state %v", want)
	}
	if got := f.State(); got != want {
		t.Fatalf("fiber %q state = %v, want %v", f.Name(), got, want)
	}
}

// catchPanic runs fn and returns the recovered panic value (nil when fn did not
// panic).
func catchPanic(fn func()) (recovered any) {
	defer func() { recovered = recover() }()
	fn()
	return nil
}

// disposeAllCatching calls DisposeAll and reports a re-raised cleanup panic.
func disposeAllCatching(root cordis.Context) (recovered any, err error) {
	defer func() { recovered = recover() }()
	err = root.DisposeAll()
	return nil, err
}

// pluginFunc adapts closures to cordis.Plugin (and its optional metadata
// interfaces: Inject / ConfigSchema). Each instance must have a unique Name
// inside one root so the registry runtime keys stay distinct.
type pluginFunc struct {
	name     string
	deps     []string
	schema   cordis.ConfigSchema
	activate func(cordis.Context, cordis.Value) error
}

func (p *pluginFunc) Name() string {
	if p.name == "" {
		return "plugin"
	}
	return p.name
}

func (p *pluginFunc) Inject() []string                                  { return p.deps }
func (p *pluginFunc) ConfigSchema() cordis.ConfigSchema                 { return p.schema }
func (p *pluginFunc) Activate(c cordis.Context, cfg cordis.Value) error { return p.activate(c, cfg) }

// asyncPluginFunc adapts closures to cordis.AsyncPlugin.
type asyncPluginFunc struct {
	name     string
	deps     []string
	activate func(ctx context.Context, c cordis.Context, cfg cordis.Value) error
}

func (p *asyncPluginFunc) Name() string {
	if p.name == "" {
		return "async-plugin"
	}
	return p.name
}

func (p *asyncPluginFunc) Inject() []string { return p.deps }

func (p *asyncPluginFunc) ActivateAsync(ctx context.Context, c cordis.Context, cfg cordis.Value) error {
	return p.activate(ctx, c, cfg)
}

func noopPlugin(name string) *pluginFunc {
	return &pluginFunc{name: name, activate: func(cordis.Context, cordis.Value) error { return nil }}
}

func cleanupOf(tag string, tr *trace) cordis.Cleanup {
	return cordis.CleanupOf(func() { tr.add(tag) })
}

func asyncCleanupOf(tag string, tr *trace) cordis.AsyncCleanup {
	return cordis.AsyncCleanupOf(func(context.Context) error {
		tr.add(tag)
		return nil
	})
}

// effectLabels returns the labels of the top-level effect diagnostics nodes.
func effectLabels(metas []cordis.EffectMeta) []string {
	out := make([]string, 0, len(metas))
	for _, meta := range metas {
		out = append(out, meta.Label)
	}
	return out
}

// effectTree renders an EffectMeta tree like `[outer[inner[]]]`, which makes
// exact shape assertions readable (nil vs empty children both render as []).
func effectTree(metas []cordis.EffectMeta) string {
	var b strings.Builder
	var write func(nodes []cordis.EffectMeta)
	write = func(nodes []cordis.EffectMeta) {
		b.WriteByte('[')
		for i, node := range nodes {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(node.Label)
			write(node.Children)
		}
		b.WriteByte(']')
	}
	write(metas)
	return b.String()
}

// hasLabel reports whether the top-level effect list contains label.
func hasLabel(metas []cordis.EffectMeta, label string) bool {
	for _, meta := range metas {
		if meta.Label == label {
			return true
		}
	}
	return false
}
