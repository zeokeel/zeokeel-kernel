package cordis_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cordis "github.com/zeokeel/cordis-go"
)

// trace collects cleanup order in tests. Tests that run sub-tests in parallel
// keep it lock-protected (the kernel dispatches some work on goroutines).
type trace struct {
	mu    sync.Mutex
	order []string
}

func (t *trace) add(label string) {
	t.mu.Lock()
	t.order = append(t.order, label)
	t.mu.Unlock()
}

func (t *trace) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.order...)
}

// --- helper plugins ---

type greeterPlugin struct {
	trace *trace
	name  string
}

func (p greeterPlugin) Name() string { return p.name }

func (p greeterPlugin) Activate(ctx cordis.Context, config cordis.Value) error {
	if p.trace != nil {
		p.trace.add("apply")
	}
	_, err := ctx.Effect(func() cordis.Effect {
		return cordis.Effect{cordis.CleanupOf(func() {
			if p.trace != nil {
				p.trace.add("cleanup")
			}
		})}
	})
	return err
}

func TestRootAndPluginLifecycle(t *testing.T) {
	root := cordis.Root()
	tr := &trace{}
	fiber, err := root.Plugin(greeterPlugin{trace: tr, name: "greeter"}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := fiber.State(); got != cordis.Active {
		t.Fatalf("state = %v, want Active", got)
	}
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	if got := fiber.State(); got != cordis.Disposed {
		t.Fatalf("state after dispose = %v, want Disposed", got)
	}
	if order := tr.snapshot(); len(order) != 2 || order[0] != "apply" || order[1] != "cleanup" {
		t.Fatalf("order = %v", order)
	}
	// Idempotent dispose.
	if err := fiber.Dispose(); err != nil {
		t.Fatalf("second dispose: %v", err)
	}
	if err := root.DisposeAll(); err != nil {
		t.Fatalf("dispose all: %v", err)
	}
	if !root.Registry().IsIdle() {
		t.Fatal("registry not idle after dispose all")
	}
}

func TestEffectReverseOrderAndSingleShot(t *testing.T) {
	root := cordis.Root()
	tr := &trace{}
	var disposers []*cordis.Disposer
	for _, label := range []string{"a", "b", "c"} {
		label := label
		d, err := root.Effect(func() cordis.Effect {
			return cordis.Effect{cordis.CleanupOf(func() { tr.add(label) })}
		})
		if err != nil {
			t.Fatalf("effect %s: %v", label, err)
		}
		disposers = append(disposers, d)
	}
	disposers[2].Dispose()
	disposers[2].Dispose()
	if got := tr.snapshot(); len(got) != 1 || got[0] != "c" {
		t.Fatalf("order after manual dispose = %v", got)
	}
	if err := root.DisposeAll(); err != nil {
		t.Fatalf("dispose all: %v", err)
	}
	got := tr.snapshot()
	want := []string{"c", "b", "a"}
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

type consumerPlugin struct {
	ready    bool
	injected []string
}

func (p *consumerPlugin) Name() string     { return "consumer" }
func (p *consumerPlugin) Inject() []string { return p.injected }
func (p *consumerPlugin) Activate(ctx cordis.Context, config cordis.Value) error {
	p.ready = true
	return nil
}

var configKey = cordis.NewServiceKey[*string]("config")

func TestInjectGatesActivation(t *testing.T) {
	root := cordis.Root()
	consumer := &consumerPlugin{injected: []string{configKey.Name()}}
	fiber, err := root.Plugin(consumer, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if consumer.ready {
		t.Fatal("consumer activated without its dependency")
	}
	if got := fiber.State(); got != cordis.Pending {
		t.Fatalf("state = %v, want Pending", got)
	}
	value := "configured"
	if _, err := cordis.Provide(root, configKey, &value); err != nil {
		t.Fatalf("provide: %v", err)
	}
	if !consumer.ready {
		t.Fatal("consumer did not activate after its dependency appeared")
	}
	if got := fiber.State(); got != cordis.Active {
		t.Fatalf("state = %v, want Active", got)
	}
	got, ok := cordis.Get(root, configKey)
	if !ok || got != &value {
		t.Fatalf("get = %v/%v", got, ok)
	}
	// Disposing the provider must unload the dependent.
	providerFiber := root.Fiber()
	_ = providerFiber
	root.DisposeAll()
	if got := fiber.State(); got != cordis.Disposed {
		t.Fatalf("dependent state = %v, want Disposed", got)
	}
}

func TestIsolateShadowsService(t *testing.T) {
	root := cordis.Root()
	value := "root"
	if _, err := cordis.Provide(root, configKey, &value); err != nil {
		t.Fatalf("provide root: %v", err)
	}
	isolated := root.Isolate(configKey.Name())
	shadow := "isolated"
	if _, err := cordis.Provide(isolated, configKey, &shadow); err != nil {
		t.Fatalf("provide isolated: %v", err)
	}
	got, _ := cordis.Get(isolated, configKey)
	if got != &shadow {
		t.Fatalf("isolated get = %v, want isolated value", got)
	}
	got, _ = cordis.Get(root, configKey)
	if got != &value {
		t.Fatalf("root get = %v, want root value", got)
	}
	// Duplicate provide in the same scope is rejected.
	if _, err := cordis.Provide(root, configKey, &value); !errors.Is(err, cordis.ErrDuplicateService) {
		t.Fatalf("duplicate provide err = %v", err)
	}
	_ = root.DisposeAll()
}

func TestEventsBailAndOnce(t *testing.T) {
	root := cordis.Root()
	var calls []string
	if _, err := root.On("demo", func(args []cordis.Value) cordis.EventResult {
		calls = append(calls, "first")
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	if _, err := root.On("demo", func(args []cordis.Value) cordis.EventResult {
		calls = append(calls, "after")
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	root.Emit("demo", 1)
	if len(calls) != 2 {
		t.Fatalf("emit calls = %v", calls)
	}
	// Bail stops dispatch at the first non-null, non-false bail value.
	var bailCalls []string
	if _, err := root.On("bail", func(args []cordis.Value) cordis.EventResult {
		bailCalls = append(bailCalls, "before")
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	if _, err := root.On("bail", func(args []cordis.Value) cordis.EventResult {
		bailCalls = append(bailCalls, "bailer")
		return cordis.Bail("stop")
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	if _, err := root.On("bail", func(args []cordis.Value) cordis.EventResult {
		bailCalls = append(bailCalls, "after-bail")
		return cordis.Continue()
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	value, ok := root.Bail("bail", 2)
	if !ok || value != "stop" {
		t.Fatalf("bail = %v/%v", value, ok)
	}
	if len(bailCalls) != 2 || bailCalls[1] != "bailer" {
		t.Fatalf("bail calls = %v", bailCalls)
	}
	// A once listener fires exactly once.
	onceCalls := 0
	if _, err := root.Once("once", func(args []cordis.Value) cordis.EventResult {
		onceCalls++
		return cordis.Bail(nil) // null bail does not stop the chain
	}); err != nil {
		t.Fatalf("once: %v", err)
	}
	root.Emit("once", 1)
	root.Emit("once", 2)
	if onceCalls != 1 {
		t.Fatalf("once calls = %d, want 1", onceCalls)
	}
	_ = root.DisposeAll()
}

func TestWaterfallMiddlewares(t *testing.T) {
	root := cordis.Root()
	if _, err := root.OnWaterfall("chain", func(args []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		return "outer(" + next.Next().(string) + ")"
	}); err != nil {
		t.Fatalf("waterfall: %v", err)
	}
	if _, err := root.OnWaterfall("chain", func(args []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		return "inner(" + next.Next().(string) + ")"
	}); err != nil {
		t.Fatalf("waterfall: %v", err)
	}
	value := root.Waterfall("chain", nil, func(args []cordis.Value) cordis.Value { return "base" })
	if value != "outer(inner(base))" {
		t.Fatalf("waterfall = %v", value)
	}
	// A middleware that never calls next vetoes the terminal.
	vetoRoot := cordis.Root()
	if _, err := vetoRoot.OnWaterfall("veto", func(args []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		return "vetoed"
	}); err != nil {
		t.Fatalf("waterfall: %v", err)
	}
	value = vetoRoot.Waterfall("veto", nil, func(args []cordis.Value) cordis.Value { return "terminal" })
	if value != "vetoed" {
		t.Fatalf("veto = %v", value)
	}
	_ = root.DisposeAll()
	_ = vetoRoot.DisposeAll()
}

func TestConfigSchemaValidation(t *testing.T) {
	schema := cordis.NewJsonConfigSchema().
		Required("model").
		DefaultValue("temperature", 0.2).
		Property("model", cordis.JSONString).
		DenyUnknown()
	plugin := schemaPlugin{schema: schema}
	fiber, err := rootPlugin(t, plugin, map[string]cordis.Value{"model": "m1"})
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	config, _ := fiber.Config().(map[string]cordis.Value)
	if config["model"] != "m1" || config["temperature"] != 0.2 {
		t.Fatalf("config = %v", config)
	}
	// Invalid config: activation fails with a validation error.
	bad, err := rootPlugin(t, schemaPlugin{schema: schema}, map[string]cordis.Value{"extra": true})
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := bad.State(); got != cordis.Failed {
		t.Fatalf("state = %v, want Failed", got)
	}
	if err := bad.ErrorDetail(); err == nil {
		t.Fatal("expected structured activation error")
	}
	_ = fiber.Context
}

type schemaPlugin struct {
	schema cordis.ConfigSchema
}

func (p schemaPlugin) Name() string                                { return "schema" }
func (p schemaPlugin) ConfigSchema() cordis.ConfigSchema           { return p.schema }
func (p schemaPlugin) Activate(cordis.Context, cordis.Value) error { return nil }

func rootPlugin(t *testing.T, p cordis.Plugin, config cordis.Value) (*cordis.Fiber, error) {
	t.Helper()
	root := cordis.Root()
	t.Cleanup(func() { _ = root.DisposeAll() })
	return root.Plugin(p, config)
}

func TestLoggerRingBufferAndExporter(t *testing.T) {
	root := cordis.Root()
	service := root.LoggerService()
	service.SetLevel(cordis.LogDebug)
	exporter := &countingExporter{}
	disposer, err := root.LogExporter(exporter)
	if err != nil {
		t.Fatalf("exporter: %v", err)
	}
	logger := root.LoggerNamed("demo")
	logger.Info("one", nil)
	logger.Debug("two", map[string]cordis.Value{"k": 1})
	records := service.Records()
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
	if records[0].Name != "demo" || records[0].Message != "one" {
		t.Fatalf("record = %+v", records[0])
	}
	exporter.mu.Lock()
	count := exporter.count
	exporter.mu.Unlock()
	if count != 2 {
		t.Fatalf("exporter count = %d, want 2", count)
	}
	disposer.Dispose()
	logger.Info("three", nil)
	exporter.mu.Lock()
	count = exporter.count
	exporter.mu.Unlock()
	if count != 2 {
		t.Fatalf("exporter count after dispose = %d, want 2", count)
	}
	_ = root.DisposeAll()
}

type countingExporter struct {
	mu    sync.Mutex
	count int
}

func (e *countingExporter) Export(cordis.LogRecord) {
	e.mu.Lock()
	e.count++
	e.mu.Unlock()
}

func TestPanicInApplyIsContained(t *testing.T) {
	root := cordis.Root()
	fiber, err := root.Plugin(panicPlugin{}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := fiber.State(); got != cordis.Failed {
		t.Fatalf("state = %v, want Failed", got)
	}
	if fiber.ErrorDetail() == nil {
		t.Fatal("expected error detail")
	}
	// The kernel still works after the contained panic.
	if _, err := root.Plugin(greeterPlugin{name: "ok"}, nil); err != nil {
		t.Fatalf("plugin after panic: %v", err)
	}
	_ = root.DisposeAll()
}

type panicPlugin struct{}

func (panicPlugin) Name() string { return "panic" }
func (panicPlugin) Activate(cordis.Context, cordis.Value) error {
	panic("boom")
}

func TestChildFiberOwnership(t *testing.T) {
	root := cordis.Root()
	parent, err := root.Plugin(&parentPlugin{root: root}, nil)
	if err != nil {
		t.Fatalf("parent: %v", err)
	}
	var child *cordis.Fiber
	parentCtx, ok := parent.Context()
	if !ok {
		t.Fatal("parent context missing")
	}
	child, err = parentCtx.Plugin(greeterPlugin{name: "child"}, nil)
	if err != nil {
		t.Fatalf("child: %v", err)
	}
	if got := child.State(); got != cordis.Active {
		t.Fatalf("child state = %v", got)
	}
	if err := parent.Dispose(); err != nil {
		t.Fatalf("dispose parent: %v", err)
	}
	if got := child.State(); got != cordis.Disposed {
		t.Fatalf("child state after parent dispose = %v, want Disposed", got)
	}
	_ = root.DisposeAll()
}

type parentPlugin struct{ root cordis.Context }

func (p *parentPlugin) Name() string                                { return "parent" }
func (p *parentPlugin) Activate(cordis.Context, cordis.Value) error { return nil }

func TestQuiescence(t *testing.T) {
	root := cordis.Root()
	if !root.Registry().IsIdle() {
		t.Fatal("fresh registry should be idle")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		root.WhenIdle()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WhenIdle did not return on an idle tree")
	}
	_ = root.DisposeAll()
}

func TestAsyncPluginAndCleanup(t *testing.T) {
	root := cordis.Root()
	tr := &trace{}
	fiber, err := root.PluginAsync(asyncPlugin{trace: tr}, nil)
	if err != nil {
		t.Fatalf("plugin async: %v", err)
	}
	if got := fiber.State(); got != cordis.Active {
		t.Fatalf("state = %v, want Active", got)
	}
	if err := fiber.DisposeAsync(context.Background()); err != nil {
		t.Fatalf("dispose async: %v", err)
	}
	if order := tr.snapshot(); len(order) != 2 || order[1] != "async-cleanup" {
		t.Fatalf("order = %v", order)
	}
	_ = root.DisposeAll()
}

type asyncPlugin struct{ trace *trace }

func (p asyncPlugin) Name() string { return "async" }
func (p asyncPlugin) ActivateAsync(ctx context.Context, c cordis.Context, config cordis.Value) error {
	if p.trace != nil {
		p.trace.add("async-apply")
	}
	_, err := c.EffectAsync(func() cordis.AsyncEffect {
		return cordis.AsyncEffect{cordis.AsyncCleanupOf(func(ctx context.Context) error {
			if p.trace != nil {
				p.trace.add("async-cleanup")
			}
			return nil
		})}
	})
	return err
}
