package runtime_test

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cordis "github.com/zeokeel/zeokeel-kernel"
)

// ---------------------------------------------------------------------------
// shared fixtures for CORE-023 (logger/clock) and CORE-024 (config)
// ---------------------------------------------------------------------------

// countingExporter is a LogExporter that snapshots every exported record.
type countingExporter struct {
	mu      sync.Mutex
	records []cordis.LogRecord
	panics  bool
}

func (e *countingExporter) Export(record cordis.LogRecord) {
	if e.panics {
		panic("exporter boom")
	}
	e.mu.Lock()
	e.records = append(e.records, record)
	e.mu.Unlock()
}

func (e *countingExporter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.records)
}

func (e *countingExporter) snapshot() []cordis.LogRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]cordis.LogRecord(nil), e.records...)
}

// leveledExporter exercises the optional per-exporter threshold
// (LeveledLogExporter).
type leveledExporter struct {
	*countingExporter
	threshold cordis.LogLevel
}

func (e leveledExporter) LogLevel() cordis.LogLevel { return e.threshold }

// fakeClock is a deterministic cordis.Clock.
type fakeClock struct {
	mu   sync.Mutex
	now  int64
	mono int64
}

func (c *fakeClock) NowUnixMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NowMonotonicMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mono++
	return c.mono
}

func (c *fakeClock) advance(ms int64) {
	c.mu.Lock()
	c.now += ms
	c.mu.Unlock()
}

func recordMessages(records []cordis.LogRecord) []string {
	out := make([]string, len(records))
	for i, record := range records {
		out[i] = record.Message
	}
	return out
}

func recordSeqs(records []cordis.LogRecord) []int64 {
	out := make([]int64, len(records))
	for i, record := range records {
		out[i] = record.Seq
	}
	return out
}

func assertIncreasingSeqs(t *testing.T, records []cordis.LogRecord) {
	t.Helper()
	for i := 1; i < len(records); i++ {
		if records[i].Seq <= records[i-1].Seq {
			t.Fatalf("log seq not strictly increasing: %v", recordSeqs(records))
		}
	}
}

// ---------------------------------------------------------------------------
// CORE-023: logger — threshold, exporter, ring buffer/seq, intercept,
// clock injection, numeric level clamping.
// Rust: logger.rs:62..251, api_surface.rs:236/250/600/631/659/704
// ---------------------------------------------------------------------------

func TestCore023_LoggerThresholdExporterAndBuffer(t *testing.T) {
	root := newRoot(t)
	service := root.LoggerService()
	// The root installs the built-in logger with the default Info threshold.
	if got := service.Level(); got != cordis.LogInfo {
		t.Fatalf("default logger level = %v, want info", got)
	}
	exporter := &countingExporter{}
	if _, err := root.LogExporter(exporter); err != nil {
		t.Fatalf("exporter: %v", err)
	}
	logger := root.LoggerNamed("demo")
	logger.Error("one", map[string]cordis.Value{"code": 1})
	logger.Warn("two", nil)
	logger.Info("three", nil)
	logger.Debug("four", nil) // filtered by the default Info threshold

	exported := exporter.snapshot()
	if len(exported) != 3 {
		t.Fatalf("exported records = %d (%v), want 3 (debug filtered)", len(exported), recordMessages(exported))
	}
	assertDeepEqual(t, "exported messages", recordMessages(exported), []string{"one", "two", "three"})
	assertDeepEqual(t, "exported levels", []cordis.LogLevel{exported[0].Level, exported[1].Level, exported[2].Level},
		[]cordis.LogLevel{cordis.LogError, cordis.LogWarn, cordis.LogInfo})
	for i, record := range exported {
		if record.Name != "demo" {
			t.Fatalf("exported record %d name = %q, want demo", i, record.Name)
		}
	}
	assertIncreasingSeqs(t, exported)

	records := service.Records()
	if len(records) != 3 {
		t.Fatalf("buffer records = %d (%v), want 3", len(records), recordMessages(records))
	}
	if records[0].Level != cordis.LogError {
		t.Fatalf("records[0].level = %v, want error", records[0].Level)
	}
	if got := records[0].Fields["code"]; got != 1 {
		t.Fatalf("records[0].fields[code] = %v (%T), want 1", got, got)
	}
	assertIncreasingSeqs(t, records)

	// IsEnabled follows the service threshold monotonically (Error=0 … Debug=3).
	service.SetLevel(cordis.LogWarn)
	if !service.IsEnabled(cordis.LogError) || !service.IsEnabled(cordis.LogWarn) {
		t.Fatal("IsEnabled: error/warn should be enabled at the warn threshold")
	}
	if service.IsEnabled(cordis.LogInfo) || service.IsEnabled(cordis.LogDebug) {
		t.Fatal("IsEnabled: info/debug must be disabled at the warn threshold")
	}
}

func TestCore023_RingBufferBoundedSeqAndClear(t *testing.T) {
	service := cordis.NewLoggerServiceWithBuffer(cordis.LogDebug, 3)
	if got := service.Level(); got != cordis.LogDebug {
		t.Fatalf("service level = %v, want debug (constructor level must be honored)", got)
	}
	exporter := &countingExporter{}
	disposer := service.Exporter(exporter)
	logger := service.Logger("ring")
	for i := 1; i <= 5; i++ {
		logger.Info(logMessage(i), nil)
	}
	records := service.Records()
	if len(records) != 3 {
		t.Fatalf("ring buffer length = %d, want 3 (bounded)", len(records))
	}
	assertDeepEqual(t, "ring buffer messages", recordMessages(records), []string{"m3", "m4", "m5"})
	assertDeepEqual(t, "ring buffer seqs", recordSeqs(records), []int64{3, 4, 5})
	assertIncreasingSeqs(t, records)

	// The exporter disposer is idempotent; a removed exporter stops receiving.
	disposer.Dispose()
	disposer.Dispose()
	if got := exporter.count(); got != 5 {
		t.Fatalf("exporter count = %d, want 5", got)
	}
	if got := service.ExporterCount(); got != 0 {
		t.Fatalf("exporter count = %d, want 0 after disposer", got)
	}

	// ClearBuffer only drops the buffer: the exporter keeps receiving and the
	// exporter disposer is idempotent. Rust:
	// api_surface.rs:250::api_logger_exporter_count_and_clear_buffer.
	clearService := cordis.NewLoggerServiceWithBuffer(cordis.LogInfo, 1)
	clearExporter := &countingExporter{}
	clearDisposer := clearService.Exporter(clearExporter)
	clearLogger := clearService.Logger("clear")
	clearLogger.Info("kept", nil)
	if got := clearExporter.count(); got != 1 {
		t.Fatalf("exporter count before clear = %d, want 1", got)
	}
	clearService.ClearBuffer()
	if got := clearService.Records(); len(got) != 0 {
		t.Fatalf("records after ClearBuffer = %d, want 0", len(got))
	}
	clearLogger.Info("after-clear", nil)
	clearRecords := clearService.Records()
	if len(clearRecords) != 1 || clearRecords[0].Message != "after-clear" {
		t.Fatalf("records after clear+log = %v, want [after-clear]", recordMessages(clearRecords))
	}
	if got := clearExporter.count(); got != 2 {
		t.Fatalf("exporter count after clear+log = %d, want 2 (clear only affects the buffer)", got)
	}
	clearDisposer.Dispose()
	clearDisposer.Dispose()
	clearLogger.Info("after-dispose", nil)
	if got := clearExporter.count(); got != 2 {
		t.Fatalf("disposed exporter count = %d, want 2 (idempotent disposer)", got)
	}

	// bufferSize=0 disables the ring buffer; exporters still receive records.
	noBuffer := cordis.NewLoggerServiceWithBuffer(cordis.LogDebug, 0)
	noBufferExporter := &countingExporter{}
	noBuffer.Exporter(noBufferExporter)
	noBuffer.Logger("nobuf").Info("only-exported", nil)
	if got := noBuffer.Records(); len(got) != 0 {
		t.Fatalf("buffer-disabled service kept %d records, want 0", len(got))
	}
	if got := noBufferExporter.count(); got != 1 {
		t.Fatalf("buffer-disabled service exporter count = %d, want 1", got)
	}
}

func logMessage(i int) string {
	return "m" + string(rune('0'+i))
}

// TestCore023_ConcurrentLoggingSeqUnique exercises the documented concurrent
// logging contract: seq values are unique and monotonic per allocation, the
// ring buffer stays bounded, and exporters see every record exactly once.
func TestCore023_ConcurrentLoggingSeqUnique(t *testing.T) {
	root := newRoot(t)
	service := root.LoggerService()
	service.SetLevel(cordis.LogDebug)
	exporter := &countingExporter{}
	if _, err := root.LogExporter(exporter); err != nil {
		t.Fatalf("exporter: %v", err)
	}
	logger := root.LoggerNamed("conc")
	const goroutines, perGoroutine = 8, 25
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				logger.Info("concurrent", map[string]cordis.Value{"g": i})
			}
		}()
	}
	wg.Wait()
	total := goroutines * perGoroutine
	records := service.Records()
	if len(records) != total {
		t.Fatalf("buffer records = %d, want %d", len(records), total)
	}
	seen := make(map[int64]struct{}, total)
	for _, record := range records {
		if _, duplicate := seen[record.Seq]; duplicate {
			t.Fatalf("duplicate log seq %d", record.Seq)
		}
		seen[record.Seq] = struct{}{}
	}
	if got := exporter.count(); got != total {
		t.Fatalf("exporter count = %d, want %d", got, total)
	}

	// A bounded buffer under concurrency keeps at most bufferSize records.
	bounded := cordis.NewLoggerServiceWithBuffer(cordis.LogDebug, 16)
	boundedExporter := &countingExporter{}
	bounded.Exporter(boundedExporter)
	boundedLogger := bounded.Logger("bounded")
	var boundedWG sync.WaitGroup
	boundedWG.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer boundedWG.Done()
			for j := 0; j < perGoroutine; j++ {
				boundedLogger.Info("bounded", nil)
			}
		}()
	}
	boundedWG.Wait()
	if got := len(bounded.Records()); got > 16 {
		t.Fatalf("bounded buffer grew to %d records, want <= 16", got)
	}
	if got := boundedExporter.count(); got != total {
		t.Fatalf("bounded exporter count = %d, want %d", got, total)
	}
}

func TestCore023_LoggerInterceptNameAndLevelClamp(t *testing.T) {
	root := newRoot(t)
	service := root.LoggerService()
	service.SetLevel(cordis.LogInfo)

	if got := root.Logger().Name(); got != "root" {
		t.Fatalf("root logger name = %q, want root", got)
	}
	custom := root.Intercept("logger", map[string]cordis.Value{"name": "custom.name"})
	if got := custom.Logger().Name(); got != "custom.name" {
		t.Fatalf("intercepted logger name = %q, want custom.name", got)
	}

	// level=3 (numeric) enables debug on the intercepted logger.
	debugCtx := root.Intercept("logger", map[string]cordis.Value{"name": "dbg", "level": float64(3)})
	debugLogger := debugCtx.Logger()
	if level, ok := debugLogger.Level(); !ok || level != cordis.LogDebug {
		t.Fatalf("intercepted level = %v/%v, want debug/true", level, ok)
	}
	debugLogger.Debug("debug-visible", nil)
	records := service.Records()
	if len(records) != 1 || records[0].Level != cordis.LogDebug || records[0].Name != "dbg" {
		t.Fatalf("intercepted debug record = %+v, want one debug record named dbg", records)
	}
	service.ClearBuffer()

	// An unknown numeric level (99) clamps fail-closed to Error: it must not
	// widen the threshold and let debug/info/warn through.
	badCtx := root.Intercept("logger", map[string]cordis.Value{"name": "bad", "level": float64(99)})
	badLogger := badCtx.Logger()
	if level, ok := badLogger.Level(); !ok || level != cordis.LogError {
		t.Fatalf("level 99 clamped to %v/%v, want error/true", level, ok)
	}
	badLogger.Debug("d", nil)
	badLogger.Info("i", nil)
	badLogger.Warn("w", nil)
	badLogger.Error("e", nil)
	records = service.Records()
	if len(records) != 1 || records[0].Level != cordis.LogError || records[0].Message != "e" {
		t.Fatalf("records under clamped level 99 = %v, want only the error record", recordMessages(records))
	}
	service.ClearBuffer()

	// A string level is parsed through ParseLogLevel.
	stringCtx := root.Intercept("logger", map[string]cordis.Value{"name": "str", "level": "debug"})
	if level, ok := stringCtx.Logger().Level(); !ok || level != cordis.LogDebug {
		t.Fatalf("string level = %v/%v, want debug/true", level, ok)
	}

	// LogLevelFromU8: 0..3 map exactly, everything else clamps to Error.
	for _, tc := range []struct {
		code uint8
		want cordis.LogLevel
	}{
		{0, cordis.LogError}, {1, cordis.LogWarn}, {2, cordis.LogInfo},
		{3, cordis.LogDebug}, {4, cordis.LogError}, {255, cordis.LogError},
	} {
		if got := cordis.LogLevelFromU8(tc.code); got != tc.want {
			t.Fatalf("LogLevelFromU8(%d) = %v, want %v", tc.code, got, tc.want)
		}
	}
}

func TestCore023_LoggerServiceInjectable(t *testing.T) {
	root := newRoot(t)
	key := cordis.NewServiceKey[*cordis.LoggerService]("logger")
	service, ok := cordis.Get(root, key)
	if !ok || service != root.LoggerService() {
		t.Fatalf("injected logger service = %p/%v, want the facade service %p", service, ok, root.LoggerService())
	}

	// A plugin that declares the logger dependency activates and sees it.
	consumer := &loggerConsumerPlugin{key: key}
	fiber, err := root.Plugin(consumer, nil)
	if err != nil {
		t.Fatalf("logger consumer plugin: %v", err)
	}
	if got := fiber.State(); got != cordis.Active {
		t.Fatalf("logger consumer state = %v, want Active", got)
	}
	if consumer.saw != root.LoggerService() {
		t.Fatalf("plugin saw logger service %p, want %p", consumer.saw, root.LoggerService())
	}
}

type loggerConsumerPlugin struct {
	key cordis.ServiceKey[*cordis.LoggerService]
	saw *cordis.LoggerService
}

func (p *loggerConsumerPlugin) Name() string     { return "logger-consumer" }
func (p *loggerConsumerPlugin) Inject() []string { return []string{p.key.Name()} }

func (p *loggerConsumerPlugin) Activate(c cordis.Context, _ cordis.Value) error {
	service, _ := cordis.Get(c, p.key)
	p.saw = service
	return nil
}

func TestCore023_LogExporterUnloadsWithFiber(t *testing.T) {
	root := newRoot(t)
	root.LoggerService().SetLevel(cordis.LogDebug)
	exporter := &countingExporter{}
	owner, err := root.Plugin(setupPlugin{name: "exporter-owner", setup: func(c cordis.Context) error {
		_, err := c.LogExporter(exporter)
		return err
	}}, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	root.LoggerNamed("x").Info("before", nil)
	if got := exporter.count(); got != 1 {
		t.Fatalf("exporter count before disposal = %d, want 1", got)
	}
	if err := owner.Dispose(); err != nil {
		t.Fatalf("dispose owner: %v", err)
	}
	if got := root.LoggerService().ExporterCount(); got != 0 {
		t.Fatalf("exporter count after disposal = %d, want 0", got)
	}
	root.LoggerNamed("x").Info("after", nil)
	if got := exporter.count(); got != 1 {
		t.Fatalf("disposed exporter received a record: count = %d, want 1", got)
	}
}

func TestCore023_ExporterPanicContainedAndLeveled(t *testing.T) {
	root := newRoot(t)
	service := root.LoggerService()
	service.SetLevel(cordis.LogDebug)
	bad := &countingExporter{panics: true}
	good := &countingExporter{}
	if _, err := root.LogExporter(bad); err != nil {
		t.Fatalf("bad exporter: %v", err)
	}
	if _, err := root.LogExporter(good); err != nil {
		t.Fatalf("good exporter: %v", err)
	}
	logger := root.LoggerNamed("x")
	logger.Info("first", nil)
	logger.Info("second", nil)
	// The panicking exporter must not skip the remaining exporter or the
	// buffer write.
	if got := good.count(); got != 2 {
		t.Fatalf("good exporter count = %d, want 2 (bad exporter panic contained)", got)
	}
	if got := service.Records(); len(got) != 2 {
		t.Fatalf("buffer records = %d, want 2", len(got))
	}

	// Per-exporter threshold via LeveledLogExporter.
	leveledRoot := newRoot(t)
	leveledService := leveledRoot.LoggerService()
	leveledService.SetLevel(cordis.LogDebug)
	leveled := &leveledExporter{countingExporter: &countingExporter{}, threshold: cordis.LogWarn}
	if _, err := leveledRoot.LogExporter(leveled); err != nil {
		t.Fatalf("leveled exporter: %v", err)
	}
	leveledLogger := leveledRoot.LoggerNamed("x")
	leveledLogger.Error("e", nil)
	leveledLogger.Warn("w", nil)
	leveledLogger.Info("i", nil)
	leveledLogger.Debug("d", nil)
	if got := leveled.count(); got != 2 {
		t.Fatalf("leveled exporter count = %d, want 2 (error+warn only)", got)
	}
	if got := len(leveledService.Records()); got != 4 {
		t.Fatalf("leveled buffer records = %d, want 4 (buffer uses the service threshold)", got)
	}
}

func TestCore023_ExporterClockInjection(t *testing.T) {
	root := newRoot(t)
	service := root.LoggerService()
	// A facade created before SetClock snapshots the old (system) clock.
	oldLogger := root.LoggerNamed("old")
	fc := &fakeClock{now: 1_700_000_000_123}
	if err := root.SetClock(fc); err != nil {
		t.Fatalf("SetClock: %v", err)
	}
	newLogger := root.LoggerNamed("new")

	oldLogger.Info("old", nil)
	newLogger.Info("new", nil)
	records := service.Records()
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
	if records[0].TsUnixMs == 1_700_000_000_123 {
		t.Fatalf("old facade used the injected clock (%d); it must keep its creation-time clock", records[0].TsUnixMs)
	}
	if records[0].TsUnixMs < 1_600_000_000_000 {
		t.Fatalf("old facade timestamp = %d, want a real system timestamp", records[0].TsUnixMs)
	}
	if records[1].TsUnixMs != 1_700_000_000_123 {
		t.Fatalf("injected clock timestamp = %d, want 1700000000123", records[1].TsUnixMs)
	}

	// Advancing the fake clock advances record timestamps.
	fc.advance(876)
	root.LoggerNamed("new").Info("advanced", nil)
	records = service.Records()
	if got := records[len(records)-1].TsUnixMs; got != 1_700_000_000_999 {
		t.Fatalf("advanced timestamp = %d, want 1700000000999", got)
	}

	// Any context derived from the root shares the tree clock slot.
	derived := root.Extend()
	sharedClock := &fakeClock{now: 7}
	if err := derived.SetClock(sharedClock); err != nil {
		t.Fatalf("derived SetClock: %v", err)
	}
	if root.Clock() != cordis.Clock(sharedClock) {
		t.Fatal("root context does not see the clock installed through its derived root scope")
	}
	derived.LoggerNamed("derived").Info("derived", nil)
	root.LoggerNamed("after-derived").Info("after", nil)
	records = service.Records()
	if got := records[len(records)-2].TsUnixMs; got != 7 {
		t.Fatalf("derived-context timestamp = %d, want 7", got)
	}
	if got := records[len(records)-1].TsUnixMs; got != 7 {
		t.Fatalf("root timestamp after derived SetClock = %d, want 7 (shared slot)", got)
	}
}

func TestCore023_SetClockOnlyOnRootContext(t *testing.T) {
	root := newRoot(t)
	result := make(chan error, 1)
	plugin := &clockSetterPlugin{result: result}
	fiber, err := root.Plugin(plugin, nil)
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := fiber.State(); got != cordis.Active {
		t.Fatalf("clock-setter state = %v, want Active", got)
	}
	setErr := waitErr(t, result, 2*time.Second, "SetClock result")
	if setErr == nil {
		t.Fatal("SetClock on a plugin context returned nil, want InvalidPlugin")
	}
	code, ok := cordis.CodeOf(setErr)
	if !ok || code != cordis.CodeInvalidPlugin {
		t.Fatalf("SetClock error code = %v/%v, want invalid_plugin", code, ok)
	}
	if !strings.Contains(setErr.Error(), "root") {
		t.Fatalf("SetClock error = %q, want the message to mention the root context", setErr.Error())
	}
}

type clockSetterPlugin struct {
	result chan error
}

func (p *clockSetterPlugin) Name() string { return "clock-setter" }

func (p *clockSetterPlugin) Activate(c cordis.Context, _ cordis.Value) error {
	p.result <- c.SetClock(&fakeClock{now: 1})
	return nil
}

func TestCore023_SystemClockMonotonic(t *testing.T) {
	var clock cordis.SystemClock
	first := clock.NowMonotonicMs()
	second := clock.NowMonotonicMs()
	if second < first {
		t.Fatalf("SystemClock monotonic went backwards: %d -> %d", first, second)
	}
	if got := clock.NowUnixMs(); got <= 1_600_000_000_000 {
		t.Fatalf("SystemClock unix ms = %d, want a plausible wall-clock timestamp", got)
	}
}

func TestCore023_SlogExporterBridge(t *testing.T) {
	var buffer bytes.Buffer
	target := slog.New(slog.NewTextHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelDebug}))
	root := newRoot(t)
	root.LoggerService().SetLevel(cordis.LogDebug)
	if _, err := root.LogExporter(cordis.SlogExporter{Logger: target}); err != nil {
		t.Fatalf("slog exporter: %v", err)
	}
	root.LoggerNamed("demo").Error("bridge-message", map[string]cordis.Value{"k": "v"})
	out := buffer.String()
	if !strings.Contains(out, "bridge-message") {
		t.Fatalf("slog output %q does not contain the message", out)
	}
	if !strings.Contains(out, "k=v") {
		t.Fatalf("slog output %q does not contain the structured field", out)
	}
	// The Go counterpart of the TracingExporter forwards LogRecord.Name as the
	// `name` field (Rust: tracing target/field `name`).
	if !strings.Contains(out, "demo") {
		t.Fatalf("slog output %q does not contain the logger name (name field)", out)
	}
}

// ---------------------------------------------------------------------------
// CORE-024: config schemas — validate/merge/deny_unknown/typed-null optional
// fields, resolved-vs-raw separation, update validation and intercept
// resolve_config ordering.
// Rust: config.rs:56..173, service.rs:190
// ---------------------------------------------------------------------------

func validationIssues(t *testing.T, err error) []string {
	t.Helper()
	if err == nil {
		t.Fatal("expected a validation error, got nil")
	}
	var validation *cordis.ValidationError
	if errors.As(err, &validation) {
		return validation.Issues()
	}
	// Fiber-level validation failures surface as a CordisError with
	// CodeValidation carrying the ValidationError in its Validation() payload.
	var cordisErr *cordis.CordisError
	if errors.As(err, &cordisErr) && cordisErr.Code() == cordis.CodeValidation && cordisErr.Validation() != nil {
		return cordisErr.Validation().Issues()
	}
	t.Fatalf("error %v (%T) carries no validation issues", err, err)
	return nil
}

func hasIssue(issues []string, substr string) bool {
	for _, issue := range issues {
		if strings.Contains(issue, substr) {
			return true
		}
	}
	return false
}

func TestCore024_JsonConfigSchemaValidate(t *testing.T) {
	schema := cordis.NewJsonConfigSchema().
		Required("model").
		DefaultValue("temperature", 0.2).
		Property("model", cordis.JSONString).
		DenyUnknown()

	normalized, err := schema.Validate(map[string]cordis.Value{"model": "a"})
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	assertDeepEqual(t, "normalized config", normalized,
		map[string]cordis.Value{"model": "a", "temperature": 0.2})

	_, err = schema.Validate(map[string]cordis.Value{"model": 1, "extra": true})
	issues := validationIssues(t, err)
	if !hasIssue(issues, "field `model` must be string, got number") {
		t.Fatalf("issues = %v, want a model type issue", issues)
	}
	if !hasIssue(issues, "unknown field `extra`") {
		t.Fatalf("issues = %v, want an unknown-field issue", issues)
	}
	if !strings.HasPrefix(err.Error(), "invalid config:") {
		t.Fatalf("validation error text = %q, want the invalid config: prefix", err.Error())
	}

	// nil input is treated as the empty object: the required field is missing.
	if _, err := schema.Validate(nil); !hasIssue(validationIssues(t, err), "missing required field `model`") {
		t.Fatalf("nil config issues = %v, want a missing-model issue", validationIssues(t, err))
	}
	// Non-object input is rejected with the exact kind name.
	if _, err := schema.Validate("nope"); !hasIssue(validationIssues(t, err), "expected an object, got string") {
		t.Fatalf("non-object config issues = %v", validationIssues(t, err))
	}

	// Optional typed field via nil default + explicit Property: absent by
	// default (no nil materialization), type-checked when provided.
	optional := cordis.NewJsonConfigSchema().Property("opt", cordis.JSONString)
	value, err := optional.Validate(map[string]cordis.Value{})
	if err != nil {
		t.Fatalf("optional field rejected when absent: %v", err)
	}
	if object, ok := value.(map[string]cordis.Value); !ok {
		t.Fatalf("normalized empty config = %#v, want an object", value)
	} else if _, present := object["opt"]; present {
		t.Fatalf("optional typed field was materialized as %v, want absent", object["opt"])
	}
	if _, err := optional.Validate(map[string]cordis.Value{"opt": 3}); !hasIssue(validationIssues(t, err), "field `opt` must be string, got number") {
		t.Fatalf("optional field type issue = %v", validationIssues(t, err))
	}

	// A nil default without a declared kind is injected as a nil value.
	untypedNil := cordis.NewJsonConfigSchema().DefaultValue("maybe", nil)
	value, err = untypedNil.Validate(map[string]cordis.Value{})
	if err != nil {
		t.Fatalf("nil default rejected: %v", err)
	}
	object := value.(map[string]cordis.Value)
	maybe, present := object["maybe"]
	if !present || maybe != nil {
		t.Fatalf("nil default injection = %v/%v, want present nil", maybe, present)
	}

	// A default that conflicts with the declared kind is a schema authoring
	// error and is reported (and never injected).
	conflicting := cordis.NewJsonConfigSchema().DefaultValue("x", 1).Property("x", cordis.JSONString)
	if _, err := conflicting.Validate(map[string]cordis.Value{}); !hasIssue(validationIssues(t, err), "default value for `x` must be string, got number") {
		t.Fatalf("conflicting default issues = %v", validationIssues(t, err))
	}

	// required alone does not declare a property, so DenyUnknown rejects it.
	requiredOnly := cordis.NewJsonConfigSchema().Required("model").DenyUnknown()
	if _, err := requiredOnly.Validate(map[string]cordis.Value{"model": "x"}); !hasIssue(validationIssues(t, err), "unknown field `model`") {
		t.Fatalf("required-only schema issues = %v, want unknown field model", validationIssues(t, err))
	}
}

func TestCore024_JsonConfigSchemaMerge(t *testing.T) {
	schema := cordis.NewJsonConfigSchema().
		Required("model").
		DefaultValue("temperature", 0.2).
		Property("model", cordis.JSONString)
	merged := schema.Merge(
		map[string]cordis.Value{"model": "base", "keep": 1},
		[]cordis.Value{
			map[string]cordis.Value{"temperature": 1.0},
			map[string]cordis.Value{"model": "top", "extra": 2},
		},
	)
	assertDeepEqual(t, "layered merge", merged,
		map[string]cordis.Value{"model": "top", "keep": 1, "temperature": 1.0, "extra": 2})

	// A default never overrides an explicitly provided value.
	assertDeepEqual(t, "merge keeps explicit default key", schema.Merge(map[string]cordis.Value{"temperature": 0.5}, nil),
		map[string]cordis.Value{"temperature": 0.5})

	// Missing keys get their defaults.
	assertDeepEqual(t, "merge injects defaults", schema.Merge(nil, nil),
		map[string]cordis.Value{"temperature": 0.2})

	// Non-object layers are skipped.
	assertDeepEqual(t, "merge skips non-object layers", schema.Merge("nope", []cordis.Value{[]cordis.Value{1}}),
		map[string]cordis.Value{"temperature": 0.2})

	// A typed nil default is not materialized (optional field), while an
	// untyped nil default is.
	typedNil := cordis.NewJsonConfigSchema().DefaultValue("opt", nil).Property("opt", cordis.JSONString)
	mergedObject := typedNil.Merge(nil, nil).(map[string]cordis.Value)
	if _, present := mergedObject["opt"]; present {
		t.Fatalf("typed nil default materialized in merge: %v", mergedObject)
	}
	untypedNil := cordis.NewJsonConfigSchema().DefaultValue("maybe", nil)
	mergedObject = untypedNil.Merge(nil, nil).(map[string]cordis.Value)
	if value, present := mergedObject["maybe"]; !present || value != nil {
		t.Fatalf("untyped nil default merge = %v/%v, want present nil", value, present)
	}
}

type configSchemaPlugin struct {
	name   string
	schema cordis.ConfigSchema
	runs   *int32
	seen   *[]cordis.Value
}

func (p *configSchemaPlugin) Name() string { return p.name }

func (p *configSchemaPlugin) ConfigSchema() cordis.ConfigSchema { return p.schema }

func (p *configSchemaPlugin) Activate(_ cordis.Context, config cordis.Value) error {
	atomic.AddInt32(p.runs, 1)
	*p.seen = append(*p.seen, config)
	return nil
}

func TestCore024_SchemaPluginDefaultsAndUpdate(t *testing.T) {
	root := newRoot(t)
	schema := cordis.NewJsonConfigSchema().
		Required("model").
		DefaultValue("temperature", 0.2).
		Property("model", cordis.JSONString).
		DenyUnknown()
	var runs int32
	var seen []cordis.Value
	plugin := &configSchemaPlugin{name: "schema-target", schema: schema, runs: &runs, seen: &seen}
	fiber, err := root.Plugin(plugin, map[string]cordis.Value{"model": "a"})
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	if got := fiber.State(); got != cordis.Active {
		t.Fatalf("state = %v, want Active", got)
	}
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("activation count = %d, want 1", got)
	}
	assertDeepEqual(t, "apply config", seen[0], map[string]cordis.Value{"model": "a", "temperature": 0.2})
	assertDeepEqual(t, "fiber.Config", fiber.Config(), seen[0])
	assertDeepEqual(t, "raw config keeps caller value", fiber.RawConfig(), map[string]cordis.Value{"model": "a"})

	// Invalid update (missing required model): Validation error, fiber keeps
	// running with the old config and the raw config is not replaced.
	err = fiber.Update(map[string]cordis.Value{"temperature": 0.9})
	issues := validationIssues(t, err)
	if !hasIssue(issues, "model") {
		t.Fatalf("invalid update issues = %v, want the missing model issue", issues)
	}
	if code, ok := cordis.CodeOf(err); !ok || code != cordis.CodeValidation {
		t.Fatalf("invalid update error code = %v/%v, want validation", code, ok)
	}
	if got := fiber.State(); got != cordis.Active {
		t.Fatalf("state after invalid update = %v, want Active", got)
	}
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("activation count after invalid update = %d, want 1", got)
	}
	assertDeepEqual(t, "config after invalid update", fiber.Config(), seen[0])
	assertDeepEqual(t, "raw config after invalid update", fiber.RawConfig(), map[string]cordis.Value{"model": "a"})

	// Valid update: restart once, new model, default still injected.
	if err := fiber.Update(map[string]cordis.Value{"model": "b"}); err != nil {
		t.Fatalf("valid update: %v", err)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("activation count after valid update = %d, want 2", got)
	}
	assertDeepEqual(t, "config after valid update", fiber.Config(), map[string]cordis.Value{"model": "b", "temperature": 0.2})
	assertDeepEqual(t, "raw config after valid update", fiber.RawConfig(), map[string]cordis.Value{"model": "b"})

	// An unknown field is rejected and does not restart the fiber.
	err = fiber.Update(map[string]cordis.Value{"model": "c", "extra": true})
	if !hasIssue(validationIssues(t, err), "unknown field `extra`") {
		t.Fatalf("unknown-field update issues = %v", validationIssues(t, err))
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("activation count after rejected update = %d, want 2", got)
	}
	assertDeepEqual(t, "config after rejected update", fiber.Config(), map[string]cordis.Value{"model": "b", "temperature": 0.2})
}

func TestCore024_PluginWithoutSchemaKeepsRawConfig(t *testing.T) {
	root := newRoot(t)
	var runs int32
	var seen []cordis.Value
	plugin := &countingPlugin{name: "no-schema", runs: &runs, seen: &seen}
	fiber, err := root.Plugin(plugin, map[string]cordis.Value{"raw": true})
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	assertDeepEqual(t, "raw config without schema", fiber.Config(), map[string]cordis.Value{"raw": true})
	// update(null) is stored verbatim for a plugin without a schema.
	if err := fiber.Update(nil); err != nil {
		t.Fatalf("update(nil): %v", err)
	}
	if got := fiber.Config(); got != nil {
		t.Fatalf("config after update(nil) = %v, want nil", got)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("activation count = %d, want 2 (no schema still restarts on update)", got)
	}
}

func TestCore024_InitialInvalidConfigFailsFiber(t *testing.T) {
	root := newRoot(t)
	schema := cordis.NewJsonConfigSchema().Required("model").Property("model", cordis.JSONString)
	var runs int32
	var seen []cordis.Value
	plugin := &configSchemaPlugin{name: "invalid-initial", schema: schema, runs: &runs, seen: &seen}
	fiber, err := root.Plugin(plugin, map[string]cordis.Value{"extra": true})
	if err != nil {
		t.Fatalf("plugin creation returned error %v, want a Failed fiber with nil error", err)
	}
	if got := fiber.State(); got != cordis.Failed {
		t.Fatalf("state = %v, want Failed", got)
	}
	if got := atomic.LoadInt32(&runs); got != 0 {
		t.Fatalf("activation count = %d, want 0 (invalid initial config must not apply)", got)
	}
	fiberErr := fiber.Error()
	if fiberErr == nil || !strings.Contains(fiberErr.Error(), "model") {
		t.Fatalf("fiber.Error() = %v, want an error mentioning model", fiberErr)
	}
	if code, ok := cordis.CodeOf(fiberErr); !ok || code != cordis.CodeValidation {
		t.Fatalf("fiber.Error() code = %v/%v, want validation", code, ok)
	}
}

func TestCore024_UpdateHookReplacementIsValidated(t *testing.T) {
	root := newRoot(t)
	schema := cordis.NewJsonConfigSchema().Required("model").Property("model", cordis.JSONString)
	var runs int32
	var seen []cordis.Value
	plugin := &configSchemaPlugin{name: "replace-target", schema: schema, runs: &runs, seen: &seen}
	fiber, err := root.Plugin(plugin, map[string]cordis.Value{"model": "a"})
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	hook, err := root.OnInternalUpdate(func(_ *cordis.Fiber, _ cordis.Value, noSave bool, next *cordis.UpdateNext) {
		// Replace the config with a schema-invalid value: the replacement must
		// be validated before it is committed.
		next.Next(map[string]cordis.Value{"bogus": true}, noSave)
	})
	if err != nil {
		t.Fatalf("internal/update hook: %v", err)
	}
	err = fiber.Update(map[string]cordis.Value{"model": "b"})
	if code, ok := cordis.CodeOf(err); !ok || code != cordis.CodeValidation {
		t.Fatalf("replacement update error = %v (%v/%v), want validation", err, code, ok)
	}
	if got := fiber.State(); got != cordis.Active {
		t.Fatalf("state after rejected replacement = %v, want Active", got)
	}
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("activation count = %d, want 1 (rejected replacement must not restart)", got)
	}
	assertDeepEqual(t, "raw config after rejected replacement", fiber.RawConfig(), map[string]cordis.Value{"model": "a"})
	assertDeepEqual(t, "config after rejected replacement", fiber.Config(), map[string]cordis.Value{"model": "a"})
	hook.Dispose()

	// Once the bad replacement hook is gone the same update succeeds.
	if err := fiber.Update(map[string]cordis.Value{"model": "b"}); err != nil {
		t.Fatalf("update after hook removal: %v", err)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("activation count = %d, want 2", got)
	}
	assertDeepEqual(t, "config after valid replacement update", fiber.Config(), map[string]cordis.Value{"model": "b"})
}

func TestCore024_ResolveConfigInterceptLayers(t *testing.T) {
	root := newRoot(t)
	// No schema: base → root intercept → child intercept → head, shallow merged.
	rootLayer := root.Intercept("svc", map[string]cordis.Value{})
	childLayer := rootLayer.Intercept("svc", map[string]cordis.Value{"a": 1, "b": 1})
	resolved := childLayer.ResolveConfig("svc",
		map[string]cordis.Value{"z": 0},
		map[string]cordis.Value{"c": 3})
	assertDeepEqual(t, "shallow resolve layers", resolved,
		map[string]cordis.Value{"z": 0, "a": 1, "b": 1, "c": 3})

	// Head has the highest precedence, then the leaf-most intercept.
	precedence := rootLayer.Intercept("svc", map[string]cordis.Value{"a": 2})
	assertDeepEqual(t, "intercept precedence without head",
		precedence.ResolveConfig("svc", map[string]cordis.Value{"a": 0}, nil),
		map[string]cordis.Value{"a": 2})
	assertDeepEqual(t, "head precedence",
		precedence.ResolveConfig("svc", map[string]cordis.Value{"a": 0}, map[string]cordis.Value{"a": 3}),
		map[string]cordis.Value{"a": 3})

	// With a schema the merged config goes through schema.Merge, so defaults
	// are injected but never override an explicitly provided value.
	schema := cordis.NewJsonConfigSchema().
		Required("model").
		DefaultValue("temperature", 0.2).
		Property("model", cordis.JSONString)
	schemaKey := cordis.NewServiceKey[string]("svc-schema")
	if _, err := cordis.ProvideWithSchema(root, schemaKey, "service-value", schema); err != nil {
		t.Fatalf("provide with schema: %v", err)
	}
	configCtx := root.Intercept("svc-schema", map[string]cordis.Value{"model": "served"})
	resolved = configCtx.ResolveConfig("svc-schema",
		map[string]cordis.Value{"model": "base", "temperature": 1.0},
		map[string]cordis.Value{"top_p": 0.5})
	assertDeepEqual(t, "schema resolve", resolved,
		map[string]cordis.Value{"model": "served", "temperature": 1.0, "top_p": 0.5})
	// Without any explicit layer the schema default is injected.
	assertDeepEqual(t, "schema resolve injects defaults",
		root.ResolveConfig("svc-schema", nil, nil),
		map[string]cordis.Value{"temperature": 0.2})
}
