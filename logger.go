package cordis

import (
	"log/slog"
	"sync"
	"sync/atomic"
)

// Severity of a log record, ordered from most to least severe. The numeric
// order is the threshold order used by the Rust reference: a record is emitted
// when its level is at or below the active threshold.
type LogLevel uint8

const (
	LogError LogLevel = iota
	LogWarn
	LogInfo
	LogDebug
)

// ParseLogLevel parses "error"/"warn"/"warning"/"info"/"debug"
// (case-insensitive).
func ParseLogLevel(value string) (LogLevel, bool) {
	switch asciiLower(value) {
	case "error":
		return LogError, true
	case "warn", "warning":
		return LogWarn, true
	case "info":
		return LogInfo, true
	case "debug":
		return LogDebug, true
	default:
		return 0, false
	}
}

// String returns the lower-case name used by exporters and diagnostics.
func (l LogLevel) String() string {
	switch l {
	case LogError:
		return "error"
	case LogWarn:
		return "warn"
	case LogInfo:
		return "info"
	case LogDebug:
		return "debug"
	default:
		return "unknown"
	}
}

// LogLevelFromU8 builds a level from the numeric code of this port
// (Error=0 … Debug=3). Values above 3 are clamped to Error (fail-closed),
// matching the configuration path ({"level": 99} → Error, XT-09).
func LogLevelFromU8(code uint8) LogLevel {
	switch code {
	case 0:
		return LogError
	case 1:
		return LogWarn
	case 2:
		return LogInfo
	case 3:
		return LogDebug
	default:
		return LogError
	}
}

func asciiLower(s string) string {
	out := []byte(s)
	for i, c := range out {
		if 'A' <= c && c <= 'Z' {
			out[i] = c + ('a' - 'A')
		}
	}
	return string(out)
}

// LogRecord is one structured log record delivered to exporters and kept in the
// service's ring buffer.
type LogRecord struct {
	// Seq is a monotonic sequence number, starting at 1.
	Seq int64
	// TsUnixMs is the wall-clock timestamp in milliseconds since the epoch.
	TsUnixMs int64
	// Name is the logger name (usually the plugin/fiber name).
	Name string
	// Level is the severity of this record.
	Level LogLevel
	// Message is the rendered message.
	Message string
	// Fields are the structured fields attached to this record.
	Fields map[string]Value
}

// LogExporter is a sink that receives structured log records. Export is called
// outside the service lock, so exporters may log again without deadlocking.
type LogExporter interface {
	Export(record LogRecord)
}

// LeveledLogExporter is an optional per-exporter verbosity threshold: the most
// verbose level the exporter wants. Exporters not implementing it fall back to
// the service threshold.
type LeveledLogExporter interface {
	LogLevel() LogLevel
}

// DefaultBufferSize is the default ring-buffer capacity (Cordis
// LoggerService.bufferSize).
const DefaultBufferSize = 1000

// LoggerService is the logging service shared by a context tree.
type LoggerService struct {
	level      atomic.Uint32
	exporters  sync.Mutex
	exporterID atomic.Uint64
	buffer     sync.Mutex
	records    []LogRecord
	bufferSize int
	seq        atomic.Int64

	// exporterList is guarded by exporters.
	exporterList []exporterEntry
}

// NewLoggerService creates a service with the given default threshold and the
// default buffer capacity.
func NewLoggerService(level LogLevel) *LoggerService {
	return NewLoggerServiceWithBuffer(level, DefaultBufferSize)
}

// NewLoggerServiceWithBuffer creates a service with an explicit buffer capacity
// (0 disables the buffer).
func NewLoggerServiceWithBuffer(level LogLevel, bufferSize int) *LoggerService {
	return &LoggerService{bufferSize: bufferSize}
}

// Level returns the default threshold of this service.
func (s *LoggerService) Level() LogLevel { return LogLevelFromU8(uint8(s.level.Load())) }

// SetLevel replaces the default threshold.
func (s *LoggerService) SetLevel(level LogLevel) { s.level.Store(uint32(level)) }

// IsEnabled reports whether a record with level passes the service threshold.
func (s *LoggerService) IsEnabled(level LogLevel) bool { return level <= s.Level() }

// Logger creates a named logger at the service threshold. The facade stamps
// records with SystemClock; Context loggers use the tree's injectable clock.
func (s *LoggerService) Logger(name string) *Logger {
	return s.loggerWith(name, nil, SystemClock{})
}

// LoggerWith creates a named logger with an explicit level override.
func (s *LoggerService) LoggerWith(name string, level *LogLevel) *Logger {
	return s.loggerWith(name, level, SystemClock{})
}

func (s *LoggerService) loggerWith(name string, level *LogLevel, clock Clock) *Logger {
	if clock == nil {
		clock = SystemClock{}
	}
	return &Logger{service: s, name: name, level: level, clock: clock}
}

// loggerWithClock is the Context-facing constructor (injected tree clock).
func (s *LoggerService) loggerWithClock(name string, level *LogLevel, clock Clock) *Logger {
	return s.loggerWith(name, level, clock)
}

type exporterEntry struct {
	id       uint64
	exporter LogExporter
}

// Exporter registers an exporter; the returned disposer removes it
// (idempotent).
func (s *LoggerService) Exporter(exporter LogExporter) *Disposer {
	id := s.exporterID.Add(1)
	s.exporters.Lock()
	s.exporterList = append(s.exporterList, exporterEntry{id: id, exporter: exporter})
	s.exporters.Unlock()
	return NewDisposer([]Cleanup{func() {
		s.exporters.Lock()
		for i, entry := range s.exporterList {
			if entry.id == id {
				s.exporterList = append(s.exporterList[:i], s.exporterList[i+1:]...)
				break
			}
		}
		s.exporters.Unlock()
	}})
}

// ExporterCount returns the number of currently registered exporters.
func (s *LoggerService) ExporterCount() int {
	s.exporters.Lock()
	defer s.exporters.Unlock()
	return len(s.exporterList)
}

// Log emits one record against the service threshold and SystemClock.
func (s *LoggerService) Log(level LogLevel, name, message string, fields Value) {
	s.logWith(level, s.Level(), name, message, fields, SystemClock{})
}

// logWith emits one record against an explicit threshold and time source.
func (s *LoggerService) logWith(level, threshold LogLevel, name, message string, fields Value, clock Clock) {
	if level > threshold {
		return
	}
	if clock == nil {
		clock = SystemClock{}
	}
	record := LogRecord{
		Seq:      s.seq.Add(1),
		TsUnixMs: clock.NowUnixMs(),
		Name:     name,
		Level:    level,
		Message:  message,
		Fields:   fieldsAsMap(fields),
	}
	// Snapshot exporters, then call each outside the lock; a broken exporter
	// must not skip the remaining exporters or the buffer write.
	s.exporters.Lock()
	exporters := make([]LogExporter, len(s.exporterList))
	for i, entry := range s.exporterList {
		exporters[i] = entry.exporter
	}
	s.exporters.Unlock()
	for _, exporter := range exporters {
		exporterThreshold := threshold
		if leveled, ok := exporter.(LeveledLogExporter); ok {
			exporterThreshold = contained("cordis::logger", "log exporter LogLevel()", threshold, func() LogLevel {
				return leveled.LogLevel()
			})
		}
		if level > exporterThreshold {
			continue
		}
		exporter := exporter
		containedVoid("cordis::logger", "log exporter Export()", func() {
			exporter.Export(record)
		})
	}
	if s.bufferSize > 0 {
		s.buffer.Lock()
		if len(s.records) == s.bufferSize {
			copy(s.records, s.records[1:])
			s.records = s.records[:len(s.records)-1]
		}
		s.records = append(s.records, record)
		s.buffer.Unlock()
	}
}

// Records returns a snapshot of the most recent records (oldest first).
func (s *LoggerService) Records() []LogRecord {
	s.buffer.Lock()
	defer s.buffer.Unlock()
	return append([]LogRecord(nil), s.records...)
}

// ClearBuffer drops the buffered records.
func (s *LoggerService) ClearBuffer() {
	s.buffer.Lock()
	s.records = nil
	s.buffer.Unlock()
}

// Logger is a named logging facade.
//
// A facade captures its time source at creation: Context.SetClock only affects
// facades created afterwards. Re-acquire the facade after installing a clock if
// it must stamp records with the injected time.
type Logger struct {
	service *LoggerService
	name    string
	level   *LogLevel
	clock   Clock
}

// Name returns the logger name shown on every record.
func (l *Logger) Name() string { return l.name }

// Level returns the threshold override of this facade, if any.
func (l *Logger) Level() (LogLevel, bool) {
	if l.level == nil {
		return 0, false
	}
	return *l.level, true
}

// Service returns the underlying service.
func (l *Logger) Service() *LoggerService { return l.service }

// Clock returns the time source snapshot used for this facade's records.
func (l *Logger) Clock() Clock { return l.clock }

// Child creates a logger for a nested subsystem (parent.child).
func (l *Logger) Child(name string) *Logger {
	return &Logger{service: l.service, name: l.name + "." + name, level: l.level, clock: l.clock}
}

// Log emits a record at level, honoring this facade's threshold override.
func (l *Logger) Log(level LogLevel, message string, fields Value) {
	threshold := l.service.Level()
	if l.level != nil {
		threshold = *l.level
	}
	l.service.logWith(level, threshold, l.name, message, fields, l.clock)
}

// Error logs at error level.
func (l *Logger) Error(message string, fields Value) { l.Log(LogError, message, fields) }

// Warn logs at warn level.
func (l *Logger) Warn(message string, fields Value) { l.Log(LogWarn, message, fields) }

// Info logs at info level.
func (l *Logger) Info(message string, fields Value) { l.Log(LogInfo, message, fields) }

// Debug logs at debug level.
func (l *Logger) Debug(message string, fields Value) { l.Log(LogDebug, message, fields) }

// SlogExporter forwards records to log/slog.
//
// It is the Go counterpart of the Rust reference's TracingExporter: it keeps the
// host's existing logging pipeline working while plugins log through the
// Cordis structured API. Use the zero value to target slog.Default(), or set
// Logger to route to a specific handler.
type SlogExporter struct {
	// Logger is the target slog logger; nil means slog.Default().
	Logger *slog.Logger
}

// Export implements LogExporter.
func (e SlogExporter) Export(record LogRecord) {
	logger := e.Logger
	if logger == nil {
		logger = slog.Default()
	}
	fields := make([]any, 0, len(record.Fields)*2+4)
	fields = append(fields, "seq", record.Seq, "ts_unix_ms", record.TsUnixMs)
	for key, value := range record.Fields {
		fields = append(fields, key, value)
	}
	switch record.Level {
	case LogError:
		logger.Error(record.Message, fields...)
	case LogWarn:
		logger.Warn(record.Message, fields...)
	case LogInfo:
		logger.Info(record.Message, fields...)
	case LogDebug:
		logger.Debug(record.Message, fields...)
	}
}

func fieldsAsMap(fields Value) map[string]Value {
	switch value := fields.(type) {
	case nil:
		return nil
	case map[string]Value:
		out := make(map[string]Value, len(value))
		for k, v := range value {
			out[k] = v
		}
		return out
	default:
		return map[string]Value{"value": value}
	}
}
