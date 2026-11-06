package cordis

import "time"

// Value is the dynamic JSON-like value type used by plugin configs, event
// payloads, logger fields and metadata.
//
// It is an alias of any: values produced by encoding/json are expected
// (map[string]any, []any, string, float64/json.Number, bool, nil), but the
// kernel treats them as opaque data and never inspects them outside the config
// schema validator.
//
// The Rust reference uses serde_json::Value; the alias keeps the same spelling
// in documentation and examples without forcing a concrete representation on
// embedders.
type Value = any

// Clock is the injectable time source of a context tree.
//
// The kernel never reasons about wall-clock deltas; it records timestamps on log
// records and compares monotonic values for activity/idle tracking. Embedders
// inject a deterministic clock in tests/replay/simulated supervisors.
type Clock interface {
	// NowUnixMs returns wall-clock milliseconds since the Unix epoch. Used for
	// log timestamps; a clock may freeze or shift it freely.
	NowUnixMs() int64
	// NowMonotonicMs returns a non-decreasing millisecond counter with an
	// arbitrary origin. Used for idle/duration measurements.
	NowMonotonicMs() int64
}

// SystemClock is the default Clock: wall time for Unix timestamps and a
// process-local monotonic origin for durations. Every SystemClock shares the
// same origin, so swapping one for another never resets durations.
type SystemClock struct{}

var processStart = time.Now()

// NowUnixMs implements Clock.
func (SystemClock) NowUnixMs() int64 { return time.Now().UnixMilli() }

// NowMonotonicMs implements Clock.
func (SystemClock) NowMonotonicMs() int64 { return time.Since(processStart).Milliseconds() }
