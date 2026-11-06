package cordis

import (
	"errors"
)

// ErrCode classifies a CordisError. The set mirrors the variants of the Rust
// reference's CordisError enum; errors.Is works on the code, so callers can
// match either a sentinel (ErrLifecycleBusy) or a concrete error returned by an
// operation (errors.Is(err, ErrDuplicateService)).
type ErrCode uint8

const (
	// CodeInactiveEffect: an effect was registered on an Unloading/Disposed
	// fiber.
	CodeInactiveEffect ErrCode = iota + 1
	// CodeDuplicateService: a service name is already provided in the same
	// isolation scope.
	CodeDuplicateService
	// CodeUnknownService: require/require_any found no available service.
	CodeUnknownService
	// CodeInvalidPlugin: plugin metadata is invalid (bad depth, clock on a
	// non-root context, duplicate accessor, ...).
	CodeInvalidPlugin
	// CodePluginFailed: a plugin lifecycle callback failed with a plain error.
	CodePluginFailed
	// CodeDisposed: the target fiber (or its context) was already disposed.
	CodeDisposed
	// CodeValidation: plugin config failed schema validation.
	CodeValidation
	// CodeServiceWriteForbidden: ctx.Set targeted a service owned by another
	// fiber.
	CodeServiceWriteForbidden
	// CodeAccessorReadOnly: ctx.Set targeted a read-only computed accessor.
	CodeAccessorReadOnly
	// CodeAsyncPluginNeedsAsync: a synchronous lifecycle API was used on an
	// async plugin.
	CodeAsyncPluginNeedsAsync
	// CodeCancelled: a cooperative checkpoint observed cancellation.
	CodeCancelled
	// CodeQuotaExceeded: a per-fiber live-resource quota was exceeded.
	CodeQuotaExceeded
	// CodeLifecycleBusy: another (non-reentrant) lifecycle operation currently
	// owns the fiber.
	CodeLifecycleBusy
	// CodeServiceTypeMismatch: Set tried to replace a service with a value of a
	// different concrete type.
	CodeServiceTypeMismatch
	// CodeRollback: Registry.Replace could not restore the previous state.
	CodeRollback
	// CodePlugin: a plugin returned a structured error (PluginError).
	CodePlugin
	// CodeAccessorWriteRejected: a computed accessor's setter inspected the
	// value and returned false.
	CodeAccessorWriteRejected
)

// String returns the stable machine name of the code (for logs and tests).
func (c ErrCode) String() string {
	switch c {
	case CodeInactiveEffect:
		return "inactive_effect"
	case CodeDuplicateService:
		return "duplicate_service"
	case CodeUnknownService:
		return "unknown_service"
	case CodeInvalidPlugin:
		return "invalid_plugin"
	case CodePluginFailed:
		return "plugin_failed"
	case CodeDisposed:
		return "disposed"
	case CodeValidation:
		return "validation"
	case CodeServiceWriteForbidden:
		return "service_write_forbidden"
	case CodeAccessorReadOnly:
		return "accessor_read_only"
	case CodeAsyncPluginNeedsAsync:
		return "async_plugin_needs_async"
	case CodeCancelled:
		return "cancelled"
	case CodeQuotaExceeded:
		return "quota_exceeded"
	case CodeLifecycleBusy:
		return "lifecycle_busy"
	case CodeServiceTypeMismatch:
		return "service_type_mismatch"
	case CodeRollback:
		return "replace_rollback_failed"
	case CodePlugin:
		return "plugin"
	case CodeAccessorWriteRejected:
		return "accessor_write_rejected"
	default:
		return "unknown"
	}
}

// CordisError is the error type returned by kernel operations. It carries the
// classification code plus the variant-specific data; use CodeOf or errors.Is
// to match.
type CordisError struct {
	code ErrCode

	// name is the service / plugin name carried by the code-specific variants.
	name string
	// detail carries variant-specific text (message, quota kind, rollback
	// detail, ...).
	detail string
	// kind is the machine-readable plugin error kind (CodePlugin only).
	kind string
	// validation is set for CodeValidation.
	validation *ValidationError
	// cause is an optional wrapped cause.
	cause error
}

// Error implements the error interface using the Rust reference's wording, so
// cross-language conformance tests can match messages where they are part of
// the observable contract.
func (e *CordisError) Error() string {
	switch e.code {
	case CodeInactiveEffect:
		return "cannot create effect on inactive context"
	case CodeDuplicateService:
		return "service already provided: " + e.name
	case CodeUnknownService:
		return "service not available: " + e.name
	case CodeInvalidPlugin:
		return "invalid plugin: " + e.detail
	case CodePluginFailed:
		return "plugin failed: " + e.detail
	case CodeDisposed:
		return "fiber already disposed"
	case CodeValidation:
		if e.validation != nil {
			return e.validation.Error()
		}
		return "validation failed"
	case CodeServiceWriteForbidden:
		return "service provided by another fiber: " + e.name + " (ctx.set)"
	case CodeAccessorReadOnly:
		return "computed accessor is read-only: " + e.name + " (ctx.set)"
	case CodeAsyncPluginNeedsAsync:
		return "plugin `" + e.name + "` implements AsyncPlugin: use plugin_async/restart_async/update_async"
	case CodeCancelled:
		return "operation cancelled"
	case CodeQuotaExceeded:
		return "resource quota exceeded: " + e.detail
	case CodeLifecycleBusy:
		return "another lifecycle operation is already in progress"
	case CodeServiceTypeMismatch:
		return "service type mismatch: " + e.name
	case CodeRollback:
		return "replace rollback failed: " + e.detail
	case CodePlugin:
		return "plugin error: " + e.name + " (" + e.kind + "): " + e.detail
	case CodeAccessorWriteRejected:
		return "computed accessor rejected the write: " + e.name
	default:
		return "cordis error"
	}
}

// Code returns the classification code.
func (e *CordisError) Code() ErrCode { return e.code }

// Name returns the service or plugin name carried by the variant (empty when
// the variant has none).
func (e *CordisError) Name() string { return e.name }

// Detail returns the free-form detail carried by the variant.
func (e *CordisError) Detail() string { return e.detail }

// PluginKind returns the machine-readable kind of a structured plugin error.
func (e *CordisError) PluginKind() string { return e.kind }

// Validation returns the schema validation issues of a CodeValidation error.
func (e *CordisError) Validation() *ValidationError { return e.validation }

// Unwrap returns the wrapped cause, if any.
func (e *CordisError) Unwrap() error { return e.cause }

// Is makes errors.Is match by classification code. Sentinels (ErrLifecycleBusy,
// ErrDuplicateService, ...) report their code and therefore match any error of
// the same variant.
func (e *CordisError) Is(target error) bool {
	var other *CordisError
	if !errors.As(target, &other) {
		return false
	}
	return e.code == other.code
}

// CodeOf reports the CordisError code of err, if it is a CordisError.
func CodeOf(err error) (ErrCode, bool) {
	var e *CordisError
	if errors.As(err, &e) {
		return e.code, true
	}
	return 0, false
}

// IsCode reports whether err is (or wraps) a CordisError with the given code.
func IsCode(err error, code ErrCode) bool {
	return errors.Is(err, &CordisError{code: code})
}

// Sentinel errors: the field-less variants, plus code-carrying sentinels that
// errors.Is can match against any instance of the variant.
var (
	ErrInactiveEffect  = &CordisError{code: CodeInactiveEffect}
	ErrDisposed        = &CordisError{code: CodeDisposed}
	ErrCancelled       = &CordisError{code: CodeCancelled}
	ErrLifecycleBusy   = &CordisError{code: CodeLifecycleBusy}
	ErrDuplicateService = &CordisError{code: CodeDuplicateService}
)

// Constructors for the data-carrying variants.

// DuplicateServiceError reports a service name already provided in the scope.
func DuplicateServiceError(name string) *CordisError {
	return &CordisError{code: CodeDuplicateService, name: name}
}

// UnknownServiceError reports a service that is not available.
func UnknownServiceError(name string) *CordisError {
	return &CordisError{code: CodeUnknownService, name: name}
}

// InvalidPluginError reports invalid plugin metadata.
func InvalidPluginError(detail string) *CordisError {
	return &CordisError{code: CodeInvalidPlugin, detail: detail}
}

// PluginFailedError reports a plugin lifecycle callback that returned an error.
func PluginFailedError(detail string) *CordisError {
	return &CordisError{code: CodePluginFailed, detail: detail}
}

// ValidationErrorOf wraps a schema validation failure.
func ValidationErrorOf(v *ValidationError) *CordisError {
	return &CordisError{code: CodeValidation, validation: v}
}

// ServiceWriteForbiddenError reports ctx.Set on a service owned by another
// fiber.
func ServiceWriteForbiddenError(name string) *CordisError {
	return &CordisError{code: CodeServiceWriteForbidden, name: name}
}

// AccessorReadOnlyError reports ctx.Set on a read-only accessor.
func AccessorReadOnlyError(name string) *CordisError {
	return &CordisError{code: CodeAccessorReadOnly, name: name}
}

// AccessorWriteRejectedError reports an accessor setter that rejected a write.
func AccessorWriteRejectedError(name string) *CordisError {
	return &CordisError{code: CodeAccessorWriteRejected, name: name}
}

// AsyncPluginNeedsAsyncError reports a synchronous lifecycle call on an async
// plugin.
func AsyncPluginNeedsAsyncError(name string) *CordisError {
	return &CordisError{code: CodeAsyncPluginNeedsAsync, name: name}
}

// QuotaExceededError reports an exceeded per-fiber resource quota (kind is
// "effects", "listeners" or "children").
func QuotaExceededError(kind string) *CordisError {
	return &CordisError{code: CodeQuotaExceeded, detail: kind}
}

// ServiceTypeMismatchError reports ctx.Set with a value of the wrong type.
func ServiceTypeMismatchError(name string) *CordisError {
	return &CordisError{code: CodeServiceTypeMismatch, name: name}
}

// RollbackError reports an incomplete Registry.Replace rollback; detail carries
// the original error and the rollback error.
func RollbackError(detail string) *CordisError {
	return &CordisError{code: CodeRollback, detail: detail}
}

// PluginError builds a structured plugin error. Plugins should return it from
// Plugin.Activate / AsyncPlugin.ActivateAsync when they can classify their
// failure; the kernel propagates it unchanged through activation, settle
// waiting, restart/update and Registry.Replace.
func PluginError(plugin, kind, message string) *CordisError {
	return &CordisError{code: CodePlugin, name: plugin, kind: kind, detail: message}
}

// wrapCause attaches a cause to a CordisError (used where the original error is
// useful for diagnostics).
func wrapCause(e *CordisError, cause error) error {
	if cause == nil {
		return e
	}
	clone := *e
	clone.cause = cause
	return &clone
}

// ValidationError is the schema validation failure container.
type ValidationError struct {
	issues []string
}

// NewValidationError builds a validation error from human-readable issues.
func NewValidationError(issues []string) *ValidationError {
	return &ValidationError{issues: append([]string(nil), issues...)}
}

// Issues returns a copy of the validation issues.
func (v *ValidationError) Issues() []string { return append([]string(nil), v.issues...) }

// IsEmpty reports whether the error carries no issue.
func (v *ValidationError) IsEmpty() bool { return v == nil || len(v.issues) == 0 }

// Error implements error using the aggregated Cordis format ("invalid config:"
// followed by one "- issue" line each).
func (v *ValidationError) Error() string {
	if v == nil || len(v.issues) == 0 {
		return "invalid config:"
	}
	message := "invalid config:"
	for _, issue := range v.issues {
		message += "\n  - " + issue
	}
	return message
}
