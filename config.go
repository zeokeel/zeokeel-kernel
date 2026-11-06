package cordis

import (
	"sort"
)

// Config schemas: validation, defaults and layered merging.
//
// A plugin declares an optional ConfigSchema; Fiber.Update validates a new
// config before restarting, and Context.ResolveConfig merges intercept layers
// through the schema. The wire format stays Value (JSON-shaped); schemas are
// optional, and a plugin without one keeps the historical shallow-merge
// behavior.

// JsonType is a JSON value kind accepted by JsonConfigSchemaProperty.
type JsonType uint8

const (
	// JSONString matches JSON string values.
	JSONString JsonType = iota + 1
	// JSONNumber matches JSON numeric values.
	JSONNumber
	// JSONBool matches JSON boolean values.
	JSONBool
	// JSONObject matches JSON objects (map-shaped values).
	JSONObject
	// JSONArray matches JSON arrays (slice-shaped values).
	JSONArray
)

// Name returns the human-readable type name used in validation issues.
func (t JsonType) Name() string {
	switch t {
	case JSONString:
		return "string"
	case JSONNumber:
		return "number"
	case JSONBool:
		return "boolean"
	case JSONObject:
		return "object"
	case JSONArray:
		return "array"
	default:
		return "value"
	}
}

// Matches reports whether value has this JSON kind.
func (t JsonType) Matches(value Value) bool {
	switch t {
	case JSONString:
		_, ok := value.(string)
		return ok
	case JSONNumber:
		return isJSONNumber(value)
	case JSONBool:
		_, ok := value.(bool)
		return ok
	case JSONObject:
		_, ok := value.(map[string]Value)
		return ok
	case JSONArray:
		_, ok := value.([]Value)
		return ok
	default:
		return false
	}
}

// isJSONNumber accepts every Go numeric representation of a JSON number.
func isJSONNumber(value Value) bool {
	switch value.(type) {
	case float64, float32, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, jsonNumber:
		return true
	default:
		return false
	}
}

// jsonNumber is the subset of encoding/json.Number the kernel relies on; using
// an interface keeps this file free of an encoding/json import while accepting
// json.Number values (which implement String()).
type jsonNumber interface{ String() string }

// jsonKind names the kind of a dynamic value (for validation issues).
func jsonKind(value Value) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case map[string]Value:
		return "object"
	case []Value:
		return "array"
	default:
		if isJSONNumber(value) {
			return "number"
		}
		return "value"
	}
}

// ConfigSchema validates and merges plugin/service configs.
//
// Validate returns the normalized config or a *ValidationError. Merge combines
// layers from lowest to highest precedence (base first).
type ConfigSchema interface {
	Validate(value Value) (Value, error)
	Merge(base Value, overrides []Value) Value
}

// containedValidate validates through an embedder schema, containing panics: a
// panicking Validate surfaces as an ordinary validation failure instead of
// unwinding through Plugin/Update.
func containedValidate(schema ConfigSchema, value Value) (Value, error) {
	type result struct {
		value Value
		err   error
	}
	r := contained("cordis::config", "ConfigSchema.Validate", result{err: NewValidationError([]string{"config schema panicked during validation"})}, func() result {
		v, err := schema.Validate(value)
		return result{value: v, err: err}
	})
	return r.value, r.err
}

// containedMerge merges through an embedder schema, containing panics: a
// panicking Merge degrades to the lowest-precedence layer.
func containedMerge(schema ConfigSchema, base Value, overrides []Value) Value {
	return contained("cordis::config", "ConfigSchema.Merge", base, func() Value {
		return schema.Merge(base, overrides)
	})
}

// JsonConfigSchema is the default JSON-object schema.
//
// Validation accepts an object (or nil, treated as an empty object), injects
// defaults for missing keys, reports missing required keys, checks declared
// property types and — when DenyUnknown was called — rejects undeclared keys.
//
// Optional typed fields are expressed with a nil default plus an explicit
// Property: the nil default then means "absent by default" and is not injected
// (injecting it would contradict the declared kind), so Validate({}) succeeds
// without the key while a provided value is still type-checked. A nil default
// without a declared kind is injected.
type JsonConfigSchema struct {
	required             []string
	defaults             map[string]Value
	properties           map[string]JsonType
	additionalProperties bool
}

// NewJsonConfigSchema returns an object schema that allows unknown keys.
func NewJsonConfigSchema() *JsonConfigSchema {
	return &JsonConfigSchema{
		defaults:             make(map[string]Value),
		properties:           make(map[string]JsonType),
		additionalProperties: true,
	}
}

// Required requires name to be present after defaults are injected.
func (s *JsonConfigSchema) Required(name string) *JsonConfigSchema {
	s.required = append(s.required, name)
	return s
}

// DefaultValue injects value when name is missing.
//
// A non-nil default also declares the property's JSON kind (unless Property
// overrides it). A nil default never declares a type.
func (s *JsonConfigSchema) DefaultValue(name string, value Value) *JsonConfigSchema {
	if value != nil {
		if _, exists := s.properties[name]; !exists {
			switch value.(type) {
			case string:
				s.properties[name] = JSONString
			case bool:
				s.properties[name] = JSONBool
			case map[string]Value:
				s.properties[name] = JSONObject
			case []Value:
				s.properties[name] = JSONArray
			default:
				if isJSONNumber(value) {
					s.properties[name] = JSONNumber
				}
			}
		}
	}
	s.defaults[name] = value
	return s
}

// DenyUnknown rejects keys that are neither declared properties nor defaults.
func (s *JsonConfigSchema) DenyUnknown() *JsonConfigSchema {
	s.additionalProperties = false
	return s
}

// Property declares the expected JSON kind of name.
func (s *JsonConfigSchema) Property(name string, kind JsonType) *JsonConfigSchema {
	s.properties[name] = kind
	return s
}

// Defaults returns a copy of the declared defaults.
func (s *JsonConfigSchema) Defaults() map[string]Value {
	out := make(map[string]Value, len(s.defaults))
	for k, v := range s.defaults {
		out[k] = v
	}
	return out
}

// RequiredKeys returns the required keys.
func (s *JsonConfigSchema) RequiredKeys() []string {
	return append([]string(nil), s.required...)
}

// Validate implements ConfigSchema.
func (s *JsonConfigSchema) Validate(value Value) (Value, error) {
	var object map[string]Value
	switch v := value.(type) {
	case nil:
		object = make(map[string]Value)
	case map[string]Value:
		object = make(map[string]Value, len(v))
		for k, item := range v {
			object[k] = item
		}
	default:
		return nil, NewValidationError([]string{"expected an object, got " + jsonKind(value)})
	}

	issues := make([]string, 0)
	// Declared defaults must match the declared kind (schema authoring error):
	// report it directly instead of letting the injected value be blamed below.
	for _, name := range sortedKeys(s.defaults) {
		def := s.defaults[name]
		if def == nil {
			continue
		}
		if expected, ok := s.properties[name]; ok && !expected.Matches(def) {
			issues = append(issues, "default value for `"+name+"` must be "+expected.Name()+", got "+jsonKind(def))
		}
	}
	// Inject defaults for missing keys.
	for _, name := range sortedKeys(s.defaults) {
		def := s.defaults[name]
		if _, present := object[name]; present {
			continue
		}
		if expected, ok := s.properties[name]; ok {
			// Typed field with a nil default models an optional field: leave the
			// key absent (injecting nil would contradict the declared kind).
			if def == nil {
				continue
			}
			// Conflicting defaults were reported above and must not be injected.
			if !expected.Matches(def) {
				continue
			}
		}
		object[name] = def
	}
	for _, name := range s.required {
		if _, present := object[name]; !present {
			issues = append(issues, "missing required field `"+name+"`")
		}
	}
	for _, name := range sortedKeys(object) {
		value := object[name]
		if expected, ok := s.properties[name]; ok {
			if !expected.Matches(value) {
				issues = append(issues, "field `"+name+"` must be "+expected.Name()+", got "+jsonKind(value))
			}
			continue
		}
		if !s.additionalProperties {
			if _, declared := s.defaults[name]; !declared {
				issues = append(issues, "unknown field `"+name+"`")
			}
		}
	}
	if len(issues) > 0 {
		return nil, NewValidationError(issues)
	}
	return object, nil
}

// Merge implements ConfigSchema. The merge is shallow (higher layers win per
// key); defaults never override an explicitly provided value, and a typed nil
// default is not materialized.
func (s *JsonConfigSchema) Merge(base Value, overrides []Value) Value {
	merged := make(map[string]Value)
	for _, layer := range append([]Value{base}, overrides...) {
		object, ok := layer.(map[string]Value)
		if !ok {
			continue
		}
		for k, v := range object {
			merged[k] = v
		}
	}
	for name, def := range s.defaults {
		if def == nil {
			if _, typed := s.properties[name]; typed {
				continue
			}
		}
		if _, present := merged[name]; !present {
			merged[name] = def
		}
	}
	return merged
}

func sortedKeys(m map[string]Value) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
