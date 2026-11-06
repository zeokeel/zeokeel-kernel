// Package module provides a compile-time module registry and config-driven
// assembly layer for the Cordis Go kernel.
//
// The registry is the "link-time" half of the universal-kernel idea: a host
// binary explicitly registers the Go modules it was compiled with (there is no
// dynamic code loading), and a JSON entry tree selects, configures and nests
// those modules at startup. The kernel remains unchanged; the
// registry adds what a real module system needs on top of it:
//
//   - typed module descriptors (ID, version, provided/required service
//     contracts, typed config, factory) instead of loose string wiring;
//   - pre-flight validation: unknown modules, duplicate instance IDs, missing
//     or type-mismatched providers, duplicate providers and dependency cycles
//     are reported before any fiber is created;
//   - optional requirements and optional entries (a missing optional
//     dependency never leaves a module silently Pending);
//   - stable instance IDs, a parent/child entry tree, locate/close operations;
//   - a startup failure policy that separates "required" from "optional"
//     modules and rolls the tree back on a required failure.
//
// # Design boundary: compile-time composition, not a plugin runtime
//
// The set of modules is fixed when the host is built. "Adopting a new plugin
// system" means adding its Go adapter package to the host's module assembly and
// rebuilding — it does not mean loading foreign code at runtime. That is a
// deliberate trade-off: no ABI, no sandbox, no dynamic linker; in exchange the
// wiring is compile-time checked by Go where possible, and runtime composition
// (instances, configs, nesting) stays fully under kernel control.
package module

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"

	cordis "github.com/zeokeel/zeokeel-kernel"
)

// Provider describes one service contract a module provides.
//
// It is normally built with the Provides helper so the contract type is
// checked by the compiler:
//
//	var DBKey = cordis.NewServiceKey[*DB]("app/db")
//	provides := module.Provides(DBKey) // Name "app/db", Type *DB
type Provider struct {
	Name string
	Type reflect.Type
}

// Requirement describes one service contract a module requires.
//
// A non-optional requirement gates activation through the kernel's inject gate;
// an optional requirement is resolved by the module at activation time (if the
// service is absent the module still loads).
type Requirement struct {
	Name     string
	Type     reflect.Type
	Optional bool
}

// Provides builds a Provider from a typed service key.
func Provides[T any](key cordis.ServiceKey[T]) Provider {
	return Provider{Name: key.Name(), Type: reflect.TypeFor[T]()}
}

// Requires builds a required contract from a typed service key.
func Requires[T any](key cordis.ServiceKey[T]) Requirement {
	return Requirement{Name: key.Name(), Type: reflect.TypeFor[T]()}
}

// RequiresOptional builds an optional contract from a typed service key.
func RequiresOptional[T any](key cordis.ServiceKey[T]) Requirement {
	return Requirement{Name: key.Name(), Type: reflect.TypeFor[T](), Optional: true}
}

// Descriptor is one statically registered module definition.
//
// T is the module's typed config. The registry decodes the entry's JSON-like
// config into T (encoding/json round-trip) and applies the optional Validate
// hook before any fiber starts, so configuration errors are reported by
// pre-flight validation rather than by a failed activation.
type Descriptor[T any] struct {
	// ID is the stable module identity used by entries (e.g.
	// "github.com/acme/app/plugins/http"). It must be unique in a Registry.
	ID string
	// Name is the display name; defaults to ID.
	Name string
	// Version is an optional semantic version used for diagnostics.
	Version string
	// Provides lists the service contracts this module publishes.
	Provides []Provider
	// Requires lists the service contracts that gate activation. Optional
	// requirements are visible to the module through ctx.Get but do not gate
	// loading.
	Requires []Requirement
	// Decode optionally overrides the default JSON round-trip config decoding.
	Decode func(raw cordis.Value) (T, error)
	// Validate optionally validates the decoded config during pre-flight.
	Validate func(config T) error
	// Factory is the module body. It runs as the module fiber's activation and
	// may register services, listeners, effects and child modules through ctx.
	Factory func(ctx cordis.Context, config T) error
}

// Define creates a module descriptor with the given ID and factory. Contract,
// config and metadata can be refined with the chainable Add*/With* methods or
// by writing the exported fields directly.
func Define[T any](id string, factory func(ctx cordis.Context, config T) error) *Descriptor[T] {
	return &Descriptor[T]{ID: id, Factory: factory}
}

// AddProvides appends provided service contracts.
func (d *Descriptor[T]) AddProvides(providers ...Provider) *Descriptor[T] {
	d.Provides = append(d.Provides, providers...)
	return d
}

// AddRequires appends required service contracts.
func (d *Descriptor[T]) AddRequires(requirements ...Requirement) *Descriptor[T] {
	d.Requires = append(d.Requires, requirements...)
	return d
}

// AddOptionalRequires appends optional service contracts.
func (d *Descriptor[T]) AddOptionalRequires(requirements ...Requirement) *Descriptor[T] {
	for _, requirement := range requirements {
		requirement.Optional = true
		d.Requires = append(d.Requires, requirement)
	}
	return d
}

// WithName overrides the display name.
func (d *Descriptor[T]) WithName(name string) *Descriptor[T] { d.Name = name; return d }

// WithVersion records a semantic version for diagnostics.
func (d *Descriptor[T]) WithVersion(version string) *Descriptor[T] {
	d.Version = version
	return d
}

// WithDecode overrides default config decoding.
func (d *Descriptor[T]) WithDecode(decode func(raw cordis.Value) (T, error)) *Descriptor[T] {
	d.Decode = decode
	return d
}

// WithValidate installs a typed config validation hook.
func (d *Descriptor[T]) WithValidate(validate func(config T) error) *Descriptor[T] {
	d.Validate = validate
	return d
}

// moduleDef is the type-erased module definition stored by the registry.
type moduleDef struct {
	id       string
	name     string
	version  string
	provides []Provider
	requires []Requirement
	decode   func(raw cordis.Value) (any, error)
	validate func(config any) error
	run      func(ctx cordis.Context, config any) error
}

func (d *Descriptor[T]) erased() (*moduleDef, error) {
	if d == nil {
		return nil, fmt.Errorf("module: nil descriptor")
	}
	if d.ID == "" {
		return nil, fmt.Errorf("module: descriptor has an empty ID")
	}
	if d.Factory == nil {
		return nil, fmt.Errorf("module %q: nil factory", d.ID)
	}
	name := d.Name
	if name == "" {
		name = d.ID
	}
	seenProviders := make(map[string]bool, len(d.Provides))
	for _, provider := range d.Provides {
		if provider.Name == "" {
			return nil, fmt.Errorf("module %q: provider with an empty service name", d.ID)
		}
		if provider.Type == nil {
			return nil, fmt.Errorf("module %q: provider %q has no contract type", d.ID, provider.Name)
		}
		if seenProviders[provider.Name] {
			return nil, fmt.Errorf("module %q: duplicate provided service %q", d.ID, provider.Name)
		}
		seenProviders[provider.Name] = true
	}
	for _, requirement := range d.Requires {
		if requirement.Name == "" {
			return nil, fmt.Errorf("module %q: requirement with an empty service name", d.ID)
		}
		if requirement.Type == nil {
			return nil, fmt.Errorf("module %q: requirement %q has no contract type", d.ID, requirement.Name)
		}
	}

	decode := func(raw cordis.Value) (any, error) {
		if d.Decode != nil {
			value, err := d.Decode(raw)
			if err != nil {
				return nil, err
			}
			return value, nil
		}
		var out T
		if raw != nil {
			data, err := json.Marshal(raw)
			if err != nil {
				return nil, fmt.Errorf("encode config: %w", err)
			}
			if err := json.Unmarshal(data, &out); err != nil {
				return nil, fmt.Errorf("decode config into %T: %w", out, err)
			}
		}
		return out, nil
	}
	var validate func(config any) error
	if d.Validate != nil {
		validate = func(config any) error {
			typed, ok := config.(T)
			if !ok {
				return fmt.Errorf("config has type %T, want %T", config, typed)
			}
			return d.Validate(typed)
		}
	}
	run := func(ctx cordis.Context, config any) error {
		typed, ok := config.(T)
		if !ok {
			return cordis.InvalidPluginError(fmt.Sprintf("module %q: config has type %T, want %T", d.ID, config, typed))
		}
		return d.Factory(ctx, typed)
	}
	return &moduleDef{
		id:       d.ID,
		name:     name,
		version:  d.Version,
		provides: append([]Provider(nil), d.Provides...),
		requires: append([]Requirement(nil), d.Requires...),
		decode:   decode,
		validate: validate,
		run:      run,
	}, nil
}

// Info is a read-only description of a registered module.
type Info struct {
	ID       string
	Name     string
	Version  string
	Provides []Provider
	Requires []Requirement
}

// Registry is an explicit, value-based set of compile-time modules.
//
// It is deliberately not a package-level singleton: a host builds one registry
// per application (or test), registers the modules it was linked with, and hands
// it to Load. This keeps tests isolated, supports multiple kernels in one
// process, and makes registration failures ordinary errors instead of panics.
type Registry struct {
	mu       sync.Mutex
	modules  map[string]*moduleDef
	order    []string
	builtins map[string]Provider
}

// NewRegistry creates an empty registry with the kernel's built-in logger
// service declared as a provider (the root context always provides it).
func NewRegistry() *Registry {
	registry := &Registry{
		modules:  make(map[string]*moduleDef),
		builtins: make(map[string]Provider),
	}
	registry.builtins["logger"] = Provider{
		Name: "logger",
		Type: reflect.TypeFor[*cordis.LoggerService](),
	}
	return registry
}

// Register adds a module definition. It is a Go 1.27 generic method: T is
// inferred from the descriptor, and the descriptor is type-erased only inside
// the registry.
//
// It returns an error for an invalid or duplicate descriptor; registration is
// idempotent only in the sense that duplicates are rejected, never silently
// merged.
func (r *Registry) Register[T any](descriptor *Descriptor[T]) error {
	def, err := descriptor.erased()
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.modules[def.id]; exists {
		return fmt.Errorf("module: module %q is already registered", def.id)
	}
	r.modules[def.id] = def
	r.order = append(r.order, def.id)
	return nil
}

// AddBuiltin declares an already-provided service contract (beyond the default
// logger) so that pre-flight validation and requirement resolution can see it.
// It is a Go 1.27 generic method; T is inferred from the key.
func (r *Registry) AddBuiltin[T any](key cordis.ServiceKey[T]) *Registry {
	r.mu.Lock()
	r.builtins[key.Name()] = Provides(key)
	r.mu.Unlock()
	return r
}

// Builtins returns a copy of the declared builtin providers.
func (r *Registry) Builtins() []Provider {
	r.mu.Lock()
	defer r.mu.Unlock()
	return copyProviders(mapValues(r.builtins))
}

// Modules returns registered module descriptions sorted by ID.
func (r *Registry) Modules() []Info {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := append([]string(nil), r.order...)
	sort.Strings(ids)
	out := make([]Info, 0, len(ids))
	for _, id := range ids {
		def := r.modules[id]
		out = append(out, Info{
			ID:       def.id,
			Name:     def.name,
			Version:  def.version,
			Provides: append([]Provider(nil), def.provides...),
			Requires: append([]Requirement(nil), def.requires...),
		})
	}
	return out
}

// Module returns the description of one registered module.
func (r *Registry) Module(id string) (Info, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	def, ok := r.modules[id]
	if !ok {
		return Info{}, false
	}
	return Info{
		ID:       def.id,
		Name:     def.name,
		Version:  def.version,
		Provides: append([]Provider(nil), def.provides...),
		Requires: append([]Requirement(nil), def.requires...),
	}, true
}

// Has reports whether a module ID is registered.
func (r *Registry) Has(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.modules[id]
	return ok
}

func (r *Registry) module(id string) (*moduleDef, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	def, ok := r.modules[id]
	return def, ok
}

func (r *Registry) builtinProviders() []Provider {
	r.mu.Lock()
	defer r.mu.Unlock()
	return copyProviders(mapValues(r.builtins))
}

func mapValues(m map[string]Provider) []Provider {
	out := make([]Provider, 0, len(m))
	for _, value := range m {
		out = append(out, value)
	}
	return out
}

func copyProviders(providers []Provider) []Provider {
	sort.Slice(providers, func(i, j int) bool { return providers[i].Name < providers[j].Name })
	return append([]Provider(nil), providers...)
}
