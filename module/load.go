package module

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	cordis "github.com/zeokeel/zeokeel-kernel"
)

// LoadError aggregates runtime startup problems (a required entry that failed
// to activate, stayed pending with missing services, or declared services it
// did not actually provide) after the whole tree had a chance to settle.
type LoadError struct {
	Issues []string
}

// Error implements error.
func (e *LoadError) Error() string {
	return "module: assembly failed to start:\n  - " + strings.Join(e.Issues, "\n  - ")
}

// Instance is one loaded module instance. Entry IDs are stable for the process
// lifetime; kernel fiber IDs are not, so hosts should key their own maps by
// Instance.ID.
type Instance struct {
	entryID   string
	moduleID  string
	fiber     *cordis.Fiber
	parent    *Instance
	children  []*Instance
	parentCtx cordis.Context
	def       *moduleDef
	entry     Entry
}

// ID returns the stable instance ID from the entry tree.
func (i *Instance) ID() string { return i.entryID }

// ModuleID returns the registered module ID.
func (i *Instance) ModuleID() string { return i.moduleID }

// State returns the fiber lifecycle state.
func (i *Instance) State() cordis.FiberState {
	if i.fiber == nil {
		return cordis.Disposed
	}
	return i.fiber.State()
}

// Fiber returns the kernel fiber handle (nil only if the instance is detached).
func (i *Instance) Fiber() *cordis.Fiber { return i.fiber }

// Config returns the raw entry config.
func (i *Instance) Config() cordis.Value { return i.entry.Config }

// Entry returns a copy of the configured entry.
func (i *Instance) Entry() Entry {
	entry := i.entry
	entry.Children = append([]Entry(nil), i.entry.Children...)
	return entry
}

// Parent returns the parent instance, or nil for a top-level entry.
func (i *Instance) Parent() *Instance { return i.parent }

// Children returns a snapshot of the child instances.
func (i *Instance) Children() []*Instance {
	return append([]*Instance(nil), i.children...)
}

// Loader owns one loaded assembly tree: stable entry IDs mapped to fibers,
// close/locate operations, warnings for optional modules, and health reporting.
type Loader struct {
	root      cordis.Context
	registry  *Registry
	plan      *Plan
	instances map[string]*Instance
	order     []*Instance
	warnings  []string
	closed    bool
	mu        sync.RWMutex
}

// Load validates the entry tree, instantiates every module as a fiber (parents
// before children), waits until the tree is quiescent, and verifies the startup
// outcome.
//
// Pre-flight validation errors are returned before any fiber is created. A
// required entry that fails to activate (or stays permanently Pending) rolls the
// whole assembly back and returns a LoadError. A failed Optional entry is kept
// and reported through Warnings; it does not fail the assembly.
func Load(ctx cordis.Context, registry *Registry, entries []Entry) (*Loader, error) {
	if registry == nil {
		return nil, fmt.Errorf("module: nil registry")
	}
	plan, err := registry.Validate(entries)
	if err != nil {
		return nil, err
	}
	loader := &Loader{
		root:      ctx,
		registry:  registry,
		plan:      plan,
		instances: make(map[string]*Instance),
		warnings:  plan.Warnings(),
	}
	if err := loader.checkBuiltins(); err != nil {
		return nil, err
	}
	for _, resolved := range plan.schedule {
		parent, skip := loader.parentInstanceFor(resolved, nil)
		if skip {
			continue
		}
		parentCtx := ctx
		if parent != nil {
			fiberCtx, ok := parent.fiber.Context()
			if !ok {
				loader.disposeAll()
				return nil, &LoadError{Issues: []string{fmt.Sprintf(
					"module: parent entry %q has no context while loading %q", parent.entryID, resolved.entry.ID)}}
			}
			parentCtx = fiberCtx
		}
		if _, err := loader.createInstance(resolved, parentCtx, parent); err != nil {
			loader.disposeAll()
			return nil, &LoadError{Issues: []string{err.Error()}}
		}
	}
	if err := loader.settle(); err != nil {
		loader.disposeAll()
		return nil, err
	}
	return loader, nil
}

// checkBuiltins verifies that every declared builtin contract is actually
// available on the host context (the kernel root provides the logger; a host may
// declare more through Registry.AddBuiltin or by providing them before Load).
func (l *Loader) checkBuiltins() error {
	for _, builtin := range l.registry.builtinProviders() {
		value, ok := l.root.GetNamed(builtin.Name)
		if !ok {
			return fmt.Errorf("module: builtin service %q is declared but not available on the host context", builtin.Name)
		}
		actual := reflect.TypeOf(value)
		if actual == nil || !actual.AssignableTo(builtin.Type) {
			return fmt.Errorf("module: builtin service %q has type %v, want %v", builtin.Name, actual, builtin.Type)
		}
	}
	return nil
}

// entryPlugin is the kernel Plugin adapter for one module instance. Its Inject
// list is exactly the module's non-optional requirements, so the kernel keeps
// the fiber Pending until they are strictly available; optional requirements are
// resolved by the module body through ctx.Get.
type entryPlugin struct {
	def     *moduleDef
	decoded any
	inject  []string
}

func (p *entryPlugin) Name() string { return p.def.name }

// Inject implements cordis.PluginInjector.
func (p *entryPlugin) Inject() []string { return p.inject }

// Activate implements cordis.Plugin.
func (p *entryPlugin) Activate(ctx cordis.Context, _ cordis.Value) error {
	return p.def.run(ctx, p.decoded)
}

func requiredInject(def *moduleDef) []string {
	seen := make(map[string]bool, len(def.requires))
	out := make([]string, 0, len(def.requires))
	for _, requirement := range def.requires {
		if requirement.Optional || seen[requirement.Name] {
			continue
		}
		seen[requirement.Name] = true
		out = append(out, requirement.Name)
	}
	return out
}

// createInstance creates the fiber for exactly one resolved entry. Children are
// created later by the load schedule, which places providers and parents before
// consumers and children; a parent may still be Pending here (the kernel allows
// loading children from a pending fiber and activates the tree reactively).
func (l *Loader) createInstance(resolved *resolvedEntry, parentCtx cordis.Context, parent *Instance) (*Instance, error) {
	plugin := &entryPlugin{
		def:     resolved.def,
		decoded: resolved.decoded,
		inject:  requiredInject(resolved.def),
	}
	fiber, err := parentCtx.Plugin(plugin, nil)
	if err != nil {
		return nil, fmt.Errorf("module: start entry %q (%s): %w", resolved.entry.ID, resolved.def.id, err)
	}
	instance := &Instance{
		entryID:   resolved.entry.ID,
		moduleID:  resolved.def.id,
		fiber:     fiber,
		parent:    parent,
		parentCtx: parentCtx,
		def:       resolved.def,
		entry:     resolved.entry,
	}
	l.instances[resolved.entry.ID] = instance
	l.order = append(l.order, instance)
	if parent != nil {
		parent.children = append(parent.children, instance)
	}
	if state := fiber.State(); state == cordis.Failed {
		if resolved.entry.Optional {
			l.warnings = append(l.warnings, fmt.Sprintf(
				"entry %q (%s) is optional and failed to activate: %v", resolved.entry.ID, resolved.def.id, fiber.ErrorDetail()))
			return instance, nil
		}
		return instance, fmt.Errorf("module: entry %q (%s) failed to activate: %v",
			resolved.entry.ID, resolved.def.id, fiber.ErrorDetail())
	}
	return instance, nil
}

// parentInstanceFor finds the already-created parent of an entry and reports
// whether the entry must be skipped because its parent failed, was disposed, or
// was itself skipped.
func (l *Loader) parentInstanceFor(resolved *resolvedEntry, skipped map[string]bool) (*Instance, bool) {
	if resolved.parent == nil {
		return nil, false
	}
	parentID := resolved.parent.entry.ID
	if skipped != nil && skipped[parentID] {
		skipped[resolved.entry.ID] = true
		l.warnings = append(l.warnings, fmt.Sprintf(
			"entry %q was skipped because its parent entry is unavailable", resolved.entry.ID))
		return nil, true
	}
	parent, ok := l.instances[parentID]
	if !ok {
		if skipped != nil {
			skipped[resolved.entry.ID] = true
		}
		l.warnings = append(l.warnings, fmt.Sprintf(
			"entry %q was skipped because its parent entry %q is not loaded", resolved.entry.ID, parentID))
		return nil, true
	}
	if state := parent.State(); state == cordis.Failed || state == cordis.Disposed || state == cordis.Unloading {
		if skipped != nil {
			skipped[resolved.entry.ID] = true
		}
		l.warnings = append(l.warnings, fmt.Sprintf(
			"entry %q was skipped because its parent entry %q is %s", resolved.entry.ID, parentID, state))
		return nil, true
	}
	return parent, false
}

// settle waits for all in-flight activation work, then verifies that every
// required entry is Active and every module actually provides the contracts it
// declared.
func (l *Loader) settle() error {
	if err := l.root.WhenIdleContext(context.Background()); err != nil {
		return err
	}
	issues := l.collectIssues(l.order)
	if len(issues) > 0 {
		return &LoadError{Issues: issues}
	}
	return nil
}

func (l *Loader) collectIssues(instances []*Instance) []string {
	issues := make([]string, 0)
	for _, instance := range instances {
		if instance.fiber == nil {
			continue
		}
		_ = instance.fiber.AwaitSettled()
		switch state := instance.fiber.State(); state {
		case cordis.Active:
			if problem := l.verifyProvides(instance); problem != "" {
				issues = append(issues, problem)
			}
		case cordis.Failed:
			if instance.entry.Optional {
				l.warnings = append(l.warnings, fmt.Sprintf(
					"entry %q (%s) is optional and failed: %v", instance.entryID, instance.moduleID, instance.fiber.ErrorDetail()))
				continue
			}
			issues = append(issues, fmt.Sprintf("entry %q (%s) failed: %v",
				instance.entryID, instance.moduleID, instance.fiber.ErrorDetail()))
		case cordis.Pending:
			missing := l.missingServices(instance)
			if instance.entry.Optional {
				l.warnings = append(l.warnings, fmt.Sprintf(
					"entry %q (%s) is optional and pending (missing: %s)", instance.entryID, instance.moduleID, strings.Join(missing, ", ")))
				continue
			}
			issues = append(issues, fmt.Sprintf("entry %q (%s) is pending; missing required services: [%s]",
				instance.entryID, instance.moduleID, strings.Join(missing, ", ")))
		case cordis.Unloading, cordis.Disposed:
			if instance.entry.Optional {
				l.warnings = append(l.warnings, fmt.Sprintf(
					"entry %q (%s) is optional and already %s", instance.entryID, instance.moduleID, state))
				continue
			}
			issues = append(issues, fmt.Sprintf("entry %q (%s) ended in state %s during startup",
				instance.entryID, instance.moduleID, state))
		}
	}
	return issues
}

func (l *Loader) missingServices(instance *Instance) []string {
	missing := make([]string, 0)
	ctx, ok := instance.fiber.Context()
	if !ok {
		return []string{"<no context>"}
	}
	for _, requirement := range instance.def.requires {
		if requirement.Optional {
			continue
		}
		if !ctx.ServiceAvailable(requirement.Name) {
			missing = append(missing, requirement.Name)
		}
	}
	if len(missing) == 0 {
		missing = append(missing, "<unknown; no declared requirement is missing>")
	}
	return missing
}

// verifyProvides checks the runtime contract against the static manifest: an
// Active module must actually expose every service it declared, with a type
// assignable to the declared contract. This catches modules whose manifest and
// body disagree.
func (l *Loader) verifyProvides(instance *Instance) string {
	if len(instance.def.provides) == 0 {
		return ""
	}
	ctx, ok := instance.fiber.Context()
	if !ok {
		return fmt.Sprintf("entry %q (%s) is active but has no context", instance.entryID, instance.moduleID)
	}
	for _, provider := range instance.def.provides {
		value, found := ctx.GetNamed(provider.Name)
		if !found {
			return fmt.Sprintf("entry %q (%s) declares service %q but does not provide it",
				instance.entryID, instance.moduleID, provider.Name)
		}
		actual := reflect.TypeOf(value)
		if actual == nil || !actual.AssignableTo(provider.Type) {
			return fmt.Sprintf("entry %q (%s) provides service %q with type %v, declared %v",
				instance.entryID, instance.moduleID, provider.Name, actual, provider.Type)
		}
	}
	return ""
}

// Instance returns one loaded instance by stable entry ID.
func (l *Loader) Instance(entryID string) (*Instance, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	instance, ok := l.instances[entryID]
	return instance, ok
}

// Instances returns a snapshot of the loaded instances in creation order.
func (l *Loader) Instances() []*Instance {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]*Instance(nil), l.order...)
}

// Locate returns the instance that owns a fiber (useful for diagnostics that
// receive a fiber handle from the kernel).
func (l *Loader) Locate(fiber *cordis.Fiber) (*Instance, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if fiber == nil {
		return nil, false
	}
	for _, instance := range l.order {
		if instance.fiber == fiber {
			return instance, true
		}
	}
	return nil, false
}

// Warnings returns optional-module findings from validation and startup.
func (l *Loader) Warnings() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]string{}, l.warnings...)
}

// Closed reports whether the loader was closed.
func (l *Loader) Closed() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.closed
}

// Close disposes every loaded fiber (children are cascaded by the kernel) and
// releases the instances. It is idempotent.
func (l *Loader) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	l.disposeAll()
	return nil
}

// MissingServices reports the non-optional services an instance is still
// waiting for (empty when the instance is not Pending, or all requirements are
// available).
func (l *Loader) MissingServices(entryID string) []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	instance, ok := l.instances[entryID]
	if !ok {
		return nil
	}
	if instance.State() != cordis.Pending {
		return nil
	}
	return l.missingServices(instance)
}

func (l *Loader) disposeAll() {
	for i := len(l.order) - 1; i >= 0; i-- {
		containedDispose(l.order[i].fiber)
	}
	l.order = nil
	l.instances = make(map[string]*Instance)
}

func containedDispose(fiber *cordis.Fiber) {
	if fiber == nil {
		return
	}
	defer func() { _ = recover() }()
	_ = fiber.Dispose()
}
