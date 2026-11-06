package module

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	cordis "github.com/zeokeel/cordis-go"
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

// Instance is one loaded module instance. Entry IDs are stable across reloads;
// kernel fiber IDs are not, so hosts should key their own maps by Instance.ID.
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
// reload/close operations, warnings for optional modules, and locate.
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

// ErrUpdateUnsupported is returned by Loader.Update when the module did not
// declare an in-place update hook. The loader never silently restarts a module
// (that would lose state); the caller must choose Reload (state-preserving,
// requires Snapshot/Restore) or Recreate (explicit state loss).
var ErrUpdateUnsupported = errors.New("module: module does not support in-place config updates (no Descriptor.Update hook)")

// ErrReloadNotStateful is returned by Loader.Reload when some entry in the
// reload closure cannot preserve its state (missing Snapshot/Restore). Reload is
// therefore a no-op: the old instances are left untouched.
var ErrReloadNotStateful = errors.New("module: state-preserving reload requires Snapshot and Restore on every entry in the reload closure")

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

// Update applies a new config to a running module instance in place.
//
// It validates the candidate with the module's typed decode/Validate hooks and
// then calls the module's Descriptor.Update hook. The fiber, its services,
// listeners, effects and internal module state stay alive; dependents are not
// re-activated because the kernel service version does not change. This is the
// preferred config-change path, and it never loses module state.
//
// Modules that do not declare Update return ErrUpdateUnsupported: the loader
// refuses to silently restart (which would lose state). Use Reload for a
// state-preserving recreate (Snapshot/Restore) or Recreate to explicitly accept
// state loss.
func (l *Loader) Update(entryID string, config cordis.Value) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return fmt.Errorf("module: loader is closed")
	}
	instance, ok := l.instances[entryID]
	if !ok {
		return fmt.Errorf("module: no instance %q", entryID)
	}
	resolved, ok := l.plan.byID[entryID]
	if !ok {
		return fmt.Errorf("module: no plan entry %q", entryID)
	}
	if resolved.def.update == nil {
		return fmt.Errorf("module: update entry %q (%s): %w", entryID, resolved.def.id, ErrUpdateUnsupported)
	}
	if state := instance.State(); state != cordis.Active {
		return fmt.Errorf("module: update entry %q: instance is %s, want Active", entryID, state)
	}
	candidate, err := resolved.def.decode(config)
	if err != nil {
		return fmt.Errorf("module: update entry %q: invalid config: %w", entryID, err)
	}
	if resolved.def.validate != nil {
		if err := resolved.def.validate(candidate); err != nil {
			return fmt.Errorf("module: update entry %q: config rejected: %w", entryID, err)
		}
	}
	moduleCtx, ok := instance.fiber.Context()
	if !ok {
		return fmt.Errorf("module: update entry %q: no context", entryID)
	}
	if err := resolved.def.update(moduleCtx, resolved.decoded, candidate); err != nil {
		// The module owns the transaction: it should validate before mutating.
		return fmt.Errorf("module: update entry %q: module rejected config: %w", entryID, err)
	}
	resolved.entry.Config = config
	resolved.decoded = candidate
	instance.entry.Config = config
	return nil
}

// Reload recreates one entry (and its dependency closure) without losing module
// state.
//
// The closure is the entry subtree plus every loader-managed fiber that
// (transitively) injects a service provided by an entry in it; otherwise the
// kernel's provider-version change would re-activate those dependents and their
// state would be lost. Every closure instance must be Active and the module must
// implement Snapshot/Restore; if that is not the case, Reload returns
// ErrReloadNotStateful before touching anything.
//
// Order: snapshot in parent-first load order (dependencies still alive), dispose
// the old closure, recreate it, restore in parent-first load order, then verify
// startup and contracts. Entry IDs stay stable; kernel fiber IDs/UIDs change.
// Failure after dispose tears down the new instances and returns the error.
func (l *Loader) Reload(entryID string) (*Instance, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, fmt.Errorf("module: loader is closed")
	}
	closure := l.plan.reloadClosure(entryID)
	if len(closure) == 0 {
		return nil, fmt.Errorf("module: no plan entry %q", entryID)
	}
	instances := make([]*Instance, 0, len(closure))
	missingHooks := make([]string, 0)
	for _, resolved := range closure {
		instance, ok := l.instances[resolved.entry.ID]
		if !ok {
			return nil, fmt.Errorf("module: reload %q: entry %q is not loaded", entryID, resolved.entry.ID)
		}
		if state := instance.State(); state != cordis.Active {
			return nil, fmt.Errorf("module: reload %q: entry %q is %s; reload requires every closure entry Active",
				entryID, resolved.entry.ID, state)
		}
		if resolved.def.snapshot == nil || resolved.def.restore == nil {
			missingHooks = append(missingHooks, resolved.entry.ID)
		}
		instances = append(instances, instance)
	}
	if len(missingHooks) > 0 {
		return nil, fmt.Errorf("module: reload %q: %w: entries %v", entryID, ErrReloadNotStateful, missingHooks)
	}

	// Snapshot while the old tree (and all its dependencies) are still alive.
	snapshots := make(map[string]any, len(instances))
	for _, instance := range instances {
		moduleCtx, ok := instance.fiber.Context()
		if !ok {
			return nil, fmt.Errorf("module: reload %q: snapshot entry %q: no context", entryID, instance.entryID)
		}
		snapshot, err := instance.def.snapshot(moduleCtx)
		if err != nil {
			return nil, fmt.Errorf("module: reload %q: snapshot entry %q: %w", entryID, instance.entryID, err)
		}
		snapshots[instance.entryID] = snapshot
	}

	included := make(map[string]bool, len(instances))
	for _, instance := range instances {
		included[instance.entryID] = true
	}
	roots := make([]*Instance, 0)
	for _, instance := range instances {
		if instance.parent == nil || !included[instance.parent.entryID] {
			roots = append(roots, instance)
		}
	}
	for _, root := range roots {
		l.removeSubtree(root)
		containedDispose(root.fiber)
	}

	// Recreate in load order and restore parent-first.
	created := make(map[string]*Instance, len(closure))
	ordered := make([]*Instance, 0, len(closure))
	for _, resolved := range closure {
		var parentInstance *Instance
		if resolved.parent != nil {
			parentInstance = created[resolved.parent.entry.ID]
			if parentInstance == nil {
				parentInstance = l.instances[resolved.parent.entry.ID]
			}
		}
		parentCtx := l.root
		if parentInstance != nil {
			fiberCtx, ok := parentInstance.fiber.Context()
			if !ok {
				l.discardCreatedMap(created)
				return nil, fmt.Errorf("module: reload %q: parent entry %q has no context", entryID, parentInstance.entryID)
			}
			parentCtx = fiberCtx
		}
		newInstance, err := l.createInstance(resolved, parentCtx, parentInstance)
		if err != nil {
			l.discardCreatedMap(created)
			return nil, &LoadError{Issues: []string{err.Error()}}
		}
		created[resolved.entry.ID] = newInstance
		ordered = append(ordered, newInstance)
	}
	if err := l.root.WhenIdleContext(context.Background()); err != nil {
		l.discardCreatedMap(created)
		return nil, err
	}
	for _, instance := range ordered {
		moduleCtx, ok := instance.fiber.Context()
		if !ok {
			l.discardCreatedMap(created)
			return nil, fmt.Errorf("module: reload %q: restore entry %q: no context", entryID, instance.entryID)
		}
		if err := instance.def.restore(moduleCtx, snapshots[instance.entryID]); err != nil {
			l.discardCreatedMap(created)
			return nil, fmt.Errorf("module: reload %q: restore entry %q: %w", entryID, instance.entryID, err)
		}
	}
	if issues := l.collectIssues(ordered); len(issues) > 0 {
		l.discardCreatedMap(created)
		return nil, &LoadError{Issues: issues}
	}
	rootInstance := created[entryID]
	if rootInstance == nil {
		l.discardCreatedMap(created)
		return nil, fmt.Errorf("module: reload %q produced no instance", entryID)
	}
	return rootInstance, nil
}

// Recreate is the explicit state-loss restart: dispose the entry subtree and
// create it again from the same config. It never runs Snapshot/Restore; callers
// opt in to losing module state. Prefer Update or Reload.
func (l *Loader) Recreate(entryID string) (*Instance, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, fmt.Errorf("module: loader is closed")
	}
	instance, ok := l.instances[entryID]
	if !ok {
		return nil, fmt.Errorf("module: no instance %q", entryID)
	}
	ordered := l.plan.subtreeOrder(entryID)
	if len(ordered) == 0 {
		return nil, fmt.Errorf("module: no plan entry %q", entryID)
	}
	parentCtx := instance.parentCtx
	l.removeSubtree(instance)
	containedDispose(instance.fiber)

	skipped := make(map[string]bool)
	created := make([]*Instance, 0, len(ordered))
	for _, resolved := range ordered {
		parentInstance, skip := l.parentInstanceFor(resolved, skipped)
		if skip {
			continue
		}
		ctx := parentCtx
		if parentInstance != nil {
			fiberCtx, ok := parentInstance.fiber.Context()
			if !ok {
				created = append(created, nil)
				l.disposeCreated(created)
				return nil, fmt.Errorf("module: parent entry %q has no context on recreate", parentInstance.entryID)
			}
			ctx = fiberCtx
		}
		newInstance, err := l.createInstance(resolved, ctx, parentInstance)
		if err != nil {
			l.disposeCreated(created)
			return nil, &LoadError{Issues: []string{err.Error()}}
		}
		created = append(created, newInstance)
	}
	if len(created) == 0 {
		return nil, fmt.Errorf("module: recreate of %q created no instance", entryID)
	}
	if err := l.root.WhenIdleContext(context.Background()); err != nil {
		l.disposeCreated(created)
		return nil, err
	}
	if issues := l.collectIssues(created); len(issues) > 0 {
		l.disposeCreated(created)
		return nil, &LoadError{Issues: issues}
	}
	return created[0], nil
}

func (l *Loader) disposeCreated(created []*Instance) {
	for i := len(created) - 1; i >= 0; i-- {
		instance := created[i]
		if instance == nil {
			continue
		}
		l.removeSubtree(instance)
		containedDispose(instance.fiber)
	}
}

// discardCreatedMap tears down a partially created reload closure.
func (l *Loader) discardCreatedMap(created map[string]*Instance) {
	if len(created) == 0 {
		return
	}
	ids := make([]string, 0, len(created))
	for id := range created {
		ids = append(ids, id)
	}
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	for _, id := range ids {
		instance := l.instances[id]
		if instance == nil || instance.parent == nil || set[instance.parent.entryID] {
			continue
		}
		kept := instance.parent.children[:0]
		for _, child := range instance.parent.children {
			if child != instance {
				kept = append(kept, child)
			}
		}
		instance.parent.children = kept
	}
	for _, id := range ids {
		instance := l.instances[id]
		if instance == nil {
			continue
		}
		if instance.parent != nil && set[instance.parent.entryID] {
			continue
		}
		containedDispose(instance.fiber)
	}
	for _, id := range ids {
		delete(l.instances, id)
	}
	kept := l.order[:0]
	for _, instance := range l.order {
		if !set[instance.entryID] {
			kept = append(kept, instance)
		}
	}
	l.order = kept
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

func (l *Loader) subtree(root *Instance) []*Instance {
	var out []*Instance
	var walk func(*Instance)
	walk = func(instance *Instance) {
		out = append(out, instance)
		for _, child := range instance.children {
			walk(child)
		}
	}
	walk(root)
	return out
}

func (l *Loader) removeSubtree(instance *Instance) {
	if instance == nil {
		return
	}
	if instance.parent != nil {
		kept := instance.parent.children[:0]
		for _, child := range instance.parent.children {
			if child != instance {
				kept = append(kept, child)
			}
		}
		instance.parent.children = kept
	}
	ids := make(map[string]bool)
	var walk func(*Instance)
	walk = func(current *Instance) {
		ids[current.entryID] = true
		delete(l.instances, current.entryID)
		for _, child := range current.children {
			walk(child)
		}
	}
	walk(instance)
	kept := l.order[:0]
	for _, current := range l.order {
		if !ids[current.entryID] {
			kept = append(kept, current)
		}
	}
	l.order = kept
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
