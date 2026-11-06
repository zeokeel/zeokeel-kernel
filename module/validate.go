package module

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	cordis "github.com/zeokeel/cordis-go"
)

// Entry is one configured module instance in an assembly tree.
//
// Config is the JSON-like input decoded into the module's typed config during
// pre-flight validation (errors are reported before any fiber starts). Children
// are loaded as child fibers of this entry's fiber, so disposing the parent
// disposes the subtree through the kernel's child-ownership effect.
type Entry struct {
	// ID is the stable instance identity (required, unique across the tree).
	// Reload/Locate/Instance use it; kernel fiber IDs are not stable across
	// restarts and should not be used by the host.
	ID string
	// Module is the registered module ID to instantiate.
	Module string
	// Config is the entry config, decoded into the module's typed config.
	Config cordis.Value
	// Disabled skips this entry and its whole subtree.
	Disabled bool
	// Optional tolerates a runtime activation failure or a permanent pending
	// state: the entry is kept (Failed/Pending) and reported as a warning, and
	// it does not roll the assembly back. Requirements still have to satisfy
	// pre-flight validation unless they are declared optional by the module.
	Optional bool
	// Children are nested module instances.
	Children []Entry
}

// ValidationError aggregates every pre-flight problem found in an entry tree.
type ValidationError struct {
	Issues []string
}

// Error implements error.
func (e *ValidationError) Error() string {
	return "module: validation failed:\n  - " + strings.Join(e.Issues, "\n  - ")
}

// Plan is a validated, resolved assembly tree ready to be loaded.
type Plan struct {
	roots      []*resolvedEntry
	byID       map[string]*resolvedEntry
	schedule   []*resolvedEntry
	warnings   []string
	moduleInfo map[string]*moduleDef
	// consumers maps a required service name to the entry IDs that inject it;
	// providerOf maps a provided service name to its entry ID (empty = builtin).
	consumers  map[string][]string
	providerOf map[string]string
}

// Warnings returns non-fatal pre-flight findings (for example an optional
// service that no enabled entry provides, or disabled entries).
func (p *Plan) Warnings() []string { return append([]string{}, p.warnings...) }

// EntryIDs returns the instance IDs in load order (a deterministic order that
// puts providers and parents before consumers and children).
func (p *Plan) EntryIDs() []string {
	out := make([]string, 0, len(p.schedule))
	for _, entry := range p.schedule {
		out = append(out, entry.entry.ID)
	}
	return out
}

// ReloadClosure returns the schedule-ordered set of entries that must be
// recreated to reload entryID without losing dependent state: the entry subtree
// plus every loader-managed fiber that (transitively) injects one of its provided
// services. Dependents outside this closure would otherwise be re-activated by
// the kernel's provider-version change and silently lose their own state.
//
// The closure only covers loader-managed entries; fibers created dynamically by
// module code are outside the assembly contract.
func (p *Plan) reloadClosure(entryID string) []*resolvedEntry {
	root, ok := p.byID[entryID]
	if !ok {
		return nil
	}
	included := make(map[string]bool)
	var addSubtree func(entry *resolvedEntry)
	addSubtree = func(entry *resolvedEntry) {
		if included[entry.entry.ID] {
			return
		}
		included[entry.entry.ID] = true
		for _, child := range entry.children {
			addSubtree(child)
		}
	}
	addSubtree(root)
	queue := make([]string, 0, len(included))
	for id := range included {
		queue = append(queue, id)
	}
	for len(queue) > 0 {
		id := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		entry := p.byID[id]
		if entry == nil {
			continue
		}
		for _, provider := range entry.def.provides {
			for _, consumerID := range p.consumers[provider.Name] {
				if included[consumerID] {
					continue
				}
				consumer, ok := p.byID[consumerID]
				if !ok {
					continue
				}
				addSubtree(consumer)
				queue = append(queue, consumerID)
			}
		}
	}
	out := make([]*resolvedEntry, 0, len(included))
	for _, entry := range p.schedule {
		if included[entry.entry.ID] {
			out = append(out, entry)
		}
	}
	return out
}

// subtreeOrder returns the schedule-ordered entries of one entry subtree.
func (p *Plan) subtreeOrder(entryID string) []*resolvedEntry {
	root, ok := p.byID[entryID]
	if !ok {
		return nil
	}
	var out []*resolvedEntry
	for _, entry := range p.schedule {
		for cursor := entry; cursor != nil; cursor = cursor.parent {
			if cursor == root {
				out = append(out, entry)
				break
			}
		}
	}
	return out
}

func (p *Plan) flatten() []*resolvedEntry {
	var out []*resolvedEntry
	var walk func(entry *resolvedEntry)
	walk = func(entry *resolvedEntry) {
		out = append(out, entry)
		for _, child := range entry.children {
			walk(child)
		}
	}
	for _, root := range p.roots {
		walk(root)
	}
	return out
}

type resolvedEntry struct {
	entry    Entry
	def      *moduleDef
	decoded  any
	parent   *resolvedEntry
	children []*resolvedEntry
}

type providerRef struct {
	typ  reflect.Type
	from string // entry ID; empty for builtins
}

// Validate resolves the entry tree against the registry and checks composition
// invariants before any fiber is created:
//
//   - every entry references a registered module and has a unique instance ID;
//   - every provided service has exactly one provider in the assembly (the
//     kernel would otherwise fail with DuplicateService at runtime);
//   - every required service has a provider whose contract type is assignable
//     to the requirement;
//   - no required-dependency cycle exists (the kernel would leave the cycle
//     permanently Pending);
//   - every config decodes and passes the module's typed validation hook.
//
// All issues are collected, not fail-fast, so a host can display a complete
// diagnostic for a broken assembly.
func (r *Registry) Validate(entries []Entry) (*Plan, error) {
	plan := &Plan{
		byID:       make(map[string]*resolvedEntry),
		moduleInfo: make(map[string]*moduleDef),
		consumers:  make(map[string][]string),
		providerOf: make(map[string]string),
	}
	issues := make([]string, 0)

	var flatten func(entries []Entry, parent *resolvedEntry, path string)
	flatten = func(entries []Entry, parent *resolvedEntry, path string) {
		for index, entry := range entries {
			entryPath := fmt.Sprintf("%s[%d]", path, index)
			if entry.ID == "" {
				issues = append(issues, fmt.Sprintf("%s: entry ID is required", entryPath))
				continue
			}
			entryPath = entryPath + "(" + entry.ID + ")"
			if _, exists := plan.byID[entry.ID]; exists {
				issues = append(issues, fmt.Sprintf("%s: duplicate instance ID %q", entryPath, entry.ID))
				continue
			}
			if entry.Module == "" {
				issues = append(issues, fmt.Sprintf("%s: module ID is required", entryPath))
				continue
			}
			def, ok := r.module(entry.Module)
			if !ok {
				issues = append(issues, fmt.Sprintf("%s: unknown module %q (is its package registered in this host?)", entryPath, entry.Module))
				continue
			}
			if entry.Disabled {
				plan.warnings = append(plan.warnings, fmt.Sprintf("%s: entry is disabled (skipped)", entryPath))
				continue
			}
			resolved := &resolvedEntry{entry: entry, def: def, parent: parent}
			decoded, err := def.decode(entry.Config)
			if err != nil {
				issues = append(issues, fmt.Sprintf("%s: invalid config for module %q: %v", entryPath, def.id, err))
			} else if def.validate != nil {
				if err := def.validate(decoded); err != nil {
					issues = append(issues, fmt.Sprintf("%s: config for module %q rejected: %v", entryPath, def.id, err))
				}
			}
			resolved.decoded = decoded
			plan.byID[entry.ID] = resolved
			plan.moduleInfo[entry.ID] = def
			if parent == nil {
				plan.roots = append(plan.roots, resolved)
			} else {
				parent.children = append(parent.children, resolved)
			}
			flatten(entry.Children, resolved, entryPath)
		}
	}
	flatten(entries, nil, "root")

	// Provider index: builtins (kernel root services) then module declarations.
	providers := make(map[string]providerRef)
	for _, builtin := range r.builtinProviders() {
		providers[builtin.Name] = providerRef{typ: builtin.Type, from: ""}
	}
	for _, entry := range plan.flatten() {
		for _, provider := range entry.def.provides {
			if existing, exists := providers[provider.Name]; exists {
				origin := "the kernel root context"
				if existing.from != "" {
					origin = fmt.Sprintf("entry %q", existing.from)
				}
				issues = append(issues, fmt.Sprintf(
					"service %q is provided both by %s and entry %q (same scope; use isolate or remove one)",
					provider.Name, origin, entry.entry.ID))
				continue
			}
			providers[provider.Name] = providerRef{typ: provider.Type, from: entry.entry.ID}
			plan.providerOf[provider.Name] = entry.entry.ID
		}
	}

	// Requirement checks and the required-dependency graph.
	edges := make(map[string][]string)
	for _, entry := range plan.flatten() {
		for _, requirement := range entry.def.requires {
			provider, exists := providers[requirement.Name]
			if !exists {
				if requirement.Optional {
					plan.warnings = append(plan.warnings, fmt.Sprintf(
						"entry %q: optional service %q is not provided by this assembly", entry.entry.ID, requirement.Name))
				} else {
					issues = append(issues, fmt.Sprintf(
						"entry %q (%s) requires service %q, but no enabled entry in the assembly provides it",
						entry.entry.ID, entry.def.id, requirement.Name))
				}
				continue
			}
			if !assignable(provider.typ, requirement.Type) {
				issues = append(issues, fmt.Sprintf(
					"service %q has incompatible contracts: entry %q requires %s, provider %q provides %s",
					requirement.Name, entry.entry.ID, typeName(requirement.Type),
					providerOrigin(provider), typeName(provider.typ)))
			}
			if provider.from == "" {
				continue // builtin: no module-level edge
			}
			if provider.from == entry.entry.ID {
				issues = append(issues, fmt.Sprintf(
					"entry %q requires service %q that it provides itself (it would stay Pending forever)",
					entry.entry.ID, requirement.Name))
				continue
			}
			if !requirement.Optional {
				edges[entry.entry.ID] = append(edges[entry.entry.ID], provider.from)
				plan.consumers[requirement.Name] = appendUnique(plan.consumers[requirement.Name], entry.entry.ID)
			}
		}
	}

	// Cycle detection over the required-dependency graph.
	issues = append(issues, detectCycles(edges)...)

	if len(issues) > 0 {
		sort.Strings(issues)
		return nil, &ValidationError{Issues: issues}
	}
	plan.schedule, plan.warnings = buildSchedule(plan, providers, plan.warnings)
	return plan, nil
}

// buildSchedule produces a deterministic load order that places every provider
// and parent before its consumers and children. Required dependencies already
// gate activation through the kernel, but scheduling providers first makes
// *optional* dependencies deterministic too: a module that probes an optional
// service at activation sees it when the provider is part of the assembly.
//
// Optional dependency cycles (impossible for required ones, which pre-flight
// rejects) fall back to configuration order with a warning.
func buildSchedule(plan *Plan, providers map[string]providerRef, warnings []string) ([]*resolvedEntry, []string) {
	index := make(map[string]int)
	for i, entry := range plan.flatten() {
		index[entry.entry.ID] = i
	}
	indegree := make(map[string]int, len(index))
	dependents := make(map[string][]*resolvedEntry, len(index))
	for id := range index {
		indegree[id] = 0
	}
	addEdge := func(dependency, dependent *resolvedEntry) {
		dependents[dependency.entry.ID] = append(dependents[dependency.entry.ID], dependent)
		indegree[dependent.entry.ID]++
	}
	for _, entry := range plan.flatten() {
		if entry.parent != nil {
			addEdge(entry.parent, entry)
		}
		seen := make(map[string]bool)
		for _, requirement := range entry.def.requires {
			provider, ok := providers[requirement.Name]
			if !ok || provider.from == "" || provider.from == entry.entry.ID || seen[provider.from] {
				continue
			}
			seen[provider.from] = true
			dependency, ok := plan.byID[provider.from]
			if !ok {
				continue
			}
			addEdge(dependency, entry)
		}
	}
	ready := make([]*resolvedEntry, 0, len(index))
	for _, entry := range plan.flatten() {
		if indegree[entry.entry.ID] == 0 {
			ready = append(ready, entry)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		return index[ready[i].entry.ID] < index[ready[j].entry.ID]
	})
	schedule := make([]*resolvedEntry, 0, len(index))
	for len(ready) > 0 {
		current := ready[0]
		ready = ready[1:]
		schedule = append(schedule, current)
		next := dependents[current.entry.ID]
		sort.Slice(next, func(i, j int) bool {
			return index[next[i].entry.ID] < index[next[j].entry.ID]
		})
		for _, dependent := range next {
			indegree[dependent.entry.ID]--
			if indegree[dependent.entry.ID] == 0 {
				ready = append(ready, dependent)
				sort.Slice(ready, func(i, j int) bool {
					return index[ready[i].entry.ID] < index[ready[j].entry.ID]
				})
			}
		}
	}
	if len(schedule) < len(index) {
		// A cycle through optional requirements: fall back to config order for
		// the remaining entries; pre-flight only rejects required cycles.
		placed := make(map[string]bool, len(schedule))
		for _, entry := range schedule {
			placed[entry.entry.ID] = true
		}
		for _, entry := range plan.flatten() {
			if !placed[entry.entry.ID] {
				schedule = append(schedule, entry)
			}
		}
		warnings = append(warnings, "load order contains a cycle through optional requirements; configuration order was used for the remaining entries")
	}
	return schedule, warnings
}

func providerOrigin(provider providerRef) string {
	if provider.from == "" {
		return "the kernel root context"
	}
	return fmt.Sprintf("entry %q", provider.from)
}

// assignable reports whether a provided contract type can satisfy a required
// contract type (identical type, or implementation of the required interface).
func assignable(provided, required reflect.Type) bool {
	return provided != nil && required != nil && provided.AssignableTo(required)
}

func typeName(value reflect.Type) string {
	if value == nil {
		return "<nil>"
	}
	return value.String()
}

// detectCycles reports each required-dependency cycle once, as "a -> b -> a".
func detectCycles(edges map[string][]string) []string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int)
	stack := make([]string, 0)
	issues := make([]string, 0)
	seen := make(map[string]bool)

	var visit func(node string) bool
	visit = func(node string) bool {
		color[node] = gray
		stack = append(stack, node)
		for _, next := range edges[node] {
			switch color[next] {
			case gray:
				// Found a back edge: extract the cycle from the stack.
				start := 0
				for i, id := range stack {
					if id == next {
						start = i
						break
					}
				}
				cycle := append(append([]string(nil), stack[start:]...), next)
				key := strings.Join(sortedCopy(cycle), "->")
				if !seen[key] {
					seen[key] = true
					issues = append(issues, "required-dependency cycle: "+strings.Join(cycle, " -> "))
				}
				return true
			case white:
				if visit(next) {
					// Keep scanning siblings for more cycles.
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[node] = black
		return false
	}
	ids := make([]string, 0, len(edges))
	for id := range edges {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if color[id] == white {
			visit(id)
		}
	}
	return issues
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func sortedCopy(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
