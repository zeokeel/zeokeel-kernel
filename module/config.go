package module

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	cordis "github.com/zeokeel/cordis-go"
)

// FileConfig is the on-disk assembly configuration.
//
// Version is reserved for future migrations (0 and 1 are accepted); entries is
// the module tree passed to Load. A bare JSON array of entries is also accepted
// by ParseConfig.
type FileConfig struct {
	Version int     `json:"version,omitempty"`
	Entries []Entry `json:"entries"`
}

// ParseConfig parses one assembly config document (strict JSON: unknown fields
// are rejected, so typos in config keys fail loudly).
//
// Both `{"version":1,"entries":[...]}` and a bare `[...]` array are accepted.
func ParseConfig(data []byte) ([]Entry, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("module: empty config document")
	}
	prepared := bytes.NewReader(trimmed)
	decoder := json.NewDecoder(prepared)
	decoder.DisallowUnknownFields()
	if trimmed[0] == '[' {
		var entries []Entry
		if err := decoder.Decode(&entries); err != nil {
			return nil, fmt.Errorf("module: parse config array: %w", err)
		}
		return entries, nil
	}
	var config FileConfig
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("module: parse config document: %w", err)
	}
	if config.Version != 0 && config.Version != 1 {
		return nil, fmt.Errorf("module: unsupported config version %d", config.Version)
	}
	return config.Entries, nil
}

// LoadConfigFile reads and parses one assembly config file.
func LoadConfigFile(path string) ([]Entry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("module: read config %q: %w", path, err)
	}
	return ParseConfig(data)
}

// LoadFile reads the assembly config file, validates it and starts the tree. It
// is the file-backed counterpart of Load.
func LoadFile(ctx cordis.Context, registry *Registry, path string) (*Loader, error) {
	entries, err := LoadConfigFile(path)
	if err != nil {
		return nil, err
	}
	return Load(ctx, registry, entries)
}

// ChangeKind classifies one entry change applied by ApplyEntries/reload.
type ChangeKind int

const (
	// ChangeMetadata means only non-activation metadata (such as Optional)
	// changed; the instance was not touched.
	ChangeMetadata ChangeKind = iota + 1
	// ChangeUpdated means the config was applied in place (Update hook): the
	// fiber and module state were preserved.
	ChangeUpdated
	// ChangeReloaded means the instance was recreated with Snapshot/Restore and
	// its state was preserved (entry ID and state kept, fiber ID changed).
	ChangeReloaded
	// ChangeRecreated means the instance was recreated without state
	// preservation (explicitly requested fallback).
	ChangeRecreated
	// ChangeAdded means a new entry was started.
	ChangeAdded
	// ChangeRemoved means an entry subtree was stopped.
	ChangeRemoved
)

// String returns a stable short name for reports/logs.
func (k ChangeKind) String() string {
	switch k {
	case ChangeMetadata:
		return "metadata"
	case ChangeUpdated:
		return "updated"
	case ChangeReloaded:
		return "reloaded"
	case ChangeRecreated:
		return "recreated"
	case ChangeAdded:
		return "added"
	case ChangeRemoved:
		return "removed"
	default:
		return "unchanged"
	}
}

// Change is one entry change in a ReloadReport.
type Change struct {
	EntryID string
	Kind    ChangeKind
	Note    string
}

// ReloadReport describes the changes applied by ApplyEntries.
type ReloadReport struct {
	Changes  []Change
	Warnings []string
}

// HasChanges reports whether any entry was actually changed.
func (r *ReloadReport) HasChanges() bool {
	return r != nil && len(r.Changes) > 0
}

// UpdateFallback selects what ApplyEntries does when an entry's config changed
// but its module has no in-place Update hook.
type UpdateFallback int

const (
	// FallbackReject reports the change as an error and applies nothing (safe
	// default: no silent state loss, no silent restart).
	FallbackReject UpdateFallback = iota
	// FallbackReload recreates the entry via Snapshot/Restore (state preserved,
	// requires the module to implement both hooks and the whole reload closure to
	// support them; otherwise the change is rejected before any mutation).
	FallbackReload
	// FallbackRecreate recreates the entry without state preservation (explicit
	// state loss).
	FallbackRecreate
)

// ApplyOption configures one ApplyEntries call.
type ApplyOption func(*applyOptions)

type applyOptions struct {
	fallback UpdateFallback
}

// WithUpdateFallback selects the policy for config changes on modules without an
// in-place Update hook.
func WithUpdateFallback(fallback UpdateFallback) ApplyOption {
	return func(options *applyOptions) { options.fallback = fallback }
}

// ApplyError aggregates problems that prevented (part of) a config update.
type ApplyError struct {
	Issues []string
}

// Error implements error.
func (e *ApplyError) Error() string {
	return "module: config update failed:\n  - " + strings.Join(e.Issues, "\n  - ")
}

func parentEntryID(entry *resolvedEntry) string {
	if entry == nil || entry.parent == nil {
		return ""
	}
	return entry.parent.entry.ID
}

// ApplyEntries reconciles the running assembly with a new entry tree, as read
// from a changed config file.
//
// Semantics:
//   - full pre-flight validation first; any validation error or rejected change
//     leaves the running tree untouched (no partial application);
//   - unchanged entries are left alone (no restart);
//   - config-only changes are applied in place when the module implements
//     Descriptor.Update (state preserved, dependents untouched); otherwise the
//     UpdateFallback policy decides (default: reject);
//   - new entries are added, removed entries are stopped (their state is gone by
//     definition), and entry IDs/module IDs/parents are immutable (changing
//     them is reported as an error, use remove+add with a new ID);
//   - runtime failures after mutations start are reported as ApplyError; the
//     plan is only committed after a successful verify, so callers can inspect
//     the report and retry.
func (l *Loader) ApplyEntries(entries []Entry, options ...ApplyOption) (*ReloadReport, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, fmt.Errorf("module: loader is closed")
	}
	settings := applyOptions{fallback: FallbackReject}
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	newPlan, err := l.registry.Validate(entries)
	if err != nil {
		return nil, err
	}
	report := &ReloadReport{Warnings: newPlan.Warnings()}
	issues := make([]string, 0)

	oldPlan := l.plan
	oldIDs := make(map[string]bool, len(l.instances))
	for id := range l.instances {
		oldIDs[id] = true
	}
	newIDs := make(map[string]bool, len(newPlan.byID))
	for id := range newPlan.byID {
		newIDs[id] = true
	}
	removed := make(map[string]bool)
	for id := range oldIDs {
		if !newIDs[id] {
			removed[id] = true
		}
	}

	type configChange struct {
		instance *Instance
		newEntry *resolvedEntry
	}
	type fallbackAction struct {
		entryID string
		kind    ChangeKind
	}
	var updates []configChange
	var fallbackActions []fallbackAction
	fallbackHandled := make(map[string]bool)

	for id, newEntry := range newPlan.byID {
		oldEntry, existed := oldPlan.byID[id]
		instance := l.instances[id]
		if !existed || instance == nil {
			continue // addition (or re-enable): handled below
		}
		if oldEntry.def.id != newEntry.def.id {
			issues = append(issues, fmt.Sprintf(
				"entry %q changed module from %q to %q; entry IDs are immutable, use a new ID for the new module",
				id, oldEntry.def.id, newEntry.def.id))
			continue
		}
		if parentEntryID(oldEntry) != parentEntryID(newEntry) {
			issues = append(issues, fmt.Sprintf(
				"entry %q changed parent from %q to %q; entry IDs are immutable, remove it and add a new ID under the new parent",
				id, parentEntryID(oldEntry), parentEntryID(newEntry)))
			continue
		}
		if reflect.DeepEqual(oldEntry.entry.Config, newEntry.entry.Config) {
			if oldEntry.entry.Optional != newEntry.entry.Optional {
				report.Changes = append(report.Changes, Change{EntryID: id, Kind: ChangeMetadata})
			}
			continue
		}
		switch {
		case newEntry.def.update != nil && instance.State() == cordis.Active:
			updates = append(updates, configChange{instance: instance, newEntry: newEntry})
		case settings.fallback == FallbackReload:
			if problem := l.canStatePreservingReload(id); problem != "" {
				issues = append(issues, fmt.Sprintf("entry %q: %s", id, problem))
				continue
			}
			fallbackActions = append(fallbackActions, fallbackAction{entryID: id, kind: ChangeReloaded})
		case settings.fallback == FallbackRecreate:
			fallbackActions = append(fallbackActions, fallbackAction{entryID: id, kind: ChangeRecreated})
		default:
			issues = append(issues, fmt.Sprintf(
				"entry %q: module %q does not support in-place config updates (no Update hook); "+
					"pass module.WithUpdateFallback(FallbackReload) or FallbackRecreate to apply this change explicitly",
				id, newEntry.def.id))
		}
	}
	// Fallback reloads must not collide with removals or with each other's
	// closure.
	for _, action := range fallbackActions {
		if action.kind != ChangeReloaded {
			continue
		}
		for _, entry := range oldPlan.reloadClosure(action.entryID) {
			id := entry.entry.ID
			if removed[id] {
				issues = append(issues, fmt.Sprintf(
					"entry %q: reload would require dependent %q which the new config removes; change the tree in two steps",
					action.entryID, id))
			}
			if fallbackHandled[id] {
				issues = append(issues, fmt.Sprintf(
					"entry %q: overlapping reload closures (also covered by %q); update them one at a time",
					action.entryID, id))
			}
		}
		for _, entry := range oldPlan.reloadClosure(action.entryID) {
			fallbackHandled[entry.entry.ID] = true
		}
	}
	if len(issues) > 0 {
		sort.Strings(issues)
		return report, &ApplyError{Issues: issues}
	}

	// Execute fallback reloads/recreates first (they consume the new configs).
	for _, action := range fallbackActions {
		l.copyNewConfigIntoPlan(newPlan, action.entryID, action.kind)
		var reloadErr error
		if action.kind == ChangeReloaded {
			_, reloadErr = l.reloadLocked(action.entryID)
		} else {
			_, reloadErr = l.recreateLocked(action.entryID)
		}
		if reloadErr != nil {
			issues = append(issues, fmt.Sprintf("entry %q: %v", action.entryID, reloadErr))
			continue
		}
		report.Changes = append(report.Changes, Change{EntryID: action.entryID, Kind: action.kind})
	}
	// In-place config updates (state preserved, dependents untouched).
	for _, update := range updates {
		if fallbackHandled[update.newEntry.entry.ID] {
			continue
		}
		if err := l.updateLocked(update.newEntry.entry.ID, update.newEntry.entry.Config); err != nil {
			issues = append(issues, fmt.Sprintf("entry %q: %v", update.newEntry.entry.ID, err))
			continue
		}
		report.Changes = append(report.Changes, Change{EntryID: update.newEntry.entry.ID, Kind: ChangeUpdated})
	}
	// Removals: only roots of removed subtrees are touched (the kernel cascades
	// children through ownership).
	removedRoots := make([]*Instance, 0)
	for id := range removed {
		instance := l.instances[id]
		if instance == nil {
			continue
		}
		if instance.parent != nil && removed[instance.parent.entryID] {
			continue
		}
		removedRoots = append(removedRoots, instance)
		report.Changes = append(report.Changes, Change{EntryID: id, Kind: ChangeRemoved})
	}
	sort.Slice(removedRoots, func(i, j int) bool { return removedRoots[i].entryID < removedRoots[j].entryID })
	for _, instance := range removedRoots {
		l.removeSubtree(instance)
		containedDispose(instance.fiber)
	}
	// Additions in provider-first schedule order.
	added := make(map[string]bool)
	for _, entry := range newPlan.schedule {
		if oldIDs[entry.entry.ID] {
			continue
		}
		added[entry.entry.ID] = true
	}
	for _, entry := range newPlan.schedule {
		if !added[entry.entry.ID] {
			continue
		}
		var parentInstance *Instance
		if entry.parent != nil {
			parentInstance = l.instances[entry.parent.entry.ID]
		}
		parentCtx := l.root
		if parentInstance != nil {
			fiberCtx, ok := parentInstance.fiber.Context()
			if !ok {
				issues = append(issues, fmt.Sprintf("entry %q: parent %q has no context", entry.entry.ID, parentInstance.entryID))
				continue
			}
			parentCtx = fiberCtx
		}
		instance, createErr := l.createInstance(entry, parentCtx, parentInstance)
		if createErr != nil {
			issues = append(issues, createErr.Error())
			continue
		}
		_ = instance
		report.Changes = append(report.Changes, Change{EntryID: entry.entry.ID, Kind: ChangeAdded})
	}
	if len(issues) == 0 {
		if err := l.root.WhenIdleContext(context.Background()); err != nil {
			issues = append(issues, err.Error())
		} else if startupIssues := l.collectIssues(l.order); len(startupIssues) > 0 {
			issues = append(issues, startupIssues...)
		}
	}
	if len(issues) > 0 {
		sort.Strings(issues)
		return report, &ApplyError{Issues: issues}
	}
	// Commit the new plan and sync per-instance entry metadata.
	l.plan = newPlan
	for id, instance := range l.instances {
		if entry, ok := newPlan.byID[id]; ok {
			instance.entry = entry.entry
			instance.def = entry.def
		}
	}
	return report, nil
}

// canStatePreservingReload reports why a state-preserving reload of entryID is
// not possible, or "" when it is.
func (l *Loader) canStatePreservingReload(entryID string) string {
	closure := l.plan.reloadClosure(entryID)
	if len(closure) == 0 {
		return fmt.Sprintf("no plan entry %q", entryID)
	}
	for _, entry := range closure {
		instance := l.instances[entry.entry.ID]
		if instance == nil {
			return fmt.Sprintf("entry %q in reload closure is not loaded", entry.entry.ID)
		}
		if instance.State() != cordis.Active {
			return fmt.Sprintf("entry %q in reload closure is %s, want Active", entry.entry.ID, instance.State())
		}
		if entry.def.snapshot == nil || entry.def.restore == nil {
			return fmt.Sprintf("entry %q in reload closure cannot preserve state (missing Snapshot/Restore)", entry.entry.ID)
		}
	}
	return ""
}

// copyNewConfigIntoPlan copies the new config/decoded value of entryID (and its
// reload closure for reloads) into the old plan so reloadLocked/recreateLocked
// recreate the instances with the new configuration.
func (l *Loader) copyNewConfigIntoPlan(newPlan *Plan, entryID string, kind ChangeKind) {
	entries := []*resolvedEntry{}
	if kind == ChangeReloaded {
		entries = l.plan.reloadClosure(entryID)
	} else {
		entries = l.plan.subtreeOrder(entryID)
	}
	for _, oldEntry := range entries {
		if newEntry, ok := newPlan.byID[oldEntry.entry.ID]; ok {
			oldEntry.entry = newEntry.entry
			oldEntry.decoded = newEntry.decoded
		}
	}
}

// ApplyFile re-reads the config file and reconciles the running tree with it.
func (l *Loader) ApplyFile(path string, options ...ApplyOption) (*ReloadReport, error) {
	entries, err := LoadConfigFile(path)
	if err != nil {
		return nil, err
	}
	return l.ApplyEntries(entries, options...)
}
