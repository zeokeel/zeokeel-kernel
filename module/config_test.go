package module_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cordis "github.com/zeokeel/cordis-go"
	"github.com/zeokeel/cordis-go/module"
)

func writeConfig(t *testing.T, path string, entries []module.Entry) {
	t.Helper()
	data, err := json.Marshal(module.FileConfig{Version: 1, Entries: entries})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

func configPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), name)
}

func TestParseConfig(t *testing.T) {
	entries, err := module.ParseConfig([]byte(`{
		"version": 1,
		"entries": [{"id":"database","module":"test/db","config":{"dsn":"mem://"}}]
	}`))
	if err != nil || len(entries) != 1 || entries[0].ID != "database" {
		t.Fatalf("object config = %+v/%v", entries, err)
	}
	entries, err = module.ParseConfig([]byte(`[{"id":"x","module":"test/parent"}]`))
	if err != nil || len(entries) != 1 || entries[0].ID != "x" {
		t.Fatalf("array config = %+v/%v", entries, err)
	}
	if _, err := module.ParseConfig([]byte(`{"version":9,"entries":[]}`)); err == nil {
		t.Fatal("unsupported version should fail")
	}
	if _, err := module.ParseConfig([]byte(`{"entries":[{"id":"x","module":"m","typo":1}]}`)); err == nil {
		t.Fatal("unknown config field should fail (strict parser)")
	}
	if _, err := module.ParseConfig(nil); err == nil {
		t.Fatal("empty document should fail")
	}
}

func TestLoadFileDeliversTypedConfig(t *testing.T) {
	path := configPath(t, "assembly.json")
	writeConfig(t, path, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "file://db", "pool": 2}},
		{ID: "app", Module: "test/parent", Children: []module.Entry{{ID: "http", Module: "test/api"}}},
	})
	root := newRoot(t)
	registry := newRegistry(t)
	loader, err := module.LoadFile(root, registry, path)
	if err != nil {
		t.Fatalf("load file: %v", err)
	}
	t.Cleanup(func() { _ = loader.Close() })
	database, ok := root.Get(dbKey)
	if !ok || database.dsn != "file://db" || database.pool != 2 {
		t.Fatalf("typed config from file = %+v/%v", database, ok)
	}
	for _, id := range []string{"database", "app", "http"} {
		instance, ok := loader.Instance(id)
		if !ok || instance.State() != cordis.Active {
			t.Fatalf("instance %q = %v/%v", id, instance, ok)
		}
	}
}

func TestApplyFileUpdateInPlace(t *testing.T) {
	resetStateful()
	path := configPath(t, "assembly.json")
	writeConfig(t, path, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 1}},
		{ID: "dependent", Module: "test/stateful-dependent"},
	})
	root := newRoot(t)
	loader, err := module.LoadFile(root, newStatefulRegistry(t), path)
	if err != nil {
		t.Fatalf("load file: %v", err)
	}
	t.Cleanup(func() { _ = loader.Close() })
	service, _ := root.Get(counterKey)
	service.value = 7
	counterInstance, _ := loader.Instance("counter")
	fiberID := counterInstance.Fiber().ID()

	writeConfig(t, path, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 4}},
		{ID: "dependent", Module: "test/stateful-dependent"},
	})
	report, err := loader.ApplyFile(path)
	if err != nil {
		t.Fatalf("apply file: %v", err)
	}
	if !report.HasChanges() || !hasKind(report, "counter", module.ChangeUpdated) {
		t.Fatalf("report = %+v", report)
	}
	updated, _ := root.Get(counterKey)
	if updated != service || updated.step != 4 || updated.value != 7 {
		t.Fatalf("in-place update after file change = %+v", updated)
	}
	if counterInstance.Fiber().ID() != fiberID || stateful.updates != 1 {
		t.Fatal("file reload should update in place without recreating the fiber")
	}
	if dependentState.activates != 1 {
		t.Fatalf("dependent reactivated: %d", dependentState.activates)
	}
}

func TestApplyFileRejectUnsupportedUpdate(t *testing.T) {
	path := configPath(t, "assembly.json")
	writeConfig(t, path, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "a://"}},
	})
	root := newRoot(t)
	loader, err := module.LoadFile(root, newRegistry(t), path)
	if err != nil {
		t.Fatalf("load file: %v", err)
	}
	t.Cleanup(func() { _ = loader.Close() })
	instance, _ := loader.Instance("database")
	fiberID := instance.Fiber().ID()

	writeConfig(t, path, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "b://"}},
	})
	report, err := loader.ApplyFile(path)
	if err == nil {
		t.Fatal("unsupported config change should be rejected by default")
	}
	var applyErr *module.ApplyError
	if !errors.As(err, &applyErr) || !strings.Contains(err.Error(), "Update hook") {
		t.Fatalf("apply err = %v", err)
	}
	if report == nil {
		t.Fatal("report should be returned with the error")
	}
	database, _ := root.Get(dbKey)
	if database.dsn != "a://" || instance.Fiber().ID() != fiberID || instance.State() != cordis.Active {
		t.Fatal("rejected config change must leave the old tree untouched")
	}
}

func TestApplyFileFallbackRecreate(t *testing.T) {
	path := configPath(t, "assembly.json")
	writeConfig(t, path, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "a://"}},
	})
	root := newRoot(t)
	loader, err := module.LoadFile(root, newRegistry(t), path)
	if err != nil {
		t.Fatalf("load file: %v", err)
	}
	t.Cleanup(func() { _ = loader.Close() })
	before, _ := loader.Instance("database")
	oldFiberID := before.Fiber().ID()

	writeConfig(t, path, []module.Entry{
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "b://"}},
	})
	report, err := loader.ApplyFile(path, module.WithUpdateFallback(module.FallbackRecreate))
	if err != nil {
		t.Fatalf("apply file: %v", err)
	}
	if !hasKind(report, "database", module.ChangeRecreated) {
		t.Fatalf("report = %+v", report)
	}
	database, _ := root.Get(dbKey)
	if database.dsn != "b://" {
		t.Fatalf("recreated service dsn = %q", database.dsn)
	}
	after, _ := loader.Instance("database")
	if after.Fiber().ID() == oldFiberID {
		t.Fatal("explicit recreate should create a new fiber")
	}
	if before.State() != cordis.Disposed {
		t.Fatalf("old instance = %v, want Disposed", before.State())
	}
}

func TestApplyFileFallbackReloadPreservesState(t *testing.T) {
	resetStateful()
	path := configPath(t, "assembly.json")
	writeConfig(t, path, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 1}},
		{ID: "dependent", Module: "test/stateful-dependent"},
	})
	registry := module.NewRegistry()
	if err := registry.Register(statefulModuleWith(false)); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := registry.Register(statefulDependentModule()); err != nil {
		t.Fatalf("register: %v", err)
	}
	root := newRoot(t)
	loader, err := module.LoadFile(root, registry, path)
	if err != nil {
		t.Fatalf("load file: %v", err)
	}
	t.Cleanup(func() { _ = loader.Close() })
	service, _ := root.Get(counterKey)
	service.value = 42
	dependentState.value = 99

	writeConfig(t, path, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 9}},
		{ID: "dependent", Module: "test/stateful-dependent"},
	})
	report, err := loader.ApplyFile(path, module.WithUpdateFallback(module.FallbackReload))
	if err != nil {
		t.Fatalf("apply file: %v", err)
	}
	if !hasKind(report, "counter", module.ChangeReloaded) {
		t.Fatalf("report = %+v", report)
	}
	reloaded, _ := root.Get(counterKey)
	if reloaded.value != 42 || reloaded.step != 9 {
		t.Fatalf("reloaded counter = %+v, want preserved value 42 and new step 9", reloaded)
	}
	if reloaded == service {
		t.Fatal("reload should recreate the module instance (new service value)")
	}
	if dependentState.value != 99 || dependentState.restores != 1 {
		t.Fatalf("dependent state not preserved: %+v", dependentState)
	}
	if counterInstance, _ := loader.Instance("counter"); counterInstance.State() != cordis.Active {
		t.Fatal("reloaded counter should be active")
	}
}

func TestApplyFileAddRemove(t *testing.T) {
	resetStateful()
	registry := module.NewRegistry()
	for _, register := range []func() error{
		func() error { return registry.Register(statefulModule()) },
		func() error { return registry.Register(statefulDependentModule()) },
		func() error { return registry.Register(dbModule()) },
	} {
		if err := register(); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	path := configPath(t, "assembly.json")
	writeConfig(t, path, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 1}},
		{ID: "dependent", Module: "test/stateful-dependent"},
	})
	root := newRoot(t)
	loader, err := module.LoadFile(root, registry, path)
	if err != nil {
		t.Fatalf("load file: %v", err)
	}
	t.Cleanup(func() { _ = loader.Close() })
	if _, ok := loader.Instance("dependent"); !ok {
		t.Fatal("dependent should be loaded initially")
	}

	writeConfig(t, path, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 1}},
		{ID: "database", Module: "test/db", Config: map[string]cordis.Value{"dsn": "added://"}},
	})
	report, err := loader.ApplyFile(path)
	if err != nil {
		t.Fatalf("apply file: %v", err)
	}
	if !hasKind(report, "dependent", module.ChangeRemoved) || !hasKind(report, "database", module.ChangeAdded) {
		t.Fatalf("report = %+v", report)
	}
	if _, ok := loader.Instance("dependent"); ok {
		t.Fatal("removed entry still loaded")
	}
	database, _ := loader.Instance("database")
	if database.State() != cordis.Active {
		t.Fatalf("added entry state = %v", database.State())
	}
	if got, _ := root.Get(dbKey); got == nil || got.dsn != "added://" {
		t.Fatalf("added module config not applied: %+v", got)
	}
}

func TestApplyFileInvalidConfigLeavesTree(t *testing.T) {
	resetStateful()
	path := configPath(t, "assembly.json")
	writeConfig(t, path, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 1}},
		{ID: "dependent", Module: "test/stateful-dependent"},
	})
	root := newRoot(t)
	loader, err := module.LoadFile(root, newStatefulRegistry(t), path)
	if err != nil {
		t.Fatalf("load file: %v", err)
	}
	t.Cleanup(func() { _ = loader.Close() })
	instance, _ := loader.Instance("counter")
	fiberID := instance.Fiber().ID()

	if err := os.WriteFile(path, []byte(`{"entries":[{"id":"counter","module":"test/stateful","config":{"step":0}}]}`), 0o600); err != nil {
		t.Fatalf("write invalid config: %v", err)
	}
	if _, err := loader.ApplyFile(path); err == nil {
		t.Fatal("invalid config should fail pre-flight")
	}
	if instance.State() != cordis.Active || instance.Fiber().ID() != fiberID {
		t.Fatal("invalid config must not touch the running tree")
	}
}

func TestWatchFileHotReload(t *testing.T) {
	resetStateful()
	path := configPath(t, "assembly.json")
	writeConfig(t, path, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 1}},
	})
	root := newRoot(t)
	loader, err := module.LoadFile(root, newStatefulRegistry(t), path)
	if err != nil {
		t.Fatalf("load file: %v", err)
	}
	t.Cleanup(func() { _ = loader.Close() })
	changes := make(chan string, 8)
	watcher, err := loader.WatchFile(path, module.WatchOptions{
		Interval: 10 * time.Millisecond,
		OnChange: func(report *module.ReloadReport, err error) {
			if err != nil {
				changes <- "error: " + err.Error()
				return
			}
			if report.HasChanges() {
				changes <- "changed"
			}
		},
	})
	if err != nil {
		t.Fatalf("watch file: %v", err)
	}
	t.Cleanup(watcher.Stop)

	writeConfig(t, path, []module.Entry{
		{ID: "counter", Module: "test/stateful", Config: map[string]cordis.Value{"step": 5}},
	})
	waitFile(t, 3*time.Second, func() bool {
		select {
		case event := <-changes:
			return event == "changed"
		default:
			return false
		}
	}, "watcher applied the config change")
	service, _ := root.Get(counterKey)
	if service.step != 5 {
		t.Fatalf("watched config step = %d, want 5", service.step)
	}
	if stateful.updates != 1 {
		t.Fatalf("watcher should use the in-place update path, updates = %d", stateful.updates)
	}
}

func hasKind(report *module.ReloadReport, entryID string, kind module.ChangeKind) bool {
	if report == nil {
		return false
	}
	for _, change := range report.Changes {
		if change.EntryID == entryID && change.Kind == kind {
			return true
		}
	}
	return false
}

// waitFile polls condition with a real sleep (the watcher is ticker-driven).
func waitFile(t *testing.T, timeout time.Duration, condition func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not reached: %s", what)
}
