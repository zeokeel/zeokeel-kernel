package module_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

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
