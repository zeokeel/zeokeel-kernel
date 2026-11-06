package module

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	cordis "github.com/zeokeel/cordis-go"
)

// FileConfig is the on-disk assembly configuration used at startup.
//
// Version is reserved for future migrations (0 and 1 are accepted); entries is
// the module tree passed to Load. A bare JSON array of entries is also accepted
// by ParseConfig.
//
// Configuration changes take effect by restarting the process with the new
// file: this package intentionally has no in-process config hot reload or
// module update/reload contract. Code changes on the compile-time route already
// require a rebuild and restart, so a config-only hot path would add module
// author contracts (Update/Snapshot/Restore) and failed/partial reload states
// without removing the deployment step for code.
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
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
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
// is the startup-file counterpart of Load.
//
// The config is read once at startup. To apply a changed config, restart the
// process (or call Load again with a new root context); hot in-process config
// reload is deliberately out of scope for the compile-time design.
func LoadFile(ctx cordis.Context, registry *Registry, path string) (*Loader, error) {
	entries, err := LoadConfigFile(path)
	if err != nil {
		return nil, err
	}
	return Load(ctx, registry, entries)
}
