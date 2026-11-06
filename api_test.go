package cordis_test

import (
	"sync/atomic"
	"testing"

	cordis "github.com/zeokeel/zeokeel-kernel"
)

// The tests in this file pin the Go 1.27 generic-method API surface: typed
// service access, typed plugin identity and typed metadata are methods on
// Context with the type argument inferred from the arguments, while package-level
// wrappers stay available. Interface methods still cannot declare type
// parameters, which is why Plugin/AsyncPlugin remain non-generic.

type apiConfig struct {
	Name string
}

var apiConfigKey = cordis.NewServiceKey[*apiConfig]("api.config")

type apiPlugin struct{ applied atomic.Int32 }

func (p *apiPlugin) Name() string { return "api-plugin" }
func (p *apiPlugin) Activate(cordis.Context, cordis.Value) error {
	p.applied.Add(1)
	return nil
}

type apiService struct{ name string }

func (s apiService) Name() string { return s.name }

func TestGenericMethodAPI(t *testing.T) {
	root := cordis.Root()
	defer func() { _ = root.DisposeAll() }()

	// Provide/Get/Require: method form, type argument inferred from the key.
	config := &apiConfig{Name: "method"}
	if _, err := root.Provide(apiConfigKey, config); err != nil {
		t.Fatalf("Provide method: %v", err)
	}
	got, ok := root.Get(apiConfigKey)
	if !ok || got != config {
		t.Fatalf("Get method = %v/%v", got, ok)
	}
	required, err := root.Require(apiConfigKey)
	if err != nil || required != config {
		t.Fatalf("Require method = %v/%v", required, err)
	}
	// The package-level wrappers remain available (same behavior).
	wrapped, ok := cordis.Get(root, apiConfigKey)
	if !ok || wrapped != config {
		t.Fatalf("package-level Get = %v/%v", wrapped, ok)
	}

	// ProvideChecked method.
	checkedKey := cordis.NewServiceKey[int]("api.checked")
	if _, err := root.ProvideChecked(checkedKey, 7, func() bool { return true }); err != nil {
		t.Fatalf("ProvideChecked method: %v", err)
	}
	if value, ok := root.Get(checkedKey); !ok || value != 7 {
		t.Fatalf("checked get = %v/%v", value, ok)
	}

	// ProvideService method (Go 1.27 generic method: S inferred from the arg).
	serviceKey := cordis.NewServiceKey[apiService]("api.service")
	if _, err := root.ProvideService(apiService{name: "api.service"}); err != nil {
		t.Fatalf("ProvideService method: %v", err)
	}
	if service, ok := root.Get(serviceKey); !ok || service.name != "api.service" {
		t.Fatalf("service get = %+v/%v", service, ok)
	}

	// PluginTyped method: the concrete plugin type is the registry identity, so
	// two instances share one runtime.
	first, err := root.PluginTyped(&apiPlugin{}, nil)
	if err != nil {
		t.Fatalf("PluginTyped method: %v", err)
	}
	second, err := root.PluginTyped(&apiPlugin{}, nil)
	if err != nil {
		t.Fatalf("PluginTyped method (second): %v", err)
	}
	firstID, secondID := first.ID(), second.ID()
	if firstID == secondID {
		t.Fatal("PluginTyped created the same fiber twice")
	}
	runtime, ok := root.Registry().Get(cordis.TypedPluginKey[*apiPlugin]())
	if !ok {
		t.Fatal("typed plugin runtime missing")
	}
	if runtime.FiberCount() != 2 {
		t.Fatalf("typed runtime fiber count = %d, want 2", runtime.FiberCount())
	}

	// Meta method: the type argument cannot be inferred from a string key, so it
	// is explicit (still a generic method).
	meta := cordis.NewContextMeta()
	meta.Insert("api.meta", "value")
	metaCtx := root.ExtendMeta(*meta)
	if value, ok := metaCtx.Meta[string]("api.meta"); !ok || value != "value" {
		t.Fatalf("Meta method = %v/%v", value, ok)
	}
	if _, ok := metaCtx.Meta[int]("api.meta"); ok {
		t.Fatal("Meta must report a type mismatch as absent")
	}
}
