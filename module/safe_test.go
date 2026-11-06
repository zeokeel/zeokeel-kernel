package module_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zeokeel/cordis-go/module"
)

func TestSafeCallContainsPanic(t *testing.T) {
	var mu sync.Mutex
	reports := make([]module.PanicReport, 0)
	module.SetPanicHandler(func(report module.PanicReport) {
		mu.Lock()
		reports = append(reports, report)
		mu.Unlock()
	})
	t.Cleanup(func() { module.SetPanicHandler(nil) })

	value := module.SafeCall("test/callback", func() { panic("boom") })
	if value != "boom" {
		t.Fatalf("recovered value = %v, want boom", value)
	}
	if value := module.SafeCall("test/callback", func() {}); value != nil {
		t.Fatalf("normal call returned panic value %v", value)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reports) != 1 || reports[0].Name != "test/callback" || reports[0].Value != "boom" {
		t.Fatalf("reports = %+v", reports)
	}
	if len(reports[0].Stack) == 0 || !strings.Contains(string(reports[0].Stack), "safe_test.go") {
		t.Fatalf("report stack is incomplete: %q", reports[0].Stack)
	}
}

func TestSafeGoContainsPanic(t *testing.T) {
	reports := make(chan module.PanicReport, 4)
	module.SetPanicHandler(func(report module.PanicReport) { reports <- report })
	t.Cleanup(func() { module.SetPanicHandler(nil) })

	module.SafeGo("test/goroutine", func() { panic("goroutine boom") })
	select {
	case report := <-reports:
		if report.Name != "test/goroutine" || report.Value != "goroutine boom" {
			t.Fatalf("report = %+v", report)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SafeGo panic was not reported")
	}

	done := make(chan struct{})
	module.SafeGo("test/normal", func() { close(done) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SafeGo did not run the normal function")
	}
	select {
	case report := <-reports:
		t.Fatalf("unexpected report for normal goroutine: %+v", report)
	default:
	}
}
