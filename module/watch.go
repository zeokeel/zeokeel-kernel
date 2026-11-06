package module

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"
)

// WatchOptions configures Loader.WatchFile.
//
// The watcher uses stdlib-only polling (no fsnotify dependency, keeping the
// module package dependency-free); a host that already has a filesystem
// notifier can call ApplyFile/ApplyEntries from its own event loop instead.
type WatchOptions struct {
	// Interval is the poll interval; defaults to 500ms and anything <= 0 uses
	// the default.
	Interval time.Duration
	// Fallback is the UpdateFallback policy passed to ApplyEntries for config
	// changes on modules without an in-place Update hook. The default
	// (FallbackReject) reports the change instead of silently restarting.
	Fallback UpdateFallback
	// OnChange receives each applied (or rejected) reload. It runs on the
	// watcher goroutine; it must be cheap and must not call back into the loader
	// synchronously (Loader methods take the same lock).
	OnChange func(report *ReloadReport, err error)
}

// FileWatcher polls one assembly config file and reconciles the running tree
// when its content changes. Stop it to release the goroutine.
type FileWatcher struct {
	loader  *Loader
	path    string
	options WatchOptions
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

// WatchFile starts a polling watcher for path and applies changes through
// ApplyEntries. The file is parsed once up front so an invalid initial config
// fails immediately instead of on the first edit.
func (l *Loader) WatchFile(path string, options WatchOptions) (*FileWatcher, error) {
	if options.Interval <= 0 {
		options.Interval = 500 * time.Millisecond
	}
	initial, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("module: watch config %q: %w", path, err)
	}
	if _, err := ParseConfig(initial); err != nil {
		return nil, err
	}
	watcher := &FileWatcher{
		loader:  l,
		path:    path,
		options: options,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go watcher.loop(initial)
	return watcher, nil
}

func (w *FileWatcher) loop(last []byte) {
	defer close(w.done)
	ticker := time.NewTicker(w.options.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
		}
		data, err := os.ReadFile(w.path)
		if err != nil {
			w.notify(nil, fmt.Errorf("module: read config %q: %w", w.path, err))
			continue
		}
		if bytes.Equal(data, last) {
			continue
		}
		last = data
		entries, err := ParseConfig(data)
		if err != nil {
			w.notify(nil, err)
			continue
		}
		report, err := w.loader.ApplyEntries(entries, WithUpdateFallback(w.options.Fallback))
		w.notify(report, err)
	}
}

func (w *FileWatcher) notify(report *ReloadReport, err error) {
	if w.options.OnChange != nil {
		func() {
			defer func() { _ = recover() }()
			w.options.OnChange(report, err)
		}()
	}
}

// Stop stops the watcher and waits for its goroutine. It is idempotent.
func (w *FileWatcher) Stop() {
	if w == nil {
		return
	}
	w.once.Do(func() { close(w.stop) })
	<-w.done
}
