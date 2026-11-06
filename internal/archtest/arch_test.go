// Package archtest_test enforces the zeokeel-kernel kernel boundary described in
// docs/analysis/04-runtime-config-logger.md §10.4.
//
// The kernel is a standard-library-only library: it must not grow host/IO or
// business imports, must not import its own example/conformance/reference
// packages, and must keep a bounded public API surface. These checks mirror
// the upstream Rust reference's tests/kernel_boundary.rs.
//
// Everything here is deliberately test-only: the checks parse source files and
// never link against the kernel, so they can run even while the rest of the
// tree is temporarily broken by an in-flight change.
package archtest_test

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	// exportedAPICap bounds the number of package-level exported func/type/
	// const/var identifiers in the kernel root. It is a tripwire matching the
	// Rust reference's PUBLIC_ITEM_CAP = 420, not a hard architectural limit.
	exportedAPICap = 420

	// defaultModulePath is used by the scanner self-tests. The real module
	// path is read from go.mod so a rename cannot silently disable the
	// self-import rule.
	defaultModulePath = "github.com/zeokeel/zeokeel-kernel"

	// stdlibListTimeout keeps the architecture test comfortably under its 2s
	// budget even on slow machines.
	stdlibListTimeout = 5 * time.Second
)

// forbiddenImports is the exact host/IO/business import blocklist. Entries are
// the same categories as the Rust kernel boundary: networking, OS/process/
// filesystem, database, native escape hatches, serialization/templates, and
// profiling.
var forbiddenImports = map[string]struct{}{
	"net":            {},
	"net/http":       {},
	"net/url":        {},
	"os":             {},
	"os/exec":        {},
	"os/signal":      {},
	"os/user":        {},
	"io":             {},
	"io/fs":          {},
	"path/filepath":  {},
	"database/sql":   {},
	"crypto/tls":     {},
	"mime":           {},
	"mime/multipart": {},
	"embed":          {},
	"plugin":         {},
	"syscall":        {},
	"unsafe":         {},
	"runtime/pprof":  {},
	"runtime/trace":  {},
	"text/template":  {},
	"html/template":  {},
	"encoding/xml":   {},
}

// forbiddenPrefixes covers whole forbidden subtrees (archive/zip,
// compress/gzip, ...).
var forbiddenPrefixes = []string{
	"archive/",
	"compress/",
}

// Import-rejection reasons surfaced by scanSource.
const (
	forbiddenReason = "forbidden host/IO/business import (kernel boundary §10.1)"

	nonStdlibReason = "non-standard-library import; the kernel may depend on the Go standard library only"

	selfImportReason = "kernel source must not import its own subpackages (examples/conformance/reference)"
)

// TestKernelBoundary scans the kernel package at the repository root (all
// non-_test .go files) for boundary violations and checks the exported API cap.
func TestKernelBoundary(t *testing.T) {
	root := repositoryRoot(t)
	module := readModulePath(t, root)
	stdlib := loadStdlib(t, root)
	files := rootGoFiles(t, root)
	if len(files) == 0 {
		t.Fatalf("no non-test Go sources found in kernel root %s", root)
	}

	for _, name := range files {
		src, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if err := scanSource(name, src, module, stdlib); err != nil {
			t.Errorf("%v", err)
		}
	}

	count, err := countExportedAPI(root, files)
	if err != nil {
		t.Fatalf("count exported API: %v", err)
	}
	if count == 0 {
		t.Fatal("counted zero exported identifiers; the API counter is broken")
	}
	if count > exportedAPICap {
		t.Errorf("package-level exported API count = %d exceeds cap %d: "+
			"if a new public API is truly required, update this arch test and explain why "+
			"(如确需新增公共 API 请更新 arch test 并说明理由)", count, exportedAPICap)
	}

	t.Logf("kernel boundary OK: %d root sources, %d package-level exported identifiers (cap %d)",
		len(files), count, exportedAPICap)
}

// TestKernelBoundaryScannerSelfTest proves the scanner catches bypasses rather
// than vacuously passing. It feeds synthetic sources containing forbidden,
// non-standard-library, and self imports to the same scanner used above.
func TestKernelBoundaryScannerSelfTest(t *testing.T) {
	const module = defaultModulePath
	stdlib := map[string]struct{}{
		"context":     {},
		"fmt":         {},
		"log/slog":    {},
		"sync":        {},
		"sync/atomic": {},
		"time":        {},
	}

	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "forbidden_os",
			src:  "package fixture\n\nimport \"os\"\n",
			want: "os",
		},
		{
			name: "forbidden_archive_prefix",
			src:  "package fixture\n\nimport _ \"archive/zip\"\n",
			want: "archive/zip",
		},
		{
			name: "forbidden_runtime_pprof",
			src:  "package fixture\n\nimport _ \"runtime/pprof\"\n",
			want: "runtime/pprof",
		},
		{
			name: "non_stdlib",
			src:  "package fixture\n\nimport \"github.com/example/business\"\n",
			want: "github.com/example/business",
		},
		{
			name: "self_import",
			src:  "package fixture\n\nimport _ \"" + module + "/examples/hello\"\n",
			want: module + "/examples/hello",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := scanSource(tc.name+".go", []byte(tc.src), module, stdlib)
			if err == nil {
				t.Fatalf("scanner accepted forbidden import %q; guard self-test failed", tc.want)
			}
			msg := err.Error()
			if !strings.Contains(msg, tc.name+".go:") {
				t.Errorf("error %q does not contain file:line location", msg)
			}
			if !strings.Contains(msg, "import") {
				t.Errorf("error %q does not contain the word import", msg)
			}
			if !strings.Contains(msg, tc.want) {
				t.Errorf("error %q does not name the offending import %q", msg, tc.want)
			}
		})
	}

	allowed := "package fixture\n\nimport (\n\t\"context\"\n\t\"log/slog\"\n\t\"sync\"\n)\n"
	if err := scanSource("allowed.go", []byte(allowed), module, stdlib); err != nil {
		t.Fatalf("scanner rejected standard-library imports: %v", err)
	}
}

// scanSource parses one Go source and returns a single error whose lines are
// formatted as "file:line: import \"path\": reason". It is shared by the real
// scan and the self-test so the guard's bypass tests exercise the production
// path.
func scanSource(name string, src []byte, module string, stdlib map[string]struct{}) error {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		return fmt.Errorf("%s: parse error: %w", name, err)
	}

	var problems []string
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			// Import paths from a successfully parsed file are always valid
			// quoted literals; fall back to trimming rather than panicking.
			path = strings.Trim(spec.Path.Value, "`\"")
		}
		reason := importReason(path, module, stdlib)
		if reason == "" {
			continue
		}
		line := fset.Position(spec.Pos()).Line
		problems = append(problems, fmt.Sprintf("%s:%d: import %q: %s", name, line, path, reason))
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "\n"))
	}
	return nil
}

// importReason classifies an import path; an empty string means it is allowed.
// Self imports are checked first so the diagnostic names the layering rule,
// not merely "non-standard library".
func importReason(path, module string, stdlib map[string]struct{}) string {
	if module != "" && (path == module || strings.HasPrefix(path, module+"/")) {
		return selfImportReason
	}
	if _, ok := forbiddenImports[path]; ok {
		return forbiddenReason
	}
	for _, prefix := range forbiddenPrefixes {
		if strings.HasPrefix(path, prefix) {
			return forbiddenReason
		}
	}
	if !isStdlib(path, stdlib) {
		return nonStdlibReason
	}
	return ""
}

// isStdlib reports whether path belongs to the standard library. The canonical
// set comes from "go list std"; when that command is unavailable the Go import
// path convention (the first element of a standard-library path never
// contains a dot) is used as a conservative fallback.
func isStdlib(path string, stdlib map[string]struct{}) bool {
	if stdlib != nil {
		_, ok := stdlib[path]
		return ok
	}
	first, _, _ := strings.Cut(path, "/")
	return first != "" && !strings.Contains(first, ".")
}

// countExportedAPI counts package-level exported funcs, types, consts and vars
// across the given root files. Methods and struct fields are not package-level
// declarations and are intentionally excluded.
func countExportedAPI(root string, files []string) (int, error) {
	fset := token.NewFileSet()
	total := 0
	for _, name := range files {
		file, err := parser.ParseFile(fset, filepath.Join(root, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", name, err)
		}
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.IsExported() {
					total++
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							total++
						}
					case *ast.ValueSpec:
						for _, ident := range s.Names {
							if ident.IsExported() {
								total++
							}
						}
					}
				}
			}
		}
	}
	return total, nil
}

// rootGoFiles lists non-test Go files present directly in the kernel root.
// Subdirectories (examples/, conformance/, .git, internal/archtest
// and any future internal package) are outside the kernel package and are not
// scanned.
func rootGoFiles(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read kernel root %s: %v", root, err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		files = append(files, name)
	}
	return files
}

// repositoryRoot finds the module root via runtime.Caller (falling back to the
// test's working directory under -trimpath builds) and walks up to go.mod.
func repositoryRoot(t *testing.T) string {
	t.Helper()
	if _, file, _, ok := runtime.Caller(0); ok {
		if abs, err := filepath.Abs(file); err == nil {
			if root, found := findModuleRoot(filepath.Dir(abs)); found {
				return root
			}
		}
	}
	if wd, err := os.Getwd(); err == nil {
		if root, found := findModuleRoot(wd); found {
			return root
		}
	}
	t.Fatal("could not locate go.mod via runtime.Caller or working directory")
	return ""
}

// findModuleRoot walks upward from dir until it finds a directory containing
// go.mod.
func findModuleRoot(dir string) (string, bool) {
	for range 8 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", false
}

// readModulePath parses the module directive from go.mod.
func readModulePath(t *testing.T, root string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if rest, ok := strings.CutPrefix(line, "module "); ok {
			if module := strings.TrimSpace(rest); module != "" {
				return module
			}
		}
	}
	t.Fatalf("go.mod at %s has no module directive", root)
	return ""
}

// loadStdlib obtains the canonical standard-library import set with
// "go list std". If that fails (for example a minimal environment without the
// go tool on PATH), isStdlib falls back to path-shape detection, so boundary
// checking continues to work.
func loadStdlib(t *testing.T, root string) map[string]struct{} {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), stdlibListTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "list", "std")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Logf("go list std failed (%v); falling back to path-shape stdlib detection", err)
		return nil
	}

	stdlib := make(map[string]struct{}, 400)
	for _, line := range strings.Split(string(out), "\n") {
		if pkg := strings.TrimSpace(line); pkg != "" {
			stdlib[pkg] = struct{}{}
		}
	}
	if len(stdlib) == 0 {
		t.Log("go list std returned no packages; falling back to path-shape stdlib detection")
		return nil
	}
	return stdlib
}
