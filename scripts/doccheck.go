//go:build ignore

// doccheck verifies that every exported symbol in the release packages carries
// a doc comment. It is run by scripts/release-preflight.sh:
//
//	go run scripts/doccheck.go
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	packages := []struct{ dir, name string }{
		{".", "cordis"},
		{filepath.Join(".", "module"), "module"},
	}
	total, missingTotal := 0, 0
	for _, pkg := range packages {
		entries, err := os.ReadDir(pkg.dir)
		if err != nil {
			fmt.Fprintln(os.Stderr, "doccheck:", err)
			os.Exit(2)
		}
		var files []string
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			files = append(files, filepath.Join(pkg.dir, name))
		}
		sort.Strings(files)
		fset := token.NewFileSet()
		missing := make([]string, 0)
		exported := 0
		for _, file := range files {
			parsed, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
			if err != nil {
				fmt.Fprintln(os.Stderr, "doccheck:", err)
				os.Exit(2)
			}
			for _, decl := range parsed.Decls {
				switch d := decl.(type) {
				case *ast.FuncDecl:
					if !d.Name.IsExported() {
						continue
					}
					if d.Recv != nil && len(d.Recv.List) > 0 && !exportedType(d.Recv.List[0].Type) {
						continue // method on an unexported type is not public API
					}
					exported++
					label := d.Name.Name
					if d.Recv != nil && len(d.Recv.List) > 0 {
						label = exprString(d.Recv.List[0].Type) + "." + label
					}
					if d.Doc == nil || strings.TrimSpace(d.Doc.Text()) == "" {
						missing = append(missing, fmt.Sprintf("%s: func %s", file, label))
					}
				case *ast.GenDecl:
					if d.Tok != token.TYPE && d.Tok != token.VAR && d.Tok != token.CONST {
						continue
					}
					for _, spec := range d.Specs {
						switch s := spec.(type) {
						case *ast.TypeSpec:
							if !s.Name.IsExported() {
								continue
							}
							exported++
							if !hasDoc(s.Doc, d.Doc) {
								missing = append(missing, fmt.Sprintf("%s: type %s", file, s.Name.Name))
							}
						case *ast.ValueSpec:
							for _, name := range s.Names {
								if !name.IsExported() {
									continue
								}
								exported++
								if !hasDoc(s.Doc, d.Doc) {
									missing = append(missing, fmt.Sprintf("%s: value %s", file, name.Name))
								}
							}
						}
					}
				}
			}
		}
		fmt.Printf("package %s: exported=%d missing-doc=%d\n", pkg.name, exported, len(missing))
		for _, line := range missing {
			fmt.Println("  MISSING", line)
		}
		total += exported
		missingTotal += len(missing)
	}
	fmt.Printf("total: exported=%d missing-doc=%d\n", total, missingTotal)
	if missingTotal > 0 {
		os.Exit(1)
	}
}

func hasDoc(specDoc, declDoc *ast.CommentGroup) bool {
	return (specDoc != nil && strings.TrimSpace(specDoc.Text()) != "") ||
		(declDoc != nil && strings.TrimSpace(declDoc.Text()) != "")
}

func exportedType(expr ast.Expr) bool {
	switch t := expr.(type) {
	case *ast.StarExpr:
		return exportedType(t.X)
	case *ast.Ident:
		return t.IsExported()
	case *ast.IndexExpr:
		return exportedType(t.X)
	case *ast.IndexListExpr:
		return exportedType(t.X)
	case *ast.SelectorExpr:
		return t.Sel.IsExported()
	default:
		return false
	}
}

func exprString(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return "*" + exprString(t.X)
	case *ast.SelectorExpr:
		return exprString(t.X) + "." + t.Sel.Name
	case *ast.IndexExpr:
		return exprString(t.X) + "[" + exprString(t.Index) + "]"
	case *ast.IndexListExpr:
		parts := make([]string, 0, len(t.Indices))
		for _, index := range t.Indices {
			parts = append(parts, exprString(index))
		}
		return exprString(t.X) + "[" + strings.Join(parts, ", ") + "]"
	default:
		return "?"
	}
}
