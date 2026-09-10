// movecheck verifies that a Go package refactor was move-only: the multiset of
// top-level declarations (with doc comments, printed canonically) is identical
// between two directories, ignoring import declarations and file boundaries.
//
// usage: movecheck <beforeDir> <afterDir>
package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func decls(dir string) (map[string]int, map[string]string, error) {
	counts := map[string]int{}
	where := map[string]string{}
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, nil, err
	}
	for _, f := range files {
		fset := token.NewFileSet()
		af, err := parser.ParseFile(fset, f, nil, parser.ParseComments)
		if err != nil {
			return nil, nil, err
		}
		for _, d := range af.Decls {
			if g, ok := d.(*ast.GenDecl); ok && g.Tok == token.IMPORT {
				continue
			}
			var buf bytes.Buffer
			cfg := printer.Config{Mode: printer.UseSpaces | printer.TabIndent, Tabwidth: 8}
			node := &printer.CommentedNode{Node: d, Comments: af.Comments}
			if err := cfg.Fprint(&buf, fset, node); err != nil {
				return nil, nil, err
			}
			key := strings.TrimSpace(buf.String())
			counts[key]++
			where[key] = filepath.Base(f)
		}
	}
	return counts, where, nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: movecheck <beforeDir> <afterDir>")
		os.Exit(2)
	}
	before, bw, err := decls(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	after, aw, err := decls(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var problems []string
	for k, n := range before {
		if after[k] != n {
			problems = append(problems, fmt.Sprintf("REMOVED/CHANGED (%s, x%d before, x%d after):\n%s", bw[k], n, after[k], head(k)))
		}
	}
	for k, n := range after {
		if before[k] != n {
			problems = append(problems, fmt.Sprintf("ADDED/CHANGED (%s, x%d before, x%d after):\n%s", aw[k], before[k], n, head(k)))
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		fmt.Println(p)
		fmt.Println("----")
	}
	fmt.Printf("decls before=%d after=%d problems=%d\n", total(before), total(after), len(problems))
	if len(problems) > 0 {
		os.Exit(1)
	}
}

func head(s string) string {
	lines := strings.SplitN(s, "\n", 6)
	if len(lines) > 5 {
		return strings.Join(lines[:5], "\n") + "\n..."
	}
	return s
}

func total(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}
