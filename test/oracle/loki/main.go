// Command loki checks LogQL with Loki's own parser (pkg/logql/syntax), as an oracle for the proxy's LogQL.
//
// Loki is AGPL-3.0. This is a module of its own, used only in tests and never part of the proxy build or of
// anything the project distributes: the proxy's go.mod and go.sum never name github.com/grafana/loki, which
// internal/querycorpus TestProductModuleHasNoOracleDependency holds. The proxy's LogQL code is written without
// Loki's: the similar command measures how much of it matches Loki's LogQL sources.
//
//	go run . cases -src "$(go list -m -f '{{.Dir}}' github.com/grafana/loki/v3)" -out cases.json
//	go run . check -in inputs.jsonl -out verdicts.jsonl
//	go run . similar -src "$(go list -m -f '{{.Dir}}' github.com/grafana/loki/v3)" -ours ../../internal/logql -out similar.json
//	go run . facts -in queries.jsonl -out facts.jsonl
//	go run . ipfilter -in lines.jsonl -out verdicts.jsonl
//	go run . pipeline -in queries.jsonl -out verdicts.jsonl
//
// cases writes the queries of Loki's syntax tests (pkg/logql/syntax/*_test.go); check parses every input with
// Loki, builds the pipeline of a log query, and writes whether Loki accepts it, its error text and whether the proxy's printed form means the same
// to Loki; similar compares the proxy's Go files with every Go file under Loki's pkg/, tests included, by token
// fingerprints and writes the share of each proxy file that matches; facts writes what Loki's tree says about each
// query (outer aggregation, offsets, pipeline stages), which the proxy's query plan resolver is compared with.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

// Input is a query to check: its LogQL, and what the proxy made of it.
type Input struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Query   string `json:"q"`
	Ours    bool   `json:"ours"` // the proxy accepts it; copied into the verdict
	Error   string `json:"error,omitempty"`
	Printed string `json:"printed,omitempty"`
}

// Verdict is what Loki's parser makes of an input.
type Verdict struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Accept bool   `json:"accept"`
	Ours   bool   `json:"ours"`
	Error  string `json:"error,omitempty"`
	// SameError is set when both reject: the proxy's error text is Loki's.
	SameError *bool `json:"same_error,omitempty"`
	// SameMeaning is set when the input has a printed form: Loki reads both to the same expression.
	SameMeaning *bool `json:"same_meaning,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		fail(fmt.Errorf("usage: loki cases -src DIR -out FILE | loki check -in FILE -out FILE | loki similar -src DIR -ours DIR -out FILE | loki facts -in FILE -out FILE | loki ipfilter -in FILE -out FILE"))
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	src := fs.String("src", "", "the Loki module directory (cases, similar)")
	in := fs.String("in", "", "inputs, one JSON object per line (check, facts, ipfilter)")
	ours := fs.String("ours", "", "comma-separated directories of the proxy's Go files (similar)")
	out := fs.String("out", "", "output file")
	_ = fs.Parse(os.Args[2:])
	var err error
	switch os.Args[1] {
	case "cases":
		err = writeCases(*src, *out)
	case "check":
		err = check(*in, *out)
	case "similar":
		err = similar(*src, strings.Split(*ours, ","), *out)
	case "facts":
		err = facts(*in, *out)
	case "ipfilter":
		err = ipfilter(*in, *out)
	case "pipeline":
		err = pipeline(*in, *out)
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "loki:", err)
	os.Exit(1)
}

// parse is Loki's form of q, or the error its parser returns. A parser panic is an error.
func parse(q string) (canonical string, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("parser panic: %v", p)
		}
	}()
	expr, err := syntax.ParseExpr(q)
	if err != nil {
		return "", err
	}
	return expr.String(), nil
}

// caseKeys are the struct fields Loki's syntax tests hold their query in.
var caseKeys = map[string]bool{"in": true, "input": true, "query": true, "expr": true, "q": true, "qs": true}

// caseFuncs are the calls whose first argument is a query in Loki's syntax tests.
var caseFuncs = map[string]bool{"ParseExpr": true, "MustParseExpr": true, "ParseSampleExpr": true, "ParseLogSelector": true}

// writeCases collects the query strings of Loki's pkg/logql/syntax tests. Whether Loki accepts each is the
// oracle's verdict, not the test's expectation.
func writeCases(src, out string) error {
	files, err := filepath.Glob(filepath.Join(src, "pkg", "logql", "syntax", "*_test.go"))
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	var cases []string
	add := func(e ast.Expr) {
		if s, ok := literal(e); ok && strings.TrimSpace(s) != "" && !seen[s] {
			seen[s] = true
			cases = append(cases, s)
		}
	}
	for _, f := range files {
		file, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.KeyValueExpr:
				if k, ok := v.Key.(*ast.Ident); ok && caseKeys[k.Name] {
					add(v.Value)
				}
			case *ast.CallExpr:
				name := ""
				switch fn := v.Fun.(type) {
				case *ast.Ident:
					name = fn.Name
				case *ast.SelectorExpr:
					name = fn.Sel.Name
				}
				if caseFuncs[name] && len(v.Args) > 0 {
					add(v.Args[0])
				}
			}
			return true
		})
	}
	if len(cases) == 0 {
		return fmt.Errorf("no syntax test queries under %s", src)
	}
	sort.Strings(cases)
	b, err := json.MarshalIndent(cases, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(out, append(b, '\n'), 0o644)
}

func literal(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	return s, err == nil
}

func check(in, out string) error {
	src, err := os.Open(in)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }() // read only: a close error loses nothing
	var inputs []Input
	sc := bufio.NewScanner(src)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var input Input
		if err := json.Unmarshal(sc.Bytes(), &input); err != nil {
			return err
		}
		inputs = append(inputs, input)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	verdicts := make([]Verdict, len(inputs))
	next := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				verdicts[i] = verdict(inputs[i])
			}
		}()
	}
	for i := range inputs {
		next <- i
	}
	close(next)
	wg.Wait()
	return writeLines(out, verdicts)
}

// build is parse, then the pipeline of a log query, as Loki builds it before it answers: a stage Loki parses but
// refuses to build (a pattern line filter with two captures in a row or a named capture) is a rejection there, the
// 400 its HTTP API answers. A metric query's pipelines are built after Loki rewrites the query (a line_format no
// stage reads is dropped), which its exported API does not expose, so a metric query is only parsed.
func build(q string) (canonical string, err error) {
	canonical, err = parse(q)
	if err != nil {
		return "", err
	}
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("pipeline panic: %v", p)
		}
	}()
	expr, err := syntax.ParseExpr(q)
	if err != nil {
		return "", err
	}
	if sel, ok := expr.(syntax.LogSelectorExpr); ok {
		if _, err := sel.Pipeline(); err != nil {
			return "", err
		}
	}
	return canonical, nil
}

func verdict(input Input) Verdict {
	v := Verdict{ID: input.ID, Kind: input.Kind, Ours: input.Ours}
	canonical, err := build(input.Query)
	v.Accept = err == nil
	if err != nil {
		v.Error = err.Error()
		if !input.Ours {
			same := input.Error == v.Error
			v.SameError = &same
		}
	}
	if input.Printed != "" {
		printed, perr := parse(input.Printed)
		same := err == nil && perr == nil && printed == canonical
		v.SameMeaning = &same
	}
	return v
}

func writeLines[T any](out string, rows []T) error {
	dst, err := os.Create(out)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(dst)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return dst.Close()
}

// The fingerprints: hashes of every run of shingle tokens, comments dropped and identifiers and literals kept, of
// which the smallest in each window of window hashes is kept (winnowing). Two files share a fingerprint only when
// they share a run of at least shingle tokens; any shared run of shingle+window-1 tokens is always found.
const (
	shingle = 50
	window  = 8
)

// Similarity is one proxy file against Loki's LogQL sources.
type Similarity struct {
	File string `json:"file"`
	// Shared is the share of the file's fingerprints that Loki's files also have; Best is the Loki file with most.
	Fingerprints int     `json:"fingerprints"`
	Shared       int     `json:"shared"`
	Share        float64 `json:"share"`
	Best         string  `json:"best,omitempty"`
	BestShared   int     `json:"best_shared,omitempty"`
	// Examples are up to three of the matching token runs, so a reviewer can see what matched.
	Examples []string `json:"examples,omitempty"`
}

func similar(src string, ours []string, out string) error {
	loki := map[uint64]map[string]bool{} // fingerprint -> Loki files
	var dirs []string
	err := filepath.WalkDir(filepath.Join(src, "pkg"), func(path string, d os.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			dirs = append(dirs, path)
		}
		return err
	})
	if err != nil {
		return err
	}
	for _, dir := range dirs {
		files, err := goFiles(dir)
		if err != nil {
			return err
		}
		for _, f := range files {
			fps, err := fingerprints(f)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(src, f)
			for fp := range fps {
				if loki[fp] == nil {
					loki[fp] = map[string]bool{}
				}
				loki[fp][filepath.ToSlash(rel)] = true
			}
		}
	}
	var rows []Similarity
	for _, dir := range ours {
		files, err := goFiles(dir)
		if err != nil {
			return err
		}
		for _, f := range files {
			fps, err := fingerprints(f)
			if err != nil {
				return err
			}
			s := Similarity{File: filepath.ToSlash(f), Fingerprints: len(fps)}
			byFile := map[string]int{}
			var runs []string
			for fp, run := range fps {
				if hit := loki[fp]; hit != nil {
					s.Shared++
					runs = append(runs, run)
					for lf := range hit {
						byFile[lf]++
					}
				}
			}
			sort.Strings(runs)
			if len(runs) > 3 {
				runs = runs[:3]
			}
			s.Examples = runs
			if s.Fingerprints > 0 {
				s.Share = float64(s.Shared) / float64(s.Fingerprints)
			}
			for lf, n := range byFile {
				if n > s.BestShared || (n == s.BestShared && lf < s.Best) {
					s.Best, s.BestShared = lf, n
				}
			}
			rows = append(rows, s)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Share > rows[j].Share || (rows[i].Share == rows[j].Share && rows[i].File < rows[j].File)
	})
	b, err := json.MarshalIndent(rows, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(out, append(b, '\n'), 0o644)
}

// goFiles are the Go files of dir, tests included: the rule is about the repository, not only the binary.
func goFiles(dir string) ([]string, error) {
	return filepath.Glob(filepath.Join(dir, "*.go"))
}

// fingerprints winnows the token shingles of a Go file: fingerprint to its token run.
func fingerprints(path string) (map[uint64]string, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	file := fset.AddFile(path, fset.Base(), len(src))
	var s scanner.Scanner
	s.Init(file, src, nil, 0) // comments are skipped
	var toks []string
	for {
		_, tok, lit := s.Scan()
		if tok == token.EOF {
			break
		}
		if tok == token.SEMICOLON && lit == "\n" {
			continue // an inserted semicolon is layout, not code
		}
		if lit != "" {
			toks = append(toks, lit)
		} else {
			toks = append(toks, tok.String())
		}
	}
	hashes := make([]uint64, 0, len(toks))
	for i := 0; i+shingle <= len(toks); i++ {
		h := fnv.New64a()
		for _, t := range toks[i : i+shingle] {
			_, _ = h.Write([]byte(t))
			_, _ = h.Write([]byte{0})
		}
		hashes = append(hashes, h.Sum64())
	}
	out := map[uint64]string{}
	keep := func(i int) { out[hashes[i]] = strings.Join(toks[i:i+shingle], " ") }
	for i := 0; i+window <= len(hashes); i++ {
		least := i
		for j := i + 1; j < i+window; j++ {
			if hashes[j] < hashes[least] {
				least = j
			}
		}
		keep(least)
	}
	if len(hashes) > 0 && len(hashes) < window {
		for i := range hashes {
			keep(i)
		}
	}
	return out, nil
}
