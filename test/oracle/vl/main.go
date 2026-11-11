// Command vl checks LogsQL with VictoriaLogs' own parser (lib/logstorage), as an oracle for the proxy's LogsQL.
//
// It is a module of its own, never part of the proxy build, and runs against one pinned VictoriaLogs version
// at a time. go.mod pins the compatibility matrix's pinned version and go.v<version>.mod each other one, with a
// replace so no other module can move it; -modfile picks the file (internal/querycorpus TestVLOracle runs it):
//
//	go run -modfile=go.v1.49.0.mod . cases -src "$(go list -modfile=go.v1.49.0.mod -m -f '{{.Replace.Dir}}' github.com/VictoriaMetrics/VictoriaLogs)" -out cases.json
//	go run -modfile=go.v1.49.0.mod . check -in inputs.jsonl -out verdicts.jsonl
//	go run -modfile=go.v1.49.0.mod . store -in rows-and-queries.jsonl -out answers.jsonl
//
// cases writes VictoriaLogs' own parser test cases of that version (TestParseQuery_Success and
// TestParseQuery_Failure in lib/logstorage/parser_test.go); constructs writes its pipes, stats functions and
// filters, one per pipe_*.go, stats_*.go and filter_*.go source file; check parses every input query, and the
// proxy's printed form of it, and writes whether VictoriaLogs accepts each and whether they mean the same to it;
// store stores rows the way the ingestion routes hand them to lib/logstorage and writes what each query returns
// over them (store.go).
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Case is one of VictoriaLogs' parser test cases: a query its parser must accept (with its canonical form)
// or reject.
type Case struct {
	Query     string `json:"q"`
	Accept    bool   `json:"accept"`
	Canonical string `json:"canonical,omitempty"`
}

// Input is a query to check: its LogsQL text and, when the proxy's parser reads it, the proxy's printed form.
type Input struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Query   string `json:"q"`
	Ours    bool   `json:"ours"` // the proxy's parser reads it; copied into the verdict
	Printed string `json:"printed,omitempty"`
}

// Verdict is what VictoriaLogs' parser makes of an input.
type Verdict struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Accept bool   `json:"accept"`
	Ours   bool   `json:"ours"`
	Error  string `json:"error,omitempty"`
	// SameMeaning is set when the input has a printed form: VictoriaLogs reads both to the same canonical query.
	SameMeaning *bool `json:"same_meaning,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		fail(fmt.Errorf("usage: vl cases|constructs -src DIR -out FILE | vl check|store -in FILE -out FILE"))
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	src := fs.String("src", "", "the VictoriaLogs module directory (cases, constructs)")
	in := fs.String("in", "", "inputs, one JSON object per line (check)")
	out := fs.String("out", "", "output file")
	_ = fs.Parse(os.Args[2:])
	var err error
	switch os.Args[1] {
	case "cases":
		err = writeCases(*src, *out)
	case "constructs":
		err = writeConstructs(*src, *out)
	case "check":
		err = check(*in, *out)
	case "store":
		err = store(*in, *out)
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "vl:", err)
	os.Exit(1)
}

// parse is VictoriaLogs' canonical form of q, or the error its parser returns. A parser panic is an error.
func parse(q string) (canonical string, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("parser panic: %v", p)
		}
	}()
	pq, err := logstorage.ParseQuery(q)
	if err != nil {
		return "", err
	}
	return pq.String(), nil
}

// writeCases reads the f(query, ...) calls of VictoriaLogs' TestParseQuery_Success and TestParseQuery_Failure.
func writeCases(src, out string) error {
	path := filepath.Join(src, "lib", "logstorage", "parser_test.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		return err
	}
	var cases []Case
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || (fn.Name.Name != "TestParseQuery_Success" && fn.Name.Name != "TestParseQuery_Failure") {
			continue
		}
		accept := fn.Name.Name == "TestParseQuery_Success"
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "f" {
				return true
			}
			q, ok := literal(call.Args[0])
			if !ok {
				return true
			}
			c := Case{Query: q, Accept: accept}
			if accept && len(call.Args) > 1 {
				c.Canonical, _ = literal(call.Args[1])
			}
			cases = append(cases, c)
			return true
		})
	}
	if len(cases) == 0 {
		return fmt.Errorf("no parser test cases in %s", path)
	}
	b, err := json.MarshalIndent(cases, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(out, append(b, '\n'), 0o644)
}

// writeConstructs lists the pipes, stats functions and filters of lib/logstorage by their source files'
// names: pipe_<name>.go, stats_<name>.go and filter_<name>.go, tests aside.
func writeConstructs(src, out string) error {
	entries, err := os.ReadDir(filepath.Join(src, "lib", "logstorage"))
	if err != nil {
		return err
	}
	constructs := map[string][]string{"pipes": {}, "stats": {}, "filters": {}}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		stem := strings.TrimSuffix(name, ".go")
		for prefix, category := range map[string]string{"pipe_": "pipes", "stats_": "stats", "filter_": "filters"} {
			if rest, ok := strings.CutPrefix(stem, prefix); ok && rest != "" {
				constructs[category] = append(constructs[category], rest)
			}
		}
	}
	for _, list := range constructs {
		sort.Strings(list)
	}
	b, err := json.MarshalIndent(constructs, "", " ")
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
	// The parser compiles regexps and some queries are long: check on every core, write in input order.
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
	dst, err := os.Create(out)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(dst)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, v := range verdicts {
		if err := enc.Encode(v); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return dst.Close()
}

func verdict(input Input) Verdict {
	v := Verdict{ID: input.ID, Kind: input.Kind, Ours: input.Ours}
	canonical, err := parse(input.Query)
	v.Accept = err == nil
	if err != nil {
		v.Error = err.Error()
	}
	if input.Printed != "" {
		printed, perr := parse(input.Printed)
		same := err == nil && perr == nil && printed == canonical
		v.SameMeaning = &same
	}
	return v
}
