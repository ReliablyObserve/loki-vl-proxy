package proxy

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// errorLayerSitesPath holds the ratchet of client errors written outside the
// typed error layer, counted from the source by TestErrorLayerSites.
var errorLayerSitesPath = filepath.Join("..", "..", "conformance", "registry", "error-layer-sites.json")

// freeTextWriters are the proxy's writers that take the client text as a
// string (the message argument's index): every call to one is a client error
// the typed error layer does not build. The writers' own bodies are not
// call sites.
var freeTextWriters = map[string]int{
	"writeError":         2,
	"writeErrorCause":    2,
	"writeErrorShaped":   2,
	"writeDeleteError":   2,
	"writeLokiTextError": 2,
}

// layerWriters write the text the error layer chose (lokiClientError's or a
// typed error's); the free-text writers they call inside are the layer.
var layerWriters = map[string]bool{
	"writeErr":                 true,
	"writeErrShaped":           true,
	"writeBackendError":        true,
	"writeGrafanaStatsFailure": true,
	"fail":                     true,
	"writeFailure":             true,
}

// errorShapeFuncs write a status and body for the error layer; a raw error
// status inside them is the layer itself.
var errorShapeFuncs = map[string]bool{
	"writeLokiErrorBody":        true,
	"writeBodylessError":        true,
	"writeLokiHTTPError":        true,
	"writeLokiRulerJSON":        true,
	"writeLokiFormatQueryError": true,
}

// successStatuses are the net/http status names below 400.
var successStatuses = map[string]bool{
	"StatusContinue": true, "StatusSwitchingProtocols": true, "StatusProcessing": true, "StatusEarlyHints": true,
	"StatusOK": true, "StatusCreated": true, "StatusAccepted": true, "StatusNonAuthoritativeInfo": true,
	"StatusNoContent": true, "StatusResetContent": true, "StatusPartialContent": true, "StatusMultiStatus": true,
	"StatusAlreadyReported": true, "StatusIMUsed": true, "StatusMultipleChoices": true, "StatusMovedPermanently": true,
	"StatusFound": true, "StatusSeeOther": true, "StatusNotModified": true, "StatusUseProxy": true,
	"StatusTemporaryRedirect": true, "StatusPermanentRedirect": true,
}

// errorLayerSiteCategories, in the file's order:
//   - free_text_sites: calls to a free-text writer or http.Error, whose client
//     text is whatever string the caller passes;
//   - error_text_sites: those whose text holds an error's own text (a call of
//     an Error method), the widest path for text the proxy did not write;
//   - raw_error_status_writes: WriteHeader with a constant status of 400 or
//     more outside the error shape functions;
//   - parser_message_calls: calls of lokierr.ParserMessage, the bridge for the
//     LogQL parser's own rendered message, which must not carry any other
//     error's text.
var errorLayerSiteCategories = []string{"free_text_sites", "error_text_sites", "raw_error_status_writes", "parser_message_calls"}

type siteCounts map[string]map[string]int

func (c siteCounts) add(cat, file string) {
	if c[cat] == nil {
		c[cat] = map[string]int{}
	}
	c[cat][file]++
}

func (c siteCounts) total(cat string) int {
	n := 0
	for _, v := range c[cat] {
		n += v
	}
	return n
}

// countErrorLayerSites scans the production files of internal/proxy.
func countErrorLayerSites(t *testing.T) siteCounts {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	counts := siteCounts{}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		rel := "internal/proxy/" + name
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			countFuncSites(counts, rel, fn)
		}
	}
	return counts
}

func countFuncSites(counts siteCounts, rel string, fn *ast.FuncDecl) {
	enclosing := fn.Name.Name
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		recv := receiverName(sel.X)
		msgAt := -1
		if recv == "lokierr" && sel.Sel.Name == "ParserMessage" {
			counts.add("parser_message_calls", rel)
		}
		switch {
		case recv == "http" && sel.Sel.Name == "Error":
			msgAt = 1
		case recv == "p":
			if at, ok := freeTextWriters[sel.Sel.Name]; ok {
				if _, own := freeTextWriters[enclosing]; !own && !layerWriters[enclosing] {
					msgAt = at
				}
			}
		case sel.Sel.Name == "WriteHeader" && len(call.Args) == 1 && !errorShapeFuncs[enclosing] && errorStatusConst(call.Args[0]):
			counts.add("raw_error_status_writes", rel)
		}
		if msgAt >= 0 && msgAt < len(call.Args) {
			counts.add("free_text_sites", rel)
			if holdsErrorText(call.Args[msgAt]) {
				counts.add("error_text_sites", rel)
			}
		}
		return true
	})
}

// receiverName is the last name of a call's receiver: p for p.f and l.p.f.
func receiverName(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return v.Sel.Name
	}
	return ""
}

// errorStatusConst reports a constant status of 400 or more.
func errorStatusConst(e ast.Expr) bool {
	switch v := e.(type) {
	case *ast.BasicLit:
		n, err := strconv.Atoi(v.Value)
		return v.Kind == token.INT && err == nil && n >= 400
	case *ast.SelectorExpr:
		pkg, ok := v.X.(*ast.Ident)
		return ok && pkg.Name == "http" && strings.HasPrefix(v.Sel.Name, "Status") && !successStatuses[v.Sel.Name]
	}
	return false
}

// holdsErrorText reports a message expression that calls an Error method.
func holdsErrorText(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && len(call.Args) == 0 {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Error" {
				found = true
			}
		}
		return !found
	})
	return found
}

// errorLayerSitesFile is the committed ratchet.
type errorLayerSitesFile struct {
	Description string                    `json:"description"`
	Totals      map[string]int            `json:"totals"`
	Files       map[string]map[string]int `json:"files"`
	// KnownLeakPairs is the ceiling of error-layer-known-leaks.json: the
	// (route x failure-mode cell, registry case) pairs of
	// TestErrorLayer_NoVictoriaLogsLeak that still leak.
	KnownLeakPairs int `json:"known_leak_pairs"`
}

// TestErrorLayerSites holds the client errors written outside the typed error
// layer to the counts committed in conformance/registry/error-layer-sites.json:
// a category that grows fails, naming the files that grew; one that falls
// fails until the file holds the new count (LVP_ERROR_LAYER_SITES_UPDATE=1
// rewrites it). The error-layer gate's known leaks (cell and case pairs) are
// held the same way. Every category goes to 0 as call sites move to the typed layer.
// conformance: semantics/error-layer-internal-go-text, semantics/error-layer-backend-text, semantics/error-layer-silent-success, semantics/error-layer-proxy-limit-status, architecture-syntax-package-layering
func TestErrorLayerSites(t *testing.T) {
	counts := countErrorLayerSites(t)
	got := errorLayerSitesFile{Totals: map[string]int{}, Files: map[string]map[string]int{}}
	for _, cat := range errorLayerSiteCategories {
		got.Totals[cat] = counts.total(cat)
		got.Files[cat] = counts[cat]
		if got.Files[cat] == nil {
			got.Files[cat] = map[string]int{}
		}
	}
	leaks := 0
	for _, ids := range errLayerKnownLeaks(t) {
		leaks += len(ids)
	}
	got.KnownLeakPairs = leaks

	var want errorLayerSitesFile
	raw, err := os.ReadFile(errorLayerSitesPath)
	if err == nil {
		err = json.Unmarshal(raw, &want)
	}
	if os.Getenv("LVP_ERROR_LAYER_SITES_UPDATE") == "1" {
		got.Description = want.Description
		out, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(errorLayerSitesPath, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s: %v, known leak pairs %d", errorLayerSitesPath, got.Totals, got.KnownLeakPairs)
		return
	}
	if err != nil {
		t.Fatalf("read %s: %v", errorLayerSitesPath, err)
	}
	const update = "run LVP_ERROR_LAYER_SITES_UPDATE=1 go test ./internal/proxy -run TestErrorLayerSites and commit conformance/registry/error-layer-sites.json"
	for _, cat := range errorLayerSiteCategories {
		have, limit := got.Totals[cat], want.Totals[cat]
		grown := grownFiles(got.Files[cat], want.Files[cat])
		switch {
		case have > limit:
			t.Errorf("%s grew from %d to %d (write the client error through the typed error layer, p.fail with an internal/lokierr error); files above their committed count: %s",
				cat, limit, have, grown)
		case grown != "":
			// A site removed in one file does not pay for one added in another.
			t.Errorf("%s gained sites in %s although its total did not grow: write them through the typed error layer", cat, grown)
		case have < limit:
			t.Errorf("%s fell from %d to %d: %s", cat, limit, have, update)
		}
	}
	switch {
	case got.KnownLeakPairs > want.KnownLeakPairs:
		t.Errorf("the error-layer gate's known leak pairs grew from %d to %d", want.KnownLeakPairs, got.KnownLeakPairs)
	case got.KnownLeakPairs < want.KnownLeakPairs:
		t.Errorf("the error-layer gate's known leak pairs fell from %d to %d: %s", want.KnownLeakPairs, got.KnownLeakPairs, update)
	}
}

func grownFiles(got, want map[string]int) string {
	var out []string
	for file, n := range got {
		if n > want[file] {
			out = append(out, file+" "+strconv.Itoa(want[file])+"→"+strconv.Itoa(n))
		}
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// TestErrorLayerSitesMatchTheCensus: the scan finds the sites the
// conformance census (conformance/registry/generated/proxy/errors.json, from
// conformance/scripts/sync_errors.py) lists as free text and error text, so
// the ratchet and the published census count the same thing.
// conformance: semantics/error-layer-internal-go-text
func TestErrorLayerSitesMatchTheCensus(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "conformance", "registry", "generated", "proxy", "errors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var census struct {
		Entries []struct {
			Kind      string `json:"kind"`
			ErrorText bool   `json:"error_text"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(raw, &census); err != nil {
		t.Fatal(err)
	}
	free, errText := 0, 0
	for _, e := range census.Entries {
		if e.Kind == "literal" || e.Kind == "dynamic" {
			free++
		}
		if e.ErrorText {
			errText++
		}
	}
	counts := countErrorLayerSites(t)
	if got := counts.total("free_text_sites"); got != free {
		t.Errorf("scan finds %d free-text sites, the census %d", got, free)
	}
	if got := counts.total("error_text_sites"); got != errText {
		t.Errorf("scan finds %d error-text sites, the census %d", got, errText)
	}
}

// internalClientWording are words no text written to a client may carry: the
// translator's and the plan's vocabulary, LogsQL, the backend's name and the
// proxy's own name.
var internalClientWording = []string{
	"loki-vl-proxy", "translator", "translatable", "LogsQL", "not a valid LogQL parser",
	"requires a range metric", "unsupported metric query", "unsupported aggregation", "unsupported instant",
	"invalid range metric query", "uniform offset", "distinct offsets", "invalid range selector",
}

// internalWordingAllowed lists the sites whose text is a proxy-only policy or
// limit that names its own flag or component (the owner's documented
// exceptions), by file and the words they may carry. The list only shrinks.
var internalWordingAllowed = map[string][]string{}

// TestClientErrorTextsCarryNoInternalWording scans the string literals of every
// client error the proxy writes outside the typed layer (the message argument of
// a free-text writer) and of the typed constructors that take a text (lokierr.Parse
// details): none holds the translator's or the plan's wording, LogsQL or the
// proxy's name. The typed layer builds Loki's own messages; this holds the sites
// that are left to it.
// conformance: semantics/error-layer-internal-go-text
func TestClientErrorTextsCarryNoInternalWording(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var parsed []*ast.File
	var names []string
	consts := map[string]string{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		parsed, names = append(parsed, f), append(names, name)
		collectStringConsts(f, consts)
	}
	for i, f := range parsed {
		name := names[i]
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			enclosing := fn.Name.Name
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				var texts []ast.Expr
				switch recv := receiverName(sel.X); {
				case recv == "http" && sel.Sel.Name == "Error" && len(call.Args) > 1:
					texts = call.Args[1:2]
				case recv == "p" && freeTextWriters[sel.Sel.Name] > 0 && len(call.Args) > freeTextWriters[sel.Sel.Name]:
					if _, own := freeTextWriters[enclosing]; !own && !layerWriters[enclosing] {
						texts = call.Args[freeTextWriters[sel.Sel.Name] : freeTextWriters[sel.Sel.Name]+1]
					}
				case recv == "lokierr" && sel.Sel.Name == "Parse" && len(call.Args) == 3:
					texts = call.Args[2:3]
				}
				for _, e := range texts {
					for _, lit := range stringLiterals(e, consts) {
						for _, word := range internalClientWording {
							if strings.Contains(lit, word) && !wordingAllowed(name, word) {
								t.Errorf("%s: %s writes %q to a client: it carries internal wording %q", name, enclosing, lit, word)
							}
						}
					}
				}
				return true
			})
		}
	}
}

func wordingAllowed(file, word string) bool {
	for _, w := range internalWordingAllowed[file] {
		if w == word {
			return true
		}
	}
	return false
}

// stringLiterals are the string literals inside an expression (a message, or
// a fmt.Sprintf format and its arguments), and the values of the package's
// string constants it names.
func stringLiterals(e ast.Expr, consts map[string]string) []string {
	var out []string
	ast.Inspect(e, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.BasicLit:
			if v.Kind == token.STRING {
				if s, err := strconv.Unquote(v.Value); err == nil {
					out = append(out, s)
				}
			}
		case *ast.Ident:
			if s, ok := consts[v.Name]; ok {
				out = append(out, s)
			}
		}
		return true
	})
	return out
}

// collectStringConsts adds the package-level string constants of f whose value
// is a literal or a concatenation of literals and of constants already read
// (the values the scan above reads when a message names one).
func collectStringConsts(f *ast.File, into map[string]string) {
	var value func(e ast.Expr) (string, bool)
	value = func(e ast.Expr) (string, bool) {
		switch v := e.(type) {
		case *ast.BasicLit:
			if v.Kind != token.STRING {
				return "", false
			}
			s, err := strconv.Unquote(v.Value)
			return s, err == nil
		case *ast.Ident:
			s, ok := into[v.Name]
			return s, ok
		case *ast.ParenExpr:
			return value(v.X)
		case *ast.BinaryExpr:
			l, lok := value(v.X)
			r, rok := value(v.Y)
			return l + r, v.Op == token.ADD && lok && rok
		}
		return "", false
	}
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok || len(vs.Names) != len(vs.Values) {
				continue
			}
			for i, name := range vs.Names {
				if s, ok := value(vs.Values[i]); ok {
					into[name.Name] = s
				}
			}
		}
	}
}
