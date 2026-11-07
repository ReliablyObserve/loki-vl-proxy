package querycorpus_test

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// sitesPath holds the committed counts of the places production code handles query text as strings. They may
// only fall: TestQueryTextSites fails when a count grows, and when one falls until the file holds the new count.
const sitesPath = "../../conformance/registry/query-text-sites.json"

const module = "github.com/ReliablyObserve/Loki-VL-proxy/"

// The categories, each counted over the production (non-test) Go files of cmd/ and internal/:
//
//	parse_calls             a call of the LogQL or LogsQL parser outside its own package (or of the proxy's
//	                        helpers that wrap it): a place that parses query text again instead of reusing a
//	                        parsed query
//	string_reads            a strings or regexp call whose literal argument is LogQL or LogsQL: query text read as
//	                        a string
//	text_built              any other LogQL or LogsQL literal: query text built as a string
//	deferred_raw            a logsql.DeferredExpr literal: raw text inside the LogsQL AST
//	translator_string_ops   every strings and regexp call in internal/translator, the string translator the AST
//	                        translation replaces
//
// internal/logql and internal/logsql, which own the parsers and printers, are not counted, and neither is
// internal/querycorpus. The literal rules (queryLiteral) count a literal only when it carries query syntax.
var categories = []string{"parse_calls", "string_reads", "text_built", "deferred_raw", "translator_string_ops"}

var parserFuncs = map[string]map[string]bool{
	module + "internal/logql":  {"Parse": true, "ParseLogQuery": true, "ParseAndValidate": true},
	module + "internal/logsql": {"Parse": true, "ParseFilter": true},
}

// proxyParseHelpers are the proxy's own calls of the LogQL parser (internal/proxy/query_flow.go): each times the
// parse for the request's flow. A call of one is a parse_calls site exactly like a direct call, so moving a parse
// behind them cannot lower the count; the helpers' own bodies are not counted.
var proxyParseHelpers = map[string]bool{"parseLogQLExpr": true, "parseLogQLQuery": true, "validateLogQL": true}

const proxyParseHelperFile = "internal/proxy/query_flow.go"

// stringReads are the strings functions that inspect or cut text.
var stringReads = map[string]bool{
	"Contains": true, "ContainsAny": true, "Index": true, "IndexAny": true, "LastIndex": true, "HasPrefix": true,
	"HasSuffix": true, "Count": true, "Cut": true, "CutPrefix": true, "CutSuffix": true, "Split": true, "SplitN": true,
	"SplitAfter": true, "TrimPrefix": true, "TrimSuffix": true, "Replace": true, "ReplaceAll": true, "EqualFold": true,
}

// queryLiteral matches query syntax: LogsQL pipes and field filters, LogQL stages and filters, range and vector
// aggregation syntax and stream selectors. A bare field name ("_msg") or label name is not query text.
var queryLiteral = regexp.MustCompile(strings.Join([]string{
	`\|\s*(stats|unpack_json|unpack_logfmt|extract|extract_regexp|format|math|sort|limit|rename|copy|replace|replace_regexp|uniq|top|pack_json|pack_logfmt|drop_empty_fields|field_names|field_values|filter|fields|delete|coalesce|running_stats|total_stats|hash|len|first|last|offset|sample|join|union|facets|collapse_nums|set_stream_fields|split|unroll|time_add|json_array_len|json_array_concat|stream_context|query_stats|generate_sequence|unpack_syslog|unpack_words|update)\b`,
	`\|\s*(json|logfmt|regexp|pattern|line_format|label_format|unwrap|keep|drop|decolorize|ip)\b`,
	`\|[=~]`, `!~\s*"`, `\b_(stream|msg|time|stream_id):`,
	`_over_time\s*\(`, `\b(rate|bytes_rate|count_over_time|absent_over_time)\s*\(`,
	// A stream selector starts the text or follows a space, "(" or ","; after a name it is a Prometheus series.
	`\b(by|without)\s*\(`, `(^|[\s(,])\{\s*[A-Za-z_][\w.]*\s*(=|!=|=~|!~)\s*"`,
}, "|"))

// prose matches three plain words in a row, which query text never has: help texts, descriptions and error messages
// that quote a query are not query text.
var prose = regexp.MustCompile(`\b[A-Za-z]{3,} [A-Za-z]{3,} [A-Za-z]{3,}\b`)

type siteCounts map[string]map[string]int // category -> file -> count

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

// countSites walks the production Go files under root/cmd and root/internal.
func countSites(t *testing.T, root string) siteCounts {
	t.Helper()
	counts := siteCounts{}
	fset := token.NewFileSet()
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			for _, own := range []string{"internal/logql/", "internal/logsql/", "internal/querycorpus/"} {
				if strings.HasPrefix(rel, own) {
					return nil
				}
			}
			f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			countFile(counts, rel, f)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return counts
}

func countFile(counts siteCounts, rel string, f *ast.File) {
	imports := map[string]string{} // local name -> import path
	for _, imp := range f.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		name := p[strings.LastIndex(p, "/")+1:]
		if imp.Name != nil {
			name = imp.Name.Name
		}
		imports[name] = p
	}
	translator := strings.HasPrefix(rel, "internal/translator/")
	read := map[*ast.BasicLit]bool{} // literals already counted as a string read
	ast.Inspect(f, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.CallExpr:
			if id, ok := v.Fun.(*ast.Ident); ok && proxyParseHelpers[id.Name] && strings.HasPrefix(rel, "internal/proxy/") {
				counts.add("parse_calls", rel)
				return true
			}
			sel, ok := v.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			path := imports[pkg.Name]
			if parserFuncs[path][sel.Sel.Name] && rel != proxyParseHelperFile {
				counts.add("parse_calls", rel)
			}
			if translator && (path == "strings" || path == "regexp") {
				counts.add("translator_string_ops", rel)
				return true
			}
			if (path == "strings" && stringReads[sel.Sel.Name]) || path == "regexp" {
				for _, arg := range v.Args {
					if lit := queryLit(arg); lit != nil && !translator {
						counts.add("string_reads", rel)
						read[lit] = true
						break
					}
				}
			}
		case *ast.CompositeLit:
			if sel, ok := v.Type.(*ast.SelectorExpr); ok {
				if pkg, ok := sel.X.(*ast.Ident); ok && imports[pkg.Name] == module+"internal/logsql" && sel.Sel.Name == "DeferredExpr" {
					counts.add("deferred_raw", rel)
				}
			}
		}
		return true
	})
	if translator {
		return
	}
	ast.Inspect(f, func(n ast.Node) bool {
		if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING && !read[lit] && queryLit(lit) != nil {
			counts.add("text_built", rel)
		}
		return true
	})
}

// queryLit is e when it is a string literal carrying query syntax.
func queryLit(e ast.Expr) *ast.BasicLit {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return nil
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil || !queryLiteral.MatchString(s) || prose.MatchString(s) {
		return nil
	}
	return lit
}

// sitesFile is the committed file: the total per category and its files.
type sitesFile struct {
	Description string                    `json:"description"`
	Totals      map[string]int            `json:"totals"`
	Files       map[string]map[string]int `json:"files"`
}

// TestQueryTextSites holds the places production code handles query text as strings to the committed counts in
// conformance/registry/query-text-sites.json. A category that grows fails, naming the files that grew; one that
// falls fails until the file holds the new counts: run with LVP_QUERY_TEXT_SITES_UPDATE=1 to rewrite it.
func TestQueryTextSites(t *testing.T) {
	counts := countSites(t, "../..")
	got := sitesFile{Totals: map[string]int{}, Files: map[string]map[string]int{}}
	for _, cat := range categories {
		got.Totals[cat] = counts.total(cat)
		got.Files[cat] = counts[cat]
		if got.Files[cat] == nil {
			got.Files[cat] = map[string]int{}
		}
	}
	var want sitesFile
	b, err := os.ReadFile(sitesPath)
	if err == nil {
		err = json.Unmarshal(b, &want)
	}
	if os.Getenv("LVP_QUERY_TEXT_SITES_UPDATE") == "1" {
		got.Description = want.Description
		out, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(sitesPath, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s: %v", sitesPath, got.Totals)
		return
	}
	if err != nil {
		t.Fatalf("read %s: %v", sitesPath, err)
	}
	for _, cat := range categories {
		have, limit := got.Totals[cat], want.Totals[cat]
		switch {
		case have > limit:
			t.Errorf("%s grew from %d to %d; files above their committed count: %s", cat, limit, have, grown(got.Files[cat], want.Files[cat]))
		case have < limit:
			t.Errorf("%s fell from %d to %d: run LVP_QUERY_TEXT_SITES_UPDATE=1 go test ./internal/querycorpus -run TestQueryTextSites and commit %s",
				cat, limit, have, strings.TrimPrefix(sitesPath, "../../"))
		}
	}
}

func grown(got, want map[string]int) string {
	var out []string
	for file, n := range got {
		if n > want[file] {
			out = append(out, file+" "+strconv.Itoa(want[file])+"→"+strconv.Itoa(n))
		}
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}
