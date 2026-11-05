//go:build corpuscapture

package querycorpus

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

type entry struct {
	Kind   Kind   `json:"kind"`
	Query  string `json:"q"`
	Source string `json:"src"`
	// Via is the function that called the parser or translator.
	Via string `json:"via"`
	// Direct is true when Via is test code, so the test passed the query
	// in. False means the proxy built or parsed the query again itself.
	Direct bool `json:"direct"`
}

var (
	mu   sync.Mutex
	seen = map[entry]struct{}{}
	out  *os.File
	dir  = os.Getenv("LVP_QUERY_CORPUS_DIR")
)

// Record appends the query to $LVP_QUERY_CORPUS_DIR/<pid>.jsonl, once per
// kind, query and calling test function. It does nothing when the variable
// is unset.
func Record(kind Kind, query string) {
	if dir == "" {
		return
	}
	src, via, direct := callers()
	e := entry{Kind: kind, Query: query, Source: src, Via: via, Direct: direct || kind == Client}
	mu.Lock()
	defer mu.Unlock()
	if _, ok := seen[e]; ok {
		return
	}
	seen[e] = struct{}{}
	if out == nil {
		f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d.jsonl", os.Getpid())), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return
		}
		out = f
	}
	line, _ := json.Marshal(e)
	_, _ = out.Write(append(line, '\n'))
}

// callers returns the calling test as "path/x_test.go:TestName" ("" when no
// test file called in), the short name of the function that called into the
// parser or translator package from outside it, and whether that function
// is test code.
func callers() (src, via string, direct bool) {
	pcs := make([]uintptr, 128)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	entry := ""
	for {
		f, more := frames.Next()
		name := f.Function[strings.LastIndex(f.Function, "/")+1:]
		pkg := name[:strings.Index(name, ".")+1]
		switch {
		case entry == "":
			entry = pkg
		case via == "" && pkg != entry:
			via = name
			direct = strings.HasSuffix(f.File, "_test.go")
		}
		if strings.HasSuffix(f.File, "_test.go") {
			if via == "" {
				via, direct = name, true
			}
			fn := name[strings.Index(name, ".")+1:]
			if i := strings.Index(fn, ".func"); i >= 0 {
				fn = fn[:i]
			}
			src = testFile(f.File) + ":" + fn
			// Prefer the test function itself over shared test helpers.
			if strings.HasPrefix(fn, "Test") || strings.HasPrefix(fn, "Fuzz") || strings.HasPrefix(fn, "Benchmark") {
				return src, via, direct
			}
		}
		if !more {
			return src, via, direct
		}
	}
}

// Handler records the query parameter of every request as a client query,
// before any middleware rewrites it.
func Handler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if q := r.URL.Query().Get("query"); q != "" {
			Record(Client, q)
		}
		h.ServeHTTP(w, r)
	})
}

// testFile trims a test file path to its repository-relative form.
func testFile(file string) string {
	for _, dir := range []string{"/internal/", "/test/", "/cmd/"} {
		if i := strings.LastIndex(file, dir); i >= 0 {
			return file[i+1:]
		}
	}
	return file
}
