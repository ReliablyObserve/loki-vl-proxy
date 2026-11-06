//go:build corpuscapture

package querycorpus

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// Entry is one recorded query.
type Entry = entry

type entry struct {
	Kind   Kind   `json:"kind"`
	Query  string `json:"q"`
	Source string `json:"src"`
	// Via is the function that called the parser or translator.
	Via string `json:"via"`
	// Direct is true when Via is test code, so the test passed the query
	// in. False means the proxy built or parsed the query again itself.
	Direct bool `json:"direct"`
	// Path is the VictoriaLogs endpoint a VictoriaLogs record was sent to.
	Path string `json:"path,omitempty"`
}

var (
	mu   sync.Mutex
	seen = map[entry]struct{}{}
	out  *os.File
	dir  = os.Getenv("LVP_QUERY_CORPUS_DIR")
	// stacks holds the resolved caller of every distinct call stack seen.
	// Table-driven tests send many queries from one stack, so each stack is
	// resolved to function names once.
	stacks = map[string]caller{}
)

type caller struct {
	src, via string
	direct   bool
}

// Record appends the query to $LVP_QUERY_CORPUS_DIR/<pid>.jsonl, once per
// kind, query and calling test function. It does nothing when the variable
// is unset.
func Record(kind Kind, query string) {
	record(kind, query, "", 3)
}

// record writes one entry; skip is runtime.Callers' skip that makes the first
// frame the function that called into this package.
func record(kind Kind, query, path string, skip int) {
	if dir == "" {
		return
	}
	var pcs [128]uintptr
	n := runtime.Callers(skip, pcs[:])
	key := stackKey(pcs[:n])
	mu.Lock()
	defer mu.Unlock()
	c, ok := stacks[key]
	if !ok {
		c.src, c.via, c.direct = callers(pcs[:n])
		stacks[key] = c
	}
	e := entry{Kind: kind, Query: query, Source: c.src, Via: c.via, Direct: c.direct || kind == Client, Path: path}
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

// stackKey is a map key for a call stack.
func stackKey(pcs []uintptr) string {
	b := make([]byte, 0, len(pcs)*8)
	for _, pc := range pcs {
		b = binary.LittleEndian.AppendUint64(b, uint64(pc))
	}
	return string(b)
}

// callers returns, for the call stack pcs of Record's caller, the calling test as "path/x_test.go:TestName" ("" when no
// test file called in), the short name of the function that called into the
// parser or translator package from outside it, and whether that function
// is test code. A query an unexported function of the entry package passed
// in (a sub-query the translator or parser built itself) is never direct.
func callers(pcs []uintptr) (src, via string, direct bool) {
	frames := runtime.CallersFrames(pcs)
	entry, internalCall := "", false
	for {
		f, more := frames.Next()
		name := f.Function[strings.LastIndex(f.Function, "/")+1:]
		pkg := name[:strings.Index(name, ".")+1]
		switch {
		case entry == "":
			entry = pkg
		case via == "" && pkg == entry && !exported(name[len(pkg):]) && !strings.HasSuffix(f.File, "_test.go"):
			internalCall, via = true, name
		case via == "" && pkg != entry && pkg != "http.":
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
				return src, via, direct && !internalCall
			}
		}
		if !more {
			return src, via, direct && !internalCall
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
		h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), routedKey{}, true)))
	})
}

type routedKey struct{}

// RecordRequest records the query a handler read from its request as a
// client query, for tests that call the handler directly. A request that
// came through Handler was recorded there, before any middleware could
// rewrite its query, so it is skipped.
func RecordRequest(r *http.Request, query string) {
	if query == "" || r.Context().Value(routedKey{}) != nil {
		return
	}
	Record(Client, query)
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

// exported reports whether a function name without its package, such as
// "Parse", "(*Proxy).Handle" or "parse.func1", names an exported function.
func exported(fn string) bool {
	if strings.HasPrefix(fn, "(") {
		if i := strings.Index(fn, ")."); i >= 0 {
			fn = fn[i+2:]
		}
	}
	return fn != "" && fn[0] >= 'A' && fn[0] <= 'Z'
}

// CaptureForTest runs fn with recording redirected to a temporary directory
// and returns what fn recorded. Test helper for the corpuscapture build.
func CaptureForTest(t interface {
	TempDir() string
	Cleanup(func())
	Fatal(...any)
}, fn func()) []Entry {
	d := t.TempDir()
	mu.Lock()
	oldDir, oldOut, oldSeen := dir, out, seen
	dir, out, seen = d, nil, map[entry]struct{}{}
	mu.Unlock()
	defer func() {
		mu.Lock()
		if out != nil {
			out.Close()
		}
		dir, out, seen = oldDir, oldOut, oldSeen
		mu.Unlock()
	}()
	fn()
	mu.Lock()
	if out != nil {
		_ = out.Sync()
	}
	mu.Unlock()
	files, _ := filepath.Glob(filepath.Join(d, "*.jsonl"))
	var got []Entry
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if line == "" {
				continue
			}
			var e Entry
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				t.Fatal(err)
			}
			got = append(got, e)
		}
	}
	return got
}

// RecordBackend records query as sent to the VictoriaLogs endpoint path, when
// path is a /select/logsql/ endpoint. The proxy calls it where it builds each
// backend request, with the parameters it is about to send.
func RecordBackend(path, query string) {
	if query != "" && strings.HasPrefix(path, "/select/logsql/") {
		record(VictoriaLogs, query, path, 3)
	}
}
