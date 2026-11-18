package proxy

import (
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// A query longer than the short memos keep is validated, respelled and parsed once, and every later read of the
// request takes the same answers from longQueryMemo.
//
// conformance: semantics/nested-calls-read-once
func TestLongQueryMemoReadsOnce(t *testing.T) {
	q := deepLabelReplace(200, "memo-reads-once")
	if !longQuery(q) {
		t.Fatalf("a %d byte query is not long", len(q))
	}
	v := cachedQueryValidation(q)
	if v.msg != "" || !v.sample {
		t.Fatalf("validation = %+v, want a valid metric query", v)
	}
	if again := cachedQueryValidation(q); again != v {
		t.Fatalf("the second validation is %+v, want the first %+v", again, v)
	}
	e, ok := planMemoLoad(q)
	if !ok || e.expr == nil {
		t.Fatal("the validation keeps no parse of the long query")
	}
	if expr, ok := sharedLogQLExpr(q); !ok || expr != e.expr {
		t.Fatal("sharedLogQLExpr parses the long query again")
	}
	if p := resolvedPlan(q); p == nil || resolvedPlan(q) != p {
		t.Fatal("the long query's plan is not built once")
	}
	if e, _ := planMemoLoad(q); e.plan == nil {
		t.Fatal("the long query's plan is not kept")
	}
	_, want := logqlpkg.ParseLogQuery(q)
	for i := 0; i < 2; i++ {
		if _, err := parsedLogQuery(q); err == nil || want == nil || err.Error() != want.Error() {
			t.Fatalf("parsedLogQuery error = %v, want logql.ParseLogQuery's %v", err, want)
		}
	}
	// A long log query no validation read is parsed once, then read from the memo.
	logQuery := `{app="memo-log-query"}` + strings.Repeat(` |= "x"`, 700)
	first, err := parsedLogQuery(logQuery)
	if err != nil || first == nil {
		t.Fatalf("parsedLogQuery(long log query) = %v, %v", first, err)
	}
	if again, _ := parsedLogQuery(logQuery); again != first {
		t.Fatal("a long log query is parsed again")
	}
	text := requestQueryText(q, defaultExecutionLimits().QueryLengthBytes)
	if again := requestQueryText(q, defaultExecutionLimits().QueryLengthBytes); again != text {
		t.Fatal("the request text of a long query changes on a repeat")
	}
	if _, ok := longQueryMemo.load(longRequestTextKey(q)); !ok {
		t.Fatal("the request text of a long query is not kept")
	}
	// A query over Loki's input size limit is rejected before it is parsed, and kept nowhere.
	over := strings.Repeat(" ", logqlpkg.MaxInputSize) + "{app=\"a\"}"
	if longQuery(over) {
		t.Fatal("a query over Loki's input size limit is kept")
	}
}

// The memo of long queries holds at most its limit per generation, two generations: an entry too large for a
// generation is not kept, and a full generation becomes the previous one.
//
// conformance: semantics/nested-calls-read-once
func TestTextMemoBoundedByCost(t *testing.T) {
	m := newTextMemo(100)
	m.store("too-large", 101, 1, false)
	if _, ok := m.load("too-large"); ok {
		t.Fatal("an entry costing more than a generation is kept")
	}
	for i := 0; i < 10; i++ {
		m.store(i, 40, i, false)
		if c := m.cur.Load().cost.Load(); c > 100 {
			t.Fatalf("a generation costs %d, over its limit 100", c)
		}
	}
	if _, ok := m.load(0); ok {
		t.Fatal("an entry of two generations ago is kept")
	}
	if v, ok := m.load(9); !ok || v != 9 {
		t.Fatalf("the last entry is %v, %v", v, ok)
	}
	if v, ok := m.load(7); !ok || v != 7 {
		t.Fatalf("an entry of the previous generation is %v, %v", v, ok)
	}
	if _, ok := m.cur.Load().entries.Load(7); !ok {
		t.Fatal("a hit in the previous generation does not move the entry to the current one")
	}
	// A hit in the previous generation moves the entry to the current one; a replacing store keeps one entry's cost.
	m.store("a", 10, "x", false)
	m.store("a", 30, "y", true)
	if v, _ := m.load("a"); v != "y" {
		t.Fatalf("the replaced entry is %v", v)
	}
	before := m.cur.Load().cost.Load()
	m.store("a", 30, "z", true)
	if after := m.cur.Load().cost.Load(); after != before {
		t.Fatalf("replacing an entry changed the generation's cost from %d to %d", before, after)
	}
}
