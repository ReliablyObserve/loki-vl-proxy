package lower

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// vectorBinaryGrid is a binary operation with a vector(n) operand on either side or both, next to a range or vector
// aggregation or a number, under every operator and vector matching the lowering writes, and the spellings of the
// operator that stay with the translator (bool, the set operators).
func vectorBinaryGrid() []string {
	var qs []string
	rate, sum := `rate({app="a"}[5m])`, `sum by (app) (count_over_time({app="b"}[5m]))`
	matchings := []string{``, ` on()`, ` on(app)`, ` ignoring(pod)`, ` on(app) group_left(zone)`, ` ignoring(pod) group_right()`}
	for _, v := range []string{`vector(0)`, `vector(1)`, `vector(1.5)`, `vector(1e3)`, `vector(100)`} {
		for _, op := range []string{"+", "-", "*", "/", "%", "^", "==", "!=", ">", "<", ">=", "<=", "> bool", "and", "or", "unless"} {
			for _, m := range matchings {
				qs = append(qs, rate+` `+op+m+` `+v, v+` `+op+m+` `+sum, v+` `+op+m+` vector(2)`, `2 `+op+m+` `+v, v+` `+op+m+` 0.5`)
			}
		}
	}
	return qs
}

// TestVectorOperandLoweringMatchesLoki holds what the lowering writes for a binary operation with a vector(n) operand
// to Loki's own syntax tree: the operand is a vector expression in Loki's tree (a VectorOp), not a number (a
// ScalarOp), and the operator, the bool modifier and the vector matching around it are Loki's, on every grid row the
// lowering writes natively. Loki's tree does not report the number of a vector(n); the number is held by the
// lowering's own tables and the proxy's pin of the route. It also holds the proxy's validation to Loki's on the
// numbers a vector(n) takes: Loki accepts a number, an exponent, a leading dot, a blank before the parenthesis and
// underscores, and rejects a sign, a hexadecimal integer, Inf and NaN, as the proxy does. LVP_RUN_ORACLES=1 runs it
// (it builds the oracle).
// conformance: architecture-syntax-package-layering
func TestVectorOperandLoweringMatchesLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 is not set")
	}
	work := t.TempDir()
	bin, err := oraclerun.Build(filepath.Join("..", "..", "..", "test", "oracle", "loki"), "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		q  string
		op physical.Op
	}
	var rows []row
	var queries []string
	prof := Profile{Caps: logsql.CapabilitiesFor("v1.53.0"), Label: func(s string) string { return s }}
	for _, q := range vectorBinaryGrid() {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			t.Fatal(err)
		}
		queryplan.Resolve(p)
		op, err := PlanQuery(q, p, prof)
		if err != nil {
			op = nil // delegated: Loki's reading is recorded, not compared
		}
		rows = append(rows, row{q, op})
		queries = append(queries, q)
	}
	// The numbers a vector(n) takes: the proxy accepts what Loki accepts.
	numbers := []string{`1`, `0`, `.5`, `1e3`, `2.50`, `1_0`, `-1`, `+1`, `0x10`, `Inf`, `NaN`, `1e400`, `a`, ``, `1 2`}
	for _, n := range numbers {
		queries = append(queries, `rate({app="a"}[5m]) + vector(`+n+`)`)
	}
	queries = append(queries, `rate({app="a"}[5m]) + vector (1)`, `rate({app="a"}[5m]) + vector( 1 )`, `-vector(1)`)
	in := filepath.Join(work, "facts-in.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	for i, q := range queries {
		b, _ := json.Marshal(map[string]string{"id": strconv.Itoa(i), "q": q})
		w.Write(append(b, '\n'))
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	out := filepath.Join(work, "facts.jsonl")
	if err := oraclerun.Run(bin, "facts", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	compared, delegated, accepted, rejected, setRejected := 0, 0, 0, 0, 0
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var lf struct {
			ID     string `json:"id"`
			Accept bool   `json:"accept"`
			Error  string `json:"error"`
			Binary *struct {
				Op         string   `json:"op"`
				ReturnBool bool     `json:"return_bool"`
				On         bool     `json:"on"`
				Labels     []string `json:"labels"`
				Card       string   `json:"card"`
				Include    []string `json:"include"`
				Left       string   `json:"left"`
				Right      string   `json:"right"`
				LeftValue  float64  `json:"left_value"`
				RightValue float64  `json:"right_value"`
			} `json:"binary"`
		}
		if err := json.Unmarshal([]byte(line), &lf); err != nil {
			t.Fatal(err)
		}
		i, _ := strconv.Atoi(lf.ID)
		q := queries[i]
		if i >= len(rows) {
			// The proxy's validation reads the number as Loki's parser does.
			_, msg := logql.ValidateLogQLExpr(q)
			if lf.Accept != (msg == "") {
				t.Errorf("%s: Loki accepts %v (%s), the proxy's validation says %q", q, lf.Accept, lf.Error, msg)
			}
			if lf.Accept {
				accepted++
			} else {
				rejected++
			}
			continue
		}
		if !lf.Accept {
			// Loki rejects a number next to a set operator; so does the proxy's validation, and the lowering
			// never sees the query.
			if _, msg := logql.ValidateLogQLExpr(q); msg == "" || !strings.Contains(lf.Error, "unexpected literal for") || rows[i].op != nil {
				t.Errorf("%s: Loki rejects it (%s), the proxy's validation says %q, lowered %v", q, lf.Error, msg, rows[i].op != nil)
			}
			setRejected++
			continue
		}
		if lf.Binary == nil {
			t.Errorf("%s: Loki reads no binary operation", q)
			continue
		}
		bin, ok := rows[i].op.(physical.Binary)
		if !ok {
			delegated++
			continue
		}
		compared++
		lb := lf.Binary
		if bin.Op != lb.Op || bin.Bool || lb.ReturnBool {
			t.Errorf("%s: operator %q bool %v, Loki's %q bool %v", q, bin.Op, bin.Bool, lb.Op, lb.ReturnBool)
		}
		m := bin.Match
		if m == nil {
			m = &physical.Matching{}
		}
		labels := m.Ignoring
		if m.MatchOn {
			labels = m.On
		}
		card, include := "one-to-one", []string(nil)
		switch m.GroupSide {
		case "group_left":
			card, include = "many-to-one", m.GroupLeft
		case "group_right":
			card, include = "one-to-many", m.GroupRight
		}
		if m.MatchOn != lb.On || !slices.Equal(labels, lb.Labels) || card != lb.Card || !slices.Equal(include, lb.Include) {
			t.Errorf("%s: matching %+v, Loki's on %v labels %v card %s include %v", q, *m, lb.On, lb.Labels, lb.Card, lb.Include)
		}
		for side, o := range [2]physical.Op{bin.L, bin.R} {
			kind, value := lb.Left, lb.LeftValue
			if side == 1 {
				kind, value = lb.Right, lb.RightValue
			}
			want := ""
			switch v := o.(type) {
			case physical.VectorOp:
				want = "*syntax.VectorExpr"
			case physical.ScalarOp:
				want = "*syntax.LiteralExpr"
				if v.V != value {
					t.Errorf("%s: operand %d is the number %v, Loki's %v", q, side, v.V, value)
				}
			case physical.VLQuery, physical.Fold:
				if kind != "*syntax.RangeAggregationExpr" && kind != "*syntax.VectorAggregationExpr" {
					t.Errorf("%s: operand %d is a query, Loki's %s", q, side, kind)
				}
				continue
			default:
				t.Errorf("%s: operand %d is %T", q, side, o)
			}
			if kind != want {
				t.Errorf("%s: operand %d is %T, Loki's %s", q, side, o, kind)
			}
		}
	}
	t.Logf("%d lowered rows compared with Loki's tree, %d delegated rows (bool, set operators, constants only) read by Loki, %d set operations with a number both reject; number spellings: Loki and the proxy accept %d, reject %d", compared, delegated, setRejected, accepted, rejected)
	if compared < 700 || delegated < 1000 || setRejected == 0 || accepted == 0 || rejected == 0 {
		t.Fatalf("compared %d, delegated %d, set operations rejected %d, accepted %d, rejected %d", compared, delegated, setRejected, accepted, rejected)
	}
}
