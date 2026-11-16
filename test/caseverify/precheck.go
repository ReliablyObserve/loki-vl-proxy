package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The data precheck: before two answers are compared, Loki and VictoriaLogs must hold the same, non-empty data
// for the case's selectors over the case's window. Each selector's lines are counted on both sides, Loki with
// sum(count_over_time(<selector>[<window>])) at the window's end and VictoriaLogs with the same matchers as
// field filters and `stats count()` over the same window. A difference of the answers on unequal data is a
// difference of the data, never of the proxy.

// stackWide is the selector of a request that names none (the label endpoints without a query): every stream.
const stackWide = ""

// CountKey names one count: a selector (LogQL text, or stackWide) over a window in Unix nanoseconds, for a tenant.
type CountKey struct {
	Selector string `json:"selector"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
	// Tenant is the case's own X-Scope-OrgID when it is not the run's (-tenant); "" counts the run's tenant, so a
	// recording made before tenants were counted keeps matching.
	Tenant string `json:"tenant,omitempty"`
}

// countableTenant reports whether VictoriaLogs can count a tenant's lines: the proxy routes a canonical decimal
// X-Scope-OrgID (no sign, no leading zero, up to 4294967295) to the VictoriaLogs account of that number
// (-tenant.numeric-passthrough, which the stack runs), so the count is asked of that account. Any other tenant (a
// name, "007", a multi-tenant "a|b" list) is refused or needs a tenant map this check does not read.
func countableTenant(tenant string) bool {
	if tenant == "" || (len(tenant) > 1 && tenant[0] == '0') {
		return false
	}
	_, err := strconv.ParseUint(tenant, 10, 32)
	return err == nil && !strings.HasPrefix(tenant, "+")
}

// CountAnswer is one side's count. Empty is set when Loki's count query answered no series at all (its
// fresh-stack blank window answers a metric query that way while it holds the lines).
type CountAnswer struct {
	Count float64 `json:"count"`
	Empty bool    `json:"empty,omitempty"`
	Err   string  `json:"error,omitempty"`
}

// Counter counts the lines of a selector over a window on a side ("loki" or "vl").
type Counter interface {
	Count(side string, key CountKey) (CountAnswer, bool)
}

var rangeLiteral = regexp.MustCompile(`\[(\d+(?:\.\d+)?(?:ns|us|µs|ms|s|m|h|d|w|y))(?::[^\]]*)?\]`)

// Selectors are the stream selectors of a LogQL query, each written once, sorted. A query that does not parse
// gives none (the check then counts the whole stack).
func Selectors(query string) []string {
	expr, err := logql.Parse(query)
	if err != nil || expr == nil {
		return nil
	}
	seen := map[string]bool{}
	collectSelectors(reflect.ValueOf(expr), seen, 0)
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

var selectorType = reflect.TypeOf(&logql.StreamSelector{})

// collectSelectors walks the typed tree for every *logql.StreamSelector; depth bounds a cyclic value.
func collectSelectors(v reflect.Value, seen map[string]bool, depth int) {
	if depth > 64 || !v.IsValid() {
		return
	}
	if v.Type() == selectorType {
		if !v.IsNil() {
			seen[v.Interface().(*logql.StreamSelector).String()] = true
		}
		return
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			collectSelectors(v.Elem(), seen, depth+1)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				collectSelectors(v.Field(i), seen, depth+1)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			collectSelectors(v.Index(i), seen, depth+1)
		}
	}
}

// Lookback is the longest range a query reads before its evaluation time ([5m], [1h:1m]); 5 minutes, Loki's
// lookback of a log query, when it names none.
func Lookback(query string) time.Duration {
	longest := 5 * time.Minute
	for _, m := range rangeLiteral.FindAllStringSubmatch(query, -1) {
		if d, ok := logql.ParseUnitDuration(m[1]); ok && d > longest {
			longest = d
		}
	}
	return longest
}

// lokiCountQuery is Loki's count of a selector's lines over the window ending at the evaluation time.
func lokiCountQuery(selector string, window time.Duration) string {
	if selector == stackWide {
		selector = `{service_name=~".+"}`
	}
	seconds := int64(window / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	return fmt.Sprintf("sum(count_over_time(%s[%ds]))", selector, seconds)
}

// vlFilter is the selector's matchers as VictoriaLogs field filters (a field filter matches whether or not the
// field is a stream field). Loki anchors a matcher's regexp; VictoriaLogs does not, so it is anchored here.
func vlFilter(selector string) (string, error) {
	if selector == stackWide {
		return "*", nil
	}
	expr, err := logql.Parse(selector)
	if err != nil {
		return "", err
	}
	var sel *logql.StreamSelector
	switch x := expr.(type) {
	case *logql.StreamSelector:
		sel = x
	case *logql.LogQuery:
		sel = x.Selector
	}
	if sel == nil || len(sel.Matchers) == 0 {
		return "", fmt.Errorf("%q is not a stream selector", selector)
	}
	parts := make([]string, 0, len(sel.Matchers))
	for _, m := range sel.Matchers {
		// LogQL label names are identifiers, which LogsQL reads bare as field names.
		name := m.Name
		switch m.Op {
		case logql.MatchEq:
			parts = append(parts, name+":="+strconv.Quote(m.Value))
		case logql.MatchNeq:
			parts = append(parts, "!"+name+":="+strconv.Quote(m.Value))
		case logql.MatchRe:
			parts = append(parts, name+":~"+strconv.Quote("^(?:"+m.Value+")$"))
		case logql.MatchNotRe:
			parts = append(parts, "!"+name+":~"+strconv.Quote("^(?:"+m.Value+")$"))
		}
	}
	return strings.Join(parts, " "), nil
}

// LiveCounter counts on a running Loki and VictoriaLogs and records every count.
type LiveCounter struct {
	Loki, VL string
	Tenant   string
	Client   *http.Client
	rec      *Recorder
	cache    map[CountKey]map[string]CountAnswer
}

// Count asks the side once per key.
func (c *LiveCounter) Count(side string, key CountKey) (CountAnswer, bool) {
	if c.cache == nil {
		c.cache = map[CountKey]map[string]CountAnswer{}
	}
	if a, ok := c.cache[key][side]; ok {
		return a, true
	}
	var a CountAnswer
	switch side {
	case "loki":
		if c.Loki == "" {
			return a, false
		}
		a = c.lokiCount(key)
	default:
		if c.VL == "" {
			return a, false
		}
		a = c.vlCount(key)
	}
	if c.cache[key] == nil {
		c.cache[key] = map[string]CountAnswer{}
	}
	c.cache[key][side] = a
	if c.rec != nil {
		c.rec.addCount(side, key, a)
	}
	return a, true
}

func (c *LiveCounter) get(target string, headers map[string]string) ([]byte, int, error) {
	r, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return nil, 0, err
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	resp, err := c.Client.Do(r)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return raw, resp.StatusCode, err
}

// tenantOf is the tenant a count is asked for: the key's own, else the run's.
func (c *LiveCounter) tenantOf(key CountKey) string {
	if key.Tenant != "" {
		return key.Tenant
	}
	return c.Tenant
}

func (c *LiveCounter) lokiCount(key CountKey) CountAnswer {
	q := url.Values{}
	q.Set("query", lokiCountQuery(key.Selector, time.Duration(key.End-key.Start)))
	q.Set("time", strconv.FormatInt(key.End, 10))
	raw, status, err := c.get(strings.TrimRight(c.Loki, "/")+"/loki/api/v1/query?"+q.Encode(), map[string]string{"X-Scope-OrgID": c.tenantOf(key)})
	if err != nil {
		return CountAnswer{Err: err.Error()}
	}
	if status != http.StatusOK {
		return CountAnswer{Err: fmt.Sprintf("Loki answered %d: %s", status, oneLine(string(raw), 200))}
	}
	return parseLokiCount(raw)
}

// parseLokiCount reads the vector of a sum(count_over_time(...)) query: no series is Empty, one is its value.
func parseLokiCount(raw []byte) CountAnswer {
	var doc struct {
		Data struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return CountAnswer{Err: "Loki's count is not JSON"}
	}
	if len(doc.Data.Result) == 0 {
		return CountAnswer{Empty: true}
	}
	total := 0.0
	for _, r := range doc.Data.Result {
		if len(r.Value) != 2 {
			return CountAnswer{Err: "Loki's count has no value"}
		}
		s, _ := r.Value[1].(string)
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return CountAnswer{Err: "Loki's count is not a number"}
		}
		total += f
	}
	return CountAnswer{Count: total}
}

func (c *LiveCounter) vlCount(key CountKey) CountAnswer {
	filter, err := vlFilter(key.Selector)
	if err != nil {
		return CountAnswer{Err: err.Error()}
	}
	q := url.Values{}
	q.Set("query", filter+" | stats count() as c")
	q.Set("start", time.Unix(0, key.Start).UTC().Format(time.RFC3339Nano))
	q.Set("end", time.Unix(0, key.End).UTC().Format(time.RFC3339Nano))
	// VictoriaLogs' default account is 0; another tenant's count is asked of its own account.
	var headers map[string]string
	if t := c.tenantOf(key); t != "" && countableTenant(t) {
		headers = map[string]string{"AccountID": t}
	}
	raw, status, err := c.get(strings.TrimRight(c.VL, "/")+"/select/logsql/query?"+q.Encode(), headers)
	if err != nil {
		return CountAnswer{Err: err.Error()}
	}
	if status != http.StatusOK {
		return CountAnswer{Err: fmt.Sprintf("VictoriaLogs answered %d: %s", status, oneLine(string(raw), 200))}
	}
	return parseVLCount(raw)
}

// parseVLCount reads the rows of `stats count() as c`: one JSON object per line, the count a string.
func parseVLCount(raw []byte) CountAnswer {
	total := 0.0
	sc := bufio.NewScanner(strings.NewReader(string(raw)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var row map[string]string
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return CountAnswer{Err: "VictoriaLogs' count is not JSON"}
		}
		f, err := strconv.ParseFloat(row["c"], 64)
		if err != nil {
			return CountAnswer{Err: "VictoriaLogs' count is not a number"}
		}
		total += f
	}
	return CountAnswer{Count: total}
}
