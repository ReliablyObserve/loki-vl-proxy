package proxy

import (
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// TestLokiTimeParam: a start, end or time is read as Loki reads it: an integer
// is seconds up to ten characters and nanoseconds beyond, a number with a
// decimal point is float seconds rounded to the millisecond, anything else an
// RFC 3339 time, and no value is trimmed. A value that is none of these fails
// with the integer parse's own text. The values and errors are the recorded
// Loki rows (r0535-r0560, r0654-r0694).
// conformance: semantics/error-request-parameter-validation-order
func TestLokiTimeParam(t *testing.T) {
	sec := func(n int64) time.Time { return time.Unix(n, 0) }
	for _, tc := range []struct {
		raw  string
		want time.Time
		err  string
	}{
		{"1794567075", sec(1794567075), ""},
		{"0", sec(0), ""},
		{"-1", sec(-1), ""},
		{"1794567075000000000", time.Unix(0, 1794567075000000000), ""},
		{"32503680000", time.Unix(0, 32503680000), ""}, // eleven digits: nanoseconds (r0552)
		{"-62135596800", time.Unix(0, -62135596800), ""},
		{"1794567075.5", time.Unix(1794567075, 500_000_000), ""},
		{"1794570675.123", time.Unix(1794570675, 123_000_000), ""},
		{"1794570675.1234", time.Unix(1794570675, 123_000_000), ""},
		{"2026-11-13T10:51:15Z", time.Date(2026, 11, 13, 10, 51, 15, 0, time.UTC), ""},
		{"2026-11-13T10:51:15.5Z", time.Date(2026, 11, 13, 10, 51, 15, 500_000_000, time.UTC), ""},
		{"abc", time.Time{}, `strconv.ParseInt: parsing "abc": invalid syntax`},
		{"99999999999999999999", time.Time{}, `strconv.ParseInt: parsing "99999999999999999999": value out of range`},
		{"1e30", time.Time{}, `strconv.ParseInt: parsing "1e30": invalid syntax`},
		{"2026-13-45T00:00:00Z", time.Time{}, `strconv.ParseInt: parsing "2026-13-45T00:00:00Z": invalid syntax`},
		{"now", time.Time{}, `strconv.ParseInt: parsing "now": invalid syntax`},
		{"now-1h", time.Time{}, `strconv.ParseInt: parsing "now-1h": invalid syntax`},
		{" 1", time.Time{}, `strconv.ParseInt: parsing " 1": invalid syntax`},
		{"1 ", time.Time{}, `strconv.ParseInt: parsing "1 ": invalid syntax`},
		{"1.5.5", time.Time{}, `strconv.ParseInt: parsing "1.5.5": invalid syntax`},
	} {
		got, err := lokiTimeParam(tc.raw)
		switch {
		case tc.err != "":
			if err == nil || err.Error() != tc.err {
				t.Errorf("lokiTimeParam(%q) = %v, %v; want the error %q", tc.raw, got, err, tc.err)
			}
		case err != nil:
			t.Errorf("lokiTimeParam(%q) failed: %v", tc.raw, err)
		case !got.Equal(tc.want):
			t.Errorf("lokiTimeParam(%q) = %v, want %v", tc.raw, got.UTC(), tc.want.UTC())
		}
	}
}

// TestPromDuration: since, and a step or interval that is not a number, are
// read as Prometheus reads a duration: whole numbers of y, w, d, h, m, s and ms,
// each once and in that order, or "0". Go's grammar is wider: it reads 1ns,
// 1h1h and 1.5h, which Loki rejects (r0568, r0572, r0562).
// conformance: semantics/error-request-parameter-validation-order
func TestPromDuration(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want time.Duration
		ok   bool
	}{
		{"0", 0, true},
		{"0s", 0, true},
		{"1m", time.Minute, true},
		{"1h", time.Hour, true},
		{"5m", 5 * time.Minute, true},
		{"1h30m", 90 * time.Minute, true},
		{"2d", 48 * time.Hour, true},
		{"1w", 7 * 24 * time.Hour, true},
		{"1y", 365 * 24 * time.Hour, true},
		{"1m30s500ms", 90*time.Second + 500*time.Millisecond, true},
		{"100ms", 100 * time.Millisecond, true},
		{"1ms", time.Millisecond, true},
		{"", 0, false},
		{"abc", 0, false},
		{"1ns", 0, false},
		{"1us", 0, false},
		{"1h1h", 0, false},
		{"30m1h", 0, false},
		{"1.5h", 0, false},
		{"-1h", 0, false},
		{"+1h", 0, false},
		{"1", 0, false},
		{"60", 0, false},
		{"h", 0, false},
		{"5x", 0, false},
		{"1H", 0, false},
		{" 1h", 0, false},
		{"1h ", 0, false},
		{"00", 0, false},
		{"99999999999999999999h", 0, false},
		{"300y", 0, false}, // beyond int64 nanoseconds
	} {
		got, ok := promDuration(tc.raw)
		if ok != tc.ok || got != tc.want {
			t.Errorf("promDuration(%q) = %v, %v; want %v, %v", tc.raw, got, ok, tc.want, tc.ok)
		}
	}
}

// TestLokiStepParam: a step or interval is float seconds or a Prometheus
// duration, with Loki's text for one that is neither and for one whose
// nanoseconds overflow int64 (r0564-r0580).
// conformance: semantics/error-request-parameter-validation-order
func TestLokiStepParam(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want time.Duration
		err  string
	}{
		{"60", time.Minute, ""},
		{"1", time.Second, ""},
		{"0.001", time.Millisecond, ""},
		{"0.1", 100 * time.Millisecond, ""},
		{"1e3", 1000 * time.Second, ""},
		{"1m", time.Minute, ""},
		{"1h30m", 90 * time.Minute, ""},
		{"0", 0, ""},
		{"0s", 0, ""},
		{"-1", -time.Second, ""},
		{"abc", 0, `cannot parse "abc" to a valid duration`},
		{"1ns", 0, `cannot parse "1ns" to a valid duration`},
		{"5x", 0, `cannot parse "5x" to a valid duration`},
		{"1h1h", 0, `cannot parse "1h1h" to a valid duration`},
		{"-1h", 0, `cannot parse "-1h" to a valid duration`},
		{"100000000000", 0, `cannot parse "100000000000" to a valid duration. It overflows int64`},
	} {
		got, err := lokiStepParam(tc.raw)
		switch {
		case tc.err != "":
			if err == nil || err.Text() != tc.err {
				t.Errorf("lokiStepParam(%q) = %v, %v; want the error %q", tc.raw, got, err, tc.err)
			}
		case err != nil:
			t.Errorf("lokiStepParam(%q) failed: %v", tc.raw, err)
		case got != tc.want:
			t.Errorf("lokiStepParam(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}

// paramColumns maps a check to the request parameters a recorded row must send
// for the check to be recorded.
var paramColumns = map[paramCheck][]string{
	ckSince: {"since"}, ckEnd: {"end"}, ckStart: {"start"}, ckTime: {"time"},
	ckLimitPositive: {"limit"}, ckLimitSigned: {"limit"}, ckDirection: {"direction"},
	ckStep: {"step"}, ckInterval: {"interval"}, ckAggregateBy: {"aggregateBy"}, ckDelayFor: {"delay_for"},
	ckEndAfterStart: {"end"}, ckResolution: {"step"}, ckLength: {"start"},
}

// TestEveryParamCheckIsRecorded: a route checks a parameter only where a
// recorded Loki row sends that parameter to that route, with a rejected value
// among them (a check no recorded request exercises would be a guess).
// conformance: semantics/error-request-parameter-validation-order
func TestEveryParamCheckIsRecorded(t *testing.T) {
	rejected := map[string]map[string]bool{} // route -> parameter -> a row with it was rejected
	sent := map[string]map[string]bool{}
	for _, row := range readRequestParamAnswers(t) {
		route := strings.TrimSuffix(row.Route, "_upgrade")
		switch route {
		case "index_stats", "index_volume", "index_volume_range":
			route = strings.TrimPrefix(route, "index_")
			if route == "stats" {
				route = "index_stats"
			}
		}
		raw := row.Form
		if i := strings.IndexByte(row.Target, '?'); i >= 0 {
			raw = row.Target[i+1:]
		}
		values, err := url.ParseQuery(raw)
		if err != nil {
			continue
		}
		for name := range values {
			if sent[route] == nil {
				sent[route], rejected[route] = map[string]bool{}, map[string]bool{}
			}
			sent[route][name] = true
			if row.Loki.Status >= 400 {
				rejected[route][name] = true
			}
		}
	}
	for route, checks := range requestParamChecks {
		for _, c := range checks {
			for _, name := range paramColumns[c] {
				if !sent[route][name] || !rejected[route][name] {
					t.Errorf("%s checks %q (check %d) but no recorded Loki row sends it to that route with a value Loki rejects", route, name, c)
				}
			}
		}
	}
}

// requestParamModel is the rules of requestParamChecks written a second time,
// as which parameters are rejected for a request, independent of the check
// order (the order of a pair no recorded request separates is not Loki's).
type requestParamModel struct {
	since, end, start, limit, direction, step, interval, aggregateBy, delayFor, timeParam bool
	limitLeast                                                                            int
	endBefore, resolution, length                                                         bool
}

var requestParamModels = map[string]requestParamModel{
	"query_range":           {since: true, end: true, start: true, limit: true, limitLeast: 1, direction: true, step: true, interval: true, endBefore: true, resolution: true},
	"query":                 {limit: true, limitLeast: 1, direction: true, timeParam: true},
	"labels":                {since: true, end: true, start: true, length: true},
	"label_values":          {since: true, end: true, start: true, length: true},
	"series":                {since: true, end: true, start: true, length: true},
	"detected_labels":       {end: true, start: true, length: true},
	"index_stats":           {since: true, end: true, start: true, endBefore: true, length: true},
	"volume":                {since: true, end: true, start: true, limit: true, limitLeast: 0, aggregateBy: true, endBefore: true, length: true},
	"volume_range":          {since: true, end: true, start: true, limit: true, limitLeast: 0, aggregateBy: true, step: true, endBefore: true, resolution: true, length: true},
	"patterns":              {since: true, end: true, start: true, step: true, resolution: true},
	"detected_fields":       {since: true, end: true, start: true, limit: true, limitLeast: 1, step: true, endBefore: true, length: true},
	"detected_field_values": {end: true, start: true, limit: true, limitLeast: 1, length: true},
	"tail":                  {limit: true, limitLeast: 1, delayFor: true, end: true, start: true},
}

var (
	modelDuration = regexp.MustCompile(`^(?:(\d+)y)?(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?(?:(\d+)ms)?$`)
	modelInteger  = regexp.MustCompile(`^[+-]?[0-9]+$`)
)

// modelPromDuration: the Prometheus grammar by regular expression, in
// milliseconds as an exact integer; ok is false beyond int64 nanoseconds.
func modelPromDuration(s string) (ms *big.Int, ok bool) {
	if s == "0" {
		return big.NewInt(0), true
	}
	m := modelDuration.FindStringSubmatch(s)
	if s == "" || m == nil {
		return nil, false
	}
	unit := []int64{365 * 86400000, 7 * 86400000, 86400000, 3600000, 60000, 1000, 1}
	total := new(big.Int)
	for i, part := range m[1:] {
		if part == "" {
			continue
		}
		n, _ := new(big.Int).SetString(part, 10)
		total.Add(total, n.Mul(n, big.NewInt(unit[i])))
	}
	limit := new(big.Int).Div(big.NewInt(math.MaxInt64), big.NewInt(int64(time.Millisecond)))
	return total, total.Cmp(limit) <= 0
}

// modelInt64 reads an integer by exact arithmetic.
func modelInt64(s string) (int64, bool) {
	if !modelInteger.MatchString(s) {
		return 0, false
	}
	n, _ := new(big.Int).SetString(strings.TrimPrefix(s, "+"), 10)
	return n.Int64(), n.IsInt64()
}

// modelTime is Loki's time parameter: nanoseconds as an exact integer.
func modelTime(s string) (ns *big.Int, ok bool) {
	if n, isInt := modelInt64(s); isInt {
		if len(s) <= 10 {
			return new(big.Int).Mul(big.NewInt(n), big.NewInt(1e9)), true
		}
		return big.NewInt(n), true
	}
	if modelInteger.MatchString(s) {
		return nil, false // an integer beyond int64
	}
	if strings.Contains(s, ".") {
		if f, err := strconv.ParseFloat(s, 64); err == nil && math.Abs(f) < math.MaxInt64 {
			whole, frac := math.Modf(f)
			frac = math.Round(frac*1000) / 1000
			v := new(big.Int).Mul(big.NewInt(int64(whole)), big.NewInt(1e9))
			return v.Add(v, big.NewInt(int64(frac*1e9))), true
		}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return new(big.Int).Add(new(big.Int).Mul(big.NewInt(t.Unix()), big.NewInt(1e9)), big.NewInt(int64(t.Nanosecond()))), true
	}
	return nil, false
}

// modelStep is a step or interval in nanoseconds (an exact big integer), or
// not ok when it is neither float seconds nor a duration.
func modelStep(s string) (ns *big.Int, ok bool) {
	if ms, isDur := modelPromDuration(s); isDur {
		return new(big.Int).Mul(ms, big.NewInt(1e6)), true
	}
	if _, isDur := modelPromDuration(s); !isDur && modelDuration.MatchString(s) && s != "" {
		return nil, false // a duration beyond int64: neither seconds nor a valid duration
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, false
	}
	if math.IsNaN(f) {
		return big.NewInt(0), true
	}
	v := f * 1e9
	if v >= math.MaxInt64 {
		return nil, false
	}
	if v <= math.MinInt64 {
		return big.NewInt(math.MinInt64), true
	}
	return big.NewInt(int64(v)), true
}

// modelStepFailure is the message of a step that is not readable: Loki's
// overflow text for float seconds beyond int64 nanoseconds, its duration text
// otherwise.
func modelStepFailure(s string) string {
	if f, err := strconv.ParseFloat(s, 64); err == nil && f*1e9 >= math.MaxInt64 {
		return "step-overflow"
	}
	return "step-duration"
}

// rejectedBy is the set of message ids the model says a request is rejected
// with: every check on its own.
func (m requestParamModel) rejectedBy(v map[string]string, now time.Time, maxLen time.Duration) map[string]bool {
	out := map[string]bool{}
	since := big.NewInt(int64(time.Hour))
	if m.since && v["since"] != "" {
		if ms, ok := modelPromDuration(v["since"]); ok {
			since = new(big.Int).Mul(ms, big.NewInt(1e6))
		} else {
			out["param-duration"] = true
		}
	}
	nowNs := big.NewInt(now.UnixNano())
	end, start := nowNs, (*big.Int)(nil)
	endOK, startOK := true, true
	if m.end && v["end"] != "" {
		if end, endOK = modelTime(v["end"]); !endOK {
			out["param-number"] = true
		}
	}
	if m.start {
		if v["start"] != "" {
			if start, startOK = modelTime(v["start"]); !startOK {
				out["param-number"] = true
			}
		} else if endOK {
			base := end
			if base.Cmp(nowNs) > 0 {
				base = nowNs
			}
			start = new(big.Int).Sub(base, since)
		}
	}
	if m.timeParam && v["time"] != "" {
		if _, ok := modelTime(v["time"]); !ok {
			out["number"] = true
		}
	}
	if m.limit && v["limit"] != "" {
		if n, ok := modelInt64(v["limit"]); !ok {
			out["number"] = true
		} else if n < int64(m.limitLeast) {
			out["limit-not-positive"] = true
		}
	}
	if m.direction && v["direction"] != "" && !strings.EqualFold(v["direction"], "forward") && !strings.EqualFold(v["direction"], "backward") {
		out["direction"] = true
	}
	var step *big.Int
	if m.step && v["step"] != "" {
		switch ns, ok := modelStep(v["step"]); {
		case !ok:
			out[modelStepFailure(v["step"])] = true
		case ns.Sign() <= 0:
			out["step-not-positive"] = true
		default:
			step = ns
		}
	}
	if m.interval && v["interval"] != "" {
		switch ns, ok := modelStep(v["interval"]); {
		case !ok:
			out[modelStepFailure(v["interval"])] = true
		case ns.Sign() < 0:
			out["interval-negative"] = true
		}
	}
	if m.aggregateBy && v["aggregateBy"] != "" && v["aggregateBy"] != "series" && v["aggregateBy"] != "labels" {
		out["aggregate-by"] = true
	}
	if m.delayFor && v["delay_for"] != "" {
		if n, ok := modelInt64(v["delay_for"]); !ok {
			out["number"] = true
		} else if n < 0 || n > 5 {
			out["delay-for"] = true
		}
	}
	if !endOK || !startOK || start == nil {
		return out
	}
	// The window as time.Time keeps it: a difference saturates at the largest duration.
	diff := new(big.Int).Sub(end, start)
	if limit := big.NewInt(math.MaxInt64); diff.Cmp(limit) > 0 {
		diff = limit
	}
	if m.endBefore && end.Cmp(start) < 0 {
		out["end-before-start"] = true
	}
	if m.resolution && step != nil && new(big.Int).Quo(diff, step).Cmp(big.NewInt(lokiMaxPointsPerSeries)) > 0 {
		out["step-too-many-points"] = true
	}
	if m.length && maxLen > 0 && end.Cmp(start) > 0 {
		ms := new(big.Int).Quo(new(big.Int).Sub(floorMillis(end), floorMillis(start)), big.NewInt(1))
		if ms.Cmp(big.NewInt(int64(maxLen/time.Millisecond))) > 0 {
			out["limit-query-length"] = true
		}
	}
	return out
}

// floorMillis is a time in nanoseconds in whole milliseconds, rounded down.
func floorMillis(ns *big.Int) *big.Int {
	q, r := new(big.Int).QuoRem(ns, big.NewInt(1e6), new(big.Int))
	if r.Sign() < 0 {
		q.Sub(q, big.NewInt(1))
	}
	return q
}

var (
	fuzzParamProxyOnce sync.Once
	fuzzParamProxy     *Proxy
)

// paramTestProxy is a proxy whose limits the checks read: max_query_length of
// Loki's recorded stack (30d1h), no lookback.
func paramTestProxy(tb testing.TB) *Proxy {
	tb.Helper()
	fuzzParamProxyOnce.Do(func() {
		c := cache.New(time.Minute, 100)
		p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: c, LogLevel: "error", DefaultMaxQueryLength: 721 * time.Hour})
		if err != nil {
			panic(err)
		}
		fuzzParamProxy = p
	})
	return fuzzParamProxy
}

var fuzzParamRoutes = []string{"query_range", "query", "labels", "label_values", "series", "detected_labels", "index_stats", "volume", "volume_range", "patterns", "detected_fields", "detected_field_values", "tail", "format_query"}

// checkParamRequest runs one request through the route's checks and the model
// and reports a difference.
func checkParamRequest(p *Proxy, route string, v map[string]string) error {
	now := time.Unix(1794570675, 0)
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Form = url.Values{}
	for name, value := range v {
		if value != "" {
			r.Form.Set(name, value)
		}
	}
	got := p.requestParamError(requestParamChecks[route], r, now)
	model, known := requestParamModels[route]
	if !known {
		if got != nil {
			return fmt.Errorf("%s has no checks and was rejected: %v", route, got)
		}
		return nil
	}
	want := model.rejectedBy(v, now, 721*time.Hour)
	switch {
	case got == nil && len(want) > 0:
		return fmt.Errorf("%s %q: accepted; the model rejects it with %v", route, v, want)
	case got != nil && len(want) == 0:
		return fmt.Errorf("%s %q: rejected with %q (%s); the model accepts it", route, v, got.Text(), got.Msg().ID())
	case got != nil && !want[got.Msg().ID()]:
		return fmt.Errorf("%s %q: rejected with %s %q; the model's rejections are %v", route, v, got.Msg().ID(), got.Text(), want)
	}
	if got != nil && got.Kind() != lokierr.KindParam && got.Kind() != lokierr.KindLimit {
		return fmt.Errorf("%s: kind %v", route, got.Kind())
	}
	return nil
}

// FuzzRequestParamChecks: for any route and parameter values the route's
// checks accept exactly the requests the model of the recorded rules accepts,
// reject with one of the messages the model gives, finish at once and
// allocate a bounded amount.
// conformance: semantics/error-request-parameter-validation-order
func FuzzRequestParamChecks(f *testing.F) {
	for _, seed := range [][]string{
		{"0", "1794567075", "1794570675", "", "60", "", "100", "", "", "", ""},
		{"0", "abc", "1794570675", "", "60", "", "", "", "", "", ""},
		{"0", "", "", "1h", "60", "", "", "", "", "", ""},
		{"0", "", "", "abc", "", "", "", "", "", "", ""},
		{"0", "1794570675", "1794567075", "", "60", "", "", "", "", "", ""},
		{"0", "0", "1794570675", "", "60", "", "", "", "", "", ""},
		{"0", "1794559675", "1794570675", "", "1", "", "", "", "", "", ""},
		{"0", "1794559674", "1794570675", "", "1", "", "", "", "", "", ""},
		{"0", "1794567075", "1794570675", "", "100000000000", "", "", "", "", "", ""},
		{"0", "1794567075", "1794570675", "", "0.001", "", "", "", "", "", ""},
		{"0", "1794567075", "1794570675", "", "1h1h", "", "", "", "", "", ""},
		{"0", "1794567075", "1794570675", "", "1e3", "", "", "", "", "", ""},
		{"0", "1794567075", "1794570675", "", "60", "-1", "", "", "", "", ""},
		{"0", "1794567075", "1794570675", "", "60", "abc", "", "", "", "", ""},
		{"0", "1794567075", "1794570675", "", "60", "", "0", "", "", "", ""},
		{"0", "1794567075", "1794570675", "", "60", "", "1.5", "", "", "", ""},
		{"0", "1794567075", "1794570675", "", "60", "", "", "bad", "", "", ""},
		{"1", "", "", "", "", "", "abc", "", "", "", "abc"},
		{"1", "", "", "", "", "", "3", "FORWARD", "", "", "2026-11-13T11:51:15Z"},
		{"7", "1794567075", "1794570675", "", "", "", "-1", "", "foo", "", ""},
		{"8", "1794567075", "1794570675", "", "0", "", "0", "", "series", "", ""},
		{"12", "1794567075", "1794570675", "", "", "", "0", "", "", "10", ""},
		{"12", "", "", "", "", "", "", "", "", "-1", ""},
		{"2", "0", "1794570675", "", "", "", "", "", "", "", ""},
		{"3", "1794567075.5", "2026-11-13T11:51:15Z", "", "", "", "", "", "", "", ""},
		{"0", "32503680000", "", "", "", "", "", "", "", "", ""},
		{"0", "", "9999999999", "", "1", "", "", "", "", "", ""},
		{"0", "-9999999999", "9999999999", "", "1", "", "", "", "", "", ""},
		{"0", "1", "1.", "", ".", "", "", "", "", "", ""},
	} {
		if len(seed) != 11 {
			f.Fatalf("seed %q", seed)
		}
		route, _ := strconv.Atoi(seed[0])
		f.Add(uint8(route), seed[1], seed[2], seed[3], seed[4], seed[5], seed[6], seed[7], seed[8], seed[9], seed[10])
	}
	p := paramTestProxy(f)
	f.Fuzz(func(t *testing.T, route uint8, start, end, since, step, interval, limit, direction, aggregateBy, delayFor, timeParam string) {
		if len(start)+len(end)+len(since)+len(step)+len(interval)+len(limit)+len(direction)+len(aggregateBy)+len(delayFor)+len(timeParam) > 4096 {
			t.Skip()
		}
		name := fuzzParamRoutes[int(route)%len(fuzzParamRoutes)]
		v := map[string]string{"start": start, "end": end, "since": since, "step": step, "interval": interval, "limit": limit,
			"direction": direction, "aggregateBy": aggregateBy, "delay_for": delayFor, "time": timeParam}
		began := time.Now()
		if err := checkParamRequest(p, name, v); err != nil {
			t.Fatal(err)
		}
		if took := time.Since(began); took > 2*time.Second {
			t.Fatalf("%s took %v", name, took)
		}
	})
}

// TestRequestParamChecksAllocate: the checks of a valid request, in the forms
// Grafana sends, allocate nothing.
// conformance: semantics/error-request-parameter-validation-order
func TestRequestParamChecksAllocate(t *testing.T) {
	p := paramTestProxy(t)
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Form = url.Values{"query": {`{app="x"}`}, "start": {"1794567075000000000"}, "end": {"1794570675000000000"}, "step": {"15"}, "limit": {"1000"}, "direction": {"backward"}}
	now := time.Unix(1794570675, 0)
	for _, route := range []string{"query_range", "volume_range", "labels", "series", "tail"} {
		checks := requestParamChecks[route]
		if allocs := testing.AllocsPerRun(100, func() {
			if err := p.requestParamError(checks, r, now); err != nil {
				t.Fatalf("%s: %v", route, err)
			}
		}); allocs > 0 {
			t.Errorf("%s: %v allocations for a valid request, want none", route, allocs)
		}
	}
}
