package proxy

import (
	"errors"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// lokiParseTimestamp is Loki's parseTimestamp (pkg/loghttp/params.go:160-182),
// verbatim: a decimal is seconds with its fraction rounded to milliseconds, an
// integer of at most 10 characters is seconds and a longer one nanoseconds,
// anything else RFC3339Nano.
func lokiParseTimestamp(value string, def time.Time) (time.Time, error) {
	if value == "" {
		return def, nil
	}
	if strings.Contains(value, ".") {
		if t, err := strconv.ParseFloat(value, 64); err == nil {
			s, ns := math.Modf(t)
			ns = math.Round(ns*1000) / 1000
			return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
		}
	}
	nanos, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		if ts, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return ts, nil
		}
		return time.Time{}, err
	}
	if len(value) <= 10 {
		return time.Unix(nanos, 0), nil
	}
	return time.Unix(0, nanos), nil
}

// lokiModelParseDuration is Prometheus' model.ParseDuration
// (vendor/github.com/prometheus/common/model/time.go:189-259 in Loki v3.7.9),
// which Loki uses for a step or since that is not a number.
func lokiModelParseDuration(s string) (time.Duration, error) {
	units := map[string]struct {
		pos  int
		mult uint64
	}{
		"ms": {7, uint64(time.Millisecond)}, "s": {6, uint64(time.Second)}, "m": {5, uint64(time.Minute)},
		"h": {4, uint64(time.Hour)}, "d": {3, uint64(24 * time.Hour)}, "w": {2, uint64(7 * 24 * time.Hour)},
		"y": {1, uint64(365 * 24 * time.Hour)},
	}
	switch s {
	case "0":
		return 0, nil
	case "":
		return 0, errors.New("empty duration string")
	}
	isdigit := func(c byte) bool { return c >= '0' && c <= '9' }
	var dur uint64
	lastUnitPos := 0
	for s != "" {
		if !isdigit(s[0]) {
			return 0, errors.New("not a valid duration string")
		}
		i := 0
		for ; i < len(s) && isdigit(s[i]); i++ {
		}
		v, err := strconv.ParseUint(s[:i], 10, 0)
		if err != nil {
			return 0, err
		}
		s = s[i:]
		for i = 0; i < len(s) && !isdigit(s[i]); i++ {
		}
		if i == 0 {
			return 0, errors.New("not a valid duration string")
		}
		unit, ok := units[s[:i]]
		s = s[i:]
		if !ok || unit.pos <= lastUnitPos {
			return 0, errors.New("not a valid duration string")
		}
		lastUnitPos = unit.pos
		if v > 1<<63/unit.mult {
			return 0, errors.New("duration out of range")
		}
		dur += v * unit.mult
		if dur > 1<<63-1 {
			return 0, errors.New("duration out of range")
		}
	}
	return time.Duration(dur), nil
}

// lokiParseSecondsOrDuration is Loki's parseSecondsOrDuration
// (pkg/loghttp/params.go:198-210), verbatim.
func lokiParseSecondsOrDuration(value string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(value, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, errors.New("overflows int64")
		}
		return time.Duration(ts), nil
	}
	if d, err := lokiModelParseDuration(value); err == nil {
		return d, nil
	}
	return 0, errors.New("cannot parse duration")
}

// lokiDecodeAndStepAlign is what a Loki query frontend with
// align_queries_with_step does to the bounds of a metric range request:
// decode them (pkg/loghttp/query.go:473-512, ParseRangeQuery: bounds, end not
// before start, step, step > 0, at most 11,000 points), keep the step in
// milliseconds (pkg/querier/queryrange/codec.go:436), then move both bounds
// down (pkg/querier/queryrange/queryrangebase/step_align.go:20-24). accepted
// is false where Loki answers 400 at decode; stepMs is 0 where the step is
// under a millisecond, which Loki's middleware would divide by.
func lokiDecodeAndStepAlign(form url.Values, now time.Time) (start, end int64, stepMs int64, accepted bool) {
	since := time.Hour
	if raw := form.Get("since"); raw != "" {
		d, err := lokiModelParseDuration(raw)
		if err != nil {
			return 0, 0, 0, false
		}
		since = d
	}
	e, err := lokiParseTimestamp(form.Get("end"), now)
	if err != nil {
		return 0, 0, 0, false
	}
	endOrNow := e
	if e.After(now) {
		endOrNow = now
	}
	s, err := lokiParseTimestamp(form.Get("start"), endOrNow.Add(-since))
	if err != nil || e.Before(s) {
		return 0, 0, 0, false
	}
	if form.Get("step") == "" {
		return 0, 0, 0, false // the proxy does not evaluate a step-less request on Loki's derived step
	}
	step, err := lokiParseSecondsOrDuration(form.Get("step"))
	if err != nil || step <= 0 || e.Sub(s)/step > 11000 {
		return 0, 0, 0, false
	}
	stepMs = step.Milliseconds()
	if stepMs == 0 {
		return 0, 0, 0, true
	}
	start = (s.UnixMilli() / stepMs) * stepMs * int64(time.Millisecond)
	end = (e.UnixMilli() / stepMs) * stepMs * int64(time.Millisecond)
	return start, end, stepMs, true
}

// alignDiffBound renders a Unix nanosecond time in one of the forms both Loki
// and the proxy read the same way: seconds as an integer of at most 10
// characters, nanoseconds as a 19-digit integer, decimal seconds, or RFC3339
// in UTC or with a zone offset.
func alignDiffBound(rng *rand.Rand, ns int64) string {
	t := time.Unix(0, ns)
	switch rng.Intn(5) {
	case 0:
		return strconv.FormatInt(ns/int64(time.Second), 10)
	case 1:
		if ns >= 1e18 {
			return strconv.FormatInt(ns, 10)
		}
		return strconv.FormatInt(ns/int64(time.Second), 10)
	case 2:
		digits := rng.Intn(10)
		return strconv.FormatFloat(float64(ns)/1e9, 'f', digits, 64)
	case 3:
		return t.UTC().Format(time.RFC3339Nano)
	default:
		zone := time.FixedZone("", (rng.Intn(27)-12)*3600+rng.Intn(2)*1800)
		return t.In(zone).Format(time.RFC3339Nano)
	}
}

// alignDiffStep renders a step in milliseconds as integer seconds, decimal
// seconds or a Prometheus duration.
func alignDiffStep(rng *rand.Rand, ms int64) string {
	switch {
	case ms%1000 == 0 && rng.Intn(3) == 0:
		return strconv.FormatInt(ms/1000, 10)
	case rng.Intn(2) == 0:
		return strconv.FormatFloat(float64(ms)/1000, 'f', -1, 64)
	case ms%60000 == 0 && rng.Intn(2) == 0:
		return strconv.FormatInt(ms/60000, 10) + "m"
	default:
		return strconv.FormatInt(ms, 10) + "ms"
	}
}

// The proxy's alignment, through the proxy's own parsing of the bounds and
// the step, agrees with Loki's decode-then-align over random requests: the
// same requests are moved, and to the same nanosecond. Bounds come in every
// form both read alike (integer seconds, nanoseconds, decimal seconds with up
// to 9 digits, RFC3339 with offsets), before and after the epoch; steps from
// sub-millisecond to longer than the range; ranges from equal bounds to past
// 11,000 points.
// conformance: semantics/query-range-step-alignment, loki_api_v1_query_range
func TestAlignQueryRangeToStep_DifferentialAgainstLokiDecode(t *testing.T) {
	iterations := 20000
	if testing.Short() {
		iterations = 4000
	}
	steps := []int64{1, 3, 100, 250, 500, 999, 1000, 1500, 7000, 15000, 60000, 137000, 300000, 3600000, 86400000}
	now := time.Unix(1700003600, 123456789)
	rng := rand.New(rand.NewSource(20261028))
	moved, kept := 0, 0
	for i := 0; i < iterations; i++ {
		var startNs int64
		switch rng.Intn(4) {
		case 0:
			startNs = -int64(rng.Intn(100000)) * int64(time.Millisecond) * 7
		default:
			startNs = 1700000000*int64(time.Second) + rng.Int63n(int64(48*time.Hour))
		}
		stepMs := steps[rng.Intn(len(steps))]
		var span int64
		switch rng.Intn(4) {
		case 0:
			span = rng.Int63n(stepMs * int64(time.Millisecond)) // less than a step
		case 1:
			span = stepMs * int64(time.Millisecond) * (10990 + rng.Int63n(20)) // around 11,000 points
		default:
			span = rng.Int63n(int64(24 * time.Hour))
		}
		if rng.Intn(20) == 0 {
			span = -rng.Int63n(int64(time.Minute)) // end before start
		}
		form := url.Values{
			"query": {`sum by (app) (rate({app="a"}[5m]))`},
			"start": {alignDiffBound(rng, startNs)},
			"end":   {alignDiffBound(rng, startNs+span)},
			"step":  {alignDiffStep(rng, stepMs)},
		}
		if rng.Intn(10) == 0 {
			form.Set("step", strconv.FormatFloat(float64(rng.Intn(999)+1)/1e6, 'f', -1, 64)) // sub-millisecond
		}
		if rng.Intn(15) == 0 {
			form.Del("start")
			form.Set("since", strconv.Itoa(rng.Intn(120)+1)+"m")
		}
		wantStart, wantEnd, wantStepMs, accepted := lokiDecodeAndStepAlign(form, now)
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+form.Encode(), nil)
		got, ok := alignQueryRangeToStep(req, now)
		if wantAlign := accepted && wantStepMs > 0; ok != wantAlign {
			t.Fatalf("#%d %v: aligned = %v, Loki aligns = %v (accepted %v, step %dms)", i, form, ok, wantAlign, accepted, wantStepMs)
		}
		if !ok {
			kept++
			continue
		}
		moved++
		gs, _ := parseLokiTimeToUnixNano(got.FormValue("start"))
		ge, _ := parseLokiTimeToUnixNano(got.FormValue("end"))
		if gs != wantStart || ge != wantEnd {
			t.Fatalf("#%d %v: bounds [%s, %s] read as [%d, %d], Loki's [%d, %d]", i, form, got.FormValue("start"), got.FormValue("end"), gs, ge, wantStart, wantEnd)
		}
	}
	if moved < iterations/2 || kept < iterations/20 {
		t.Fatalf("the generator must exercise both outcomes: %d moved, %d kept", moved, kept)
	}
}

// FuzzAlignQueryRangeToStep checks the alignment over arbitrary bounds and
// steps: it never panics or modifies the caller's request; a moved request
// is idempotent (moving it again changes nothing), each bound moves by less
// than a step toward zero onto a multiple of the step; and wherever Loki's
// decode reads the bounds as the proxy does, the result is Loki's.
func FuzzAlignQueryRangeToStep(f *testing.F) {
	for _, seed := range [][3]string{
		{"1700000000000000000", "1700003600000000000", "137"},
		{"1700000000.3", "1700000010.9996", "0.1"},
		{"-90", "30", "60"},
		{"2026-03-29T03:00:17+02:00", "2026-03-29T04:00:00+02:00", "1m"},
		{"1700000010", "1700000030", "10m"},
		{"1700000017", "1700000017", "60000ms"},
		{"1700000000", "1700011001", "1"},
		{"1700000000", "1700000001", "0.0001"},
		{"", "", "60"},
	} {
		f.Add(seed[0], seed[1], seed[2])
	}
	now := time.Unix(1700003600, 123456789)
	f.Fuzz(func(t *testing.T, start, end, step string) {
		form := url.Values{"query": {`sum(count_over_time({app="a"}[5m]))`}, "start": {start}, "end": {end}, "step": {step}}
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+form.Encode(), nil)
		got, ok := alignQueryRangeToStep(req, now)
		if req.FormValue("start") != start || req.FormValue("end") != end || requestStepAligned(req) {
			t.Fatal("the caller's request was modified")
		}
		if !ok {
			return
		}
		stepD, _ := parsePositiveStepDuration(step)
		stepNs := stepD.Milliseconds() * int64(time.Millisecond)
		rawStart, rawEnd, _ := lokiRangeBounds(req, now)
		for _, b := range []struct {
			name    string
			raw     time.Time
			aligned string
		}{
			{"start", rawStart, got.FormValue("start")},
			{"end", rawEnd, got.FormValue("end")},
		} {
			ns, ok := parseLokiTimeToUnixNano(b.aligned)
			if !ok || encodeAlignedBound(ns) != b.aligned {
				t.Fatalf("%s %q does not read back as the instant it was written for", b.name, b.aligned)
			}
			if ns%stepNs != 0 {
				t.Fatalf("%s %d is not a multiple of the step %d", b.name, ns, stepNs)
			}
			rawMs := b.raw.UnixMilli() * int64(time.Millisecond)
			if d := rawMs - ns; d >= stepNs || d <= -stepNs || (rawMs >= 0 && d < 0) || (rawMs < 0 && d > 0) {
				t.Fatalf("%s moved from %d to %d, step %d: not less than a step toward zero", b.name, rawMs, ns, stepNs)
			}
		}
		again, ok := alignQueryRangeToStep(got, now)
		if !ok || again.FormValue("start") != got.FormValue("start") || again.FormValue("end") != got.FormValue("end") {
			t.Fatalf("not idempotent: [%s, %s] then [%s, %s] (%v)", got.FormValue("start"), got.FormValue("end"), again.FormValue("start"), again.FormValue("end"), ok)
		}
		lokiStart, lokiEnd, lokiStepMs, accepted := lokiDecodeAndStepAlign(form, now)
		if !accepted || lokiStepMs != stepD.Milliseconds() {
			return // Loki rejects it, or reads the step differently (a form such as "1.5m" or "100us" only the proxy accepts)
		}
		ls, _ := lokiParseTimestamp(start, now)
		le, _ := lokiParseTimestamp(end, now)
		if (start != "" && ls.UnixMilli() != rawStart.UnixMilli()) || (end != "" && le.UnixMilli() != rawEnd.UnixMilli()) {
			return // a form the two read differently (an 11-18 digit integer, "now"); not an alignment question
		}
		if got.FormValue("start") != encodeAlignedBound(lokiStart) || got.FormValue("end") != encodeAlignedBound(lokiEnd) {
			t.Fatalf("bounds [%s, %s], Loki's [%d, %d]", got.FormValue("start"), got.FormValue("end"), lokiStart, lokiEnd)
		}
	})
}
