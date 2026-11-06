package proxy

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// stepAlignLokiAnswers holds what a Loki query frontend with align_queries_with_step makes of the requests
// stepAlignDiffForms generates: whether it decodes each one, the step it keeps in milliseconds and the bounds it
// passes on. The Loki oracle's align command (test/oracle/loki/align) recorded them with Loki's own range query parser;
// TestPinStepAlignLokiAnswers records them again. The first line names the Loki version.
const stepAlignLokiAnswers = "testdata/step_align_loki_answers.jsonl.gz"

// stepAlignReplayNow is the proxy's clock when it replays the recorded requests. A request without a start has
// its start derived from the earlier of its end and now, so those requests end before this instant, which is
// before any time the answers are recorded at: Loki's clock and this one then give the same start.
var stepAlignReplayNow = time.Unix(1750000000, 0)

// stepAlignLokiAnswer is one recorded answer (test/oracle/loki/align AlignAnswer).
type stepAlignLokiAnswer struct {
	ID       string `json:"id"`
	Form     string `json:"form"`
	Accepted bool   `json:"accepted"`
	Error    string `json:"error,omitempty"`
	StepMs   int64  `json:"step_ms"`
	StartNs  int64  `json:"start_ns"`
	EndNs    int64  `json:"end_ns"`
}

type stepAlignLokiHeader struct {
	Loki string `json:"loki"`
}

func readStepAlignLokiAnswers(t *testing.T) (string, []stepAlignLokiAnswer) {
	t.Helper()
	fh, err := os.Open(stepAlignLokiAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()
	zr, err := gzip.NewReader(fh)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	var header stepAlignLokiHeader
	var rows []stepAlignLokiAnswer
	for first := true; sc.Scan(); first = false {
		if first {
			if err := json.Unmarshal(sc.Bytes(), &header); err != nil {
				t.Fatal(err)
			}
			continue
		}
		var row stepAlignLokiAnswer
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return header.Loki, rows
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

// stepAlignDiffForms generates the range query requests the alignment is compared with Loki on. Bounds come in
// every form both read alike (integer seconds, nanoseconds, decimal seconds with up to 9 digits, RFC3339 with
// offsets), before and after the epoch; steps from sub-millisecond to longer than the range; ranges from equal
// bounds to past 11,000 points, and ends before starts. Every request has an end and a step: Loki derives a missing
// end from its clock, and the proxy does not align a step-less request.
func stepAlignDiffForms(n int) []url.Values {
	steps := []int64{1, 3, 100, 250, 500, 999, 1000, 1500, 7000, 15000, 60000, 137000, 300000, 3600000, 86400000}
	rng := rand.New(rand.NewSource(20261028))
	forms := make([]url.Values, 0, n)
	for len(forms) < n {
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
		if rng.Intn(15) == 0 && startNs+span < stepAlignReplayNow.UnixNano() {
			form.Del("start")
			form.Set("since", strconv.Itoa(rng.Intn(120)+1)+"m")
		}
		forms = append(forms, form)
	}
	return forms
}

// The proxy's alignment, through the proxy's own parsing of the bounds and the step, agrees with Loki's
// decode-then-align on every recorded request: the same requests are moved, and to the same nanosecond.
// conformance: semantics/query-range-step-alignment, loki_api_v1_query_range
func TestAlignQueryRangeToStep_DifferentialAgainstLokiDecode(t *testing.T) {
	version, rows := readStepAlignLokiAnswers(t)
	if want := lokiMatrixVersion(t); version != want {
		t.Fatalf("%s was recorded with Loki %s, the compatibility matrix pins %s: record it again (TestPinStepAlignLokiAnswers)", stepAlignLokiAnswers, version, want)
	}
	moved, kept := 0, 0
	for _, row := range rows {
		form, err := url.ParseQuery(row.Form)
		if err != nil {
			t.Fatalf("%s: %v", row.ID, err)
		}
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+form.Encode(), nil)
		got, ok := alignQueryRangeToStep(req, stepAlignReplayNow)
		if wantAlign := row.Accepted && row.StepMs > 0; ok != wantAlign {
			t.Fatalf("#%s %v: aligned = %v, Loki aligns = %v (accepted %v %q, step %dms)", row.ID, form, ok, wantAlign, row.Accepted, row.Error, row.StepMs)
		}
		if !ok {
			kept++
			continue
		}
		moved++
		gs, _ := parseLokiTimeToUnixNano(got.FormValue("start"))
		ge, _ := parseLokiTimeToUnixNano(got.FormValue("end"))
		if gs != row.StartNs || ge != row.EndNs {
			t.Fatalf("#%s %v: bounds [%s, %s] read as [%d, %d], Loki's [%d, %d]", row.ID, form, got.FormValue("start"), got.FormValue("end"), gs, ge, row.StartNs, row.EndNs)
		}
	}
	if moved < len(rows)/2 || kept < len(rows)/20 {
		t.Fatalf("the recorded requests must exercise both outcomes: %d moved, %d kept", moved, kept)
	}
}

// TestPinStepAlignLokiAnswers records stepAlignLokiAnswers again with the Loki oracle, for a new Loki pin or a
// changed generator: LVP_ORACLE_PIN_STEP_ALIGN=1 go test ./internal/proxy -run TestPinStepAlignLokiAnswers.
func TestPinStepAlignLokiAnswers(t *testing.T) {
	if os.Getenv("LVP_ORACLE_PIN_STEP_ALIGN") != "1" {
		t.Skip("LVP_ORACLE_PIN_STEP_ALIGN=1 is not set")
	}
	version := lokiMatrixVersion(t)
	if mod := readText(t, filepath.Join(lokiOracleDir, "go.mod")); !strings.Contains(mod, "github.com/grafana/loki/v3 v"+version+"\n") {
		t.Fatalf("test/oracle/loki/go.mod does not require Loki v%s, the compatibility matrix's pinned version", version)
	}
	if !time.Now().After(stepAlignReplayNow) {
		t.Fatal("the answers must be recorded after stepAlignReplayNow")
	}
	work := t.TempDir()
	bin, err := oraclerun.Build(filepath.Join(lokiOracleDir, "align"), "", work, "align")
	if err != nil {
		t.Fatal(err)
	}
	forms := stepAlignDiffForms(5000)
	for _, seed := range stepAlignFuzzSeeds {
		if seed[1] != "" {
			forms = append(forms, url.Values{"query": {`sum(count_over_time({app="a"}[5m]))`}, "start": {seed[0]}, "end": {seed[1]}, "step": {seed[2]}})
		}
	}
	var inputs []byte
	for i, form := range forms {
		line, err := json.Marshal(map[string]string{"id": strconv.Itoa(i), "form": form.Encode()})
		if err != nil {
			t.Fatal(err)
		}
		inputs = append(append(inputs, line...), '\n')
	}
	in, out := filepath.Join(work, "forms.jsonl"), filepath.Join(work, "answers.jsonl")
	if err := os.WriteFile(in, inputs, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Run(bin, "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	answers, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	header, err := json.Marshal(stepAlignLokiHeader{Loki: version})
	if err != nil {
		t.Fatal(err)
	}
	recorded := filepath.Join(work, "recorded.jsonl")
	if err := os.WriteFile(recorded, append(append(header, '\n'), answers...), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Gzip(recorded, stepAlignLokiAnswers); err != nil {
		t.Fatal(err)
	}
	t.Logf("recorded %s with Loki %s", stepAlignLokiAnswers, version)
}

// stepAlignFuzzSeeds are FuzzAlignQueryRangeToStep's seeds: start, end and step. Those with an end are also
// recorded with Loki (TestPinStepAlignLokiAnswers).
var stepAlignFuzzSeeds = [][3]string{
	{"1700000000000000000", "1700003600000000000", "137"},
	{"1700000000.3", "1700000010.9996", "0.1"},
	{"-90", "30", "60"},
	{"2026-03-29T03:00:17+02:00", "2026-03-29T04:00:00+02:00", "1m"},
	{"1700000010", "1700000030", "10m"},
	{"1700000017", "1700000017", "60000ms"},
	{"1700000000", "1700011001", "1"},
	{"1700000000", "1700000001", "0.0001"},
	{"", "", "60"},
	// No start, so Loki derives it an hour before an end near the earliest
	// int64 nanosecond: that start in nanoseconds is outside int64.
	{"", "-9223368436854551616", "1000000"},
}

// FuzzAlignQueryRangeToStep checks the alignment over arbitrary bounds and
// steps: it never panics or modifies the caller's request; what the
// middleware passes through unaligned (boundsOnStepGrid) is what alignment
// leaves at the same instants; a moved request
// is idempotent (moving it again changes nothing), and each bound moves by less
// than a step toward zero onto a multiple of the step. That the result is
// Loki's is TestAlignQueryRangeToStep_DifferentialAgainstLokiDecode's.
func FuzzAlignQueryRangeToStep(f *testing.F) {
	for _, seed := range stepAlignFuzzSeeds {
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
		{
			d, okD := stepWithoutAllocating(step)
			want, okW := parsePositiveStepDuration(step)
			if d != want || okD != okW {
				t.Fatalf("step %q read as %v %v, want %v %v", step, d, okD, want, okW)
			}
		}
		// The middleware's pass-through is exactly the requests alignment
		// leaves at the same instants.
		if ok && boundsOnStepGrid(req) {
			for _, key := range []string{"start", "end"} {
				was, _ := parseLokiTimeToUnixNano(req.FormValue(key))
				if moved, _ := parseLokiTimeToUnixNano(got.FormValue(key)); moved != was {
					t.Fatalf("passed %s %q through, alignment moves it to %q", key, req.FormValue(key), got.FormValue(key))
				}
			}
		}
		if !ok {
			return
		}
		assertBoundsReplaced(t, req.URL.RawQuery, got.URL.RawQuery, got.FormValue("start"), got.FormValue("end"))
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
			// Compared in milliseconds: a bound near either int64 limit times 1e6
			// would leave int64 and wrap.
			rawMs, alignedMs, stepMs := b.raw.UnixMilli(), ns/int64(time.Millisecond), stepNs/int64(time.Millisecond)
			if d := rawMs - alignedMs; d >= stepMs || d <= -stepMs || (rawMs >= 0 && d < 0) || (rawMs < 0 && d > 0) {
				t.Fatalf("%s moved from %d ms to %d ms, step %d ms: not less than a step toward zero", b.name, rawMs, alignedMs, stepMs)
			}
		}
		again, ok := alignQueryRangeToStep(got, now)
		if !ok || again.FormValue("start") != got.FormValue("start") || again.FormValue("end") != got.FormValue("end") {
			t.Fatalf("not idempotent: [%s, %s] then [%s, %s] (%v)", got.FormValue("start"), got.FormValue("end"), again.FormValue("start"), again.FormValue("end"), ok)
		}
	})
}
