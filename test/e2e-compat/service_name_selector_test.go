//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Loki assigns service_name once per stream at ingest: an existing service_name
// label, else the first non-empty label of discover_service_name, else
// unknown_service. A {service_name<op>"v"} selector must return exactly the
// streams whose assigned name satisfies the matcher, never a stream that only
// carries the value in a lower-priority label.
func TestCompat_ServiceNameSelectorDerivedValue(t *testing.T) {
	probe := fmt.Sprintf("snsel%d", time.Now().UnixNano())
	streams := map[string]map[string]string{
		"own":        {"service_name": "other", "app": "checkout"},
		"app":        {"app": "checkout", "container": "web"},
		"container2": {"app": "web", "container": "checkout"},
		"container":  {"container": "checkout"},
		"k8sjob":     {"k8s_job_name": "checkout"},
		"job":        {"job": "ns/checkout"},
		"none":       {"namespace": "prod"},
		"prefix":     {"app": "checkout-api"},
	}
	const linesPerStream = 2
	base := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	var vlRows strings.Builder
	var lokiStreams []any
	for name, labels := range streams {
		lokiLabels := map[string]string{"probe": probe, "case": name}
		fields := []string{"probe", "case"}
		for k, v := range labels {
			lokiLabels[k] = v
			fields = append(fields, k)
		}
		var values [][]string
		for i := 0; i < linesPerStream; i++ {
			stamp := base.Add(time.Duration(i) * time.Second)
			line := fmt.Sprintf("service name selector %s line %d", name, i)
			values = append(values, []string{strconv.FormatInt(stamp.UnixNano(), 10), line})
			row := map[string]string{"_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": line}
			for k, v := range lokiLabels {
				row[k] = v
			}
			encoded, _ := json.Marshal(row)
			vlRows.Write(encoded)
			vlRows.WriteByte('\n')
		}
		// One jsonline request per stream so each row set gets its own _stream_fields.
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape(strings.Join(fields, ",")), vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VL ingest %s: %d %s", name, status, body)
		}
		vlRows.Reset()
		lokiStreams = append(lokiStreams, map[string]any{"stream": lokiLabels, "values": values})
	}
	payload, _ := json.Marshal(map[string]any{"streams": lokiStreams})
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"}); status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)

	all := fmt.Sprintf(`{probe=%q}`, probe)
	deadline := time.Now().Add(60 * time.Second)
	for {
		lokiCases := serviceNameSelectorLines(t, lokiURL, all, base)
		proxyCases := serviceNameSelectorLines(t, proxyURL, all, base)
		if serviceNameFixtureComplete(lokiCases, len(streams)) && serviceNameFixtureComplete(proxyCases, len(streams)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not visible on both sides: loki=%v proxy=%v", lokiCases, proxyCases)
		}
		time.Sleep(time.Second)
	}

	// Expected cases are the values Loki 3.7.1 returns for this fixture.
	for _, tc := range []struct {
		matcher string
		want    []string
	}{
		{`service_name="checkout"`, []string{"app", "container", "k8sjob"}},
		{`service_name="web"`, []string{"container2"}},
		{`service_name="other"`, []string{"own"}},
		{`service_name="ns/checkout"`, []string{"job"}},
		{`service_name="unknown_service"`, []string{"none"}},
		{`service_name=~"check.*"`, []string{"app", "container", "k8sjob", "prefix"}},
		{`service_name=~"check"`, nil},
		{`service_name=~"unknown.*|web"`, []string{"container2", "none"}},
		{`service_name!="checkout"`, []string{"container2", "job", "none", "own", "prefix"}},
		{`service_name!~"check.*|other"`, []string{"container2", "job", "none"}},
		{`service_name=~".+"`, []string{"app", "container", "container2", "job", "k8sjob", "none", "own", "prefix"}},
		// Every stream has a service_name, so "" matches none and != "" all.
		{`service_name=""`, nil},
		{`service_name!=""`, []string{"app", "container", "container2", "job", "k8sjob", "none", "own", "prefix"}},
	} {
		want := append([]string{}, tc.want...)
		sort.Strings(want)
		queries := []string{fmt.Sprintf(`{probe=%q, %s}`, probe, tc.matcher)}
		if !strings.Contains(tc.matcher, "~") {
			// An exact matcher behaves the same as a label filter stage, with
			// or without a parser stage before it.
			queries = append(queries,
				fmt.Sprintf(`{probe=%q} | %s`, probe, tc.matcher),
				fmt.Sprintf(`{probe=%q} | json | %s`, probe, tc.matcher))
		}
		for _, query := range queries {
			t.Run(query, func(t *testing.T) {
				loki := serviceNameSelectorCases(t, lokiURL, query, base)
				proxy := serviceNameSelectorCases(t, proxyURL, query, base)
				if strings.Join(loki, ",") != strings.Join(want, ",") {
					t.Fatalf("Loki answered %v, expected %v; the fixture or Loki's discover_service_name changed", loki, want)
				}
				if strings.Join(proxy, ",") != strings.Join(loki, ",") {
					t.Fatalf("proxy=%v loki=%v", proxy, loki)
				}
			})
		}
	}

	// by (service_name) groups by the same derived value.
	t.Run("sum by service_name", func(t *testing.T) {
		query := fmt.Sprintf(`sum by (service_name) (count_over_time({probe=%q}[5m]))`, probe)
		at := base.Add(time.Minute)
		want := map[string]float64{"checkout": 6, "web": 2, "other": 2, "ns/checkout": 2, "unknown_service": 2, "checkout-api": 2}
		var loki, proxy map[string]float64
		deadline := time.Now().Add(60 * time.Second)
		for {
			loki = serviceNameGroupedInstant(t, lokiURL, query, at)
			proxy = serviceNameGroupedInstant(t, proxyURL, query, at)
			if fmt.Sprint(loki) == fmt.Sprint(want) || time.Now().After(deadline) {
				break
			}
			time.Sleep(2 * time.Second)
		}
		if fmt.Sprint(loki) != fmt.Sprint(want) {
			t.Fatalf("Loki answered %v, expected %v", loki, want)
		}
		if fmt.Sprint(proxy) != fmt.Sprint(loki) {
			t.Fatalf("proxy=%v loki=%v", proxy, loki)
		}
	})

	// Every listed label value selects rows: the values are the derived names.
	t.Run("label values", func(t *testing.T) {
		params := url.Values{
			"query": {fmt.Sprintf(`{probe=%q}`, probe)},
			"start": {strconv.FormatInt(base.Add(-time.Minute).UnixNano(), 10)},
			"end":   {strconv.FormatInt(base.Add(time.Minute).UnixNano(), 10)},
		}
		want := []string{"checkout", "checkout-api", "ns/checkout", "other", "unknown_service", "web"}
		loki := serviceNameLabelValues(t, lokiURL, params)
		proxy := serviceNameLabelValues(t, proxyURL, params)
		if strings.Join(loki, ",") != strings.Join(want, ",") {
			t.Fatalf("Loki answered %v, expected %v", loki, want)
		}
		if strings.Join(proxy, ",") != strings.Join(loki, ",") {
			t.Fatalf("proxy=%v loki=%v", proxy, loki)
		}
	})

	// Loki simplifies label-filter regexps (pkg/logql/log/filter.go): a bare
	// literal is an equality check, a literal wrapped in `.*` a substring
	// check, and alternation legs follow the same rules; only what it cannot
	// simplify stays an anchored match. Stream selectors stay anchored.
	for _, tc := range []struct {
		matcher string
		want    []string
	}{
		{`service_name=~"checkout"`, []string{"app", "container", "k8sjob"}},
		{`service_name=~"check.*"`, []string{"app", "container", "job", "k8sjob", "prefix"}},
		{`service_name=~".*checkout.*"`, []string{"app", "container", "job", "k8sjob", "prefix"}},
		{`service_name=~"checkout|web"`, []string{"app", "container", "container2", "k8sjob"}},
		{`service_name!~"check.*"`, []string{"container2", "none", "own"}},
	} {
		want := append([]string{}, tc.want...)
		sort.Strings(want)
		query := fmt.Sprintf(`{probe=%q} | %s`, probe, tc.matcher)
		t.Run(query, func(t *testing.T) {
			loki := serviceNameSelectorCases(t, lokiURL, query, base)
			proxy := serviceNameSelectorCases(t, proxyURL, query, base)
			if strings.Join(loki, ",") != strings.Join(want, ",") {
				t.Fatalf("Loki answered %v, expected %v; Loki's label-filter regexp simplification changed", loki, want)
			}
			if strings.Join(proxy, ",") != strings.Join(loki, ",") {
				t.Fatalf("proxy=%v loki=%v", proxy, loki)
			}
		})
	}

	// Every surface that names a service uses the derivation.
	t.Run("series and detected_labels", func(t *testing.T) {
		params := url.Values{
			"match[]": {fmt.Sprintf(`{probe=%q, service_name="checkout"}`, probe)},
			"query":   {fmt.Sprintf(`{probe=%q, service_name="checkout"}`, probe)},
			"start":   {strconv.FormatInt(base.Add(-time.Minute).UnixNano(), 10)},
			"end":     {strconv.FormatInt(base.Add(time.Minute).UnixNano(), 10)},
		}
		loki := serviceNameSeriesCases(t, lokiURL, params)
		proxy := serviceNameSeriesCases(t, proxyURL, params)
		want := []string{"app", "container", "k8sjob"}
		if strings.Join(loki, ",") != strings.Join(want, ",") {
			t.Fatalf("Loki /series answered %v, expected %v", loki, want)
		}
		if strings.Join(proxy, ",") != strings.Join(loki, ",") {
			t.Fatalf("/series proxy=%v loki=%v", proxy, loki)
		}
		// /detected_labels lists label names; service_name is one of them.
		for _, base := range []string{lokiURL, proxyURL} {
			status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/detected_labels?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
			if status != http.StatusOK {
				t.Fatalf("%s /detected_labels: %d %s", base, status, body)
			}
		}
	})

	t.Run("index volume by service_name", func(t *testing.T) {
		params := url.Values{
			"query":        {fmt.Sprintf(`{probe=%q}`, probe)},
			"start":        {strconv.FormatInt(base.Add(-time.Minute).UnixNano(), 10)},
			"end":          {strconv.FormatInt(base.Add(time.Minute).UnixNano(), 10)},
			"targetLabels": {"service_name"},
		}
		loki := serviceNameVolumeSeries(t, lokiURL, params)
		proxy := serviceNameVolumeSeries(t, proxyURL, params)
		want := []string{"checkout", "checkout-api", "ns/checkout", "other", "unknown_service", "web"}
		if strings.Join(loki, ",") != strings.Join(want, ",") {
			t.Fatalf("Loki index/volume answered %v, expected %v", loki, want)
		}
		if strings.Join(proxy, ",") != strings.Join(loki, ",") {
			t.Fatalf("index/volume proxy=%v loki=%v", proxy, loki)
		}
	})

	t.Run("detected field values", func(t *testing.T) {
		params := url.Values{
			"query": {fmt.Sprintf(`{probe=%q}`, probe)},
			"start": {strconv.FormatInt(base.Add(-time.Minute).UnixNano(), 10)},
			"end":   {strconv.FormatInt(base.Add(time.Minute).UnixNano(), 10)},
		}
		status, body := hardeningRequest(t, http.MethodGet, proxyURL+"/loki/api/v1/detected_field/service_name/values?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
		var response struct {
			Values []string `json:"values"`
		}
		if status != http.StatusOK || json.Unmarshal(body, &response) != nil {
			t.Fatalf("detected_field values: %d %s", status, body)
		}
		got := append([]string{}, response.Values...)
		sort.Strings(got)
		want := []string{"checkout", "checkout-api", "ns/checkout", "other", "unknown_service", "web"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("detected_field/service_name/values = %v, want %v", got, want)
		}
	})
}

func serviceNameFixtureComplete(lines map[string]int, streams int) bool {
	if len(lines) != streams {
		return false
	}
	for _, n := range lines {
		if n != 2 {
			return false
		}
	}
	return true
}

// serviceNameSelectorCases returns the sorted `case` labels of the streams a
// log query returns, checking each stream carries every fixture line.
func serviceNameSelectorCases(t *testing.T, base, selector string, start time.Time) []string {
	t.Helper()
	lines := serviceNameSelectorLines(t, base, selector, start)
	cases := make([]string, 0, len(lines))
	for name, n := range lines {
		if n != 2 {
			t.Fatalf("%s %s: case %q returned %d lines, want 2", base, selector, name, n)
		}
		cases = append(cases, name)
	}
	sort.Strings(cases)
	return cases
}

// serviceNameSelectorLines counts the lines a log query returns per `case` label.
func serviceNameSelectorLines(t *testing.T, base, selector string, start time.Time) map[string]int {
	t.Helper()
	params := url.Values{
		"query": {selector},
		"start": {strconv.FormatInt(start.Add(-time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(start.Add(time.Minute).UnixNano(), 10)},
		"limit": {"1000"},
	}
	status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/query_range?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
	var response struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Stream map[string]string `json:"stream"`
				Values [][]string        `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || response.Status != "success" || response.Data.ResultType != "streams" {
		t.Fatalf("%s %s: %d %s", base, selector, status, body)
	}
	lines := map[string]int{}
	for _, stream := range response.Data.Result {
		lines[stream.Stream["case"]] += len(stream.Values)
	}
	return lines
}

// serviceNameGroupedInstant returns an instant vector grouped by service_name
// as service name -> value; a series without exactly that label fails the test.
func serviceNameGroupedInstant(t *testing.T, base, query string, at time.Time) map[string]float64 {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.Unix(), 10)}}
	status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/query?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
	var response struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || response.Status != "success" || response.Data.ResultType != "vector" {
		t.Fatalf("%s %s: %d %s", base, query, status, body)
	}
	out := map[string]float64{}
	for _, sample := range response.Data.Result {
		var raw string
		name, ok := sample.Metric["service_name"]
		if !ok || len(sample.Metric) != 1 || len(sample.Value) != 2 || json.Unmarshal(sample.Value[1], &raw) != nil {
			t.Fatalf("%s %s: unexpected sample in %s", base, query, body)
		}
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Fatalf("%s: invalid value in %s", base, body)
		}
		out[name] = value
	}
	return out
}

// serviceNameLabelValues returns the sorted service_name label values.
func serviceNameLabelValues(t *testing.T, base string, params url.Values) []string {
	t.Helper()
	status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/label/service_name/values?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
	var response struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || response.Status != "success" {
		t.Fatalf("%s: %d %s", base, status, body)
	}
	values := append([]string{}, response.Data...)
	sort.Strings(values)
	return values
}

// serviceNameSeriesCases returns the sorted `case` labels of /series results.
func serviceNameSeriesCases(t *testing.T, base string, params url.Values) []string {
	t.Helper()
	status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/series?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
	var response struct {
		Status string              `json:"status"`
		Data   []map[string]string `json:"data"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || response.Status != "success" {
		t.Fatalf("%s /series: %d %s", base, status, body)
	}
	cases := make([]string, 0, len(response.Data))
	for _, labels := range response.Data {
		cases = append(cases, labels["case"])
	}
	sort.Strings(cases)
	return cases
}

// serviceNameVolumeSeries returns the sorted service_name labels of an
// index/volume response.
func serviceNameVolumeSeries(t *testing.T, base string, params url.Values) []string {
	t.Helper()
	status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/index/volume?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
	var response struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || response.Status != "success" {
		t.Fatalf("%s index/volume: %d %s", base, status, body)
	}
	names := make([]string, 0, len(response.Data.Result))
	for _, series := range response.Data.Result {
		names = append(names, series.Metric["service_name"])
	}
	sort.Strings(names)
	return names
}
