//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestCompat_MetadataScanAdmission reproduces the inventory refresh storm that
// OOM-killed the e2e VictoriaLogs: many long-range /labels and
// /label/<name>/values requests at once (distinct selectors, so nothing is
// coalesced), the shape several replicas send when their label keep-warm
// ticks coincide. With the metadata-scan limiter every request is answered
// with 200 or the documented 429 naming -backend-max-concurrent-metadata-scans,
// a short-range /labels keeps answering throughout, VictoriaLogs does not
// restart, and the 7-day label list still carries every label Loki reports.
//
// conformance: backend-admission-and-heavy-query-queueing, limits/concurrent-full-retention-scans-exhaust-backend, limits/metadata-scan-queue-429, limits/metadata-not-queued, loki_api_v1_labels, loki_api_v1_label_name_values
func TestCompat_MetadataScanAdmission(t *testing.T) {
	ensureDataIngested(t)
	startedBefore := victoriaLogsStartTimestamp(t)
	end := time.Now().Truncate(time.Minute)
	start := end.Add(-7 * 24 * time.Hour)
	window := func(query string) string {
		q := url.Values{
			"start": {strconv.FormatInt(start.UnixNano(), 10)},
			"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		}
		if query != "" {
			q.Set("query", query)
		}
		return q.Encode()
	}
	selectors := []string{``, `{env="production"}`, `{env="staging"}`, `{app=~".+"}`, `{namespace=~".+"}`, `{level=~".+"}`, `{service_name=~".+"}`, `{cluster=~".+"}`}
	targets := make([]string, 0, 2*len(selectors))
	for _, selector := range selectors {
		targets = append(targets, "/loki/api/v1/labels?"+window(selector), "/loki/api/v1/label/env/values?"+window(selector))
	}

	client := &http.Client{Timeout: 3 * time.Minute}
	get := func(base, target string) (int, string, error) {
		req, err := http.NewRequest(http.MethodGet, base+target, nil)
		if err != nil {
			return 0, "", err
		}
		req.Header.Set("X-Scope-OrgID", "0")
		resp, err := client.Do(req)
		if err != nil {
			return 0, "", err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		return resp.StatusCode, string(body), nil
	}

	var mu sync.Mutex
	var failures []string
	stop := make(chan struct{})
	probesDone := make(chan int)
	go func() {
		probes := 0
		for {
			probeAt := time.Now()
			status, body, err := get(proxyURL, "/loki/api/v1/labels?start="+strconv.FormatInt(probeAt.Add(-time.Hour).UnixNano(), 10)+"&end="+strconv.FormatInt(probeAt.UnixNano(), 10))
			probes++
			if err != nil || status != http.StatusOK {
				mu.Lock()
				failures = append(failures, fmt.Sprintf("short-range labels probe during the storm: %d %v %.200s", status, err, body))
				mu.Unlock()
			}
			select {
			case <-stop:
				probesDone <- probes
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	}()

	var wg sync.WaitGroup
	var served, rejected int
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < 2; round++ {
				target := targets[(worker+round*len(selectors))%len(targets)]
				status, body, err := get(proxyURL, target)
				mu.Lock()
				switch {
				case err == nil && status == http.StatusOK:
					served++
				case err == nil && status == http.StatusTooManyRequests && strings.Contains(body, "too many outstanding requests") && strings.Contains(body, "-backend-max-concurrent-metadata-scans"):
					rejected++
				default:
					failures = append(failures, fmt.Sprintf("%s: %d %v %.300s", target, status, err, body))
				}
				mu.Unlock()
			}
		}(worker)
	}
	wg.Wait()
	close(stop)
	if probes := <-probesDone; probes == 0 {
		t.Fatal("short-range labels probe never ran")
	}
	for _, failure := range failures {
		t.Error(failure)
	}
	t.Logf("long-range metadata requests: %d served, %d rejected with the documented 429", served, rejected)
	if served == 0 {
		t.Fatal("no long-range metadata request was served")
	}
	if status, body := hardeningRequest(t, http.MethodGet, vlURL+"/health", "", nil); status != http.StatusOK {
		t.Fatalf("VictoriaLogs /health = %d %s after the storm", status, body)
	}
	if startedAfter := victoriaLogsStartTimestamp(t); startedAfter != startedBefore {
		t.Fatalf("VictoriaLogs restarted during the storm: start timestamp %s -> %s", startedBefore, startedAfter)
	}

	// The bound changes when a request is answered, never what it answers:
	// the 7-day label list must still carry every label Loki reports.
	t.Run("7d label list matches Loki", func(t *testing.T) {
		lokiLabels := metadataList(t, lokiURL, "/loki/api/v1/labels?"+window(""))
		var proxyLabels []string
		for attempt := 0; attempt < 6; attempt++ {
			status, body, err := get(proxyURL, "/loki/api/v1/labels?"+window(""))
			if err == nil && status == http.StatusOK {
				proxyLabels = decodeMetadataList(t, body)
				break
			}
			if err == nil && status == http.StatusTooManyRequests {
				time.Sleep(5 * time.Second) // the storm's scans are still draining
				continue
			}
			t.Fatalf("proxy 7d labels: %d %v %.300s", status, err, body)
		}
		if len(lokiLabels) == 0 || len(proxyLabels) == 0 {
			t.Fatalf("empty label list: loki=%d proxy=%d", len(lokiLabels), len(proxyLabels))
		}
		have := toSet(proxyLabels)
		for _, label := range lokiLabels {
			if !have[label] {
				t.Errorf("label %q is in Loki's 7d answer but missing from the proxy's", label)
			}
		}
		t.Logf("7d labels: loki=%d proxy=%d", len(lokiLabels), len(proxyLabels))
	})
}

// metadataList fetches a Loki metadata endpoint and returns its sorted data
// array; a non-200 fails the test.
func metadataList(t *testing.T, base, target string) []string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+target, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Scope-OrgID", "0")
	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
	if err != nil {
		t.Fatalf("%s: %v", target, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: %d %.300s", target, resp.StatusCode, body)
	}
	return decodeMetadataList(t, string(body))
}

func decodeMetadataList(t *testing.T, body string) []string {
	t.Helper()
	var parsed struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("metadata list: %v: %.300s", err, body)
	}
	sort.Strings(parsed.Data)
	return parsed.Data
}
