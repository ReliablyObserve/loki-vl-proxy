//go:build e2e

package e2e_compat

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRecorderReadTransportRetriesTruncatedSuccessfulPOST(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || r.Method != http.MethodPost || string(body) != "query=%2A" {
			t.Errorf("replayed request: method=%s body=%q error=%v", r.Method, body, err)
		}
		if requests.Add(1) == 1 {
			w.Header().Set("Content-Length", "20")
			_, _ = io.WriteString(w, "partial")
			return
		}
		_, _ = io.WriteString(w, `{"status":"success"}`)
	}))
	defer upstream.Close()
	target, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = recorderReadTransport{base: http.DefaultTransport}
	server := httptest.NewServer(forward)
	defer server.Close()

	resp, err := http.Post(server.URL+"/select/logsql/query", "application/x-www-form-urlencoded", strings.NewReader("query=%2A"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != `{"status":"success"}` || requests.Load() != 2 {
		t.Fatalf("status=%d body=%q upstream requests=%d", resp.StatusCode, body, requests.Load())
	}
}

func TestRecorderReadTransportDoesNotRetryBackendError(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "backend failure")
	}))
	defer upstream.Close()
	target, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = recorderReadTransport{base: http.DefaultTransport}
	server := httptest.NewServer(forward)
	defer server.Close()

	resp, err := http.Get(server.URL + "/select/logsql/query")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusInternalServerError || string(body) != "backend failure" || requests.Load() != 1 {
		t.Fatalf("status=%d body=%q upstream requests=%d", resp.StatusCode, body, requests.Load())
	}
}

func TestRecorderReadTransportDoesNotRetryTruncatedBackendError(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Length", "20")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "partial")
	}))
	defer upstream.Close()
	req, err := http.NewRequest(http.MethodGet, upstream.URL+"/select/logsql/query", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = (recorderReadTransport{base: http.DefaultTransport}).RoundTrip(req)
	if err == nil || requests.Load() != 1 {
		t.Fatalf("error=%v upstream requests=%d, want one failed request", err, requests.Load())
	}
}
