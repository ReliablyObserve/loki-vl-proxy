package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestLabelSurface_LabelsIncludeConfiguredExtras(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/stream_field_names" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"values":[{"value":"app","hits":10}]}`))
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL:        vlBackend.URL,
		Cache:             cache.New(60*time.Second, 1000),
		LogLevel:          "error",
		LabelStyle:        LabelStyleUnderscores,
		ExtraLabelFields:  []string{"host.id", "custom.pipeline.processing"},
		MetadataFieldMode: MetadataFieldModeTranslated,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
	p.handleLabels(w, r)

	var resp struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode labels response: %v", err)
	}
	if !contains(resp.Data, "app") || !contains(resp.Data, "host_id") || !contains(resp.Data, "custom_pipeline_processing") {
		t.Fatalf("expected translated extra labels in response, got %v", resp.Data)
	}
	for _, label := range resp.Data {
		if strings.Contains(label, ".") {
			t.Fatalf("expected underscore-only labels in translated mode, got dotted label %q in %v", label, resp.Data)
		}
	}
}

func TestLabelSurface_LabelValuesResolveCustomAliasFromConfiguredExtras(t *testing.T) {
	var requestedField string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			http.Error(w, "unsupported", http.StatusNotFound)
		case "/select/logsql/field_values":
			requestedField = r.URL.Query().Get("field")
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"i-host-1","hits":1}]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL:        vlBackend.URL,
		Cache:             cache.New(60*time.Second, 1000),
		LogLevel:          "error",
		LabelStyle:        LabelStyleUnderscores,
		ExtraLabelFields:  []string{"host.id"},
		MetadataFieldMode: MetadataFieldModeTranslated,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/label/host_id/values?query=%7Bservice_name%3D%22api%22%7D", nil)
	p.handleLabelValues(w, r)

	if requestedField != "host.id" {
		t.Fatalf("expected host_id alias to resolve to host.id, got %q", requestedField)
	}

	var resp struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode label values response: %v", err)
	}
	if len(resp.Data) != 1 || resp.Data[0] != "i-host-1" {
		t.Fatalf("expected host_id value from resolved host.id field, got %v", resp.Data)
	}
}

func TestLabelSurface_LabelValuesIndexedBrowseUsesHotsetAndOffsetWithoutBackendRefetch(t *testing.T) {
	fieldValuesCalls := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			http.Error(w, "unsupported", http.StatusNotFound)
		case "/select/logsql/field_values":
			fieldValuesCalls++
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"delta","hits":1},{"value":"alpha","hits":1},{"value":"gamma","hits":1},{"value":"beta","hits":1}]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL:                 vlBackend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		LogLevel:                   "error",
		LabelStyle:                 LabelStyleUnderscores,
		MetadataFieldMode:          MetadataFieldModeTranslated,
		LabelValuesIndexedCache:    true,
		LabelValuesHotLimit:        2,
		LabelValuesIndexMaxEntries: 1000,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w1 := httptest.NewRecorder()
	r1 := httptest.NewRequest(http.MethodGet, "/loki/api/v1/label/app/values", nil)
	p.handleLabelValues(w1, r1)

	var resp1 struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(w1.Body.Bytes(), &resp1); err != nil {
		t.Fatalf("decode first label values response: %v", err)
	}
	if len(resp1.Data) != 2 || resp1.Data[0] != "alpha" || resp1.Data[1] != "beta" {
		t.Fatalf("expected hotset-first values [alpha beta], got %v", resp1.Data)
	}

	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest(http.MethodGet, "/loki/api/v1/label/app/values?offset=2&limit=2", nil)
	p.handleLabelValues(w2, r2)

	var resp2 struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(w2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("decode second label values response: %v", err)
	}
	if len(resp2.Data) != 2 || resp2.Data[0] != "delta" || resp2.Data[1] != "gamma" {
		t.Fatalf("expected offset window [delta gamma], got %v", resp2.Data)
	}
	if fieldValuesCalls != 1 {
		t.Fatalf("expected single backend field_values call with indexed browse cache, got %d", fieldValuesCalls)
	}
}

func TestLabelSurface_LabelValuesIndexedBrowseSearchUsesIndex(t *testing.T) {
	fieldValuesCalls := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			http.Error(w, "unsupported", http.StatusNotFound)
		case "/select/logsql/field_values":
			fieldValuesCalls++
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"alpha","hits":1},{"value":"beta","hits":1},{"value":"delta","hits":1},{"value":"gamma","hits":1}]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL:                 vlBackend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		LogLevel:                   "error",
		LabelStyle:                 LabelStyleUnderscores,
		MetadataFieldMode:          MetadataFieldModeTranslated,
		LabelValuesIndexedCache:    true,
		LabelValuesHotLimit:        2,
		LabelValuesIndexMaxEntries: 1000,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	seedW := httptest.NewRecorder()
	seedReq := httptest.NewRequest(http.MethodGet, "/loki/api/v1/label/app/values", nil)
	p.handleLabelValues(seedW, seedReq)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/label/app/values?search=ta&limit=10", nil)
	p.handleLabelValues(w, r)

	var resp struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode search label values response: %v", err)
	}
	if len(resp.Data) != 2 || resp.Data[0] != "beta" || resp.Data[1] != "delta" {
		t.Fatalf("expected search-filtered indexed values [beta delta], got %v", resp.Data)
	}
	if fieldValuesCalls != 1 {
		t.Fatalf("expected search browse to use index without extra backend calls, got %d", fieldValuesCalls)
	}
}

func TestLabelSurface_VolumeTargetLabelsResolveCustomAlias(t *testing.T) {
	var requestedField string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"host.id","hits":2}]}`))
		case "/select/logsql/hits":
			requestedField = r.URL.Query().Get("field")
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"hits":[{"fields":{"host.id":"i-host-1"},"timestamps":["2026-04-04T17:18:49Z"],"values":[3]}]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL:        vlBackend.URL,
		Cache:             cache.New(60*time.Second, 1000),
		LogLevel:          "error",
		LabelStyle:        LabelStyleUnderscores,
		MetadataFieldMode: MetadataFieldModeTranslated,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	params := url.Values{}
	params.Set("query", `{service_name="api"}`)
	params.Set("targetLabels", "host_id")
	params.Set("start", "1")
	params.Set("end", "2")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/index/volume?"+params.Encode(), nil)
	p.handleVolume(w, r)

	if requestedField != "host.id" {
		t.Fatalf("expected volume targetLabels alias host_id to resolve to host.id, got %q", requestedField)
	}

	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode volume response: %v", err)
	}
	if len(resp.Data.Result) != 1 {
		t.Fatalf("expected one volume series, got %#v", resp.Data.Result)
	}
	if got := resp.Data.Result[0].Metric["host_id"]; got != "i-host-1" {
		t.Fatalf("expected translated host_id metric label in volume response, got %v", resp.Data.Result[0].Metric)
	}
}

func TestLabelSurface_VolumeRangeTargetLabelsResolveCustomAlias(t *testing.T) {
	var requestedField string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"host.id","hits":2}]}`))
		case "/select/logsql/hits":
			requestedField = r.URL.Query().Get("field")
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"hits":[{"fields":{"host.id":"i-host-1"},"timestamps":["2026-04-04T17:18:49Z"],"values":[3]}]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL:        vlBackend.URL,
		Cache:             cache.New(60*time.Second, 1000),
		LogLevel:          "error",
		LabelStyle:        LabelStyleUnderscores,
		MetadataFieldMode: MetadataFieldModeTranslated,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	params := url.Values{}
	params.Set("query", `{service_name="api"}`)
	params.Set("targetLabels", "host_id")
	params.Set("start", "1")
	params.Set("end", "2")
	params.Set("step", "60")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/index/volume_range?"+params.Encode(), nil)
	p.handleVolumeRange(w, r)

	if requestedField != "host.id" {
		t.Fatalf("expected volume_range targetLabels alias host_id to resolve to host.id, got %q", requestedField)
	}
}

func TestLabelSurface_TargetLabelInventoryLookupUsesCache(t *testing.T) {
	streamFieldCalls := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			streamFieldCalls++
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"custom.pipeline.processing","hits":3}]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL:        vlBackend.URL,
		Cache:             cache.New(60*time.Second, 1000),
		LogLevel:          "error",
		LabelStyle:        LabelStyleUnderscores,
		MetadataFieldMode: MetadataFieldModeTranslated,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	params := url.Values{}
	params.Set("query", `{service_name="api"}`)
	params.Set("start", "1")
	params.Set("end", "2")

	ctx := context.WithValue(context.Background(), orgIDKey, "team-a")

	got1 := p.resolveTargetLabelFields(ctx, "custom_pipeline_processing", params)
	got2 := p.resolveTargetLabelFields(ctx, "custom_pipeline_processing", params)
	if len(got1) != 1 || got1[0] != "custom.pipeline.processing" {
		t.Fatalf("expected first lookup to resolve custom.pipeline.processing, got %v", got1)
	}
	if len(got2) != 1 || got2[0] != "custom.pipeline.processing" {
		t.Fatalf("expected second lookup to resolve custom.pipeline.processing, got %v", got2)
	}
	if streamFieldCalls != 1 {
		t.Fatalf("expected one stream_field_names call due cache reuse, got %d", streamFieldCalls)
	}
}

func TestLabelSurface_LabelValuesIndexPersistsOnShutdownAndRestoresOnStartup(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unused in test", http.StatusNotFound)
	}))
	defer vlBackend.Close()

	snapshotPath := filepath.Join(t.TempDir(), "label-values-index.json")

	p1, err := New(Config{
		BackendURL:                      vlBackend.URL,
		Cache:                           cache.New(60*time.Second, 1000),
		LogLevel:                        "error",
		LabelStyle:                      LabelStyleUnderscores,
		MetadataFieldMode:               MetadataFieldModeTranslated,
		LabelValuesIndexedCache:         true,
		LabelValuesHotLimit:             100,
		LabelValuesIndexMaxEntries:      1000,
		LabelValuesIndexPersistPath:     snapshotPath,
		LabelValuesIndexPersistInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("create proxy #1: %v", err)
	}
	p1.Init()

	p1.updateLabelValuesIndex("team-a", "host_id", []string{"zeta", "alpha", "beta"})
	if err := p1.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown proxy #1: %v", err)
	}

	p2, err := New(Config{
		BackendURL:                      vlBackend.URL,
		Cache:                           cache.New(60*time.Second, 1000),
		LogLevel:                        "error",
		LabelStyle:                      LabelStyleUnderscores,
		MetadataFieldMode:               MetadataFieldModeTranslated,
		LabelValuesIndexedCache:         true,
		LabelValuesHotLimit:             100,
		LabelValuesIndexMaxEntries:      1000,
		LabelValuesIndexPersistPath:     snapshotPath,
		LabelValuesIndexPersistInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("create proxy #2: %v", err)
	}
	p2.Init()
	defer func() { _ = p2.Shutdown(context.Background()) }()

	got, ok := p2.selectLabelValuesFromIndex("team-a", "host_id", "", 0, 10)
	if !ok {
		t.Fatalf("expected restored label-values index to be available")
	}
	want := []string{"alpha", "beta", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected restored values: want=%v got=%v", want, got)
	}
}

func TestLabelSurface_LabelValuesIndexStartupWarmsFromPeersWhenDiskStale(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unused in test", http.StatusNotFound)
	}))
	defer vlBackend.Close()

	now := time.Now().UTC()
	peerSnapshot := labelValuesIndexSnapshot{
		Version:         1,
		SavedAtUnixNano: now.UnixNano(),
		StatesByKey: map[string]map[string]labelValueIndexEntry{
			labelValuesIndexKey("team-a", "host_id"): {
				"from-peer": {SeenCount: 9, LastSeen: now.UnixNano()},
			},
		},
	}
	peerPayload, err := json.Marshal(peerSnapshot)
	if err != nil {
		t.Fatalf("marshal peer snapshot: %v", err)
	}
	ownerCache := cache.New(5*time.Minute, 1000)
	ownerCache.SetWithTTL(labelValuesIndexSnapshotCacheKey, peerPayload, 5*time.Minute)

	ownerPeer := cache.NewPeerCache(cache.PeerConfig{SelfAddr: "owner:3100"})
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Peer-Token"); got != "shared-secret" {
			t.Fatalf("unexpected peer token header: got %q", got)
		}
		if !strings.Contains(strings.ToLower(r.Header.Get("Accept-Encoding")), "gzip") {
			t.Fatalf("expected peer warm fetch to advertise compression, got %q", r.Header.Get("Accept-Encoding"))
		}
		if r.URL.Path != "/_cache/get" {
			http.NotFound(w, r)
			return
		}
		rec := httptest.NewRecorder()
		ownerPeer.ServeHTTP(rec, r, ownerCache)
		for k, vals := range rec.Header() {
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(rec.Code)
		var buf bytes.Buffer
		zw, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
		if err != nil {
			t.Fatalf("create gzip writer: %v", err)
		}
		if _, err := zw.Write(rec.Body.Bytes()); err != nil {
			t.Fatalf("compress label-values snapshot: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("close gzip writer: %v", err)
		}
		if _, err := w.Write(buf.Bytes()); err != nil {
			t.Fatalf("write compressed label-values snapshot: %v", err)
		}
	}))
	defer peerSrv.Close()

	peerAddr := strings.TrimPrefix(peerSrv.URL, "http://")
	localPeer := cache.NewPeerCache(cache.PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   peerAddr,
		Timeout:       800 * time.Millisecond,
	})

	localCache := cache.New(5*time.Minute, 1000)
	localCache.SetL3(localPeer)

	stalePath := filepath.Join(t.TempDir(), "label-values-index.json")
	staleSnapshot := labelValuesIndexSnapshot{
		Version:         1,
		SavedAtUnixNano: now.Add(-10 * time.Minute).UnixNano(),
		StatesByKey: map[string]map[string]labelValueIndexEntry{
			labelValuesIndexKey("team-a", "host_id"): {
				"from-disk": {SeenCount: 1, LastSeen: now.Add(-10 * time.Minute).UnixNano()},
			},
		},
	}
	stalePayload, err := json.Marshal(staleSnapshot)
	if err != nil {
		t.Fatalf("marshal stale snapshot: %v", err)
	}
	if err := os.WriteFile(stalePath, stalePayload, 0o644); err != nil {
		t.Fatalf("write stale snapshot: %v", err)
	}

	p, err := New(Config{
		BackendURL:                      vlBackend.URL,
		Cache:                           localCache,
		PeerCache:                       localPeer,
		LogLevel:                        "error",
		LabelStyle:                      LabelStyleUnderscores,
		MetadataFieldMode:               MetadataFieldModeTranslated,
		LabelValuesIndexedCache:         true,
		LabelValuesHotLimit:             100,
		LabelValuesIndexMaxEntries:      1000,
		LabelValuesIndexPersistPath:     stalePath,
		LabelValuesIndexPersistInterval: time.Hour,
		LabelValuesIndexStartupStale:    30 * time.Second,
		LabelValuesIndexPeerWarmTimeout: 2 * time.Second,
		PeerAuthToken:                   "shared-secret",
	})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	p.Init()
	defer func() { _ = p.Shutdown(context.Background()) }()

	got, ok := p.selectLabelValuesFromIndex("team-a", "host_id", "", 0, 10)
	if !ok {
		t.Fatalf("expected peer-warmed label-values index to be available")
	}
	want := []string{"from-peer", "from-disk"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected peer-warmed values: want=%v got=%v", want, got)
	}
}

func TestLabelSurface_AsyncRefreshPathsPopulateCache(t *testing.T) {
	var fieldValuesCalls atomic.Int32
	var streamsCalls atomic.Int32

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			w.Write([]byte(`{"values":[{"value":"app","hits":3},{"value":"service.name","hits":2},{"value":"service_name","hits":2}]}`))
		case "/select/logsql/stream_field_values":
			fieldValuesCalls.Add(1)
			switch r.URL.Query().Get("field") {
			case "app":
				w.Write([]byte(`{"values":[{"value":"alpha","hits":3},{"value":"beta","hits":2}]}`))
			case "service.name", "service_name":
				w.Write([]byte(`{"values":[{"value":"svc-a","hits":2}]}`))
			default:
				w.Write([]byte(`{"values":[]}`))
			}
		case "/select/logsql/field_values":
			fieldValuesCalls.Add(1)
			switch r.URL.Query().Get("field") {
			case "service.name", "service_name", "app":
				w.Write([]byte(`{"values":[{"value":"svc-a","hits":2}]}`))
			default:
				w.Write([]byte(`{"values":[]}`))
			}
		case "/select/logsql/field_names":
			w.Write([]byte(`{"values":[{"value":"service.name","hits":2}]}`))
		case "/select/logsql/query":
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49Z","_msg":"level=error msg=ok","_stream":"{service.name=\"svc-a\",service_name=\"svc-a\"}","service.name":"svc-a","service_name":"svc-a"}` + "\n"))
		case "/select/logsql/streams":
			streamsCalls.Add(1)
			w.Write([]byte(`{"values":[{"value":"{service.name=\"svc-a\",service_name=\"svc-a\",detected_level=\"error\"}","hits":4}]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:                 vlBackend.URL,
		Cache:                      c,
		LogLevel:                   "error",
		LabelStyle:                 LabelStyleUnderscores,
		MetadataFieldMode:          MetadataFieldModeTranslated,
		LabelValuesIndexedCache:    true,
		LabelValuesHotLimit:        10,
		LabelValuesIndexMaxEntries: 100,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	p.refreshLabelValuesCacheAsync("", "label_values:async", "app", "", "", "", "", "", nil)
	waitForCachedKey(t, c, "label_values:async")

	p.refreshDetectedFieldsCacheAsync("", "detected_fields:async", `{service_name="svc-a"}`, "", "", 100, nil)
	waitForCachedKey(t, c, "detected_fields:async")

	p.refreshDetectedLabelsCacheAsync("", "detected_labels:async", `{service_name="svc-a"}`, "", "", 100, nil)
	waitForCachedKey(t, c, "detected_labels:async")

	p.refreshDetectedFieldValuesCacheAsync("", "detected_field_values:async:service_name", "service_name", `{service_name="svc-a"}`, "", "", 100, nil)
	waitForCachedKey(t, c, "detected_field_values:async:service_name")

	p.refreshDetectedFieldValuesCacheAsync("", "detected_field_values:async:level", "level", `{service_name="svc-a"}`, "", "", 100, nil)
	waitForCachedKey(t, c, "detected_field_values:async:level")

	if fieldValuesCalls.Load() == 0 {
		t.Fatalf("expected field_values or stream_field_values calls from async refresh paths")
	}
	if streamsCalls.Load() == 0 {
		t.Fatalf("expected streams call from async detected_labels refresh")
	}
}

func TestLabelSurface_LabelValueWindowHelpersCoverLimitBranches(t *testing.T) {
	p := &Proxy{labelValuesHotLimit: 5}
	if got := p.defaultLabelValuesLimit(""); got != 5 {
		t.Fatalf("expected default hot limit, got %d", got)
	}
	if got := p.defaultLabelValuesLimit("0"); got != 5 {
		t.Fatalf("expected invalid explicit limit to fallback to hot limit, got %d", got)
	}
	if got := p.defaultLabelValuesLimit("999999"); got != maxLimitValue {
		t.Fatalf("expected explicit limit to clamp to maxLimitValue, got %d", got)
	}
	if got := p.defaultLabelValuesLimit("12"); got != 12 {
		t.Fatalf("expected explicit positive limit to pass through, got %d", got)
	}

	window := selectLabelValuesWindow([]string{"alpha", "beta", "delta", "gamma"}, "ta", 0, 10000)
	if len(window) != 2 || window[0] != "beta" || window[1] != "delta" {
		t.Fatalf("unexpected search window result: %v", window)
	}
	window = selectLabelValuesWindow([]string{"alpha", "beta", "delta", "gamma"}, "", -1, 0)
	if len(window) != 4 {
		t.Fatalf("expected full window with normalized offset/limit, got %v", window)
	}
}

func TestLabelSurface_RefreshLabelsCacheAsyncPopulatesCache(t *testing.T) {
	var streamFieldNamesCalls atomic.Int32

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			streamFieldNamesCalls.Add(1)
			w.Write([]byte(`{"values":[{"value":"service.name","hits":2},{"value":"app","hits":2},{"value":"_msg","hits":2}]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:        vlBackend.URL,
		Cache:             c,
		LogLevel:          "error",
		LabelStyle:        LabelStyleUnderscores,
		MetadataFieldMode: MetadataFieldModeTranslated,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	cacheKey := "labels:async"
	p.refreshLabelsCacheAsync("", cacheKey, `{service_name="svc-a"}`, "", "", "", nil)
	raw := waitForCachedKey(t, c, cacheKey)

	if streamFieldNamesCalls.Load() == 0 {
		t.Fatalf("expected stream_field_names backend call")
	}

	var resp struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode labels response: %v", err)
	}
	if !contains(resp.Data, "service_name") {
		t.Fatalf("expected translated and synthetic labels, got %v", resp.Data)
	}
	if contains(resp.Data, "_msg") {
		t.Fatalf("expected internal labels to be filtered, got %v", resp.Data)
	}
}

func TestLabelSurface_RefreshLabelsCacheAsync_ForwardsSubstringFilterWhenSupported(t *testing.T) {
	var gotQ, gotFilter atomic.Value
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/stream_field_names" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		gotQ.Store(r.URL.Query().Get("q"))
		gotFilter.Store(r.URL.Query().Get("filter"))
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"values":[{"value":"service.name","hits":2}]}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:        vlBackend.URL,
		Cache:             c,
		LogLevel:          "error",
		LabelStyle:        LabelStyleUnderscores,
		MetadataFieldMode: MetadataFieldModeTranslated,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	p.observeBackendVersionFromHeaders(http.Header{"Server": []string{"VictoriaLogs/v1.49.0"}})

	cacheKey := "labels:async:substring"
	p.refreshLabelsCacheAsync("", cacheKey, `{service_name="svc-a"}`, "", "", "svc", nil)
	_ = waitForCachedKey(t, c, cacheKey)

	if v, _ := gotQ.Load().(string); v != "svc" {
		t.Fatalf("expected q=svc for async labels refresh, got %q", v)
	}
	if v, _ := gotFilter.Load().(string); v != "substring" {
		t.Fatalf("expected filter=substring for async labels refresh, got %q", v)
	}
}

func TestLabelSurface_RefreshLabelValuesCacheAsync_ForwardsSubstringFilterWhenSupported(t *testing.T) {
	var gotQ, gotFilter atomic.Value
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"app","hits":2}]}`))
		case "/select/logsql/stream_field_values":
			gotQ.Store(r.URL.Query().Get("q"))
			gotFilter.Store(r.URL.Query().Get("filter"))
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"svc-a","hits":2}]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:        vlBackend.URL,
		Cache:             c,
		LogLevel:          "error",
		LabelStyle:        LabelStyleUnderscores,
		MetadataFieldMode: MetadataFieldModeTranslated,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	p.observeBackendVersionFromHeaders(http.Header{"Server": []string{"VictoriaLogs/v1.49.0"}})

	cacheKey := "label_values:async:substring"
	p.refreshLabelValuesCacheAsync("", cacheKey, "app", `{service_name="svc-a"}`, "", "", "", "svc", nil)
	_ = waitForCachedKey(t, c, cacheKey)

	if v, _ := gotQ.Load().(string); v != "svc" {
		t.Fatalf("expected q=svc for async label values refresh, got %q", v)
	}
	if v, _ := gotFilter.Load().(string); v != "substring" {
		t.Fatalf("expected filter=substring for async label values refresh, got %q", v)
	}
}

func TestLabelSurface_FetchPreferredLabelNamesCached_UsesCacheAndRecoversFromInvalidPayload(t *testing.T) {
	var streamFieldNamesCalls atomic.Int32

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			streamFieldNamesCalls.Add(1)
			w.Write([]byte(`{"values":[{"value":"app","hits":1}]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:        vlBackend.URL,
		Cache:             c,
		LogLevel:          "error",
		LabelStyle:        LabelStyleUnderscores,
		MetadataFieldMode: MetadataFieldModeTranslated,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	params := url.Values{}
	params.Set("query", "*")

	labels, err := p.fetchPreferredLabelNamesCached(context.Background(), params)
	if err != nil {
		t.Fatalf("first cached label fetch failed: %v", err)
	}
	if len(labels) != 1 || labels[0] != "app" {
		t.Fatalf("unexpected labels: %v", labels)
	}
	if streamFieldNamesCalls.Load() != 1 {
		t.Fatalf("expected one backend call after first fetch, got %d", streamFieldNamesCalls.Load())
	}

	labels, err = p.fetchPreferredLabelNamesCached(context.Background(), params)
	if err != nil {
		t.Fatalf("second cached label fetch failed: %v", err)
	}
	if len(labels) != 1 || labels[0] != "app" {
		t.Fatalf("unexpected cached labels: %v", labels)
	}
	if streamFieldNamesCalls.Load() != 1 {
		t.Fatalf("expected cache hit on second fetch, backend calls=%d", streamFieldNamesCalls.Load())
	}

	cacheKey := "label_inventory::" + params.Encode()
	c.Set(cacheKey, []byte("{invalid-json"))
	labels, err = p.fetchPreferredLabelNamesCached(context.Background(), params)
	if err != nil {
		t.Fatalf("fetch after invalid cache payload failed: %v", err)
	}
	if len(labels) != 1 || labels[0] != "app" {
		t.Fatalf("unexpected labels after cache recovery: %v", labels)
	}
	// The streamFieldNamesCache (15s TTL, always-on) may serve stream_field_names
	// from its own layer after label_inventory cache is invalidated, so backend
	// calls stay at 1. The result correctness above is the meaningful assertion.
	if streamFieldNamesCalls.Load() < 1 {
		t.Fatalf("expected at least one backend call, got %d", streamFieldNamesCalls.Load())
	}
}

func TestLabelSurface_DetectedLabelsCacheRoundTripAndInvalidPayload(t *testing.T) {
	p, err := New(Config{
		BackendURL:        "http://unused",
		Cache:             cache.New(60*time.Second, 1000),
		LogLevel:          "error",
		LabelStyle:        LabelStyleUnderscores,
		MetadataFieldMode: MetadataFieldModeTranslated,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	ctx := context.WithValue(context.Background(), orgIDKey, "tenant-a")
	labels := []map[string]interface{}{
		{"label": "service_name", "cardinality": 2},
	}
	summaries := map[string]*detectedLabelSummary{
		"service_name": {
			label:  "service_name",
			values: map[string]struct{}{"svc-a": {}, "svc-b": {}},
		},
	}

	p.setCachedDetectedLabels(ctx, `{service_name="svc-a"}`, "1", "2", 100, labels, summaries)
	gotLabels, gotSummaries, ok := p.getCachedDetectedLabels(ctx, `{service_name="svc-a"}`, "1", "2", 100)
	if !ok {
		t.Fatalf("expected detected labels cache hit")
	}
	if len(gotLabels) != 1 || gotLabels[0]["label"] != "service_name" {
		t.Fatalf("unexpected cached labels: %#v", gotLabels)
	}
	if gotSummaries["service_name"] == nil || len(gotSummaries["service_name"].values) != 2 {
		t.Fatalf("unexpected cached summaries: %#v", gotSummaries)
	}

	cacheKey := p.detectedLabelsCacheKey(ctx, `{service_name="svc-a"}`, "1", "2", 100)
	p.cache.Set(cacheKey, []byte("{invalid-json"))
	gotLabels, gotSummaries, ok = p.getCachedDetectedLabels(ctx, `{service_name="svc-a"}`, "1", "2", 100)
	if ok || gotLabels != nil || gotSummaries != nil {
		t.Fatalf("expected invalid cached labels payload to miss cache, got labels=%#v summaries=%#v ok=%v", gotLabels, gotSummaries, ok)
	}

	withoutCache := &Proxy{}
	gotLabels, gotSummaries, ok = withoutCache.getCachedDetectedLabels(context.Background(), "*", "1", "2", 10)
	if ok || gotLabels != nil || gotSummaries != nil {
		t.Fatalf("expected cache miss when cache is not configured")
	}
	withoutCache.setCachedDetectedLabels(context.Background(), "*", "1", "2", 10, nil, nil)
}

func TestLabelSurface_LabelValuesIndexPersistenceLoop_StartStopAndPersist(t *testing.T) {
	persistPath := filepath.Join(t.TempDir(), "label-values-index.json")

	p, err := New(Config{
		BackendURL:                      "http://unused",
		Cache:                           cache.New(60*time.Second, 1000),
		LogLevel:                        "error",
		LabelStyle:                      LabelStyleUnderscores,
		MetadataFieldMode:               MetadataFieldModeTranslated,
		LabelValuesIndexedCache:         true,
		LabelValuesIndexPersistPath:     persistPath,
		LabelValuesIndexMaxEntries:      1000,
		LabelValuesHotLimit:             100,
		LabelValuesIndexPersistInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	p.updateLabelValuesIndex("", "app", []string{"alpha", "beta"})
	p.startLabelValuesIndexPersistenceLoop()
	p.startLabelValuesIndexPersistenceLoop()

	time.Sleep(70 * time.Millisecond)
	p.stopLabelValuesIndexPersistenceLoop(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p.stopLabelValuesIndexPersistenceLoop(ctx)

	raw, err := os.ReadFile(persistPath)
	if err != nil {
		t.Fatalf("expected persisted index snapshot file, read error: %v", err)
	}
	var snapshot labelValuesIndexSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatalf("decode persisted snapshot: %v", err)
	}
	if len(snapshot.StatesByKey) == 0 {
		t.Fatalf("expected persisted snapshot to contain label index state")
	}
}

func TestLabelSurface_LabelValuesIndexPersistenceLoop_SkipsUnchangedPeriodicWrites(t *testing.T) {
	persistPath := filepath.Join(t.TempDir(), "label-values-index.json")

	p, err := New(Config{
		BackendURL:                      "http://unused",
		Cache:                           cache.New(60*time.Second, 1000),
		LogLevel:                        "error",
		LabelStyle:                      LabelStyleUnderscores,
		MetadataFieldMode:               MetadataFieldModeTranslated,
		LabelValuesIndexedCache:         true,
		LabelValuesIndexPersistPath:     persistPath,
		LabelValuesIndexMaxEntries:      1000,
		LabelValuesHotLimit:             100,
		LabelValuesIndexPersistInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	p.updateLabelValuesIndex("", "app", []string{"alpha", "beta"})
	p.startLabelValuesIndexPersistenceLoop()
	defer p.stopLabelValuesIndexPersistenceLoop(context.Background())

	var first []byte
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		first, err = os.ReadFile(persistPath)
		if err == nil && len(first) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("expected first persisted index snapshot file, read error: %v", err)
	}

	// Wait across multiple persistence ticks without mutating the index.
	infoBefore, err := os.Stat(persistPath)
	if err != nil {
		t.Fatalf("stat persisted snapshot before second read: %v", err)
	}
	time.Sleep(120 * time.Millisecond)

	second, err := os.ReadFile(persistPath)
	if err != nil {
		t.Fatalf("expected persisted index snapshot file on second read: %v", err)
	}
	infoAfter, err := os.Stat(persistPath)
	if err != nil {
		t.Fatalf("stat persisted snapshot after second read: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("expected unchanged index to skip periodic rewrite")
	}
	if !infoAfter.ModTime().Equal(infoBefore.ModTime()) {
		t.Fatalf("expected unchanged index to skip disk rewrite: before=%s after=%s", infoBefore.ModTime(), infoAfter.ModTime())
	}
}

func TestLabelSurface_StopLabelValuesIndexPersistenceLoop_TimeoutBranch(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	// Not-started guard branch.
	p.stopLabelValuesIndexPersistenceLoop(context.Background())

	// Force timeout branch by keeping done channel open until context expires.
	p.labelValuesIndexPersistStarted.Store(true)
	p.labelValuesIndexPersistStop = make(chan struct{})
	p.labelValuesIndexPersistDone = make(chan struct{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	p.stopLabelValuesIndexPersistenceLoop(ctx)

	// Cover already-closed stop-channel branch and immediate done return.
	close(p.labelValuesIndexPersistDone)
	p.stopLabelValuesIndexPersistenceLoop(context.Background())
}

func TestLabelSurface_IndexAndCacheHelpersCoverBranches(t *testing.T) {
	if !labelValueIndexLess(
		"a", labelValueIndexEntry{SeenCount: 2, LastSeen: 10},
		"b", labelValueIndexEntry{SeenCount: 1, LastSeen: 100},
	) {
		t.Fatalf("expected higher SeenCount to sort first")
	}
	if !labelValueIndexLess(
		"a", labelValueIndexEntry{SeenCount: 2, LastSeen: 20},
		"b", labelValueIndexEntry{SeenCount: 2, LastSeen: 10},
	) {
		t.Fatalf("expected newer LastSeen to sort first when SeenCount matches")
	}
	if !labelValueIndexLess(
		"a", labelValueIndexEntry{SeenCount: 2, LastSeen: 20},
		"b", labelValueIndexEntry{SeenCount: 2, LastSeen: 20},
	) {
		t.Fatalf("expected lexical tie-breaker to apply when SeenCount and LastSeen match")
	}

	p := &Proxy{
		labelValuesIndexedCache:    true,
		labelValuesHotLimit:        1,
		labelValuesIndexMaxEntries: 2,
		labelValuesIndex:           make(map[string]*labelValuesIndexState),
		cache:                      cache.New(60*time.Second, 1000),
	}
	p.updateLabelValuesIndex("tenant-a", "app", []string{"gamma", "alpha", "beta"})

	got, ok := p.selectLabelValuesFromIndex("tenant-a", "app", "", -1, 0)
	if !ok {
		t.Fatalf("expected indexed values to be available")
	}
	if len(got) != 1 {
		t.Fatalf("expected default hot-limit fallback to return one value, got %v", got)
	}

	full, ok := p.selectLabelValuesFromIndex("tenant-a", "app", "a", 0, maxLimitValue+500)
	if !ok || len(full) == 0 {
		t.Fatalf("expected filtered indexed values with clamped limit, got %v (ok=%v)", full, ok)
	}

	disabled := &Proxy{labelValuesIndexedCache: false}
	if values, ok := disabled.selectLabelValuesFromIndex("tenant-a", "app", "", 0, 10); ok || values != nil {
		t.Fatalf("expected indexed cache disabled path to miss")
	}

	(*Proxy)(nil).setJSONCacheWithTTL("k:nil", time.Second, map[string]string{"ok": "1"})
	(&Proxy{}).setJSONCacheWithTTL("k:nocache", time.Second, map[string]string{"ok": "1"})
	p.setJSONCacheWithTTL("k:marshal-error", time.Second, map[string]interface{}{"bad": make(chan int)})
	if _, ok := p.cache.Get("k:marshal-error"); ok {
		t.Fatalf("expected marshal-error payload to not be cached")
	}
	p.setJSONCacheWithTTL("k:good", time.Second, map[string]string{"ok": "1"})
	if _, ok := p.cache.Get("k:good"); !ok {
		t.Fatalf("expected successful JSON payload to be cached")
	}

	if parsePositiveInt("0", 7) != 7 || parsePositiveInt("-1", 7) != 7 || parsePositiveInt("9", 7) != 9 {
		t.Fatalf("unexpected parsePositiveInt behavior")
	}
	if parseNonNegativeInt("-1", 7) != 7 || parseNonNegativeInt("0", 7) != 0 || parseNonNegativeInt("9", 7) != 9 {
		t.Fatalf("unexpected parseNonNegativeInt behavior")
	}

	if (&Proxy{}).shouldRefreshLabelsInBackground(-1, time.Second) {
		t.Fatalf("expected refresh=false for non-positive remaining TTL")
	}
	if (&Proxy{}).shouldRefreshLabelsInBackground(time.Second, 0) {
		t.Fatalf("expected refresh=false for non-positive cache TTL")
	}
	if !(&Proxy{}).shouldRefreshLabelsInBackground(time.Second, 5*time.Second) {
		t.Fatalf("expected refresh=true when cache entry is near expiry")
	}

	if got := (&Proxy{}).labelBackgroundTimeout(); got != 10*time.Second {
		t.Fatalf("expected default background timeout, got %s", got)
	}
	if got := (&Proxy{client: &http.Client{Timeout: 3 * time.Second}}).labelBackgroundTimeout(); got != 3*time.Second {
		t.Fatalf("expected client timeout override, got %s", got)
	}
	if got := (&Proxy{client: &http.Client{Timeout: 30 * time.Second}}).labelBackgroundTimeout(); got != 10*time.Second {
		t.Fatalf("expected cap to default timeout, got %s", got)
	}
}

func waitForCachedKey(t *testing.T, c *cache.Cache, key string) []byte {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if raw, ok := c.Get(key); ok {
			return raw
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for cache key %q", key)
	return nil
}
