package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestDrilldownHelpers_AdditionalCoverage(t *testing.T) {
	t.Run("synthetic service name and detected level", func(t *testing.T) {
		ensureSyntheticServiceName(nil)

		labels := map[string]string{"app": "api"}
		ensureSyntheticServiceName(labels)
		if got := labels["service_name"]; got != "api" {
			t.Fatalf("expected synthetic service_name from app, got %q", got)
		}

		labels["service_name"] = "frontend"
		ensureSyntheticServiceName(labels)
		if got := labels["service_name"]; got != "frontend" {
			t.Fatalf("expected existing service_name to win, got %q", got)
		}

		levelLabels := map[string]string{"level": "warn"}
		ensureDetectedLevel(levelLabels)
		if got := levelLabels["detected_level"]; got != "warn" {
			t.Fatalf("expected detected_level from level, got %q", got)
		}
	})

	t.Run("structured field exposure and detected field accumulation", func(t *testing.T) {
		lt := NewLabelTranslator(LabelStyleUnderscores, []FieldMapping{{VLField: "service.name", LokiLabel: "service_name"}})

		if shouldExposeStructuredField("_time", nil, lt) {
			t.Fatal("expected internal field to be hidden")
		}
		if !shouldExposeStructuredField("service.name", map[string]string{"service.name": "api"}, lt) {
			t.Fatal("expected dotted field to remain exposed")
		}
		if shouldExposeStructuredField("service_name", map[string]string{"service_name": "api"}, nil) {
			t.Fatal("expected passthrough structured field to stay hidden without translator")
		}
		if !shouldExposeStructuredField("custom", map[string]string{}, lt) {
			t.Fatal("expected unknown structured field to be exposed")
		}

		fields := map[string]*detectedFieldSummary{}
		addDetectedField(fields, "", "", "string", nil, "ignored")
		addDetectedField(fields, "duration", "json", "int", []string{"duration"}, "10")
		addDetectedField(fields, "duration", "logfmt", "string", nil, "ten")

		summary := fields["duration"]
		if summary == nil {
			t.Fatal("expected detected field summary")
		}
		if summary.typ != "string" {
			t.Fatalf("expected detected type to widen to string, got %q", summary.typ)
		}
		if len(summary.parsers) != 2 {
			t.Fatalf("expected two parser sources, got %d", len(summary.parsers))
		}
		if len(summary.jsonPath) != 1 || summary.jsonPath[0] != "duration" {
			t.Fatalf("expected original json path to be preserved, got %#v", summary.jsonPath)
		}
		if got := asString(map[string]any{"line": "hello\nworld"}); !strings.Contains(got, "hello") || strings.Contains(got, "\n") {
			t.Fatalf("expected asString to flatten JSON, got %q", got)
		}
	})

	t.Run("selector and query helpers", func(t *testing.T) {
		if got := appendSyntheticLabels([]string{"app", "app"}); len(got) != 2 || got[1] != "service_name" {
			t.Fatalf("unexpected synthetic labels %#v", got)
		}
		if got := streamSelectorPrefix(`{app="api|web"} |= "error"`); got != `{app="api|web"}` {
			t.Fatalf("unexpected selector prefix %q", got)
		}
		if got := inferPrimaryTargetLabel(`{namespace="prod",pod=~"api-.*"} |= "error"`); got != "namespace" {
			t.Fatalf("unexpected primary target label %q", got)
		}
		if got := normalizeBareSelectorQuery(`app="api"`); got != `{app="api"}` {
			t.Fatalf("expected bare selector to be wrapped, got %q", got)
		}
		if got := normalizeBareSelectorQuery(`{app="api"} |= "error"`); got != `{app="api"} |= "error"` {
			t.Fatalf("expected LogQL pipeline query to remain unchanged, got %q", got)
		}

		dst := map[string]*detectedLabelSummary{
			"service_name": {label: "service_name", values: map[string]struct{}{"api": {}}},
		}
		scanned := map[string]*detectedLabelSummary{
			"level": {label: "level", values: map[string]struct{}{"error": {}}},
		}
		mergeDetectedLabelSupplements(dst, scanned)
		if dst["level"] == nil {
			t.Fatal("expected level supplement to be merged")
		}
	})
}

func TestDrilldownBackendHelpers_AdditionalCoverage(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/streams":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"{service_name=\"api\"}","hits":2},{"value":"{app=\"worker\"}","hits":1},{"value":"{service_name=\"api\"}","hits":1}]}`))
		case "/select/logsql/field_values":
			if r.URL.Query().Get("field") == "broken" {
				http.Error(w, "backend boom", http.StatusBadGateway)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"zulu","hits":1},{"value":"alpha","hits":2}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	t.Cleanup(func() { p.cache.Close() })
	p.translationCache = cache.New(5, 10)
	t.Cleanup(func() { p.translationCache.Close() })

	values, err := p.serviceNameValues(context.Background(), `{app="api"}`, "", "")
	if err != nil {
		t.Fatalf("serviceNameValues returned error: %v", err)
	}
	if strings.Join(values, ",") != "api,worker" {
		t.Fatalf("unexpected service names %v", values)
	}

	fieldValues, err := p.fetchNativeFieldValues(context.Background(), `{app="api"}`, "", "", "service.name", 2)
	if err != nil {
		t.Fatalf("fetchNativeFieldValues returned error: %v", err)
	}
	if strings.Join(fieldValues, ",") != "alpha,zulu" {
		t.Fatalf("unexpected field values %v", fieldValues)
	}

	if _, err := p.fetchNativeFieldValues(context.Background(), `{app="api"}`, "", "", "broken", 0); err == nil || !strings.Contains(err.Error(), "backend boom") {
		t.Fatalf("expected backend error to surface, got %v", err)
	}
}
