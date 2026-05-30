package proxy

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

func TestQueryTranslation_TranslateOTelFalse_StreamSelectorPreservesUnderscores(t *testing.T) {
	lt := NewLabelTranslator(LabelStyleUnderscores, nil)
	lt.translateOTel = false

	got, err := translator.TranslateLogQLWithCapabilities(
		`{k8s_app="api-notifications-chat",k8s_container_name="notificationgo-chat"}`,
		lt.ToVL,
		nil,
		logsql.Capabilities{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, `k8s_container_name:=`) {
		t.Errorf("output missing k8s_container_name:=; got %q", got)
	}
	if strings.Contains(got, `"k8s.container.name":=`) {
		t.Errorf("output unexpectedly contains dotted OTel field; got %q", got)
	}
}

func TestQueryTranslation_TranslateOTelTrue_DefaultBehavior(t *testing.T) {
	lt := NewLabelTranslator(LabelStyleUnderscores, nil)
	lt.translateOTel = true

	got, err := translator.TranslateLogQLWithCapabilities(
		`{k8s_app="api-notifications-chat",k8s_container_name="notificationgo-chat"}`,
		lt.ToVL,
		nil,
		logsql.Capabilities{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, `"k8s.container.name":=`) {
		t.Errorf("output missing dotted OTel field; got %q", got)
	}
}
