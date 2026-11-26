package metrics

import (
	"bytes"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
)

// The shipped dashboards' Cache efficiency row reads only the families and
// labels the exporters write, and shows every family.
//
// conformance: cache-eviction-signals
func TestCacheEfficiencyDashboardRowMatchesTheExporters(t *testing.T) {
	paths := []string{"../../charts/loki-vl-proxy/dashboards/loki-vl-proxy.json", "../../dashboard/loki-vl-proxy.json"}
	var contents [][]byte
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		contents = append(contents, raw)
	}
	if !bytes.Equal(contents[0], contents[1]) {
		t.Fatal("the chart and the repository dashboard differ")
	}
	var dashboard struct {
		Panels []struct {
			ID      int    `json:"id"`
			Type    string `json:"type"`
			Title   string `json:"title"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(contents[0], &dashboard); err != nil {
		t.Fatal(err)
	}
	ids := map[int]bool{}
	row := -1
	for i, panel := range dashboard.Panels {
		if ids[panel.ID] {
			t.Fatalf("duplicate panel id %d", panel.ID)
		}
		ids[panel.ID] = true
		if panel.Type == "row" && panel.Title == "Cache efficiency" {
			row = i
		}
	}
	if row < 0 {
		t.Fatal("no Cache efficiency row")
	}
	metricName := regexp.MustCompile(`loki_vl_proxy_cache_[a-z0-9_]+`)
	grouping := regexp.MustCompile(`by \(([^)]*)\)`)
	shown := map[string]bool{}
	for _, panel := range dashboard.Panels[row+1:] {
		if panel.Type == "row" {
			break
		}
		for _, target := range panel.Targets {
			for _, name := range metricName.FindAllString(target.Expr, -1) {
				family := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, "_bucket"), "_sum"), "_count")
				if _, ok := signalFamilies[family]; !ok {
					t.Fatalf("panel %q reads %s, which the signal exporters do not write", panel.Title, name)
				}
				shown[family] = true
			}
			for _, group := range grouping.FindAllStringSubmatch(target.Expr, -1) {
				for _, label := range strings.Split(group[1], ",") {
					if label = strings.TrimSpace(label); !signalSampleLabels[label] {
						t.Fatalf("panel %q groups by %q, not an exported label", panel.Title, label)
					}
				}
			}
		}
	}
	for family := range signalFamilies {
		if !shown[family] {
			t.Errorf("no panel shows %s", family)
		}
	}
}
