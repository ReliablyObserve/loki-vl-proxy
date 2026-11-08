//go:build !corpuscapture

package translator

import (
	"encoding/json"
	"os"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// captureSampleTranslations is the translation of the plan corpus's capture sample, to seed the codec's fuzz target.
// A corpus capture run does not read it: it would record each sample row as a test-sent query.
func captureSampleTranslations() []string {
	b, err := os.ReadFile("../querycorpus/testdata/plan-capture-sample.json")
	if err != nil {
		return nil
	}
	var sample struct {
		Rows []struct {
			Query string `json:"q"`
		} `json:"rows"`
	}
	if json.Unmarshal(b, &sample) != nil {
		return nil
	}
	var out []string
	for _, r := range sample.Rows {
		if s, err := TranslateLogQLWithCapabilities(r.Query, nil, nil, logsql.CapabilitiesFor("v1.53.0")); err == nil {
			out = append(out, s)
		}
	}
	return out
}
