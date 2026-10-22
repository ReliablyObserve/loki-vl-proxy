package proxy

import "testing"

// storedLevelSeries labels each series with the level its stored field
// decides, on a copy of the series' labels: the answer it was given keeps its
// own labels, and a stored detected_level hands the series to the chain.
func TestStoredLevelSeriesLabelsACopy(t *testing.T) {
	in := map[string]manualSeriesSamples{
		"a": {Metric: map[string]string{"app": "api", storedLevelField: "WARN"}},
		"b": {Metric: map[string]string{"app": "api", storedLevelField: "info"}},
	}
	out, ok := storedLevelSeries(in, nil)
	if !ok || len(out) != len(in) {
		t.Fatalf("got %d series, ok=%v; want %d, true", len(out), ok, len(in))
	}
	want := map[string]string{"a": "warn", "b": "info"}
	for key, entry := range out {
		stored := in[key].Metric[storedLevelField]
		if got := entry.Metric["detected_level"]; got != want[key] {
			t.Errorf("%s: detected_level %q, want %q", key, got, want[key])
		}
		if entry.Metric["app"] != "api" || entry.Metric[storedLevelField] != stored {
			t.Errorf("%s: labels not carried over: %v", key, entry.Metric)
		}
		if _, leaked := in[key].Metric["detected_level"]; leaked {
			t.Errorf("%s: the input series was relabelled in place: %v", key, in[key].Metric)
		}
	}

	in["c"] = manualSeriesSamples{Metric: map[string]string{storedLevelField: "info", storedDetectedLevelField: "Custom"}}
	if _, ok := storedLevelSeries(in, nil); ok {
		t.Fatal("a stored detected_level must leave the series to the chain")
	}
}
