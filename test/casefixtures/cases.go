package casefixtures

// EmptyCases are the 49 cases the live verifier answered "empty" for (both sides counted no line for the case's
// selectors and window, so nothing was proven): the run of 2026-11-14 against proxy build 58c62d70b. Every one is
// either read by a stream of Streams() or has a reason here why no stream can serve it.
var EmptyCases = []string{
	"drilldown/detected-fields-metadata-reported-as-json",
	"drilldown/detected-labels-lists-stored-field",
	"drilldown/index_volume/end-millisecond-rounding",
	"drilldown/volume-detected-level-not-normalised",
	"profiles/detected-fields-dotted-json-keys",
	"profiles/detected-fields-structured-metadata-beside-json-line",
	"profiles/json-extraction-list-exposes-only-listed",
	"profiles/json-line-stored-without-line",
	"profiles/label-filter-on-line-field",
	"profiles/label-filter-regexp-anchoring",
	"profiles/parsed-key-colliding-with-structured-metadata",
	"profiles/sanitized-json-key-filter",
	"profiles/sanitized-json-key-grouping",
	"profiles/stage-field-exposure",
	"profiles/stream-selector-regexp-anchoring",
	"profiles/structured-metadata-keys-per-profile",
	"quality/labels-backfill-into-cached-bucket",
	"quality/parity-corpus-asymmetric-visual-fixtures",
	"semantics/binary-step-grid",
	"semantics/drop-keep-plain-name-after-parser",
	"semantics/eager-pipeline-validation",
	"semantics/ip-filter-ipv4-after-dot-run",
	"semantics/ip-label-filter-embedded-address",
	"semantics/ip-line-filter-ipv4-like-loki",
	"semantics/label-filter-numeric-equality",
	"semantics/label-filter-numeric-error-label",
	"semantics/line-filter-invalid-utf8",
	"semantics/line-format-template-error-keeps-entry",
	"semantics/metadata-answered-as-parsed-after-extraction-list",
	"semantics/nested-aggregation-outer-by-native-routes",
	"semantics/numeric-label-filter-not-equal-missing-label",
	"semantics/outer-by-over-without-reads-level-as-label",
	"semantics/pattern-capture-before-bracket-text",
	"semantics/plain-name-after-parser-reads-stream-label",
	"semantics/repeated-label-filter-replaced-across-label-format",
	"semantics/request-time-bounds",
	"semantics/scalar-sample-time-units",
	"semantics/sub-second-step-grid",
	"semantics/translator-reads-blank-before-range-call-as-text",
	"semantics/translator-reads-marker-bytes-in-quoted-value",
	"semantics/translator-reads-parenthesized-metric-as-text",
	"semantics/translator-reads-quoted-text-as-syntax",
	"semantics/translator-writes-unpack-as-unpack-json",
	"semantics/two-parsers-keep-one",
	"semantics/window-edge-float64-nanoseconds",
	"semantics/window-wider-than-step-raw-rows",
	"semantics/without-instant-detected-level-unknown",
	"severity/normalisation-table",
	"severity/otlp-severity-before-vl-1.50",
}

// OwnStreamCases are cases that were not empty in that run, whose request now selects a stream of its own (a probe
// strong enough to separate the proxy from Loki needs lines no shared stream holds). They are served like the empty
// cases above and must not be listed as unservable.
var OwnStreamCases = []string{
	"semantics/ip-filter-keeps-parser-error-lines",
	"semantics/logfmt-malformed-lines",
	"semantics/stream-level-shadowed-by-ingest-json-key",
	"semantics/unwrap-sum-without-extracted-label-collision",
	// The case was blocked (it needs a stream no shared stream holds), not empty; its stream makes it a live case.
	"semantics/json-empty-value-label-series",
}

// Unservable are the empty cases no seeded stream can fix, each with the reason. The window is the case, or the
// data is not a stream of lines the seeder can write to both sides.
var Unservable = map[string]string{
	"drilldown/index_volume/end-millisecond-rounding":             "the window is two absolute nanosecond instants (2026-09-15); the case is the rounding of the end's last millisecond, and a relative window cannot keep it",
	"quality/labels-backfill-into-cached-bucket":                  "the case is lines arriving into a window after a first read cached it empty; one seed before the run cannot be a backfill after a cached read",
	"quality/parity-corpus-asymmetric-visual-fixtures":            "the visual stack seeds this service with different data on each side on purpose (a stored level on VictoriaLogs only): data that is not identical on both sides is not comparable, and a seed would have to fake equality",
	"semantics/binary-step-grid":                                  "the window is absolute and in the future (2027-01-15, unix 1800000017): both stores refuse or hold no such data, and the case is its 17 second start phase",
	"semantics/eager-pipeline-validation":                         "the window is 30 days ago and empty on purpose: the case is that a range without data still answers Loki's 200 empty",
	"semantics/outer-by-over-without-reads-level-as-label":        "the window is the absolute hour 2025-12-31T23:00:00Z, outside the stores' retention",
	"semantics/request-time-bounds":                               "the window is two absolute nanosecond instants whose sub-second part is the case",
	"semantics/scalar-sample-time-units":                          "a constant scalar at an absolute instant (unix 1791811168123456789); the precheck counts every stream in the five minutes before it, long outside the stores' retention",
	"semantics/sub-second-step-grid":                              "the window starts at an absolute fractional second (1793653187.704) and its step is 0.2 s: a relative window cannot keep the phase",
	"semantics/window-edge-float64-nanoseconds":                   "the window is absolute with a 0.999 s step on a 600 ms range: the case is the float rounding of those instants",
	"semantics/window-wider-than-step-raw-rows":                   "the window is absolute at a fractional second with a 0.02 s step: the case is the phase of the grid",
	"semantics/translator-reads-marker-bytes-in-quoted-value":     "the label value is the bytes 0x02 0x00 0x02: a NUL byte in structured metadata is unverified on both stores, and a write one side refuses would leave lines on one side inside the shared selector {app=\"x\"} and turn the cases that read it into data_differs",
	"profiles/parsed-key-colliding-with-structured-metadata":      "the case is a structured metadata key on {app=\"x\"} that a parsed key collides with; Loki splits the metric series of every query on that selector by structured metadata and the proxy does not, so the stream turns the fixed nested aggregation case on the same selector into a difference of its own",
	"semantics/metadata-answered-as-parsed-after-extraction-list": "its selector is the fixed json-extraction-list case's; the line that shows its difference (structured metadata msg answered as parsed) turns that case into a difference, and its compare names only the line values, which agree",
	"severity/normalisation-table":                                "a generated corpus row (the query names the placeholder <corpus>): the corpus streams are written by the generator the case names, not a fixed stream",
	"severity/otlp-severity-before-vl-1.50":                       "OTLP records reach Loki and VictoriaLogs by two OTLP endpoints (TestCompat_DetectedLevelQueryRange, ensureDetectedLevelFixture), and the severity each stores is the case; the seeder writes Loki push bodies only",
}
