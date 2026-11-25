package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The bare `logfmt | json` order reads every key of every row, so a key whose name Loki turns into the extracted
// label's name, or a name the evaluator does not admit, changes what a row means. The class guards those spellings
// per row, in the message text (a quoted run of more than 128 characters, an alias of the label or of a prefix of
// it); they are the costliest guards of the native count. VictoriaLogs lists every key of every row's JSON in one call
// (unpack_json | field_names), nested objects flattened with a dot, and that list settles the same question once for
// the window: a name outside the list is in no row. The count runs without the three guards, then the window's names
// are listed again, after the scan, and a name that is not clean sends the request back to the class itself, so a key
// stored while the scan ran can never be counted wrongly. What the list cannot show stays a per-row guard: the
// duplicate-key guard and the closed JSON hazard predicate are unchanged.

const (
	mixedJSONKeyPrefix = "__vlmk_"
	// A window with more distinct JSON key names than this is not certified: the list is bounded, so a window that
	// fills it was not read whole.
	mixedJSONKeyNamesMax = 1024
	// The longest name the class admits (Loki accepts more; longer names keep the exact evaluator).
	mixedJSONKeyNameMax = 128
)

// mixedJSONKeyCore is a key reduced to what Loki's label sanitization can make of it: every run of characters other
// than ASCII letters and digits becomes one underscore, and the ends are trimmed. It is deliberately coarser than
// Loki's own rule (which trims only whitespace and keeps the count of underscores), so it never calls a spelling
// safe that Loki could read as the label.
func mixedJSONKeyCore(name string) string {
	var b strings.Builder
	pending := false
	for _, r := range name {
		if r < 0x80 && (r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
			if pending && b.Len() > 0 {
				b.WriteByte('_')
			}
			pending = false
			b.WriteRune(r)
			continue
		}
		pending = true
	}
	return b.String()
}

// mixedJSONKeyNamesClean reports whether no JSON key of a window can be read as the extracted label by another
// spelling: every name other than the label itself has a core that differs from the label's, is at most 128 bytes
// and is not the name VictoriaLogs gives an empty key (and a key literally named _msg). The label's own name is
// always allowed: the class's duplicate-key guard and its hazard predicate handle rows where it appears twice or
// beside an object of the same prefix. The alias test is what Loki's answers require (TestOrderedMixedStatsKeySetMatchesRecordedLoki
// fails without it); the empty-key, _msg and length tests are stricter than the recorded answers need, and are kept
// because they only ever send a window to the full class.
func mixedJSONKeyNamesClean(label string, names []string) bool {
	want := mixedJSONKeyCore(label)
	if want == "" {
		return false
	}
	for _, name := range names {
		if name == label {
			continue
		}
		if name == "" || name == "_msg" || len(name) > mixedJSONKeyNameMax || mixedJSONKeyCore(name) == want {
			return false
		}
	}
	return true
}

// mixedJSONKeyNames lists the distinct keys of the JSON message of the window's rows, widened as the field names
// call is. ok is false when the list is unavailable or reached its bound.
func (p *Proxy) mixedJSONKeyNames(ctx context.Context, base string, from, to time.Time, bucket time.Duration) ([]string, bool) {
	params := url.Values{}
	params.Set("query", appendMixedClassPipes(base, []logsql.Pipe{
		logsql.PipeUnpackJSON{From: "_msg", ResultPrefix: mixedJSONKeyPrefix},
		logsql.PipeFieldNames{},
		logsql.PipeLimit{N: mixedJSONKeyNamesMax + 1},
	}))
	params.Set("start", from.Add(-bucket).UTC().Format(time.RFC3339Nano))
	params.Set("end", to.Add(bucket).UTC().Format(time.RFC3339Nano))
	resp, err := p.vlGet(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, mixedClassFieldNamesMaxBody+1))
	if err != nil || len(body) > mixedClassFieldNamesMaxBody {
		return nil, false
	}
	var names []string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row struct {
			Name string `json:"name"`
		}
		if json.Unmarshal([]byte(line), &row) != nil {
			return nil, false
		}
		if strings.HasPrefix(row.Name, mixedJSONKeyPrefix) {
			names = append(names, strings.TrimPrefix(row.Name, mixedJSONKeyPrefix))
		}
	}
	if len(names) > mixedJSONKeyNamesMax {
		return nil, false
	}
	return names, true
}

// collectKeyChecked runs the native count with the class that lacks the three key-text guards, when the window's
// JSON key names are clean, and keeps its answer only if they are still clean when listed again after the scan: a
// key stored meanwhile can then never be counted wrongly. done is false when the request must run the class itself:
// the names are not clean, the list is unavailable, or a name appeared during the scan. The first list is remembered
// for the minute (as the certificate's is), so the next request of the window skips it; the list after the scan is
// always read.
func (p *Proxy) collectKeyChecked(ctx context.Context, plan *orderedJSONMetricPlan, base string, class *mixedJSONRowClass, key string, fetchStart, start, end time.Time, step, bucket time.Duration) (series map[string]manualSeriesSamples, served bool, err error, done bool) {
	memoKey := p.mixedCertMemoKey(ctx, base, class.label, "keyset", fetchStart, end)
	names, remembered := []string(nil), false
	if entry, ok := p.mixedCertificates.get(memoKey, time.Now()); ok {
		names, remembered = entry.names, true
	} else {
		var listed bool
		if names, listed = p.mixedJSONKeyNames(ctx, base, fetchStart, end, bucket); !listed {
			p.observeInternalOperation(ctx, "mixed_class_keys", "unlisted", 0)
			return nil, false, nil, false
		}
		p.mixedCertificates.put(memoKey, mixedCertEntry{names: names}, time.Now())
	}
	if !mixedJSONKeyNamesClean(class.label, names) {
		outcome := "dirty"
		if remembered {
			outcome = "cache_dirty"
		}
		p.observeInternalOperation(ctx, "mixed_class_keys", outcome, 0)
		return nil, false, nil, false
	}
	series, served, err = p.collectMixedStatsWithClass(ctx, plan, base, class.withoutKeyText(), key, fetchStart, start, end, step, bucket)
	if err != nil || !served {
		return series, served, err, true
	}
	if again, listed := p.mixedJSONKeyNames(ctx, base, fetchStart, end, bucket); listed && mixedJSONKeyNamesClean(class.label, again) {
		p.observeInternalOperation(ctx, "mixed_class_keys", "certified", 0)
		return series, true, nil, true
	}
	p.mixedCertificates.drop(memoKey)
	p.observeInternalOperation(ctx, "mixed_class_keys", "names_changed", 0)
	return nil, false, nil, false
}
