package proxy

import (
	"net/http"
	"strconv"
	"time"
)

// labelPointWindowMax is the widest /labels window read as a point. Grafana asks for label names with start equal
// to end at the clicked log row when it builds that row's "Show context" query, and adds the row's structured
// metadata labels (detected_level, for one) to the query only when they are in the answer.
const labelPointWindowMax = time.Minute

// labelIngesterWindow is how far back Loki asks its ingesters for label names (query_ingesters_within, 3h by
// default). The ingesters answer by stream, not by time: every stream they hold is listed.
const labelIngesterWindow = 3 * time.Hour

// widenLabelPointWindow returns the window to list for a point request. Loki does not list label names for the
// requested instants. On a live stack its answer for a window of 0 ms, 1 ms, 1 s and 5 min is one answer: a point
// within the last three hours lists every stream the ingesters hold (a stream written after the instant too), and
// an older point lists the index period (a day) it falls in, even hours after a rare label's last line. A point
// window listed from its own instants holds only the streams written at that moment, so a label on another stream
// (the level a row carries as detected_level, or a sparse one) is missing, and Grafana, which adds a clicked row's
// structured metadata to its "Show context" query only for labels in this answer, builds a different query.
//
// A point within the last three hours lists the last three hours, which is where the streams the ingesters hold
// were written. An older point lists the UTC day of its end, so every point of one past day shares one window and
// one cached answer. A window of a minute or more, an unparseable bound and a request without both bounds keep
// their own window.
func widenLabelPointWindow(startRaw, endRaw string, now time.Time) (string, string, bool) {
	if startRaw == "" || endRaw == "" {
		return "", "", false
	}
	startNs, endNs, parsed := parseLokiTimeRangeToUnixNano(startRaw, endRaw)
	if !parsed || time.Duration(endNs-startNs) >= labelPointWindowMax {
		return "", "", false
	}
	now = now.UTC()
	anchor := time.Unix(0, endNs).UTC()
	if anchor.After(now) {
		// Loki answers an instant after now like a recent one.
		anchor = now
	}
	if now.Sub(anchor) <= labelIngesterWindow {
		return strconv.FormatInt(now.Add(-labelIngesterWindow).UnixNano(), 10), strconv.FormatInt(now.UnixNano(), 10), true
	}
	day := anchor.Truncate(24 * time.Hour)
	return strconv.FormatInt(day.UnixNano(), 10), strconv.FormatInt(day.Add(24*time.Hour).UnixNano(), 10), true
}

// withWidenedLabelPoint is the request with its point window replaced (see widenLabelPointWindow), or the request
// itself. It runs before the tenant fan-out and the cache key, so every read of the request sees one window.
func withWidenedLabelPoint(r *http.Request, now time.Time) *http.Request {
	query := r.URL.Query()
	start, end, ok := widenLabelPointWindow(query.Get("start"), query.Get("end"), now)
	if !ok {
		return r
	}
	query.Set("start", start)
	query.Set("end", end)
	widened := r.Clone(r.Context())
	widened.URL.RawQuery = query.Encode()
	widened.Form, widened.PostForm = nil, nil
	return widened
}
