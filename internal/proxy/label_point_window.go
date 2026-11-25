package proxy

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

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
// Only a zero-width window (start equal to end) is a point: that is what Grafana sends for a clicked log row when
// it builds the row's "Show context" query, and it adds the row's structured metadata labels (detected_level, for
// one) to that query only when they are in this answer. Loki answers a window of a millisecond, a second or five
// minutes the same way; no client is known to ask that, so those keep their own window and cost nothing here.
//
// A point within the last three hours lists the last three hours, which is where the streams the ingesters hold
// were written. An older point lists the UTC day of its end, so every point of one past day shares one window and
// one cached answer. A window with any width, an unparseable bound and a request without both bounds keep their
// own window.
func widenLabelPointWindow(startRaw, endRaw string, now time.Time) (string, string, bool) {
	if startRaw == "" || endRaw == "" {
		return "", "", false
	}
	startNs, endNs, parsed := parseLokiTimeRangeToUnixNano(startRaw, endRaw)
	if !parsed || endNs != startNs {
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
//
// It reads the bounds with FormValue, which the handler parses anyway, so a request that is not a point costs
// nothing here. The replacement is a shallow copy with a rebuilt query string, not Request.Clone, which copies the
// headers. Only a GET is widened: a form body is read once, and the copy could not read it again.
func withWidenedLabelPoint(r *http.Request, now time.Time) *http.Request {
	if q := r.URL.RawQuery; r.Method != http.MethodGet || !strings.Contains(q, "%") && (!strings.Contains(q, "start") || !strings.Contains(q, "end")) {
		return r // the usual request is settled by substring scans
	}
	// A point is written the same way twice (Grafana sends one number as both bounds), so equal text is the cheap
	// first test and no time is parsed for an ordinary window.
	startRaw, endRaw := r.FormValue("start"), r.FormValue("end")
	if startRaw == "" || startRaw != endRaw {
		return r
	}
	start, end, ok := widenLabelPointWindow(startRaw, endRaw, now)
	if !ok {
		return r
	}
	widened := new(http.Request)
	*widened = *r
	u := *r.URL
	u.RawQuery = queryWithTimes(r.URL.RawQuery, start, end)
	widened.URL = &u
	widened.Form, widened.PostForm = nil, nil
	return widened
}

// queryWithTimes is raw without its start and end parameters (whatever their spelling) and with the given ones.
func queryWithTimes(raw, start, end string) string {
	var b strings.Builder
	b.Grow(len(raw) + len(start) + len(end) + 16)
	for raw != "" {
		var pair string
		pair, raw, _ = strings.Cut(raw, "&")
		key, _, _ := strings.Cut(pair, "=")
		if strings.ContainsAny(key, "%+") {
			if decoded, err := url.QueryUnescape(key); err == nil {
				key = decoded
			}
		}
		if pair == "" || key == "start" || key == "end" {
			continue
		}
		b.WriteString(pair)
		b.WriteByte('&')
	}
	b.WriteString("start=")
	b.WriteString(start)
	b.WriteString("&end=")
	b.WriteString(end)
	return b.String()
}
