package proxy

import (
	"net/url"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// guardOutboundQuery is the last step of every request that carries a LogsQL query to VictoriaLogs: the hot backend's
// GET and POST (vlGetInner, vlPostHTTP, so every endpoint, probe and coalesced or cached fetch), the live tail and
// the cold tier (ColdGet, coldRequest). It runs logsql.GuardReservedFieldCase on the query arg: a filter on a field
// named _time, _stream or _stream_id in another letter case is rewritten to the copy-pipe form VictoriaLogs reads
// back from its own print, so no query the proxy sends makes VictoriaLogs' clone of it fail and the process restart
// (VictoriaMetrics/VictoriaLogs#1942). Any other query, the common case, is passed on as it is: params itself,
// with no copy and no allocation. TestEveryBackendSenderGuardsItsQuery holds every sender to it.
func guardOutboundQuery(params url.Values) url.Values {
	query := params.Get("query")
	guarded := logsql.GuardReservedFieldCase(query)
	if guarded == query {
		return params
	}
	out := make(url.Values, len(params))
	for k, v := range params {
		out[k] = v
	}
	out["query"] = []string{guarded}
	return out
}
