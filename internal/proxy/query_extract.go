package proxy

import (
	"fmt"
	"net/http"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// extractQuery reads the "query" form parameter from r, validates it as
// syntactically valid LogQL, and returns the raw query string.
//
// It is a pure function — callers decide how to handle the returned error
// (typically by calling p.writeError and returning from the handler).
func extractQuery(r *http.Request) (string, error) {
	q := r.FormValue("query")
	if q == "" {
		return "", fmt.Errorf("parse error : syntax error: unexpected $end")
	}
	if err := logql.ParseAndValidate(q); err != nil {
		return "", err
	}
	return q, nil
}
