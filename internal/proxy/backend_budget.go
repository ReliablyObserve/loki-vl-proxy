package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
)

// backendBudget limits actual upstream operations independently of outer request
// admission. A permit covers headers and body consumption, including failures.
func newBackendBudget(limit int) chan struct{} {
	if limit <= 0 {
		return nil
	}
	return make(chan struct{}, limit)
}

func (p *Proxy) doBackendRequest(req *http.Request, client *http.Client) (*http.Response, error) {
	if p.backendBudget == nil {
		return p.doBackendClient(client, req)
	}
	select {
	case p.backendBudget <- struct{}{}:
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	var once sync.Once
	release := func() { once.Do(func() { <-p.backendBudget }) }
	resp, err := p.doBackendClient(client, req)
	if err != nil {
		release()
		return resp, err
	}
	resp.Body = &budgetResponseBody{ReadCloser: resp.Body, release: release}
	return resp, nil
}

// doBackendClient sends req and, when the transport fails it with a
// cancellation its own context did not cause, sends it once more. net/http
// recycles a connection as soon as an empty response arrives, so a sibling
// call of the same request that the proxy cancels at that moment (a probe whose
// answer is no longer needed, a client that went away) can mark the recycled
// connection cancelled after another call has taken it; that call then fails
// with context.Canceled although nothing cancelled it, and the client would
// get a 499 for a request it never cancelled. The failed attempt used a
// connection the transport closed, so the retry opens a new one. The calls
// are reads (queries, listings, metrics), so repeating one is safe; the
// delete call is not repeated.
func (p *Proxy) doBackendClient(client *http.Client, req *http.Request) (*http.Response, error) {
	resp, err := client.Do(req)
	if err == nil || !errors.Is(err, context.Canceled) || req.Context().Err() != nil || !isBackendRead(req) {
		return resp, err
	}
	retry := req.Clone(req.Context())
	if req.Body != nil && req.Body != http.NoBody {
		if req.GetBody == nil {
			return resp, err
		}
		body, bodyErr := req.GetBody()
		if bodyErr != nil {
			return resp, err
		}
		retry.Body = body
	}
	p.observeInternalOperation(req.Context(), "backend_request_retry", "stale_connection_cancel", 0)
	return client.Do(retry)
}

// isBackendRead reports whether req only reads: a GET, or a POST to a select
// endpoint (VictoriaLogs takes long queries in the body) other than delete.
func isBackendRead(req *http.Request) bool {
	switch req.Method {
	case http.MethodGet:
		return true
	case http.MethodPost:
		return strings.HasPrefix(req.URL.Path, "/select/") && !strings.HasSuffix(req.URL.Path, "/delete")
	}
	return false
}

type budgetResponseBody struct {
	io.ReadCloser
	release func()
}

func (b *budgetResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.release()
	}
	return n, err
}
func (b *budgetResponseBody) Close() error { defer b.release(); return b.ReadCloser.Close() }
