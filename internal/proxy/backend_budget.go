package proxy

import (
	"io"
	"net/http"
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
		return client.Do(req)
	}
	select {
	case p.backendBudget <- struct{}{}:
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	var once sync.Once
	release := func() { once.Do(func() { <-p.backendBudget }) }
	resp, err := client.Do(req)
	if err != nil {
		release()
		return resp, err
	}
	resp.Body = &budgetResponseBody{ReadCloser: resp.Body, release: release}
	return resp, nil
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
