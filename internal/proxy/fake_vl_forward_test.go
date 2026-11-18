package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"
)

// lateTailBody answers the reads after a request body's last byte only once the server has closed the body,
// that is once the handler started its response: what the transport's EOF check of a forwarded body does
// when its goroutine is descheduled on a loaded CI runner.
type lateTailBody struct {
	io.ReadCloser
	left int64
}

func (b *lateTailBody) Read(p []byte) (int, error) {
	if b.left > 0 {
		n, err := b.ReadCloser.Read(p)
		b.left -= int64(n)
		return n, err
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		n, err := b.ReadCloser.Read(p)
		if err != io.EOF || time.Now().After(deadline) {
			return n, err
		}
		time.Sleep(100 * time.Microsecond)
	}
}

// A fake VictoriaLogs that fronts another server with a reverse proxy answers whole even when the transport's
// read after the forwarded body's last byte comes after the backend started its answer: forwarded unbuffered,
// that read fails on the closed request body, the transport closes the backend connection and the proxy under
// test read a cut answer (TestCanonicalSpelling_AnswersMatchLokiEngine and TestKeepSeriesGrouping_InstantCount
// answered 500 "invalid backend metric row: unexpected end of JSON input" on loaded runners).
func TestForwardWithBufferedBody_SurvivesLateBodyRead(t *testing.T) {
	inner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		_, _ = io.WriteString(w, `{"_time":"2023-11-14T22:20:00Z"}`+"\n")
		w.(http.Flusher).Flush()
		time.Sleep(50 * time.Millisecond)
		_, _ = io.WriteString(w, `{"_time":"2023-11-14T22:21:00Z"}`+"\n")
	}))
	t.Cleanup(inner.Close)
	target, _ := url.Parse(inner.URL)
	rest := httputil.NewSingleHostReverseProxy(target)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = &lateTailBody{ReadCloser: r.Body, left: r.ContentLength}
		forwardWithBufferedBody(rest, w, r)
	}))
	t.Cleanup(front.Close)
	form := url.Values{"query": {strings.Repeat("x", 16<<10)}}.Encode()
	resp, err := http.Post(front.URL+"/select/logsql/query", "application/x-www-form-urlencoded", strings.NewReader(form))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || strings.Count(string(body), "\n") != 2 {
		t.Fatalf("answer %q, read error %v; want both lines", body, err)
	}
}
