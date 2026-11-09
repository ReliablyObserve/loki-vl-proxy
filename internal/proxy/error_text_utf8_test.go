package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// A regexp with bytes that are not UTF-8 is rejected with Loki's 400 and Loki's text: the message quotes the
// regexp from its first invalid byte, each such byte written as U+FFFD (the bytes Loki 3.7 sends), so the body is
// valid UTF-8.
// conformance: semantics/line-filter-invalid-utf8
func TestInvalidUTF8RegexpErrorTextLikeLoki(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("VictoriaLogs queried for a query Loki rejects: %s", r.URL)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	q := url.Values{"query": {`{app="x"} |~ "a\xff\xfeb\xffc"`}, "start": {"1767225600"}, "end": {"1767225602"}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+q.Encode(), nil)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %q", res.Code, res.Body)
	}
	const want = "parse error : stage '|~ \"a\\xff\\xfeb\\xffc\"' : error parsing regexp: invalid UTF-8: `��b�c`"
	if got := readErrorRec(t, res).Error; got != want {
		t.Fatalf("error text\n got %q\nwant %q", got, want)
	}
}

func TestValidUTF8Text(t *testing.T) {
	for in, want := range map[string]string{
		"":                     "",
		"plain ✓":              "plain ✓",
		"a\xff\xfeb":           "a��b",
		"\xe2\x9c":             "��",
		"ok \xe2\x9c\x93 \xc3": "ok ✓ �",
	} {
		if got := validUTF8Text(in); got != want {
			t.Errorf("validUTF8Text(%q) = %q, want %q", in, got, want)
		}
	}
}
