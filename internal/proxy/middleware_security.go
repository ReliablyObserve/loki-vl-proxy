package proxy

import (
	"net/http"
	"strings"
)

// SecurityHeadersMiddleware adds hardening response headers to every response.
// It also answers a path Loki's router would clean (a repeated slash, a . or
// .. element) with Loki's 301 before the mux can redirect it, without the
// headers: Loki's redirect has neither a body nor a Content-Type. It is
// applied globally in production and must be included in tests that verify
// the shipped server path.
func SecurityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if redirectUncleanPath(w, r) {
			return
		}
		header := w.Header()
		if strings.TrimSpace(header.Get("X-Content-Type-Options")) == "" {
			header.Set("X-Content-Type-Options", "nosniff")
		}
		if strings.TrimSpace(header.Get("X-Frame-Options")) == "" {
			header.Set("X-Frame-Options", "DENY")
		}
		if strings.TrimSpace(header.Get("Cross-Origin-Resource-Policy")) == "" {
			header.Set("Cross-Origin-Resource-Policy", "same-origin")
		}
		if strings.TrimSpace(header.Get("Cache-Control")) == "" {
			header.Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
		}
		if strings.TrimSpace(header.Get("Pragma")) == "" {
			header.Set("Pragma", "no-cache")
		}
		if strings.TrimSpace(header.Get("Expires")) == "" {
			header.Set("Expires", "0")
		}
		next.ServeHTTP(w, r)
	})
}
