package api

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// statusRecorder captures the status code for the access log, because
// http.ResponseWriter offers no way to read back what was written.
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wrote {
		r.status, r.wrote = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	// A handler that writes without calling WriteHeader implicitly sends 200.
	if !r.wrote {
		r.status, r.wrote = http.StatusOK, true
	}
	return r.ResponseWriter.Write(b)
}

// logging emits one structured line per request.
func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		lvl := slog.LevelInfo
		if rec.status >= 500 {
			lvl = slog.LevelError
		}
		// Successful claim polling is extremely chatty (every worker, every
		// poll interval), so an empty-queue response is logged at debug.
		if r.URL.Path == "/jobs/claim" && rec.status == http.StatusNotFound {
			lvl = slog.LevelDebug
		}
		slog.Log(r.Context(), lvl, "http_request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"remote", r.RemoteAddr,
		)
	})
}

// recovery turns a panic in any handler into a 500 instead of killing the
// process. Without it, one nil dereference in one handler takes down the queue
// for every worker.
func recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("panic in handler", "panic", rec, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, CodeInternal, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// maxBody caps request bodies before any handler reads them, so a client cannot
// stream gigabytes into the server's memory.
func maxBody(limit int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		next.ServeHTTP(w, r)
	})
}

// auth enforces a shared bearer token when one is configured.
//
// This is deliberately minimal -- the queue executes shell commands, so it is
// meant to run on localhost or a private network. The token is a second line of
// defense, not the primary one.
func auth(token string, next http.Handler) http.Handler {
	if token == "" {
		return next // no token configured: authentication disabled
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The health endpoint stays open so orchestrators can probe it.
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		// Constant-time compare: a byte-by-byte == leaks how many leading
		// characters were right, which is enough to guess a token one char at a
		// time. This is the standard defense against a timing attack.
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid or missing bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// jsonErrors makes net/http's built-in 404 and 405 replies match the API's JSON
// error shape.
//
// The standard mux writes those itself as plain text before any handler of ours
// runs, so the only way to keep the contract "every response is JSON" is to
// intercept the write. The wrapper below swallows the plain-text body and
// substitutes the JSON one.
func jsonErrors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&jsonErrorWriter{ResponseWriter: w}, r)
	})
}

type jsonErrorWriter struct {
	http.ResponseWriter
	substituted bool
}

func (w *jsonErrorWriter) WriteHeader(code int) {
	// Intercept only non-JSON replies. net/http's built-in 404/405 go through
	// http.Error, which sets a text/plain content type *before* calling
	// WriteHeader -- so testing for an empty header here would never match.
	// Our own handlers always set application/json, so they are left alone.
	if (code == http.StatusNotFound || code == http.StatusMethodNotAllowed) &&
		!strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		// http.Error already wrote a text/plain content type; replace it.
		w.Header().Del("Content-Type")
		w.substituted = true
		msg, ec := "resource not found", CodeNotFound
		if code == http.StatusMethodNotAllowed {
			msg, ec = "method not allowed for this path", CodeBadRequest
		}
		writeError(w.ResponseWriter, code, ec, msg)
		return
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *jsonErrorWriter) Write(b []byte) (int, error) {
	if w.substituted {
		return len(b), nil // discard the plain-text body we already replaced
	}
	return w.ResponseWriter.Write(b)
}
