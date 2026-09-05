package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/manideep7286/queue/internal/jobs"
)

// ErrorResponse is the single error shape every endpoint returns, so a client
// never has to guess whether a failure is JSON or HTML.
type ErrorResponse struct {
	Error string `json:"error"` // human-readable
	Code  string `json:"code"`  // stable machine-readable identifier
}

// Error codes. These are part of the API contract: the message text may be
// reworded, but a client may branch on the code.
const (
	CodeBadRequest    = "bad_request"
	CodeNotFound      = "not_found"
	CodeConflict      = "conflict"
	CodeUnauthorized  = "unauthorized"
	CodeNoJobs        = "no_jobs_available"
	CodeInternal      = "internal_error"
	CodePayloadTooBig = "payload_too_large"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	// The header and status are already sent, so a marshal failure cannot be
	// turned into an error response -- all that is left is to log it.
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg, Code: code})
}

// writeStoreError translates a store error into the right HTTP status.
//
// This function is the reason the store returns sentinel errors instead of
// formatted strings: the alternative is string-matching in the handler, which
// breaks the moment someone rewords a message.
func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, jobs.ErrNotFound):
		writeError(w, http.StatusNotFound, CodeNotFound, "job not found")
	case errors.Is(err, jobs.ErrNoJobs):
		// 404 with a distinct code: an empty queue is the normal case for a
		// polling worker, not an error it should log loudly.
		writeError(w, http.StatusNotFound, CodeNoJobs, "no jobs available")
	case errors.Is(err, jobs.ErrUnknownWorker):
		writeError(w, http.StatusNotFound, CodeNotFound, "unknown worker: register first")
	case errors.Is(err, jobs.ErrNotOwned):
		// 409, not 403: the request was authorized, but the job's current state
		// (reassigned after a lease expiry, or already terminal) contradicts it.
		writeError(w, http.StatusConflict, CodeConflict,
			"job is not RUNNING under this worker (lease may have expired and been reassigned)")
	case errors.Is(err, jobs.ErrNotCancelable):
		writeError(w, http.StatusConflict, CodeConflict, "only PENDING jobs can be canceled")
	case errors.Is(err, jobs.ErrNotClaimable):
		writeError(w, http.StatusConflict, CodeConflict, "job is not claimable")
	default:
		// Database errors are logged in full but never echoed to the client:
		// driver messages leak table names, queries, and sometimes credentials.
		slog.Error("unhandled store error", "error", err)
		writeError(w, http.StatusInternalServerError, CodeInternal, "internal server error")
	}
}
