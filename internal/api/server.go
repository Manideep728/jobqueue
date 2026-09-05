// Package api exposes the queue over HTTP. It holds no queue state of its own:
// every decision is made by a SQL statement in the jobs package, which is what
// lets several server instances run against one database.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
	"github.com/manideep7286/queue/internal/jobs"
)


type Config struct {
	
	
	DefaultLease time.Duration
	
	
	MinLease time.Duration
	MaxLease time.Duration
	
	
	HeartbeatTimeout time.Duration
	
	AuthToken string
	
	MaxRequestBytes int64
}


func DefaultConfig() Config {
	return Config{
		DefaultLease:     30 * time.Second,
		MinLease:         5 * time.Second,
		MaxLease:         10 * time.Minute,
		HeartbeatTimeout: 60 * time.Second,
		MaxRequestBytes:  256 * 1024,
	}
}


type Server struct {
	store *jobs.Store
	cfg   Config
	mux   *http.ServeMux
}


func New(store *jobs.Store, cfg Config) *Server {
	s := &Server{store: store, cfg: cfg, mux: http.NewServeMux()}
	s.routes()
	return s
}







func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /stats", s.handleStats)

	s.mux.HandleFunc("POST /jobs", s.handleCreateJob)
	s.mux.HandleFunc("GET /jobs", s.handleListJobs)
	
	
	
	s.mux.HandleFunc("POST /jobs/claim", s.handleClaim)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("POST /jobs/{id}/cancel", s.handleCancel)
	s.mux.HandleFunc("POST /jobs/{id}/complete", s.handleComplete)
	s.mux.HandleFunc("POST /jobs/{id}/fail", s.handleFail)
	s.mux.HandleFunc("POST /jobs/{id}/lease", s.handleRenewLease)

}



func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var h http.Handler = s.mux
	h = auth(s.cfg.AuthToken, h)
	h = maxBody(s.cfg.MaxRequestBytes, h)
	h = logging(h)
	h = recovery(h)
	h.ServeHTTP(w, r)
}










func decode(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return errors.New("request body too large")
		}
		if errors.Is(err, io.EOF) {
			return errors.New("request body is empty; expected a JSON object")
		}
		return errors.New("malformed JSON: " + err.Error())
	}
	
	
	if dec.More() {
		return errors.New("unexpected trailing content after JSON object")
	}
	return nil
}


func pathID(r *http.Request) (int64, error) {
	raw := r.PathValue("id")
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("invalid job id: must be a positive integer")
	}
	return id, nil
}


func (s *Server) clampLease(seconds float64) time.Duration {
	if seconds <= 0 {
		return s.cfg.DefaultLease
	}
	d := time.Duration(seconds * float64(time.Second))
	if d < s.cfg.MinLease {
		return s.cfg.MinLease
	}
	if d > s.cfg.MaxLease {
		return s.cfg.MaxLease
	}
	return d
}





func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	
	
	if _, err := s.store.Stats(r.Context()); err != nil {
		slog.Error("health check failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, CodeInternal, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": stats})
}





type createJobRequest struct {
	Type        string `json:"type"`
	Payload     string `json:"payload"`
	MaxAttempts int    `json:"max_attempts"`
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeBadRequest, err.Error())
		return
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = 3 
	}
	req.Type = strings.TrimSpace(req.Type)
	if err := jobs.ValidateNew(req.Type, req.Payload, req.MaxAttempts); err != nil {
		writeError(w, http.StatusBadRequest, CodeBadRequest, err.Error())
		return
	}
	job, err := s.store.Create(r.Context(), req.Type, req.Payload, req.MaxAttempts)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	slog.Info("job_created", "job_id", job.ID, "type", job.Type, "max_attempts", job.MaxAttempts)
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	
	
	status := jobs.Status(strings.ToUpper(strings.TrimSpace(q.Get("status"))))
	if status != "" && !jobs.ValidStatus(status) {
		writeError(w, http.StatusBadRequest, CodeBadRequest,
			"invalid status filter: use PENDING, RUNNING, COMPLETED, FAILED or CANCELED")
		return
	}
	limit, err := intParam(q.Get("limit"), 100)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeBadRequest, "invalid limit: "+err.Error())
		return
	}
	offset, err := intParam(q.Get("offset"), 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeBadRequest, "invalid offset: "+err.Error())
		return
	}
	list, err := s.store.List(r.Context(), status, limit, offset)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": list, "count": len(list)})
}

func intParam(raw string, def int) (int, error) {
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New("must be an integer")
	}
	if n < 0 {
		return 0, errors.New("must not be negative")
	}
	return n, nil
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeBadRequest, err.Error())
		return
	}
	job, err := s.store.Get(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeBadRequest, err.Error())
		return
	}
	job, err := s.store.Cancel(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	slog.Info("job_canceled", "job_id", job.ID)
	writeJSON(w, http.StatusOK, job)
}

type claimRequest struct {
	WorkerID     string  `json:"worker_id"`
	LeaseSeconds float64 `json:"lease_seconds"`
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	var req claimRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeBadRequest, err.Error())
		return
	}
	req.WorkerID = strings.TrimSpace(req.WorkerID)
	if req.WorkerID == "" {
		writeError(w, http.StatusBadRequest, CodeBadRequest, "worker_id is required")
		return
	}
	lease := s.clampLease(req.LeaseSeconds)
	job, err := s.store.Claim(r.Context(), req.WorkerID, lease)
	if err != nil {
		writeStoreError(w, err) 
		return
	}
	slog.Info("job_claimed", "job_id", job.ID, "worker_id", req.WorkerID,
		"attempt", job.Attempts, "lease_seconds", lease.Seconds())
	writeJSON(w, http.StatusOK, job)
}


type reportRequest struct {
	WorkerID     string  `json:"worker_id"`
	Stdout       string  `json:"stdout"`
	Stderr       string  `json:"stderr"`
	ExitCode     *int    `json:"exit_code"`
	DurationMS   *int64  `json:"duration_ms"`
	Error        string  `json:"error"`
	LeaseSeconds float64 `json:"lease_seconds"`
}


func (s *Server) parseReport(w http.ResponseWriter, r *http.Request) (int64, reportRequest, bool) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeBadRequest, err.Error())
		return 0, reportRequest{}, false
	}
	var req reportRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeBadRequest, err.Error())
		return 0, reportRequest{}, false
	}
	req.WorkerID = strings.TrimSpace(req.WorkerID)
	if req.WorkerID == "" {
		writeError(w, http.StatusBadRequest, CodeBadRequest, "worker_id is required")
		return 0, reportRequest{}, false
	}
	return id, req, true
}

func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request) {
	id, req, ok := s.parseReport(w, r)
	if !ok {
		return
	}
	job, err := s.store.Complete(r.Context(), id, req.WorkerID, jobs.Result{
		Stdout: req.Stdout, Stderr: req.Stderr, ExitCode: req.ExitCode, DurationMS: req.DurationMS,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	slog.Info("job_completed", "job_id", job.ID, "worker_id", req.WorkerID,
		"attempts", job.Attempts, "duration_ms", req.DurationMS)
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleFail(w http.ResponseWriter, r *http.Request) {
	id, req, ok := s.parseReport(w, r)
	if !ok {
		return
	}
	if strings.TrimSpace(req.Error) == "" {
		req.Error = "job failed without an error message"
	}
	job, err := s.store.Fail(r.Context(), id, req.WorkerID, jobs.Result{
		Stdout: req.Stdout, Stderr: req.Stderr, ExitCode: req.ExitCode,
		DurationMS: req.DurationMS, Error: req.Error,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	
	if job.Status == jobs.StatusPending {
		slog.Warn("job_retrying", "job_id", job.ID, "worker_id", req.WorkerID,
			"attempts", job.Attempts, "max_attempts", job.MaxAttempts,
			"retry_at", job.AvailableAt, "error", req.Error)
	} else {
		slog.Error("job_failed", "job_id", job.ID, "worker_id", req.WorkerID,
			"attempts", job.Attempts, "error", req.Error)
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleRenewLease(w http.ResponseWriter, r *http.Request) {
	id, req, ok := s.parseReport(w, r)
	if !ok {
		return
	}
	lease := s.clampLease(req.LeaseSeconds)
	job, err := s.store.RenewLease(r.Context(), id, req.WorkerID, lease)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	slog.Debug("lease_renewed", "job_id", job.ID, "worker_id", req.WorkerID)
	writeJSON(w, http.StatusOK, job)
}
