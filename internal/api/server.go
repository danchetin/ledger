// Package api exposes the ledger over HTTP/JSON.
package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/danchetin/ledger/internal/ledger"
)

//go:embed openapi.yaml
var openAPISpec []byte

// MaxBodyBytes caps request bodies.
const MaxBodyBytes = 1 << 20

// Config configures the HTTP server.
type Config struct {
	// APIKey, when set, is required as "Authorization: Bearer <key>" on /v1 routes.
	APIKey string
	Logger *slog.Logger
	// Registry receives the HTTP metrics. A new one is created when nil.
	Registry *prometheus.Registry
}

// Server routes HTTP requests to the ledger service.
type Server struct {
	svc      *ledger.Service
	cfg      Config
	log      *slog.Logger
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	replays  prometheus.Counter
}

// New creates a Server.
func New(svc *ledger.Service, cfg Config) *Server {
	s := &Server{svc: svc, cfg: cfg, log: cfg.Logger, registry: cfg.Registry}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	if s.registry == nil {
		s.registry = prometheus.NewRegistry()
		s.registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	}
	s.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_http_requests_total", Help: "HTTP requests by route, method and status.",
	}, []string{"route", "method", "status"})
	s.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ledger_http_request_duration_seconds",
		Help:    "HTTP request latency by route.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"route", "method"})
	s.replays = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ledger_idempotent_replays_total", Help: "Responses served from a stored Idempotency-Key result.",
	})
	s.registry.MustRegister(s.requests, s.duration, s.replays)
	return s
}

// Handler returns the HTTP handler with all middleware applied.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.Write(openAPISpec) //nolint:errcheck
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))

	mux.HandleFunc("POST /v1/accounts", s.createAccount)
	mux.HandleFunc("GET /v1/accounts", s.listAccounts)
	mux.HandleFunc("GET /v1/accounts/{id}", s.getAccount)
	mux.HandleFunc("GET /v1/accounts/{id}/entries", s.listEntries)
	mux.HandleFunc("POST /v1/transfers", s.createTransfer)
	mux.HandleFunc("GET /v1/transactions/{id}", s.getTransaction)
	mux.HandleFunc("POST /v1/transactions/{id}/reverse", s.reverseTransaction)
	mux.HandleFunc("POST /v1/holds", s.authorizeHold)
	mux.HandleFunc("GET /v1/holds/{id}", s.getHold)
	mux.HandleFunc("POST /v1/holds/{id}/capture", s.captureHold)
	mux.HandleFunc("POST /v1/holds/{id}/void", s.voidHold)
	mux.HandleFunc("GET /v1/reconcile", s.reconcile)

	return s.recoverer(s.requestID(s.observe(mux, s.auth(mux))))
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.svc.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "database unavailable"})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}

type ctxKey int

const requestIDKey ctxKey = 0

func newID() string {
	b := make([]byte, 12)
	rand.Read(b) //nolint:errcheck // crypto/rand.Read never fails
	return hex.EncodeToString(b)
}

func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if id == "" || len(id) > 100 {
			id = newID()
		}
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

func requestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// observe logs every request and records metrics labelled by route pattern
// (not raw path, so ids do not blow up label cardinality). The pattern is looked
// up on the mux, so requests rejected by middleware are still attributed.
func (s *Server) observe(mux *http.ServeMux, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		r.Body = http.MaxBytesReader(sw, r.Body, MaxBodyBytes)
		next.ServeHTTP(sw, r)
		if sw.status == 0 {
			sw.status = http.StatusOK
		}
		route := r.Pattern
		if route == "" {
			_, route = mux.Handler(r)
		}
		if route == "" {
			route = "unmatched"
		} else if i := strings.IndexByte(route, ' '); i >= 0 {
			route = route[i+1:]
		}
		elapsed := time.Since(start)
		status := http.StatusText(sw.status)
		if status == "" {
			status = "unknown"
		}
		s.requests.WithLabelValues(route, r.Method, strings.ToLower(strings.ReplaceAll(status, " ", "_"))).Inc()
		s.duration.WithLabelValues(route, r.Method).Observe(elapsed.Seconds())
		level := slog.LevelInfo
		if sw.status >= 500 {
			level = slog.LevelError
		}
		s.log.Log(r.Context(), level, "request",
			"request_id", requestIDFrom(r.Context()), "method", r.Method, "path", r.URL.Path,
			"status", sw.status, "duration_ms", elapsed.Milliseconds())
	})
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.APIKey != "" && strings.HasPrefix(r.URL.Path, "/v1/") {
			token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.APIKey)) != 1 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="ledger"`)
				writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid API key")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				s.log.Error("panic", "request_id", requestIDFrom(r.Context()), "panic", v)
				writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
