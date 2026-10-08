package o11y

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Status string

const (
	Healthy   Status = "healthy"
	Degraded  Status = "degraded"
	Unhealthy Status = "unhealthy"
)

type Kind string

const (
	KindService   Kind = "service"
	KindFleet     Kind = "fleet"
	KindDatastore Kind = "datastore"
	KindExternal  Kind = "external"
)

// Criticality decides what a failed check does to /ready: a Critical failure
// makes the service unhealthy (503), a Soft failure only makes it degraded (200).
type Criticality string

const (
	Critical Criticality = "critical"
	Soft     Criticality = "soft"
)

const (
	DefaultAddr          = ":9090"
	DefaultCheckInterval = 5 * time.Second
	DefaultCheckTimeout  = time.Second
	DefaultVersion       = "dev"
	DefaultKind          = KindService
	DefaultCriticality   = Soft
)

type CheckFunc func(ctx context.Context) error

// Dependency declares something the service relies on. A nil Check means the
// dependency is declared only: listed by /dependencies, never evaluated by /ready.
type Dependency struct {
	Name        string
	Kind        Kind
	Criticality Criticality
	Check       CheckFunc
}

func (d Dependency) critical() bool { return d.Criticality == Critical }

type Version struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
}

// VersionFunc is how a service hands over its version in whatever form it
// already has it: ldflags vars, an embedded file, a package struct.
type VersionFunc func() Version

type Config struct {
	Service       string
	Addr          string
	Version       VersionFunc
	Dependencies  []Dependency
	CheckInterval time.Duration
	CheckTimeout  time.Duration

	// AuthToken is required. Callers must send "Authorization: Bearer <token>"
	// for /metrics, /dependencies, /version and the detailed /ready body.
	// /health and the /ready status code stay open for probes.
	AuthToken string
}

type depResult struct {
	Status    Status `json:"status"`
	Critical  bool   `json:"critical"`
	LatencyMS int64  `json:"latency_ms"`
	Error     string `json:"error,omitempty"`
}

type readySnapshot struct {
	Status       Status               `json:"status"`
	Dependencies map[string]depResult `json:"dependencies"`
}

type Server struct {
	cfg      Config
	registry *prometheus.Registry
	metrics  *metrics
	http     *http.Server
	snapshot atomic.Pointer[readySnapshot]
	draining atomic.Bool
	stop     context.CancelFunc
	done     chan struct{}
}

type metrics struct {
	buildInfo  *prometheus.GaugeVec
	ready      prometheus.Gauge
	depHealthy *prometheus.GaugeVec
	depLatency *prometheus.GaugeVec
	depInfo    *prometheus.GaugeVec
}

func Init(cfg Config) (*Server, error) {
	if err := applyDefaults(&cfg); err != nil {
		return nil, err
	}

	s := &Server{cfg: cfg, registry: prometheus.NewRegistry(), done: make(chan struct{})}
	s.metrics = newMetrics(s.registry)
	s.snapshot.Store(&readySnapshot{Status: Unhealthy, Dependencies: map[string]depResult{}})

	v := cfg.Version()
	s.metrics.buildInfo.WithLabelValues(orUnknown(v.Version), orUnknown(v.Commit)).Set(1)
	for _, d := range cfg.Dependencies {
		s.metrics.depInfo.WithLabelValues(d.Name, string(d.Kind), strconv.FormatBool(d.critical())).Set(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.Handle("GET /dependencies", s.requireAuth(http.HandlerFunc(s.handleDependencies)))
	mux.Handle("GET /version", s.requireAuth(http.HandlerFunc(s.handleVersion)))
	mux.Handle("GET /metrics", s.requireAuth(promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{})))
	s.http = &http.Server{Addr: cfg.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s, nil
}

var dependencyNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

func applyDefaults(cfg *Config) error {
	if cfg.Service == "" {
		return errors.New("o11y: Service is required")
	}
	if strings.TrimSpace(cfg.AuthToken) == "" {
		return errors.New("o11y: AuthToken is required")
	}
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	if cfg.Version == nil {
		cfg.Version = func() Version { return Version{Version: DefaultVersion} }
	}
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = DefaultCheckInterval
	}
	if cfg.CheckTimeout == 0 {
		cfg.CheckTimeout = DefaultCheckTimeout
	}
	if cfg.CheckTimeout >= cfg.CheckInterval {
		return fmt.Errorf("o11y: CheckTimeout (%s) must be shorter than CheckInterval (%s)", cfg.CheckTimeout, cfg.CheckInterval)
	}

	seen := make(map[string]bool, len(cfg.Dependencies))
	for i := range cfg.Dependencies {
		d := &cfg.Dependencies[i]
		if !dependencyNameRe.MatchString(d.Name) {
			return fmt.Errorf("o11y: invalid dependency name %q (want lowercase letters, digits, '-' or '_')", d.Name)
		}
		if seen[d.Name] {
			return fmt.Errorf("o11y: duplicate dependency %q", d.Name)
		}
		seen[d.Name] = true

		if d.Kind == "" {
			d.Kind = DefaultKind
		}
		if d.Criticality == "" {
			d.Criticality = DefaultCriticality
		}

		switch d.Kind {
		case KindService, KindFleet, KindDatastore, KindExternal:
		default:
			return fmt.Errorf("o11y: dependency %q has invalid kind %q", d.Name, d.Kind)
		}
		switch d.Criticality {
		case Critical, Soft:
		default:
			return fmt.Errorf("o11y: dependency %q has invalid criticality %q", d.Name, d.Criticality)
		}

		if d.Check == nil && d.critical() {
			return fmt.Errorf("o11y: dependency %q is critical but has no Check", d.Name)
		}
		if d.Check == nil && d.Kind == KindDatastore {
			return fmt.Errorf("o11y: datastore dependency %q must have a Check", d.Name)
		}
	}
	return nil
}

func newMetrics(reg *prometheus.Registry) *metrics {
	m := &metrics{
		buildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "build_info",
			Help: "Build metadata of the running binary; always 1.",
		}, []string{"version", "commit"}),
		ready: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "service_ready",
			Help: "1 when /ready returns 200, otherwise 0.",
		}),
		depHealthy: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "service_dependency_healthy",
			Help: "1 when the last check of the dependency succeeded, otherwise 0.",
		}, []string{"dependency", "critical"}),
		depLatency: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "service_dependency_latency_seconds",
			Help: "Duration of the last dependency check.",
		}, []string{"dependency"}),
		depInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "service_dependency_info",
			Help: "Declared dependencies of this service; always 1.",
		}, []string{"dependency", "kind", "critical"}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.buildInfo, m.ready, m.depHealthy, m.depLatency, m.depInfo,
	)
	return m
}

// Start binds the admin port, runs one readiness pass so /ready is meaningful
// immediately, then keeps re-evaluating in the background. It does not block.
func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	ctx, s.stop = context.WithCancel(ctx)
	s.evaluate(ctx)

	go func() {
		defer close(s.done)
		t := time.NewTicker(s.cfg.CheckInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.evaluate(ctx)
			}
		}
	}()
	go func() {
		if err := s.http.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("o11y: admin server: %v", err)
		}
	}()
	return nil
}

// SetDraining makes /ready return 503 while the admin listener keeps serving,
// so probes and load balancers stop routing traffic before the service's
// public listener closes. Call it first in shutdown, then wait, then close.
func (s *Server) SetDraining() {
	s.draining.Store(true)
	s.metrics.ready.Set(0)
}

// Shutdown stops the readiness checker and the admin listener. It implies
// SetDraining and should be the last step of a service's shutdown.
func (s *Server) Shutdown(ctx context.Context) error {
	s.SetDraining()
	if s.stop != nil {
		s.stop()
		<-s.done
	}
	return s.http.Shutdown(ctx)
}

// Registry is where a service registers its own metrics; they are served on
// the same /metrics endpoint as the built-in ones.
func (s *Server) Registry() *prometheus.Registry { return s.registry }

func (s *Server) Addr() string { return s.cfg.Addr }

func (s *Server) evaluate(ctx context.Context) {
	results := make(map[string]depResult, len(s.cfg.Dependencies))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, d := range s.cfg.Dependencies {
		if d.Check == nil {
			continue
		}
		wg.Add(1)
		go func(d Dependency) {
			defer wg.Done()
			r := s.runCheck(ctx, d)
			mu.Lock()
			results[d.Name] = r
			mu.Unlock()
		}(d)
	}
	wg.Wait()

	snap := &readySnapshot{Status: Healthy, Dependencies: results}
	for _, r := range results {
		switch {
		case r.Status == Healthy:
		case r.Critical:
			snap.Status = Unhealthy
		case snap.Status == Healthy:
			snap.Status = Degraded
		}
	}
	s.snapshot.Store(snap)
	s.observe(snap)
}

// runCheck returns within CheckTimeout even if the check ignores its context;
// such a check's goroutine is abandoned rather than allowed to stall the round.
func (s *Server) runCheck(ctx context.Context, d Dependency) depResult {
	cctx, cancel := context.WithTimeout(ctx, s.cfg.CheckTimeout)
	defer cancel()

	start := time.Now()
	errc := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				errc <- fmt.Errorf("check panicked: %v", p)
			}
		}()
		errc <- d.Check(cctx)
	}()

	var err error
	select {
	case err = <-errc:
	case <-cctx.Done():
		if errors.Is(cctx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("check timed out after %s", s.cfg.CheckTimeout)
		} else {
			err = cctx.Err()
		}
	}

	r := depResult{Status: Healthy, Critical: d.critical(), LatencyMS: time.Since(start).Milliseconds()}
	if err != nil {
		r.Status, r.Error = Unhealthy, err.Error()
	}
	return r
}

func (s *Server) observe(snap *readySnapshot) {
	if snap.Status == Unhealthy || s.draining.Load() {
		s.metrics.ready.Set(0)
	} else {
		s.metrics.ready.Set(1)
	}
	for name, r := range snap.Dependencies {
		healthy := 0.0
		if r.Status == Healthy {
			healthy = 1
		}
		s.metrics.depHealthy.WithLabelValues(name, strconv.FormatBool(r.Critical)).Set(healthy)
		s.metrics.depLatency.WithLabelValues(name).Set(float64(r.LatencyMS) / 1000)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]Status{"status": Healthy})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	auth := s.authenticate(r)
	if auth == authInvalid {
		unauthorized(w)
		return
	}

	snap := *s.snapshot.Load()
	if s.draining.Load() {
		snap.Status = Unhealthy
	}
	code := http.StatusOK
	if snap.Status == Unhealthy {
		code = http.StatusServiceUnavailable
	}
	if auth == authMissing {
		writeJSON(w, code, map[string]Status{"status": snap.Status})
		return
	}
	writeJSON(w, code, snap)
}

type authResult int

const (
	authValid authResult = iota
	authMissing
	authInvalid
)

func (s *Server) authenticate(r *http.Request) authResult {
	header := r.Header.Get("Authorization")
	if header == "" {
		return authMissing
	}
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return authInvalid
	}
	if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(token)), []byte(s.cfg.AuthToken)) != 1 {
		return authInvalid
	}
	return authValid
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.authenticate(r) != authValid {
			unauthorized(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="o11y"`)
	writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
}

func (s *Server) handleDependencies(w http.ResponseWriter, _ *http.Request) {
	type dep struct {
		Name     string `json:"name"`
		Kind     Kind   `json:"kind"`
		Critical bool   `json:"critical"`
		Checked  bool   `json:"checked"`
	}
	out := make([]dep, 0, len(s.cfg.Dependencies))
	for _, d := range s.cfg.Dependencies {
		out = append(out, dep{d.Name, d.Kind, d.critical(), d.Check != nil})
	}
	writeJSON(w, http.StatusOK, map[string]any{"service": s.cfg.Service, "dependencies": out})
}

func (s *Server) handleVersion(w http.ResponseWriter, _ *http.Request) {
	v := s.cfg.Version()
	writeJSON(w, http.StatusOK, map[string]string{
		"service":    s.cfg.Service,
		"version":    v.Version,
		"commit":     v.Commit,
		"build_time": v.BuildTime,
		"go_version": runtime.Version(),
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func orUnknown(s string) string {
	if strings.TrimSpace(s) == "" {
		return "unknown"
	}
	return s
}
