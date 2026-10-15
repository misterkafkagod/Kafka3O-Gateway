package api

import (
	"log/slog"
	"net/http"
	"time"
)

// Default timeouts for calls to Kafka.
const (
	DefaultPingTimeout = 3 * time.Second
	DefaultCallTimeout = 10 * time.Second
)

// Server holds the handlers' dependencies.
type Server struct {
	cluster     Cluster
	log         *slog.Logger
	uiDir       string
	pingTimeout time.Duration
	callTimeout time.Duration
}

// NewServer returns a Server using the default timeouts. If uiDir is not
// empty, the server also serves the built UI from it.
func NewServer(cluster Cluster, log *slog.Logger, uiDir string) (*Server, error) {
	if uiDir != "" {
		if err := checkUIDir(uiDir); err != nil {
			return nil, err
		}
	}
	return &Server{
		cluster:     cluster,
		log:         log,
		uiDir:       uiDir,
		pingTimeout: DefaultPingTimeout,
		callTimeout: DefaultCallTimeout,
	}, nil
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.health)
	mux.HandleFunc("GET /api/cluster", s.describeCluster)
	mux.HandleFunc("GET /api/", apiNotFound)
	if s.uiDir != "" {
		mux.Handle("GET /", staticHandler(s.uiDir))
	}
	return mux
}
