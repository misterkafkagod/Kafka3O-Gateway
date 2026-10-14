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
	pingTimeout time.Duration
	callTimeout time.Duration
}

// NewServer returns a Server using the default timeouts.
func NewServer(cluster Cluster, log *slog.Logger) *Server {
	return &Server{
		cluster:     cluster,
		log:         log,
		pingTimeout: DefaultPingTimeout,
		callTimeout: DefaultCallTimeout,
	}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.health)
	mux.HandleFunc("GET /api/cluster", s.describeCluster)
	return mux
}
