package api

import (
	"context"
	"net/http"
)

type healthResponse struct {
	Gateway string        `json:"gateway"`
	Cluster clusterHealth `json:"cluster"`
}

type clusterHealth struct {
	Reachable bool   `json:"reachable"`
	Error     string `json:"error,omitempty"`
}

// health always answers 200: the gateway is up if it can respond at all.
// Cluster reachability is reported separately.
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.pingTimeout)
	defer cancel()

	resp := healthResponse{Gateway: "up", Cluster: clusterHealth{Reachable: true}}
	if err := s.cluster.Ping(ctx); err != nil {
		resp.Cluster = clusterHealth{Reachable: false, Error: err.Error()}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) describeCluster(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.callTimeout)
	defer cancel()

	info, err := s.cluster.DescribeCluster(ctx)
	if err != nil {
		s.log.Warn("describe cluster failed", "error", err)
		writeError(w, err)
		return
	}
	if info.Brokers == nil {
		info.Brokers = []Broker{}
	}
	writeJSON(w, http.StatusOK, info)
}
