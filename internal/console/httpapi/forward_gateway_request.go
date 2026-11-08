package httpapi

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/gatewayproxy"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/gatewayauth"
)

// forwardGatewayRequest relays one authenticated request to the cluster's secured Gateway. The browser never
// chooses the Gateway address, headers or identity; the Gateway's own policy check decides.
func (a *SessionAPI) forwardGatewayRequest(w http.ResponseWriter, r *http.Request) {
	mutation := r.Method != http.MethodGet
	principal, token, binding, ok := a.authorizeSession(w, r, session.AccessNormal, mutation)
	if !ok {
		return
	}
	clusterID := r.PathValue("clusterID")
	prefix := "/api/clusters/" + clusterID + "/gateway"
	path := strings.TrimPrefix(r.URL.EscapedPath(), prefix)
	route, known := gatewayauth.ClassifyRoute(r.Method, path)
	if !strings.HasPrefix(r.URL.EscapedPath(), prefix+"/") || !known || route.Anonymous {
		a.writeProblem(w, http.StatusNotFound, "not_found", "Unknown Gateway route")
		return
	}
	query := r.URL.Query()
	for _, values := range query {
		if len(values) != 1 {
			a.writeRequestError(w, errInvalidRequest)
			return
		}
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, gatewayauth.MaxBodyBytes+1))
	if err != nil || len(body) > gatewayauth.MaxBodyBytes {
		a.writeRequestError(w, errRequestTooLarge)
		return
	}
	clusters, err := a.activeClusters.ListActiveClusters(r.Context())
	if err != nil {
		a.writeProblem(w, http.StatusServiceUnavailable, "authorization_unavailable", "Authorization is unavailable")
		return
	}
	gatewayURL := ""
	for _, cluster := range clusters {
		if cluster.ID == clusterID {
			gatewayURL = cluster.GatewayURL
		}
	}
	if gatewayURL == "" {
		a.writeProblem(w, http.StatusNotFound, "cluster_not_found", "Cluster is not registered")
		return
	}
	// Resolving grants here refreshes stale Cognito claims and catches ended sessions before minting.
	if _, err := a.resolveClusterGrants(r.Context(), token, binding, clusterID); errors.Is(err, session.ErrInvalidSession) {
		clearCookies(w)
		a.writeProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
		return
	} else if err != nil {
		a.writeProblem(w, http.StatusServiceUnavailable, "authorization_unavailable", "Authorization is unavailable")
		return
	}
	response, err := a.forwarder.Execute(r.Context(), gatewayproxy.Request{
		SessionID: principal.SessionID, ClusterID: clusterID, GatewayURL: gatewayURL, Method: r.Method,
		EscapedPath: path, Query: query, Body: body, Action: route.Action,
	})
	switch {
	case errors.Is(err, gatewayproxy.ErrOutcomeUnknown):
		a.logger.Error("Console Gateway write outcome unknown", "cluster", clusterID, "error_type", fmt.Sprintf("%T", err))
		a.writeProblem(w, http.StatusServiceUnavailable, "gateway_outcome_unknown", "The change may have been applied; check before retrying")
		return
	case err != nil:
		a.logger.Error("Console Gateway unavailable", "cluster", clusterID, "error_type", fmt.Sprintf("%T", err))
		a.writeProblem(w, http.StatusServiceUnavailable, "gateway_unavailable", "The Gateway could not be reached")
		return
	}
	switch {
	case response.Status == http.StatusUnauthorized:
		clearCookies(w)
		a.writeProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
	case response.Status == http.StatusServiceUnavailable && strings.Contains(string(response.Body), `"code":"policy_unavailable"`):
		a.writeProblem(w, http.StatusServiceUnavailable, "authorization_unavailable", "Authorization is unavailable; nothing was sent to Kafka")
	default:
		contentType := response.ContentType
		if contentType != "application/json" && contentType != "application/problem+json" {
			contentType = "application/json"
		}
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(response.Status)
		_, _ = w.Write(response.Body)
	}
}
