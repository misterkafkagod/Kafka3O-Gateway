package policyapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
)

type consumeBody struct {
	Handle  string `json:"handle"`
	Binding string `json:"binding"`
	Action  string `json:"action"`
}

type decisionResponse struct {
	OperationID string `json:"operationId"`
	Allowed     bool   `json:"allowed"`
	ActorID     string `json:"actorId"`
	Source      string `json:"source"`
	Denial      string `json:"denial"`
}

type problem struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
}

type handler struct {
	consume  delegation.ConsumeDelegation
	clusters RegisteredClusters
	logger   *slog.Logger
}

// NewHandler serves consume for verified Gateway identities of active registered clusters. The caller's cluster
// comes only from its certificate; the body never names it.
func NewHandler(consume delegation.ConsumeDelegation, clusters RegisteredClusters, logger *slog.Logger) (http.Handler, error) {
	if consume == nil || clusters == nil || logger == nil {
		return nil, ErrConfig
	}
	return handler{consume: consume, clusters: clusters, logger: logger}, nil
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != ConsumePath || r.URL.RawQuery != "" {
		writeJSON(w, http.StatusNotFound, problem{http.StatusNotFound, "not_found"})
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, problem{http.StatusMethodNotAllowed, "method_not_allowed"})
		return
	}
	cluster, ok := servicetls.GatewayCluster(r.TLS)
	if !ok {
		writeJSON(w, http.StatusForbidden, problem{http.StatusForbidden, "unknown_gateway"})
		return
	}
	active, err := h.clusters.ActiveRegisteredCluster(r.Context(), cluster)
	if err != nil {
		h.logger.Error("policy registry unavailable", "error_type", errorType(err))
		writeJSON(w, http.StatusServiceUnavailable, problem{http.StatusServiceUnavailable, "policy_unavailable"})
		return
	}
	if !active {
		writeJSON(w, http.StatusForbidden, problem{http.StatusForbidden, "unknown_gateway"})
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	data, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
	var body consumeBody
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var extra json.RawMessage
	if err != nil || mediaType != "application/json" || readErr != nil || decoder.Decode(&body) != nil || decoder.Decode(&extra) != io.EOF {
		writeJSON(w, http.StatusBadRequest, problem{http.StatusBadRequest, "invalid_request"})
		return
	}
	binding, err := base64.RawURLEncoding.Strict().DecodeString(body.Binding)
	if err != nil || len(binding) != 32 || body.Action == "" {
		writeJSON(w, http.StatusBadRequest, problem{http.StatusBadRequest, "invalid_request"})
		return
	}
	decision, err := h.consume.Execute(r.Context(), delegation.ConsumeRequest{
		Handle: body.Handle, CallerClusterID: cluster, Action: authorization.Action(body.Action), Binding: [32]byte(binding),
	})
	switch {
	case errors.Is(err, delegation.ErrInvalidDelegation):
		writeJSON(w, http.StatusUnauthorized, problem{http.StatusUnauthorized, "invalid_delegation"})
	case err != nil:
		h.logger.Error("policy decision unavailable", "error_type", errorType(err), "operation_id", decision.OperationID)
		writeJSON(w, http.StatusServiceUnavailable, problem{http.StatusServiceUnavailable, "policy_unavailable"})
	default:
		writeJSON(w, http.StatusOK, decisionResponse{decision.OperationID, decision.Allowed, decision.ActorID, decision.Source, string(decision.Denial)})
	}
}
