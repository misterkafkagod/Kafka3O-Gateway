package gatewayauth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

// PolicyTimeout is the Gateway's share of the request budget for the Console decision.
const PolicyTimeout = time.Second

type delegatedAuthorization struct {
	next      http.Handler
	policy    PolicyClient
	clusterID string
	logger    *slog.Logger
}

// WithDelegatedAuthorization admits a request only when it comes from the Console over verified mTLS, maps to a
// known action, and carries a delegation the Console accepts for this exact request. Liveness stays anonymous.
func WithDelegatedAuthorization(next http.Handler, policy PolicyClient, clusterID string, logger *slog.Logger) (http.Handler, error) {
	if next == nil || policy == nil || clusterID == "" || logger == nil {
		return nil, ErrConfig
	}
	return delegatedAuthorization{next: next, policy: policy, clusterID: clusterID, logger: logger}, nil
}

func (h delegatedAuthorization) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	route, known := ClassifyRoute(r.Method, r.URL.EscapedPath())
	if known && route.Anonymous {
		h.next.ServeHTTP(w, r)
		return
	}
	if !isConsolePeer(r.TLS) {
		writeProblem(w, http.StatusUnauthorized, "service_authentication_required", "Only the Kafka3O Console may call this Gateway")
		return
	}
	if !known {
		writeProblem(w, http.StatusForbidden, "action_not_classified", "This route has no authorization policy")
		return
	}
	handles := r.Header.Values(DelegationHeader)
	if len(handles) != 1 {
		writeProblem(w, http.StatusUnauthorized, "delegation_required", "A Console delegation is required")
		return
	}
	query := r.URL.Query()
	for _, values := range query {
		if len(values) != 1 {
			writeProblem(w, http.StatusBadRequest, "invalid_request", "Duplicate query parameters are not allowed")
			return
		}
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxBodyBytes+1))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_request", "The request body could not be read")
		return
	}
	if len(body) > MaxBodyBytes {
		writeProblem(w, http.StatusRequestEntityTooLarge, "request_too_large", "Request body is too large")
		return
	}
	binding := authorization.RequestBinding(h.clusterID, r.Method, r.URL.EscapedPath(), query, body)
	ctx, cancel := context.WithTimeout(r.Context(), PolicyTimeout)
	decision, err := h.policy.Consume(ctx, handles[0], binding, route.Action)
	cancel()
	switch {
	case errors.Is(err, ErrInvalidDelegation):
		writeProblem(w, http.StatusUnauthorized, "invalid_delegation", "The Console delegation is missing, expired, used or does not match")
		return
	case err != nil:
		h.logger.Error("policy decision unavailable", "action", string(route.Action), "error_type", fmt.Sprintf("%T", err))
		writeProblem(w, http.StatusServiceUnavailable, "policy_unavailable", "Authorization is unavailable; nothing was sent to Kafka")
		return
	case !decision.Allowed:
		writeProblem(w, http.StatusForbidden, "forbidden", "Not permitted: "+decision.Denial)
		return
	}
	r.Header.Del(DelegationHeader)
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	if !authorization.IsKafkaMutation(route.Action) {
		h.next.ServeHTTP(w, r)
		return
	}
	// The intent is durable; the result reaches the client only once its outcome is recorded too.
	buffered := newBufferedResponse()
	h.next.ServeHTTP(buffered, r)
	outcome := classifyOutcome(buffered.status, buffered.body.Bytes())
	recordContext, cancelRecord := context.WithTimeout(context.WithoutCancel(r.Context()), PolicyTimeout)
	err = h.policy.RecordOutcome(recordContext, decision.OperationID, outcome)
	cancelRecord()
	if err != nil {
		h.logger.Error("mutation outcome could not be recorded", "operation_id", decision.OperationID, "outcome", outcome,
			"error_type", fmt.Sprintf("%T", err))
		writeOutcomeUnknown(w, decision.OperationID)
		return
	}
	flushBufferedResponse(w, buffered)
}
