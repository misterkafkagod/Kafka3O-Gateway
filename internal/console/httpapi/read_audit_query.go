package httpapi

import (
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

// readAuditQuery strictly parses scope and filters; an absent cluster means global scope.
func readAuditQuery(r *http.Request, maxLimit, defaultLimit int) (auditQuery, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return auditQuery{}, errInvalidRequest
	}
	allowed := map[string]bool{"cluster": true, "actor": true, "action": true, "outcome": true, "from": true, "until": true, "after": true, "limit": true}
	for key, entries := range values {
		if !allowed[key] || len(entries) != 1 || entries[0] == "" || len(entries[0]) > 256 {
			return auditQuery{}, errInvalidRequest
		}
	}
	query := auditQuery{filter: AuditFilter{Limit: defaultLimit, ActorID: values.Get("actor"), Action: values.Get("action")}}
	if cluster := values.Get("cluster"); cluster != "" {
		if identity.ValidateClusterID(cluster) != nil {
			return auditQuery{}, errInvalidRequest
		}
		query.filter.ClusterID = cluster
	} else {
		query.filter.Global = true
	}
	if outcome := values.Get("outcome"); outcome != "" {
		switch outcome {
		case "attempted", "denied", "validation_rejected", "accepted", "confirmed", "failed_before_send", "unknown", "succeeded", "failed":
			query.filter.Outcome = outcome
		default:
			return auditQuery{}, errInvalidRequest
		}
	}
	for key, target := range map[string]*time.Time{"from": &query.filter.From, "until": &query.filter.Until} {
		if raw := values.Get(key); raw != "" {
			parsed, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				return auditQuery{}, errInvalidRequest
			}
			*target = parsed
		}
	}
	if raw := values.Get("after"); raw != "" {
		after, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || after < 0 {
			return auditQuery{}, errInvalidRequest
		}
		query.filter.AfterSequence = after
	}
	if raw := values.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > maxLimit {
			return auditQuery{}, errInvalidRequest
		}
		query.filter.Limit = limit
	}
	if !query.filter.From.IsZero() && !query.filter.Until.IsZero() && !query.filter.From.Before(query.filter.Until) {
		return auditQuery{}, errInvalidRequest
	}
	return query, nil
}
