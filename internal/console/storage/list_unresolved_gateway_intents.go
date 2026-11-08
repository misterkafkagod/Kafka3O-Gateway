package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
)

// ListUnresolvedGatewayIntents returns allowed mutation intents recorded before olderThan that have no outcome,
// oldest first.
func (d *Database) ListUnresolvedGatewayIntents(ctx context.Context, olderThan time.Time, limit int) ([]delegation.UnresolvedIntent, error) {
	if limit < 1 || limit > 1000 {
		return nil, ErrInvalidAuditFilter
	}
	mutations := []authorization.Action{authorization.ActionProduceMessage, authorization.ActionCreateTopic, authorization.ActionDeleteTopic,
		authorization.ActionAlterTopicConfig, authorization.ActionAlterBrokerConfig, authorization.ActionIncreasePartitions, authorization.ActionReassignReplicas}
	args := []any{olderThan.UTC().UnixNano()}
	for _, action := range mutations {
		args = append(args, string(action))
	}
	args = append(args, limit)
	rows, err := d.db.QueryContext(ctx, `SELECT intent.operation_id, intent.cluster_id, intent.action, intent.actor_id, intent.occurred_at_ns
FROM audit_events intent
WHERE intent.resource_kind = 'gateway_request' AND intent.decision = 'allowed' AND intent.occurred_at_ns < ?
AND intent.action IN (`+strings.TrimSuffix(strings.Repeat("?, ", len(mutations)), ", ")+`)
AND NOT EXISTS (SELECT 1 FROM audit_events outcome WHERE outcome.operation_id = intent.operation_id AND outcome.resource_kind = 'gateway_outcome')
ORDER BY intent.sequence ASC LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("query unresolved gateway intents: %w", err)
	}
	intents := []delegation.UnresolvedIntent{}
	for rows.Next() {
		var intent delegation.UnresolvedIntent
		var occurred int64
		if err := rows.Scan(&intent.OperationID, &intent.ClusterID, &intent.Action, &intent.ActorID, &occurred); err != nil {
			rows.Close()
			return nil, fmt.Errorf("read unresolved gateway intent: %w", err)
		}
		intent.OccurredAt = time.Unix(0, occurred).UTC()
		intents = append(intents, intent)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, fmt.Errorf("iterate unresolved gateway intents: %w", err)
	}
	return intents, nil
}
