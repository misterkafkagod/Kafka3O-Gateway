package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
)

func TestGatewayOutcomesCorrelateWithAllowedMutationIntents(t *testing.T) {
	f := newDelegationFixture(t)
	ctx := context.Background()
	_, operator := f.addAccount(t, "operator", "operator", "", "OPERATOR")
	_, viewer := f.addAccount(t, "viewer", "viewer", "", "VIEWER")
	record, err := delegation.NewRecordOutcome(f.db, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	decide := func(session, cluster string, action authorization.Action) delegation.Decision {
		t.Helper()
		handle := f.mintFor(t, session, cluster, action)
		decision, err := f.consume.Execute(ctx, delegation.ConsumeRequest{Handle: handle, CallerClusterID: cluster, Action: action, Binding: produceBinding(cluster)})
		if err != nil {
			t.Fatal(err)
		}
		return decision
	}
	allowed := decide(operator.Principal.SessionID, "dev", authorization.ActionProduceMessage)
	if err := record.Execute(ctx, "dev", allowed.OperationID, delegation.OutcomeConfirmed); err != nil {
		t.Fatal(err)
	}
	if err := record.Execute(ctx, "dev", allowed.OperationID, delegation.OutcomeFailed); !errors.Is(err, delegation.ErrDuplicateOutcome) {
		t.Fatalf("second outcome error = %v", err)
	}
	other := decide(operator.Principal.SessionID, "dev", authorization.ActionDeleteTopic)
	if err := record.Execute(ctx, "prod", other.OperationID, delegation.OutcomeConfirmed); !errors.Is(err, delegation.ErrUnknownIntent) {
		t.Fatalf("foreign cluster outcome error = %v", err)
	}
	denied := decide(viewer.Principal.SessionID, "dev", authorization.ActionProduceMessage)
	read := decide(viewer.Principal.SessionID, "dev", authorization.ActionListTopics)
	for _, operationID := range []string{denied.OperationID, read.OperationID, "nonexistent"} {
		if err := record.Execute(ctx, "dev", operationID, delegation.OutcomeConfirmed); !errors.Is(err, delegation.ErrUnknownIntent) {
			t.Fatalf("outcome for %q error = %v", operationID, err)
		}
	}
	if err := record.Execute(ctx, "dev", other.OperationID, "accepted"); !errors.Is(err, delegation.ErrInvalidOutcome) {
		t.Fatalf("invalid outcome error = %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{ClusterID: "dev", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var intent, outcome *AuditEvent
	for i := range events {
		if events[i].OperationID == allowed.OperationID && events[i].ResourceKind == "gateway_request" {
			intent = &events[i]
		}
		if events[i].OperationID == allowed.OperationID && events[i].ResourceKind == "gateway_outcome" {
			outcome = &events[i]
		}
	}
	if intent == nil || outcome == nil || outcome.Outcome != AuditConfirmed || outcome.ActorID != "operator" || outcome.Action != "messages.produce" ||
		outcome.Sequence <= intent.Sequence || intent.Outcome != AuditAttempted {
		t.Fatalf("intent=%+v outcome=%+v", intent, outcome)
	}
}
