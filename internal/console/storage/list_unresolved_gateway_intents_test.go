package storage

import (
	"context"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
)

func TestUnresolvedGatewayIntentsListOnlyMutationsWithoutOutcome(t *testing.T) {
	f := newDelegationFixture(t)
	ctx := context.Background()
	_, operator := f.addAccount(t, "operator", "operator", "", "OPERATOR")
	record, _ := delegation.NewRecordOutcome(f.db, time.Now)
	decide := func(action authorization.Action) string {
		t.Helper()
		handle := f.mintFor(t, operator.Principal.SessionID, "dev", action)
		decision, err := f.consume.Execute(ctx, delegation.ConsumeRequest{Handle: handle, CallerClusterID: "dev", Action: action, Binding: produceBinding("dev")})
		if err != nil || !decision.Allowed {
			t.Fatalf("decision = %+v, %v", decision, err)
		}
		return decision.OperationID
	}
	resolved := decide(authorization.ActionProduceMessage)
	if err := record.Execute(ctx, "dev", resolved, delegation.OutcomeConfirmed); err != nil {
		t.Fatal(err)
	}
	pending := decide(authorization.ActionDeleteTopic)
	decide(authorization.ActionListTopics)
	later := time.Now().Add(time.Hour)
	intents, err := f.db.ListUnresolvedGatewayIntents(ctx, later, 10)
	if err != nil || len(intents) != 1 || intents[0].OperationID != pending || intents[0].Action != authorization.ActionDeleteTopic || intents[0].ActorID != "operator" {
		t.Fatalf("unresolved = %+v, %v", intents, err)
	}
	if intents, err := f.db.ListUnresolvedGatewayIntents(ctx, time.Now().Add(-time.Hour), 10); err != nil || len(intents) != 0 {
		t.Fatalf("recent intents were reported unresolved: %+v, %v", intents, err)
	}
	if _, err := f.db.ListUnresolvedGatewayIntents(ctx, later, 0); err == nil {
		t.Fatal("an invalid limit was accepted")
	}
}
