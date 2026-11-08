package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
)

func TestCognitoDelegationsAreDecidedLikeLocalOnes(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	ctx := context.Background()
	registerTestClusters(t, f.db, "dev", "prod")
	if _, err := f.db.db.Exec(`UPDATE registered_clusters SET environment = 'NON_PRODUCTION' WHERE id = 'dev';
UPDATE registered_clusters SET environment = 'PRODUCTION' WHERE id = 'prod'`); err != nil {
		t.Fatal(err)
	}
	mint, err := delegation.NewMintDelegation(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	var published *cognito.RefreshTokenProtector
	consume, err := delegation.NewConsumeDelegation(f.db, f.db, f.db, func() *cognito.RefreshTokenProtector { return published }, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	decide := func(cluster string, action authorization.Action) (delegation.Decision, error) {
		t.Helper()
		binding := produceBinding(cluster)
		handle, err := mint.Execute(ctx, f.credentials.Principal.SessionID, cluster, action, "/api/v1/topics/orders/messages", binding, "")
		if err != nil {
			t.Fatal(err)
		}
		return consume.Execute(ctx, delegation.ConsumeRequest{Handle: handle, CallerClusterID: cluster, Action: action, Binding: binding})
	}
	addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "", authorization.RoleViewer)
	if decision, err := decide("dev", authorization.ActionListTopics); !errors.Is(err, delegation.ErrPolicyUnavailable) || decision.Allowed {
		t.Fatalf("decision before Cognito discovery = %+v, %v", decision, err)
	}
	published = f.protector
	for _, tt := range []struct {
		name    string
		cluster string
		action  authorization.Action
		allowed bool
		denial  authorization.Denial
	}{
		{"Cognito viewer reads", "prod", authorization.ActionListTopics, true, ""},
		{"Cognito viewer cannot produce", "dev", authorization.ActionProduceMessage, false, authorization.DenialInsufficientRole},
	} {
		decision, err := decide(tt.cluster, tt.action)
		if err != nil || decision.Allowed != tt.allowed || decision.Denial != tt.denial || decision.Source != "cognito" {
			t.Fatalf("%s = %+v, %v", tt.name, decision, err)
		}
	}
	addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "dev", authorization.RoleOperator)
	for _, tt := range []struct {
		cluster string
		allowed bool
		denial  authorization.Denial
	}{
		{"dev", true, ""}, {"prod", false, authorization.DenialInsufficientRole},
	} {
		decision, err := decide(tt.cluster, authorization.ActionProduceMessage)
		if err != nil || decision.Allowed != tt.allowed || decision.Denial != tt.denial {
			t.Fatalf("Cognito dev operator produce on %s = %+v, %v", tt.cluster, decision, err)
		}
	}
	if _, err := f.db.db.Exec("UPDATE cognito_group_bindings SET role = 'OPERATOR' WHERE cluster_id = ''"); err != nil {
		t.Fatal(err)
	}
	if decision, err := decide("prod", authorization.ActionProduceMessage); err != nil || decision.Allowed || decision.Denial != authorization.DenialProductionAdmin {
		t.Fatalf("Cognito global operator produce on prod = %+v, %v", decision, err)
	}
	if err := f.sessions.Revoke(ctx, f.credentials.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := decide("dev", authorization.ActionListTopics); !errors.Is(err, delegation.ErrInvalidDelegation) {
		t.Fatalf("revoked Cognito session error = %v", err)
	}
}
