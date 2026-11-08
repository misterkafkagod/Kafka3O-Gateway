package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

type delegationFixture struct {
	accountAdministrationFixture
	mint    delegation.MintDelegation
	consume delegation.ConsumeDelegation
	clock   *atomic.Int64
}

func newDelegationFixture(t *testing.T) delegationFixture {
	t.Helper()
	f := newAccountAdministrationFixture(t)
	registerTestClusters(t, f.db, "dev", "prod", "new")
	if _, err := f.db.db.Exec(`UPDATE registered_clusters SET environment = 'NON_PRODUCTION' WHERE id = 'dev';
UPDATE registered_clusters SET environment = 'PRODUCTION' WHERE id = 'prod'`); err != nil {
		t.Fatal(err)
	}
	clock := &atomic.Int64{}
	clock.Store(time.Now().UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()) }
	mint, err := delegation.NewMintDelegation(f.db, rand.Reader, now)
	if err != nil {
		t.Fatal(err)
	}
	consume, err := delegation.NewConsumeDelegation(f.db, f.db, f.db, nil, rand.Reader, now)
	if err != nil {
		t.Fatal(err)
	}
	return delegationFixture{accountAdministrationFixture: f, mint: mint, consume: consume, clock: clock}
}

func produceBinding(cluster string) [32]byte {
	return authorization.RequestBinding(cluster, "POST", "/api/v1/topics/orders/messages", url.Values{}, []byte(`{"value":"v"}`))
}

func (f delegationFixture) mintFor(t *testing.T, sessionID, cluster string, action authorization.Action) string {
	t.Helper()
	handle, err := f.mint.Execute(context.Background(), sessionID, cluster, action, "/api/v1/topics/orders/messages", produceBinding(cluster), "")
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func TestDelegationDecisionsFollowRolesAndClassification(t *testing.T) {
	f := newDelegationFixture(t)
	ctx := context.Background()
	_, viewer := f.addAccount(t, "viewer", "viewer", "", "VIEWER")
	_, operator := f.addAccount(t, "operator", "operator", "", "OPERATOR")
	_, clusterAdmin := f.addAccount(t, "prod-admin", "prod-admin", "prod", "ADMIN")
	_, nobody := f.addAccount(t, "nobody", "nobody", "", "")
	for _, tt := range []struct {
		name    string
		session string
		cluster string
		action  authorization.Action
		allowed bool
		denial  authorization.Denial
	}{
		{"viewer reads production", viewer.Principal.SessionID, "prod", authorization.ActionListTopics, true, ""},
		{"viewer cannot produce", viewer.Principal.SessionID, "dev", authorization.ActionProduceMessage, false, authorization.DenialInsufficientRole},
		{"operator produces non-production", operator.Principal.SessionID, "dev", authorization.ActionProduceMessage, true, ""},
		{"operator cannot produce production", operator.Principal.SessionID, "prod", authorization.ActionProduceMessage, false, authorization.DenialProductionAdmin},
		{"operator cannot produce unclassified", operator.Principal.SessionID, "new", authorization.ActionProduceMessage, false, authorization.DenialUnclassified},
		{"production admin still needs a reason", clusterAdmin.Principal.SessionID, "prod", authorization.ActionDeleteTopic, false, authorization.DenialReasonRequired},
		{"cluster admin reads only its cluster", clusterAdmin.Principal.SessionID, "dev", authorization.ActionListTopics, false, authorization.DenialInsufficientRole},
		{"no grant", nobody.Principal.SessionID, "dev", authorization.ActionListTopics, false, authorization.DenialInsufficientRole},
	} {
		t.Run(tt.name, func(t *testing.T) {
			handle := f.mintFor(t, tt.session, tt.cluster, tt.action)
			decision, err := f.consume.Execute(ctx, delegation.ConsumeRequest{Handle: handle, CallerClusterID: tt.cluster, Action: tt.action, Binding: produceBinding(tt.cluster)})
			if err != nil || decision.Allowed != tt.allowed || decision.Denial != tt.denial || decision.ActorID == "" || decision.Source != "local" {
				t.Fatalf("decision = %+v, %v", decision, err)
			}
		})
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{ClusterID: "prod", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var production []AuditEvent
	for _, event := range events {
		if event.ResourceKind == "gateway_request" {
			production = append(production, event)
		}
	}
	// The permitted read of prod is not audited; the two prod denials are.
	if len(production) != 2 || production[0].Decision != "denied:production_admin_required" || production[0].Environment != "PRODUCTION" ||
		production[0].Outcome != AuditDenied || production[1].Decision != "denied:change_reason_required" {
		t.Fatalf("production decisions audit = %+v", production)
	}
}

func TestDelegationsAreSingleUseBoundAndExpire(t *testing.T) {
	f := newDelegationFixture(t)
	ctx := context.Background()
	_, operator := f.addAccount(t, "operator", "operator", "", "OPERATOR")
	session := operator.Principal.SessionID
	request := func(handle, cluster string, action authorization.Action, binding [32]byte) error {
		_, err := f.consume.Execute(ctx, delegation.ConsumeRequest{Handle: handle, CallerClusterID: cluster, Action: action, Binding: binding})
		return err
	}
	handle := f.mintFor(t, session, "dev", authorization.ActionProduceMessage)
	if err := request(handle, "dev", authorization.ActionProduceMessage, produceBinding("dev")); err != nil {
		t.Fatal(err)
	}
	if err := request(handle, "dev", authorization.ActionProduceMessage, produceBinding("dev")); !errors.Is(err, delegation.ErrInvalidDelegation) {
		t.Fatalf("replay error = %v", err)
	}
	for _, tt := range []struct {
		name    string
		cluster string
		action  authorization.Action
		binding [32]byte
	}{
		{"wrong caller cluster", "prod", authorization.ActionProduceMessage, produceBinding("dev")},
		{"different action", "dev", authorization.ActionDeleteTopic, produceBinding("dev")},
		{"changed request", "dev", authorization.ActionProduceMessage, authorization.RequestBinding("dev", "POST", "/api/v1/topics/orders/messages", nil, []byte(`{"value":"evil"}`))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			handle := f.mintFor(t, session, "dev", authorization.ActionProduceMessage)
			if err := request(handle, tt.cluster, tt.action, tt.binding); !errors.Is(err, delegation.ErrInvalidDelegation) {
				t.Fatalf("error = %v", err)
			}
			if err := request(handle, "dev", authorization.ActionProduceMessage, produceBinding("dev")); !errors.Is(err, delegation.ErrInvalidDelegation) {
				t.Fatalf("a rejected delegation remained usable: %v", err)
			}
		})
	}
	expiring := f.mintFor(t, session, "dev", authorization.ActionProduceMessage)
	f.clock.Add(int64(delegation.Lifetime))
	if err := request(expiring, "dev", authorization.ActionProduceMessage, produceBinding("dev")); !errors.Is(err, delegation.ErrInvalidDelegation) {
		t.Fatalf("expired error = %v", err)
	}
	if err := request("not-a-handle", "dev", authorization.ActionProduceMessage, produceBinding("dev")); !errors.Is(err, delegation.ErrInvalidDelegation) {
		t.Fatalf("malformed handle error = %v", err)
	}
	var stored int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM gateway_delegations").Scan(&stored); err != nil || stored != 0 {
		t.Fatalf("delegations left after consumption: %d %v", stored, err)
	}
}

func TestDelegationsFollowRevocationAndGrantChangesAndFailClosed(t *testing.T) {
	f := newDelegationFixture(t)
	ctx := context.Background()
	_, operator := f.addAccount(t, "operator", "operator", "", "OPERATOR")
	admin := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	handle := f.mintFor(t, operator.Principal.SessionID, "dev", authorization.ActionProduceMessage)
	if _, err := roleGrantService(t, f.db).Execute(ctx, admin, "operator", "", authorization.RoleOperator, authorization.RoleViewer); err != nil {
		t.Fatal(err)
	}
	decision, err := f.consume.Execute(ctx, delegation.ConsumeRequest{Handle: handle, CallerClusterID: "dev", Action: authorization.ActionProduceMessage, Binding: produceBinding("dev")})
	if err != nil || decision.Allowed || decision.Denial != authorization.DenialInsufficientRole {
		t.Fatalf("decision minted before a downgrade = %+v, %v", decision, err)
	}
	handle = f.mintFor(t, operator.Principal.SessionID, "dev", authorization.ActionListTopics)
	if err := f.manager.Revoke(ctx, operator.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := f.consume.Execute(ctx, delegation.ConsumeRequest{Handle: handle, CallerClusterID: "dev", Action: authorization.ActionListTopics, Binding: produceBinding("dev")}); !errors.Is(err, delegation.ErrInvalidDelegation) {
		t.Fatalf("revoked session error = %v", err)
	}
	_, writer := f.addAccount(t, "writer", "writer", "", "OPERATOR")
	_, viewer := f.addAccount(t, "viewer", "viewer", "", "VIEWER")
	if _, err := f.db.db.Exec(`CREATE TRIGGER reject_decision_audit BEFORE INSERT ON audit_events
WHEN NEW.resource_kind = 'gateway_request' BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	// The intent of a mutation must be durable before dispatch: without audit there is no permission.
	handle = f.mintFor(t, writer.Principal.SessionID, "dev", authorization.ActionProduceMessage)
	if decision, err := f.consume.Execute(ctx, delegation.ConsumeRequest{Handle: handle, CallerClusterID: "dev", Action: authorization.ActionProduceMessage, Binding: produceBinding("dev")}); !errors.Is(err, delegation.ErrPolicyUnavailable) || decision.Allowed {
		t.Fatalf("unaudited mutation decision = %+v, %v", decision, err)
	}
	// A permitted read is not audited, so it is unaffected; a denied one still needs the audit.
	handle = f.mintFor(t, viewer.Principal.SessionID, "dev", authorization.ActionListTopics)
	if decision, err := f.consume.Execute(ctx, delegation.ConsumeRequest{Handle: handle, CallerClusterID: "dev", Action: authorization.ActionListTopics, Binding: produceBinding("dev")}); err != nil || !decision.Allowed {
		t.Fatalf("permitted read during an audit outage = %+v, %v", decision, err)
	}
	handle = f.mintFor(t, viewer.Principal.SessionID, "dev", authorization.ActionProduceMessage)
	if _, err := f.consume.Execute(ctx, delegation.ConsumeRequest{Handle: handle, CallerClusterID: "dev", Action: authorization.ActionProduceMessage, Binding: produceBinding("dev")}); !errors.Is(err, delegation.ErrPolicyUnavailable) {
		t.Fatalf("unaudited denial error = %v", err)
	}
}
