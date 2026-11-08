package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"sync/atomic"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/gatewayproxy"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type LocalLogin interface {
	Execute(ctx context.Context, username, password string, client netip.Addr) (session.Credentials, error)
}

type Sessions interface {
	Resolve(ctx context.Context, token string) (session.Principal, error)
	ResolveForLogout(ctx context.Context, token string) (session.Principal, error)
	Revoke(ctx context.Context, token string) error
}

type SessionAPI struct {
	login        LocalLogin
	change       identity.ChangeLocalPassword
	create       identity.CreateLocalAccount
	reset        identity.ResetLocalPassword
	setEnabled   identity.SetLocalAccountEnabled
	list         identity.ListLocalAccounts
	listSessions identity.ListAdminSessions
	revokeAdmin  identity.RevokeAdminSession
	// Access administration is mounted only by EnableAccessAdministration.
	accessList    identity.ListAccessBindings
	changeGrant   identity.ChangeRoleGrant
	changeMapping identity.ChangeGroupMapping
	// Cluster administration is mounted only by EnableClusterAdministration.
	clusterList     identity.ListRegisteredClusters
	classifyCluster identity.ChangeClusterEnvironment
	// Gateway forwarding is mounted only by EnableGatewayForwarding.
	activeClusters ActiveClusters
	localGrants    LocalSessionGrants
	forwarder      gatewayproxy.ForwardRequest
	audit          AuditStore
	sessions       Sessions
	csrf           *session.CSRFVerifier
	expectedHost   string
	markerRandom   io.Reader
	// cognitoEnabled is set before serving; cognito is published once discovery succeeds.
	cognitoEnabled bool
	cognito        atomic.Pointer[CognitoServices]
	logger         *slog.Logger
	mux            *http.ServeMux
}

var (
	ErrInvalidAPIConfig = errors.New("invalid Console session API configuration")
	errInvalidRequest   = errors.New("invalid request")
	errRequestTooLarge  = errors.New("request body is too large")
)

type localLoginBody struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type changePasswordBody struct {
	CurrentPassword string `json:"currentPassword"`
	NewPassword     string `json:"newPassword"`
}

type resetPasswordBody struct {
	TemporaryPassword string `json:"temporaryPassword"`
}

type createLocalAccountBody struct {
	Username          string `json:"username"`
	TemporaryPassword string `json:"temporaryPassword"`
}

// Every access-change field is required; empty strings mean global scope or no role.
type roleGrantChangeBody struct {
	IdentityID   *string `json:"identityId"`
	ClusterID    *string `json:"clusterId"`
	ExpectedRole *string `json:"expectedRole"`
	Role         *string `json:"role"`
}

type groupMappingChangeBody struct {
	Issuer       *string `json:"issuer"`
	Group        *string `json:"group"`
	ClusterID    *string `json:"clusterId"`
	ExpectedRole *string `json:"expectedRole"`
	Role         *string `json:"role"`
}

type clusterEnvironmentChangeBody struct {
	ExpectedEnvironment *string `json:"expectedEnvironment"`
	Environment         *string `json:"environment"`
	Reason              *string `json:"reason"`
}

type requiredBoolean struct {
	Value   bool
	Present bool
}

type setLocalAccountEnabledBody struct {
	Enabled requiredBoolean `json:"enabled"`
}

type localAccountResponse struct {
	ID                 string `json:"id"`
	Username           string `json:"username"`
	Enabled            bool   `json:"enabled"`
	MustChangePassword bool   `json:"mustChangePassword"`
}

type adminSessionResponse struct {
	ID                string `json:"id"`
	AccountID         string `json:"accountId"`
	Username          string `json:"username"`
	Source            string `json:"authSource"`
	CreatedAt         string `json:"createdAt"`
	IdleExpiresAt     string `json:"idleExpiresAt"`
	AbsoluteExpiresAt string `json:"absoluteExpiresAt"`
	IsCurrent         bool   `json:"isCurrent"`
}

type sessionResponse struct {
	AccountID          string `json:"accountId"`
	Username           string `json:"username"`
	Source             string `json:"authSource"`
	MustChangePassword bool   `json:"mustChangePassword"`
	AbsoluteExpiresAt  string `json:"absoluteExpiresAt"`
}
