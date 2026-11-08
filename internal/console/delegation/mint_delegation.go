package delegation

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

type MintDelegation interface {
	Execute(ctx context.Context, sessionID, clusterID string, action authorization.Action, resource string, binding [32]byte, reason string) (string, error)
}

type mintDelegation struct {
	store  Store
	random io.Reader
	now    func() time.Time
}

func NewMintDelegation(store Store, random io.Reader, now func() time.Time) (MintDelegation, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrConfig
	}
	return mintDelegation{store: store, random: random, now: now}, nil
}

// Execute stores a single-use delegation and returns its handle, which must only travel in the Gateway request header.
func (m mintDelegation) Execute(ctx context.Context, sessionID, clusterID string, action authorization.Action, resource string,
	binding [32]byte, reason string) (string, error) {
	if sessionID == "" || clusterID == "" || action == "" || resource == "" || binding == ([32]byte{}) {
		return "", ErrInvalidMint
	}
	raw := make([]byte, 32)
	if _, err := io.ReadFull(m.random, raw); err != nil {
		return "", err
	}
	handle := base64.RawURLEncoding.EncodeToString(raw)
	now := m.now()
	err := m.store.CreateGatewayDelegation(ctx, Record{
		HandleHash: sha256.Sum256([]byte(handle)), SessionID: sessionID, ClusterID: clusterID, Action: action,
		Resource: resource, Binding: binding, Reason: reason, CreatedAt: now, ExpiresAt: now.Add(Lifetime),
	})
	if err != nil {
		return "", err
	}
	return handle, nil
}
