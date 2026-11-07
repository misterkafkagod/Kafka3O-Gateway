package session

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

type boundLogoutFailureStore struct {
	testStore
	findErr   error
	revokeErr error
	used      bool
}

func (s *boundLogoutFailureStore) FindBoundCognitoSessionForLogout(context.Context, [32]byte, [32]byte) (Principal, error) {
	return Principal{}, s.findErr
}

func (s *boundLogoutFailureStore) UseBoundSession(context.Context, [32]byte, [32]byte, func() time.Time, time.Duration, *cognito.RefreshTokenProtector) (Principal, error) {
	s.used = true
	return Principal{}, ErrInvalidSession
}

func (s *boundLogoutFailureStore) RevokeBoundSession(context.Context, [32]byte, [32]byte, string, time.Time) error {
	return s.revokeErr
}

func TestBoundSessionManagerLogoutPreservesUncertainErrors(t *testing.T) {
	failure := errors.Join(ErrInvalidSession, ErrBrowserSessionUnknown)
	store := &boundLogoutFailureStore{findErr: failure, revokeErr: failure}
	manager := &Manager{store: store, random: rand.Reader, now: time.Now, idleLifetime: time.Minute}
	token := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if principal, err := manager.ResolveBoundForLogout(context.Background(), token, [32]byte{1}); err != failure || store.used || principal.SessionID != "" {
		t.Fatalf("uncertain logout lookup fell back or lost its error: %v", err)
	}
	if err := manager.RevokeBound(context.Background(), token, [32]byte{1}); err != failure {
		t.Fatalf("uncertain bound revocation was hidden: %v", err)
	}
	if _, err := manager.ResolveBoundForLogout(context.Background(), token, [32]byte{}); err != ErrInvalidSession {
		t.Fatalf("logout lookup accepted an empty marker: %v", err)
	}
	if err := manager.RevokeBound(context.Background(), token, [32]byte{}); err != ErrInvalidSession {
		t.Fatalf("logout revocation accepted an empty marker: %v", err)
	}
}

func TestBoundSessionManagerLogoutDoesNotRefreshAfterInvalidLookup(t *testing.T) {
	store := &boundLogoutFailureStore{findErr: ErrInvalidSession}
	coordinator := &cognitoRefreshCoordinator{store: &deadlineRefreshStore{}, active: make(map[cognitoRefreshKey]*cognitoRefreshCall)}
	manager := &Manager{store: store, now: time.Now, idleLifetime: time.Minute, cognitoRefresh: coordinator}
	token := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if _, err := manager.ResolveBoundForLogout(context.Background(), token, [32]byte{1}); err != ErrInvalidSession || !store.used ||
		len(coordinator.active) != 0 {
		t.Fatalf("invalid bound logout lookup attempted provider refresh: %v", err)
	}
}

func TestBoundSessionManagerLogoutRejectsMissingStoreSupport(t *testing.T) {
	manager := &Manager{store: testStore{}, random: rand.Reader, now: time.Now, idleLifetime: time.Minute}
	token := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if _, err := manager.ResolveBoundForLogout(context.Background(), token, [32]byte{1}); err != ErrInvalidSessionConfig {
		t.Fatalf("bound logout accepted a store without bound session support: %v", err)
	}
	if err := manager.RevokeBound(context.Background(), token, [32]byte{1}); err != ErrInvalidSessionConfig {
		t.Fatalf("bound revocation accepted a store without bound revocation support: %v", err)
	}
}
