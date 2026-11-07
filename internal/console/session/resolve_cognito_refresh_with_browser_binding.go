package session

import (
	"context"
	"errors"
)

func (m *Manager) resolveCognitoRefreshWithBrowserBinding(ctx context.Context, tokenHash, browserHash [32]byte) (Principal, error) {
	coordinator := m.cognitoRefresh
	var boundSessions BoundSessionStore
	var boundRefresh BoundCognitoRefreshStore
	if browserHash != ([32]byte{}) {
		var ok bool
		boundSessions, ok = m.store.(BoundSessionStore)
		if !ok {
			return Principal{}, ErrInvalidSessionConfig
		}
		boundRefresh, ok = coordinator.store.(BoundCognitoRefreshStore)
		if !ok {
			return Principal{}, ErrInvalidSessionConfig
		}
	}
	key := cognitoRefreshKey{tokenHash: tokenHash, browserHash: browserHash}
	coordinator.mu.Lock()
	call := coordinator.active[key]
	if call == nil {
		if len(coordinator.active) >= maxCognitoRefreshes {
			coordinator.mu.Unlock()
			return Principal{}, ErrCognitoRefreshUnavailable
		}
		call = &cognitoRefreshCall{done: make(chan struct{})}
		coordinator.active[key] = call
		go func() {
			err := m.runCognitoRefreshWithBrowserBinding(tokenHash, browserHash)
			coordinator.mu.Lock()
			call.err = err
			close(call.done)
			delete(coordinator.active, key)
			coordinator.mu.Unlock()
		}()
	}
	coordinator.mu.Unlock()
	select {
	case <-ctx.Done():
		return Principal{}, ctx.Err()
	case <-call.done:
	}
	if call.err != nil {
		return Principal{}, call.err
	}
	var principal Principal
	var err error
	if boundSessions != nil {
		principal, err = boundSessions.UseBoundSession(ctx, tokenHash, browserHash, m.now, m.idleLifetime, m.cognitoProtector)
	} else {
		principal, err = m.store.UseSession(ctx, tokenHash, m.now, m.idleLifetime, m.cognitoProtector)
	}
	if err == ErrInvalidSession {
		var inspectErr error
		if boundRefresh != nil {
			_, inspectErr = boundRefresh.InspectBoundCognitoRefresh(ctx, tokenHash, browserHash, m.now().UTC())
		} else {
			_, inspectErr = coordinator.store.InspectCognitoRefresh(ctx, tokenHash, m.now().UTC())
		}
		if inspectErr == ErrInvalidSession {
			return Principal{}, ErrInvalidSession
		}
		return Principal{}, errors.Join(ErrCognitoRefreshUnavailable, inspectErr)
	}
	return principal, err
}
