package session

import "context"

func (m *Manager) Revoke(ctx context.Context, token string) error {
	hash, err := hashSessionToken(token)
	if err != nil {
		return err
	}
	eventID, _, err := newSessionValue(m.random, 16)
	if err != nil {
		return err
	}
	return m.store.RevokeSession(ctx, hash, eventID, m.now().UTC())
}
