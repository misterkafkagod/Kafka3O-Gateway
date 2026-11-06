package session

import "crypto/subtle"

func (m *Manager) newSessionRecord() (Record, string, string, error) {
	now := m.now().UTC()
	if now.IsZero() || now.Year() < 1970 || now.Add(m.absoluteLifetime).Year() > 2261 {
		return Record{}, "", "", ErrInvalidSessionConfig
	}
	token, tokenHash, err := newSessionValue(m.random, 32)
	if err != nil {
		return Record{}, "", "", err
	}
	csrf, csrfHash, err := newSessionValue(m.random, 32)
	if err != nil {
		return Record{}, "", "", err
	}
	if subtle.ConstantTimeCompare(tokenHash[:], csrfHash[:]) == 1 {
		return Record{}, "", "", ErrInvalidSessionConfig
	}
	sessionID, _, err := newSessionValue(m.random, 16)
	if err != nil {
		return Record{}, "", "", err
	}
	eventID, _, err := newSessionValue(m.random, 16)
	if err != nil {
		return Record{}, "", "", err
	}
	return Record{
		SessionID: sessionID, TokenHash: tokenHash, CSRFHash: csrfHash, CreatedAt: now,
		IdleExpiresAt: now.Add(m.idleLifetime), AbsoluteExpiresAt: now.Add(m.absoluteLifetime), EventID: eventID,
	}, token, csrf, nil
}
