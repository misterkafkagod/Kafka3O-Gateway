package storage

import (
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func validateSessionRecord(record session.Record) error {
	if strings.TrimSpace(record.SessionID) == "" || len(record.SessionID) > 128 || record.CredentialVersion < 1 ||
		record.TokenHash == ([32]byte{}) || record.CSRFHash == ([32]byte{}) || record.TokenHash == record.CSRFHash ||
		(record.BrowserBindingHash != ([32]byte{}) &&
			(record.BrowserBindingHash == record.TokenHash || record.BrowserBindingHash == record.CSRFHash)) ||
		record.CreatedAt.IsZero() || record.CreatedAt.Year() < 1970 || record.AbsoluteExpiresAt.Year() > 2261 ||
		!record.CreatedAt.Before(record.IdleExpiresAt) || record.IdleExpiresAt.After(record.AbsoluteExpiresAt) ||
		record.AbsoluteExpiresAt.Sub(record.CreatedAt) > session.MaxAbsoluteLifetime {
		return session.ErrInvalidSessionConfig
	}
	return nil
}
