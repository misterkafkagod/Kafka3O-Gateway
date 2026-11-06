package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) ResolveSessionGrants(ctx context.Context, tokenHash [32]byte, clusterID string, requested time.Time,
	protector *cognito.RefreshTokenProtector) (grants []authorization.Grant, result error) {
	if ctx == nil || tokenHash == ([32]byte{}) || requested.IsZero() || requested.Year() < 1970 || requested.Year() > 2261 {
		return nil, session.ErrInvalidSessionConfig
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin session grant resolution: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			grants = nil
			result = errors.Join(result, fmt.Errorf("roll back session grant resolution: %w", err))
		}
	}()
	now, err := d.transactionTime(requested)
	if err != nil {
		return nil, err
	}
	var sessionID, accountID, source string
	var enabled, reserved bool
	var version int64
	var revoked, localVersion, generation, freshness, tokenExpiry sql.NullInt64
	var mustChange sql.NullBool
	var issuer, subject, groupsJSON sql.NullString
	var encrypted []byte
	var deadlines sessionGrantDeadlines
	err = tx.QueryRowContext(ctx, `SELECT sessions.id, identities.id, sessions.auth_source,
sessions.credential_version, sessions.revoked_at_ns, sessions.idle_expires_at_ns, sessions.absolute_expires_at_ns,
identities.enabled, local_accounts.credential_version, local_accounts.must_change_password,
cognito_identities.issuer, cognito_identities.subject, credentials.groups_json, credentials.refresh_generation,
credentials.claims_fresh_until_ns, credentials.token_expires_at_ns, credentials.encrypted_refresh_token,
EXISTS(SELECT 1 FROM audit_events WHERE event_id =
  sessions.id || '.refresh.' || credentials.refresh_generation
  AND action = 'console.cognito.refresh' AND outcome = 'attempted')
FROM console_sessions AS sessions JOIN identities ON identities.id = sessions.identity_id
LEFT JOIN local_accounts ON local_accounts.identity_id = identities.id
LEFT JOIN cognito_identities ON cognito_identities.identity_id = identities.id
LEFT JOIN cognito_session_credentials AS credentials ON credentials.session_id = sessions.id
  AND credentials.identity_id = identities.id
WHERE sessions.token_hash = ?`, tokenHash[:]).Scan(
		&sessionID, &accountID, &source, &version, &revoked, &deadlines.idle, &deadlines.absolute,
		&enabled, &localVersion, &mustChange, &issuer, &subject, &groupsJSON, &generation, &freshness,
		&tokenExpiry, &encrypted, &reserved,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, session.ErrInvalidSession
	}
	if err != nil {
		return nil, fmt.Errorf("inspect session for grant resolution: %w", err)
	}
	if !enabled || revoked.Valid {
		return nil, session.ErrInvalidSession
	}
	switch source {
	case string(session.SourceLocal):
		if !localVersion.Valid || !mustChange.Valid || version != localVersion.Int64 {
			return nil, session.ErrInvalidSession
		}
		if mustChange.Bool {
			return nil, session.ErrPasswordChangeRequired
		}
	case string(session.SourceCognito):
		deadlines.cognito = true
		if version != 1 || !issuer.Valid || !subject.Valid || !cognito.ValidSubject(subject.String) ||
			!groupsJSON.Valid || !utf8.ValidString(groupsJSON.String) || !generation.Valid || generation.Int64 < 1 ||
			!freshness.Valid || !tokenExpiry.Valid || reserved || len(groupsJSON.String) > 1<<20 ||
			!protector.MatchesIssuer(issuer.String) {
			return nil, session.ErrCognitoRefreshUnavailable
		}
		deadlines.freshness, deadlines.tokenExpiry = freshness.Int64, tokenExpiry.Int64
		var groups []string
		if err := json.Unmarshal([]byte(groupsJSON.String), &groups); err != nil || groups == nil {
			return nil, session.ErrCognitoRefreshUnavailable
		}
		for _, group := range groups {
			if group == "" {
				return nil, session.ErrCognitoRefreshUnavailable
			}
		}
		if _, err := protector.Open(cognito.RefreshTokenBinding{
			SessionID: sessionID, IdentityID: accountID, Generation: generation.Int64,
		}, encrypted); err != nil {
			return nil, errors.Join(session.ErrCognitoRefreshUnavailable, err)
		}
	default:
		return nil, session.ErrInvalidSessionConfig
	}
	if err := validateSessionGrantDeadlines(deadlines, now); err != nil {
		return nil, err
	}
	var global, scoped int
	query := `WITH candidates AS (
  SELECT role, cluster_id FROM role_grants
    WHERE account_id = ? AND (cluster_id = '' OR cluster_id = ?)`
	args := []any{accountID, clusterID}
	if source == string(session.SourceCognito) {
		query += `
  UNION ALL
  SELECT bindings.role, bindings.cluster_id FROM cognito_group_bindings AS bindings
    WHERE bindings.issuer = ? COLLATE BINARY
      AND (bindings.cluster_id = '' OR bindings.cluster_id = ?)
      AND bindings.group_name IN (SELECT value FROM json_each(?) WHERE type = 'text')`
		args = append(args, issuer.String, clusterID, groupsJSON.String)
	}
	query += `
)
SELECT COALESCE(MAX(CASE WHEN cluster_id = '' THEN
  CASE role WHEN 'VIEWER' THEN 1 WHEN 'OPERATOR' THEN 2 WHEN 'ADMIN' THEN 3 ELSE 4 END ELSE 0 END), 0),
COALESCE(MAX(CASE WHEN cluster_id = ? AND cluster_id <> '' THEN
  CASE role WHEN 'VIEWER' THEN 1 WHEN 'OPERATOR' THEN 2 WHEN 'ADMIN' THEN 3 ELSE 4 END ELSE 0 END), 0)
FROM candidates`
	args = append(args, clusterID)
	err = tx.QueryRowContext(ctx, query, args...).Scan(&global, &scoped)
	if err != nil {
		return nil, fmt.Errorf("read current session grants: %w", err)
	}
	roles := [...]authorization.Role{authorization.RoleNone, authorization.RoleViewer, authorization.RoleOperator, authorization.RoleAdmin}
	if global < 0 || global >= len(roles) || scoped < 0 || scoped >= len(roles) {
		return nil, authorization.ErrInvalidRole
	}
	now, err = d.transactionTime(now)
	if err != nil {
		return nil, err
	}
	if err := validateSessionGrantDeadlines(deadlines, now); err != nil {
		return nil, err
	}
	resolved := make([]authorization.Grant, 0, 2)
	if global > 0 {
		resolved = append(resolved, authorization.Grant{Role: roles[global]})
	}
	if scoped > 0 {
		resolved = append(resolved, authorization.Grant{ClusterID: clusterID, Role: roles[scoped]})
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit session grant resolution: %w", err)
	}
	completedAt, err := d.transactionTime(now)
	if err != nil {
		return nil, err
	}
	if err := validateSessionGrantDeadlines(deadlines, completedAt); err != nil {
		return nil, err
	}
	return resolved, nil
}
