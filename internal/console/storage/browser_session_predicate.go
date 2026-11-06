package storage

func browserSessionPredicate(alias string, browserHash [32]byte) (string, []any) {
	if browserHash == ([32]byte{}) {
		return alias + ".browser_binding_hash IS NULL", nil
	}
	return alias + `.browser_binding_hash = ? AND EXISTS (
SELECT 1 FROM console_browser_sessions AS browser_sessions
WHERE browser_sessions.browser_hash = ? AND browser_sessions.session_id = ` + alias + `.id)`,
		[]any{browserHash[:], browserHash[:]}
}
