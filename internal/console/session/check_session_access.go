package session

func CheckSessionAccess(principal Principal, purpose AccessPurpose) error {
	if principal.SessionID == "" || principal.AccountID == "" {
		return ErrInvalidSession
	}
	switch purpose {
	case AccessNormal:
		if principal.MustChangePassword {
			return ErrPasswordChangeRequired
		}
	case AccessPasswordChange, AccessLogout, AccessSessionInfo:
	default:
		return ErrInvalidAccessPurpose
	}
	return nil
}
