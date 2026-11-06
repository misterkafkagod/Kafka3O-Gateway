package session

import (
	"crypto/subtle"
	"net/http"
)

const CSRFHeader = "X-Kafka3O-CSRF"

func (v *CSRFVerifier) Verify(request *http.Request, principal Principal) error {
	if err := v.VerifyOrigin(request); err != nil {
		return err
	}
	if principal.SessionID == "" || principal.AccountID == "" {
		return ErrInvalidCSRF
	}
	values := request.Header.Values(CSRFHeader)
	if len(values) != 1 {
		return ErrInvalidCSRF
	}
	got, err := hashSessionToken(values[0])
	if err != nil || subtle.ConstantTimeCompare(got[:], principal.CSRFHash[:]) != 1 {
		return ErrInvalidCSRF
	}
	return nil
}
