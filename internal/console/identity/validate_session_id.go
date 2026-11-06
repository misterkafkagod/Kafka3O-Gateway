package identity

import "encoding/base64"

func ValidateSessionID(id string) error {
	if len(id) != 22 {
		return ErrInvalidSessionID
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(id)
	if err != nil || len(decoded) != 16 {
		return ErrInvalidSessionID
	}
	return nil
}
