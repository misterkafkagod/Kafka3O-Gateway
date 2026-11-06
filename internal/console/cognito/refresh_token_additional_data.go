package cognito

import (
	"encoding/binary"
	"strings"
	"unicode"
	"unicode/utf8"
)

func refreshTokenAdditionalData(providerBinding [32]byte, binding RefreshTokenBinding) ([]byte, error) {
	if binding.Generation < 1 || !validRefreshBindingID(binding.SessionID) || !validRefreshBindingID(binding.IdentityID) {
		return nil, ErrInvalidRefreshToken
	}
	additional := make([]byte, 0, len("Kafka3O:Cognito:refresh:v1")+32+8+2+len(binding.SessionID)+2+len(binding.IdentityID))
	additional = append(additional, "Kafka3O:Cognito:refresh:v1"...)
	additional = append(additional, providerBinding[:]...)
	additional = binary.BigEndian.AppendUint64(additional, uint64(binding.Generation))
	additional = binary.BigEndian.AppendUint16(additional, uint16(len(binding.SessionID)))
	additional = append(additional, binding.SessionID...)
	additional = binary.BigEndian.AppendUint16(additional, uint16(len(binding.IdentityID)))
	additional = append(additional, binding.IdentityID...)
	return additional, nil
}

func validRefreshBindingID(id string) bool {
	return id != "" && len(id) <= 128 && utf8.ValidString(id) &&
		strings.TrimSpace(id) == id && strings.IndexFunc(id, unicode.IsControl) < 0
}
