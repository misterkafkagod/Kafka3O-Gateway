package cognito

import (
	"encoding/binary"
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
