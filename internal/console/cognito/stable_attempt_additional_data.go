package cognito

import (
	"crypto/sha256"
	"encoding/binary"
	"time"
)

func stableAttemptAdditionalData(stateHash, bindingHash, nonceHash, providerBinding [32]byte, operationID string, expiresAt time.Time) [168]byte {
	base := attemptAdditionalData(stateHash, bindingHash, nonceHash, providerBinding)
	operationHash := sha256.Sum256([]byte(operationID))
	var additional [168]byte
	copy(additional[:128], base[:])
	copy(additional[128:160], operationHash[:])
	binary.BigEndian.PutUint64(additional[160:], uint64(expiresAt.UnixNano()))
	return additional
}
