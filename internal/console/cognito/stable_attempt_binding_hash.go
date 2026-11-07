package cognito

import "crypto/sha256"

func stableAttemptBindingHash(transientHash, browserHash [32]byte) [32]byte {
	const domain = "Kafka3O Cognito stable browser binding v1\x00"
	var data [len(domain) + 64]byte
	copy(data[:], domain)
	copy(data[len(domain):], transientHash[:])
	copy(data[len(domain)+32:], browserHash[:])
	return sha256.Sum256(data[:])
}
