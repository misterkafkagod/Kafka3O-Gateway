package session

import "crypto/sha256"

func deriveBrowserMarkerHash(rawHash [32]byte) [32]byte {
	const domain = "Kafka3O stable browser marker v1\x00"
	var data [len(domain) + sha256.Size]byte
	copy(data[:], domain)
	copy(data[len(domain):], rawHash[:])
	return sha256.Sum256(data[:])
}
