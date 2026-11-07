package session

import "io"

func NewBrowserMarker(random io.Reader) (string, [32]byte, error) {
	if random == nil {
		return "", [32]byte{}, ErrInvalidSessionConfig
	}
	value, _, err := newSessionValue(random, 32)
	if err != nil {
		return "", [32]byte{}, browserMarkerEntropyError{cause: err}
	}
	hash, err := HashBrowserMarker(value)
	if err != nil {
		return "", [32]byte{}, err
	}
	return value, hash, nil
}
