package session

func HashBrowserMarker(value string) ([32]byte, error) {
	rawHash, err := hashSessionToken(value)
	if err != nil {
		return [32]byte{}, ErrInvalidBrowserMarker
	}
	digest := deriveBrowserMarkerHash(rawHash)
	if digest == ([32]byte{}) {
		return [32]byte{}, ErrInvalidBrowserMarker
	}
	return digest, nil
}
