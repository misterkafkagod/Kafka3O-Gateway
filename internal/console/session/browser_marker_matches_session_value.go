package session

func BrowserMarkerMatchesSessionValue(browserHash, valueHash [32]byte) bool {
	return browserHash != ([32]byte{}) &&
		(browserHash == valueHash || browserHash == deriveBrowserMarkerHash(valueHash))
}
