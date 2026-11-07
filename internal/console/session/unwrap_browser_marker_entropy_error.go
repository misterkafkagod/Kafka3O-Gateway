package session

func (e browserMarkerEntropyError) Unwrap() error {
	return e.cause
}
