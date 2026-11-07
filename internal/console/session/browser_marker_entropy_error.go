package session

type browserMarkerEntropyError struct {
	cause error
}

func (browserMarkerEntropyError) Error() string {
	return "browser marker entropy source is unavailable"
}
