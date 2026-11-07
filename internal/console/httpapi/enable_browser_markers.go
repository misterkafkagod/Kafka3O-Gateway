package httpapi

import "io"

func (a *SessionAPI) EnableBrowserMarkers(random io.Reader) error {
	if a == nil || random == nil || a.markerRandom != nil {
		return ErrInvalidAPIConfig
	}
	a.markerRandom = random
	a.mux.HandleFunc("POST /api/session/browser", a.ensureBrowserMarker)
	return nil
}
