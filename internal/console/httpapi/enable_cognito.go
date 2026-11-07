package httpapi

import "io"

func (a *SessionAPI) EnableCognito(random io.Reader) error {
	if a == nil || a.cognitoEnabled {
		return ErrInvalidAPIConfig
	}
	if err := a.EnableBrowserMarkers(random); err != nil {
		return err
	}
	a.cognitoEnabled = true
	a.mux.HandleFunc("GET /api/session/cognito", a.getCognitoAvailability)
	a.mux.HandleFunc("POST /api/session/cognito/start", a.startCognitoSignIn)
	a.mux.HandleFunc("POST /api/session/cognito/callback", a.completeCognitoSignIn)
	return nil
}
