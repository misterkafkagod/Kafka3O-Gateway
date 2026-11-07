package httpapi

func (a *SessionAPI) PublishCognito(services CognitoServices) error {
	if a == nil || !a.cognitoEnabled || services.Attempts == nil || services.Exchanger == nil ||
		services.Sessions == nil || services.LogoutURL == "" {
		return ErrInvalidAPIConfig
	}
	if !a.cognito.CompareAndSwap(nil, &services) {
		return ErrInvalidAPIConfig
	}
	return nil
}
