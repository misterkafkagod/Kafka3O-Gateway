package cognito

import "net/http"

func ReadBrowserAuthorizationCookie(request *http.Request) (string, error) {
	if request == nil {
		return "", ErrInvalidBrowserCallback
	}
	var binding string
	for _, cookie := range request.Cookies() {
		if cookie.Name != BrowserAuthorizationCookieName {
			continue
		}
		if binding != "" {
			return "", ErrInvalidBrowserCallback
		}
		if _, err := hashAttemptValue(cookie.Value); err != nil {
			return "", ErrInvalidBrowserCallback
		}
		binding = cookie.Value
	}
	if binding == "" {
		return "", ErrInvalidBrowserCallback
	}
	return binding, nil
}
