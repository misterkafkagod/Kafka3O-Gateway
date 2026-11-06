package cognito

import (
	"errors"
	"net/http"
)

type providerTransport struct {
	base         http.RoundTripper
	discoveryURL string
	jwksURL      string
}

func (t providerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodGet ||
		(request.URL.String() != t.discoveryURL && request.URL.String() != t.jwksURL) ||
		request.Header.Get("Authorization") != "" {
		return nil, ErrInvalidProviderConfig
	}
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, errors.Join(ErrProviderUnavailable, err)
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return nil, ErrProviderUnavailable
	}
	response.Body = &providerResponseBody{ReadCloser: response.Body, remaining: maxProviderResponseBytes}
	return response, nil
}
