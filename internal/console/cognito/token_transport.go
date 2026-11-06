package cognito

import (
	"bytes"
	"io"
	"net/http"
)

type tokenTransport struct {
	base     http.RoundTripper
	tokenURL string
}

func (t tokenTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodPost || request.URL.String() != t.tokenURL {
		return nil, ErrCodeExchangeConfig
	}
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, ErrProviderUnavailable
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		response.Body.Close()
		return nil, ErrProviderUnavailable
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxIDTokenBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || len(body) > maxIDTokenBytes {
		return nil, ErrProviderUnavailable
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 && !validOptionalRefreshToken(response.Header.Get("Content-Type"), body) {
		return nil, ErrProviderUnavailable
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response, nil
}
