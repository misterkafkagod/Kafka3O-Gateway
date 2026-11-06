package session

import (
	"net/http"
	"net/url"
)

type CSRFVerifier struct {
	origin string
}

func NewCSRFVerifier(origin string) (*CSRFVerifier, error) {
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.String() != origin {
		return nil, ErrInvalidSessionConfig
	}
	return &CSRFVerifier{origin: origin}, nil
}

func (v *CSRFVerifier) VerifyOrigin(request *http.Request) error {
	if request == nil {
		return ErrInvalidCSRF
	}
	origins := request.Header.Values("Origin")
	if len(origins) != 1 || origins[0] != v.origin {
		return ErrInvalidCSRF
	}
	return nil
}
