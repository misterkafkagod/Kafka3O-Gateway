package gatewayauth

import (
	"bytes"
	"net/http"
)

// bufferedResponse holds a mutation handler's response until its outcome is durably recorded.
type bufferedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newBufferedResponse() *bufferedResponse {
	return &bufferedResponse{header: http.Header{}}
}

func (b *bufferedResponse) Header() http.Header { return b.header }

func (b *bufferedResponse) Write(data []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}
	return b.body.Write(data)
}

func (b *bufferedResponse) WriteHeader(status int) {
	if b.status == 0 {
		b.status = status
	}
}
