package s3store

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// GetObject streams one object's body; the caller closes it.
type GetObject interface {
	Execute(ctx context.Context, key string) (io.ReadCloser, error)
}

type getObject struct {
	endpoint    string
	bucket      string
	region      string
	credentials Credentials
	client      *http.Client
	now         func() time.Time
}

func NewGetObject(endpoint, bucket, region string, credentials Credentials, client *http.Client, now func() time.Time) GetObject {
	return getObject{endpoint: strings.TrimRight(endpoint, "/"), bucket: bucket, region: region, credentials: credentials, client: client, now: now}
}

func (g getObject) Execute(ctx context.Context, key string) (io.ReadCloser, error) {
	if g.credentials.AccessKey == "" || g.credentials.SecretKey == "" {
		return nil, ErrNoCredentials
	}
	target := g.endpoint + "/" + EncodeURIComponent(g.bucket, false) + "/" + EncodeURIComponent(key, true)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	request.Header.Set("X-Amz-Content-Sha256", EmptyPayloadHash)
	SignV4Request(request, g.credentials, g.region, "s3", g.now())
	response, err := g.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return nil, fmt.Errorf("%w: status %d", ErrUnavailable, response.StatusCode)
	}
	return response.Body, nil
}
