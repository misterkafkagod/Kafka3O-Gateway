package s3store

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ListObjects lists the objects under a prefix of one bucket.
type ListObjects interface {
	Execute(ctx context.Context, prefix string) ([]Object, error)
}

type listObjects struct {
	endpoint    string
	bucket      string
	region      string
	credentials Credentials
	client      *http.Client
	now         func() time.Time
}

func NewListObjects(endpoint, bucket, region string, credentials Credentials, client *http.Client, now func() time.Time) ListObjects {
	return listObjects{endpoint: strings.TrimRight(endpoint, "/"), bucket: bucket, region: region, credentials: credentials, client: client, now: now}
}

type listBucketResult struct {
	Contents []struct {
		Key          string `xml:"Key"`
		Size         int64  `xml:"Size"`
		LastModified string `xml:"LastModified"`
	} `xml:"Contents"`
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
}

// Execute pages through ListObjectsV2 with path-style addressing, at most 100 pages of 1000 objects.
func (l listObjects) Execute(ctx context.Context, prefix string) ([]Object, error) {
	if l.credentials.AccessKey == "" || l.credentials.SecretKey == "" {
		return nil, ErrNoCredentials
	}
	var objects []Object
	token := ""
	for page := 0; page < 100; page++ {
		query := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			query.Set("continuation-token", token)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, l.endpoint+"/"+EncodeURIComponent(l.bucket, false)+"?"+query.Encode(), nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		request.Header.Set("X-Amz-Content-Sha256", EmptyPayloadHash)
		SignV4Request(request, l.credentials, l.region, "s3", l.now())
		response, err := l.client.Do(request)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		body, err := io.ReadAll(io.LimitReader(response.Body, 32<<20))
		response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%w: status %d", ErrUnavailable, response.StatusCode)
		}
		var result listBucketResult
		if err := xml.Unmarshal(body, &result); err != nil {
			return nil, fmt.Errorf("%w: decode listing: %w", ErrUnavailable, err)
		}
		for _, entry := range result.Contents {
			modified, err := time.Parse(time.RFC3339, entry.LastModified)
			if err != nil {
				return nil, fmt.Errorf("%w: last modified %q", ErrUnavailable, entry.LastModified)
			}
			objects = append(objects, Object{Key: entry.Key, Size: entry.Size, LastModified: modified})
		}
		if !result.IsTruncated || result.NextContinuationToken == "" {
			return objects, nil
		}
		token = result.NextContinuationToken
	}
	return nil, fmt.Errorf("%w: listing has more than 100 pages", ErrUnavailable)
}
