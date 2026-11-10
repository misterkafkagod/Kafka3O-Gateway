package s3store

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestListObjectsPagesAndSigns(t *testing.T) {
	var asked []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = append(asked, r.URL.Path+"?"+r.URL.RawQuery)
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=read-key/20261110/us-east-1/s3/aws4_request") ||
			r.Header.Get("X-Amz-Content-Sha256") != EmptyPayloadHash {
			t.Errorf("unsigned request: %v", r.Header)
		}
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("continuation-token") == "" {
			_, _ = w.Write([]byte(`<ListBucketResult><Contents><Key>kafka-a/orders/orders-0-00000000000000000000.gz</Key><Size>265</Size><LastModified>2026-11-10T07:35:00.000Z</LastModified></Contents><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken></ListBucketResult>`))
			return
		}
		_, _ = w.Write([]byte(`<ListBucketResult><Contents><Key>kafka-a/orders/orders-0-00000000000000000020.gz</Key><Size>120</Size><LastModified>2026-11-10T08:01:02Z</LastModified></Contents><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	defer server.Close()
	now := func() time.Time { return time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC) }
	objects, err := NewListObjects(server.URL+"/", "kafka3o-backups", "us-east-1", Credentials{AccessKey: "read-key", SecretKey: "s"}, server.Client(), now).
		Execute(context.Background(), "kafka-a/orders/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 || objects[0].Size != 265 || !objects[1].LastModified.Equal(time.Date(2026, 11, 10, 8, 1, 2, 0, time.UTC)) {
		t.Fatalf("objects = %+v", objects)
	}
	if len(asked) != 2 || !strings.HasPrefix(asked[0], "/kafka3o-backups?") || !strings.Contains(asked[0], "prefix=kafka-a%2Forders%2F") || !strings.Contains(asked[1], "continuation-token=next") {
		t.Fatalf("asked %v", asked)
	}
}

func TestListObjectsFailures(t *testing.T) {
	now := time.Now
	if _, err := NewListObjects("http://s3", "b", "us-east-1", Credentials{}, http.DefaultClient, now).Execute(context.Background(), "p/"); !errors.Is(err, ErrNoCredentials) {
		t.Errorf("no credentials: %v", err)
	}
	for name, handler := range map[string]http.HandlerFunc{
		"denied":  func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) },
		"not xml": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("<<")) },
		"bad time": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`<ListBucketResult><Contents><Key>k</Key><LastModified>soon</LastModified></Contents></ListBucketResult>`))
		},
		"endless": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>again</NextContinuationToken></ListBucketResult>`))
		},
	} {
		server := httptest.NewServer(handler)
		_, err := NewListObjects(server.URL, "b", "us-east-1", Credentials{AccessKey: "a", SecretKey: "s"}, server.Client(), now).Execute(context.Background(), "p/")
		server.Close()
		if !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
