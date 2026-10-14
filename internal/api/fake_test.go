package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakeCluster is an in-memory Cluster for handler tests.
type fakeCluster struct {
	pingErr     error
	info        ClusterInfo
	describeErr error
}

func (f *fakeCluster) Ping(context.Context) error { return f.pingErr }

func (f *fakeCluster) DescribeCluster(context.Context) (ClusterInfo, error) {
	return f.info, f.describeErr
}

// get runs a GET request against a server backed by c and decodes the JSON body into out.
func get(t *testing.T, c Cluster, path string, out any) *http.Response {
	t.Helper()
	srv := NewServer(c, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	res := rec.Result()
	if ct := res.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("%s: Content-Type = %q, want application/json", path, ct)
	}
	if out != nil {
		if err := json.NewDecoder(res.Body).Decode(out); err != nil {
			t.Fatalf("%s: decode body: %v", path, err)
		}
	}
	return res
}

func ptr[T any](v T) *T { return &v }
