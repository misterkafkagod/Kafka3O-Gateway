package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func TestSessionHandlerMountsAccessAndClusterAdministrationRoutes(t *testing.T) {
	db, err := storage.OpenDatabase(context.Background(), filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	api, err := newSessionHandler(db, "https://console.example", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []struct{ method, path string }{
		{http.MethodGet, "/api/admin/access"}, {http.MethodPut, "/api/admin/role-grants"}, {http.MethodPut, "/api/admin/group-mappings"},
		{http.MethodGet, "/api/admin/clusters"}, {http.MethodGet, "/api/clusters"}, {http.MethodPut, "/api/admin/clusters/prod/environment"},
	} {
		request := httptest.NewRequest(route.method, "https://console.example"+route.path, nil)
		request.Header.Set("Origin", "https://console.example")
		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s status=%d, want 401 from the mounted route", route.method, route.path, recorder.Code)
		}
	}
}
