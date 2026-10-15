package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// uiServer serves a temp UI dir containing index.html and app.js, with a
// secret file next to (outside) that dir.
func uiServer(t *testing.T) http.Handler {
	t.Helper()
	base := t.TempDir()
	ui := filepath.Join(base, "ui")
	for name, body := range map[string]string{
		filepath.Join(ui, "index.html"):    "<html>index</html>",
		filepath.Join(ui, "assets/app.js"): "console.log('app')",
		filepath.Join(base, "secret.txt"):  "TOP SECRET",
	} {
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	srv, err := NewServer(&fakeCluster{}, slog.New(slog.NewTextHandler(io.Discard, nil)), ui)
	if err != nil {
		t.Fatal(err)
	}
	return srv.Handler()
}

func serve(h http.Handler, target string) (*http.Response, string) {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	res := rec.Result()
	body, _ := io.ReadAll(res.Body)
	return res, string(body)
}

func TestStaticFileServed(t *testing.T) {
	res, body := serve(uiServer(t), "/assets/app.js")
	if res.StatusCode != http.StatusOK || body != "console.log('app')" {
		t.Errorf("status %d body %q", res.StatusCode, body)
	}
}

func TestStaticDeepLinkGetsIndex(t *testing.T) {
	for _, p := range []string{"/", "/topics/x"} {
		res, body := serve(uiServer(t), p)
		if res.StatusCode != http.StatusOK || body != "<html>index</html>" {
			t.Errorf("%s: status %d body %q", p, res.StatusCode, body)
		}
	}
}

func TestStaticUnknownAPIIsJSON404(t *testing.T) {
	res, body := serve(uiServer(t), "/api/nope")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", res.StatusCode)
	}
	var e errorBody
	if err := json.Unmarshal([]byte(body), &e); err != nil || e.Error.Code != CodeNotFound {
		t.Errorf("body %q, err %v", body, err)
	}
}

func TestStaticPathTraversalBlocked(t *testing.T) {
	h := uiServer(t)
	for _, p := range []string{"/../secret.txt", "/assets/../../secret.txt", "/%2e%2e/secret.txt"} {
		_, body := serve(h, p)
		if strings.Contains(body, "TOP SECRET") {
			t.Errorf("%s served the file outside the UI dir", p)
		}
	}
}

func TestStaticMissingIndexFailsAtStart(t *testing.T) {
	if _, err := NewServer(&fakeCluster{}, slog.New(slog.NewTextHandler(io.Discard, nil)), t.TempDir()); err == nil {
		t.Error("NewServer accepted a UI dir without index.html")
	}
}
