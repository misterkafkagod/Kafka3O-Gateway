package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbe_Exit0WhenReadyReturns200(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "reader-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if got := probeCheck(srv.URL, "/health/ready", "reader-key"); got != 0 {
		t.Errorf("probeCheck() = %d, want 0", got)
	}
}

func TestProbe_Exit1OnWrongKey(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "reader-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if got := probeCheck(srv.URL, "/health/ready", "wrong-key"); got != 1 {
		t.Errorf("probeCheck() = %d, want 1", got)
	}
}

func TestProbe_Exit1On503(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	if got := probeCheck(srv.URL, "/health/ready", "reader-key"); got != 1 {
		t.Errorf("probeCheck() = %d, want 1", got)
	}
}

func TestProbe_PathSelection(t *testing.T) {
	t.Parallel()
	cases := []struct {
		args    []string
		want    string
		wantOK  bool
		message string
	}{
		{[]string{"--live"}, "/health/live", true, "live"},
		{[]string{"--ready"}, "/health/ready", true, "ready"},
		{[]string{}, "", false, "neither"},
		{[]string{"--live", "--ready"}, "", false, "both"},
	}
	for _, c := range cases {
		path, ok := probePath(c.args)
		if ok != c.wantOK || (ok && path != c.want) {
			t.Errorf("%s: probePath(%v) = (%q, %v), want (%q, %v)", c.message, c.args, path, ok, c.want, c.wantOK)
		}
	}
}

func TestProbe_EnvValue(t *testing.T) {
	t.Parallel()
	environ := []string{"FOO=bar", "KGW_PROBE_API_KEY=secret-key"}
	if got := envValue(environ, "KGW_PROBE_API_KEY"); got != "secret-key" {
		t.Errorf("envValue() = %q, want secret-key", got)
	}
	if got := envValue(environ, "MISSING"); got != "" {
		t.Errorf("envValue(missing) = %q, want empty", got)
	}
}
