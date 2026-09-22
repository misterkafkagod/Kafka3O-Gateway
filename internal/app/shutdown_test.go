package app

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// orderRecorder records the names of shutdown steps as they complete, and
// asserts (Shutdown/Close on the fakes below) they happen in the right
// order relative to the real http.Server drain.
type orderRecorder struct {
	mu    sync.Mutex
	order []string
}

func (r *orderRecorder) record(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, name)
}

func (r *orderRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

type fakeTelemetry struct{ rec *orderRecorder }

func (f fakeTelemetry) Shutdown(context.Context) error {
	f.rec.record("telemetry")
	return nil
}

type fakeKafka struct{ rec *orderRecorder }

func (f fakeKafka) Close() { f.rec.record("kafka") }

func TestApp_ShutdownOrder(t *testing.T) {
	t.Parallel()

	inFlight := make(chan struct{})
	release := make(chan struct{})
	requestCompleted := make(chan struct{}, 1)

	server := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(inFlight)
			<-release
			w.WriteHeader(http.StatusOK)
			requestCompleted <- struct{}{}
		}),
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	rec := &orderRecorder{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- runServer(ctx, listener, server, 5*time.Second, fakeTelemetry{rec}, []kafkaCloser{fakeKafka{rec}}, logger)
	}()

	// Start a request and let it reach the handler before triggering
	// shutdown, so Shutdown must genuinely wait for it (drain), not just
	// happen to run after an already-finished request.
	go func() {
		//nolint:gosec // G107: server.Addr is this test's own loopback listener, not user input.
		resp, err := http.Get("http://" + listener.Addr().String() + "/")
		if err != nil {
			t.Errorf("in-flight request error: %v", err)
			return
		}
		_ = resp.Body.Close()
	}()

	select {
	case <-inFlight:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	cancel()

	// Give runServer a moment to call server.Shutdown, then release the
	// in-flight handler — proving Shutdown was waiting on it, not skipping it.
	time.Sleep(50 * time.Millisecond)
	close(release)

	select {
	case <-requestCompleted:
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request never completed — Shutdown did not drain it")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runServer() error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runServer() never returned")
	}

	if got := rec.snapshot(); len(got) != 2 || got[0] != "telemetry" || got[1] != "kafka" {
		t.Fatalf("shutdown order = %v, want [telemetry kafka] (after the HTTP drain)", got)
	}

	found := false
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry["msg"] == "shutdown complete" {
			found = true
		}
	}
	if !found {
		t.Errorf("log output missing a %q line: %s", "shutdown complete", buf.String())
	}
}
