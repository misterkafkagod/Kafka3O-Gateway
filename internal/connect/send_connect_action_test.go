package connect

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSendConnectActionUsesTheDiscoveredRequests(t *testing.T) {
	var asked []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = append(asked, r.Method+" "+r.URL.EscapedPath()+"?"+r.URL.RawQuery)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	send := NewSendConnectAction(server.URL+"/", server.Client())
	for _, action := range []Action{
		{Kind: ActionPause, Connector: "orders sink"},
		{Kind: ActionResume, Connector: "orders sink"},
		{Kind: ActionRestart, Connector: "orders-sink"},
		{Kind: ActionRestartTask, Connector: "orders-sink", Task: 3},
	} {
		if err := send.Execute(context.Background(), action); err != nil {
			t.Fatalf("%+v: %v", action, err)
		}
	}
	want := []string{
		"PUT /connectors/orders%20sink/pause?",
		"PUT /connectors/orders%20sink/resume?",
		"POST /connectors/orders-sink/restart?includeTasks=true&onlyFailed=false",
		"POST /connectors/orders-sink/tasks/3/restart?",
	}
	for i := range want {
		if asked[i] != want[i] {
			t.Fatalf("request %d = %q, want %q", i, asked[i], want[i])
		}
	}
}

func TestSendConnectActionRejectsInvalidRequestsWithoutCallingConnect(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	defer server.Close()
	send := NewSendConnectAction(server.URL, server.Client())
	for _, tt := range []struct {
		action Action
		want   error
	}{
		{Action{Kind: ActionPause, Connector: "a/b"}, ErrInvalidConnectorName},
		{Action{Kind: "delete", Connector: "sink"}, ErrInvalidConnectAction},
		{Action{Kind: ActionRestartTask, Connector: "sink", Task: -1}, ErrInvalidConnectAction},
	} {
		if err := send.Execute(context.Background(), tt.action); !errors.Is(err, tt.want) {
			t.Errorf("%+v: %v", tt.action, err)
		}
	}
	if err := NewSendConnectAction("", http.DefaultClient).Execute(context.Background(), Action{Kind: ActionPause, Connector: "sink"}); !errors.Is(err, ErrConnectNotConfigured) {
		t.Errorf("not configured: %v", err)
	}
	if called {
		t.Fatal("Connect was called")
	}
}

func TestSendConnectActionMapsConnectResponses(t *testing.T) {
	for _, tt := range []struct {
		status int
		want   error
	}{
		{http.StatusNoContent, nil},
		{http.StatusNotFound, ErrConnectorNotFound},
		{http.StatusConflict, ErrConnectRejected},
		{http.StatusBadRequest, ErrConnectRejected},
		{http.StatusInternalServerError, ErrConnectUnreachable},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tt.status)
			_, _ = w.Write([]byte(`{"error_code":0,"message":"x"}`))
		}))
		err := NewSendConnectAction(server.URL, server.Client()).Execute(context.Background(), Action{Kind: ActionPause, Connector: "sink"})
		server.Close()
		if (tt.want == nil && err != nil) || (tt.want != nil && !errors.Is(err, tt.want)) {
			t.Errorf("status %d: %v", tt.status, err)
		}
	}
	// A connection that closes without answering leaves the outcome unknown.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			buffer := make([]byte, 4096)
			_, _ = conn.Read(buffer)
			_ = conn.Close()
		}
	}()
	defer listener.Close()
	if err := NewSendConnectAction("http://"+listener.Addr().String(), &http.Client{}).Execute(context.Background(), Action{Kind: ActionRestart, Connector: "sink"}); !errors.Is(err, ErrConnectActionUnknown) {
		t.Fatalf("dropped connection: %v", err)
	}
}
