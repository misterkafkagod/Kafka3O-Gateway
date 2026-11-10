package connect

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// connectFixture serves the discovery fixture recorded from the EKS-D test worker.
func connectFixture(t *testing.T, status int, body string) (*httptest.Server, *[]string) {
	t.Helper()
	var asked []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = append(asked, r.Method+" "+r.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server, &asked
}

func TestFetchConnectConnectorsMapsTheStatusFixture(t *testing.T) {
	fixture, err := os.ReadFile("testdata/connectors-expand-status.json")
	if err != nil {
		t.Fatal(err)
	}
	server, asked := connectFixture(t, http.StatusOK, string(fixture))
	connectors, err := NewFetchConnectConnectors(server.URL+"/", server.Client()).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(*asked) != 1 || (*asked)[0] != "GET /connectors?expand=status" {
		t.Fatalf("asked %v", *asked)
	}
	names := []string{}
	for _, connector := range connectors {
		names = append(names, connector.Name)
	}
	if strings.Join(names, ",") != "kafka3o-verify-failing,kafka3o-verify-paused,kafka3o-verify-running" {
		t.Fatalf("names = %v", names)
	}
	failing, paused := connectors[0], connectors[1]
	if failing.Type != "sink" || failing.State != "RUNNING" || failing.WorkerID != "0.0.0.0:8083" || len(failing.Tasks) != 1 ||
		failing.Tasks[0].State != "FAILED" || !strings.HasPrefix(failing.Tasks[0].Error, "org.apache.kafka.connect.errors.ConnectException: Couldn't find or create file") ||
		strings.Contains(failing.Tasks[0].Error, "\n") {
		t.Fatalf("failing = %+v", failing)
	}
	if paused.State != "PAUSED" || paused.Tasks[0].State != "PAUSED" || paused.Tasks[0].Error != "" {
		t.Fatalf("paused = %+v", paused)
	}
}

func TestFetchConnectConnectorsReportsMissingOrBrokenConnect(t *testing.T) {
	if _, err := NewFetchConnectConnectors("", http.DefaultClient).Execute(context.Background()); !errors.Is(err, ErrConnectNotConfigured) {
		t.Fatalf("not configured: %v", err)
	}
	for name, server := range map[string]*httptest.Server{
		"server error": func() *httptest.Server {
			s, _ := connectFixture(t, http.StatusInternalServerError, `{"error_code":500,"message":"x"}`)
			return s
		}(),
		"malformed json": func() *httptest.Server { s, _ := connectFixture(t, http.StatusOK, `{"x":`); return s }(),
	} {
		if _, err := NewFetchConnectConnectors(server.URL, server.Client()).Execute(context.Background()); !errors.Is(err, ErrConnectUnreachable) {
			t.Errorf("%s: %v", name, err)
		}
	}
	closed := httptest.NewServer(http.NotFoundHandler())
	address := closed.URL
	closed.Close()
	if _, err := NewFetchConnectConnectors(address, http.DefaultClient).Execute(context.Background()); !errors.Is(err, ErrConnectUnreachable) {
		t.Fatalf("closed port: %v", err)
	}
}

func TestListConnectorsAppliesItsTimeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	t.Cleanup(slow.Close)
	started := time.Now()
	_, err := NewListConnectors(NewFetchConnectConnectors(slow.URL, slow.Client()), 200*time.Millisecond).Execute(context.Background())
	if !errors.Is(err, ErrConnectUnreachable) || time.Since(started) > 2*time.Second {
		t.Fatalf("slow Connect: %v after %v", err, time.Since(started))
	}
}
