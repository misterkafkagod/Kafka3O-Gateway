package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2/humatest"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

type fakeListConnectors struct {
	connectors []connect.Connector
	err        error
}

func (f fakeListConnectors) Execute(context.Context) ([]connect.Connector, error) {
	return f.connectors, f.err
}

func TestConnectListRouteReturnsStatusOnly(t *testing.T) {
	_, api := humatest.New(t)
	RegisterListConnectors(api, fakeListConnectors{connectors: []connect.Connector{{
		Name: "orders-sink", Type: "sink", State: "RUNNING", WorkerID: "w:8083",
		Tasks: []connect.Task{{ID: 0, State: "FAILED", WorkerID: "w:8083", Error: "ConnectException: boom"}},
	}}})
	resp := api.Get("/api/v1/connect/connectors")
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d %s", resp.Code, resp.Body)
	}
	var body []map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body) != 1 || body[0]["name"] != "orders-sink" || body[0]["state"] != "RUNNING" || body[0]["config"] != nil {
		t.Fatalf("body = %s", resp.Body)
	}
	task := body[0]["tasks"].([]any)[0].(map[string]any)
	if task["state"] != "FAILED" || task["error"] != "ConnectException: boom" || task["workerId"] != "w:8083" {
		t.Fatalf("task = %v", task)
	}
}

func TestConnectListRouteMapsErrors(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
		code   string
	}{
		{connect.ErrConnectNotConfigured, http.StatusNotFound, "connect_not_configured"},
		{fmt.Errorf("%w: status 500", connect.ErrConnectUnreachable), http.StatusServiceUnavailable, "connect_unreachable"},
	} {
		_, api := humatest.New(t)
		RegisterListConnectors(api, fakeListConnectors{err: tt.err})
		resp := api.Get("/api/v1/connect/connectors")
		if resp.Code != tt.status || !strings.Contains(resp.Body.String(), `"code":"`+tt.code+`"`) {
			t.Errorf("%v: %d %s", tt.err, resp.Code, resp.Body)
		}
	}
}
