package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2/humatest"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

type fakeManageConnector struct {
	err     error
	actions []connect.Action
}

func (f *fakeManageConnector) Execute(_ context.Context, action connect.Action) error {
	f.actions = append(f.actions, action)
	return f.err
}

func TestConnectActionRoutesAcceptEachAction(t *testing.T) {
	_, api := humatest.New(t)
	fake := &fakeManageConnector{}
	RegisterConnectorActions(api, fake)
	for path, want := range map[string]string{
		"/api/v1/connect/connectors/orders%20sink/pause":           `"action":"pause"`,
		"/api/v1/connect/connectors/orders%20sink/resume":          `"action":"resume"`,
		"/api/v1/connect/connectors/orders%20sink/restart":         `"action":"restart"`,
		"/api/v1/connect/connectors/orders%20sink/tasks/2/restart": `"task":2`,
	} {
		resp := api.Post(path, strings.NewReader(""))
		if resp.Code != http.StatusAccepted || !strings.Contains(resp.Body.String(), want) || !strings.Contains(resp.Body.String(), `"connector":"orders sink"`) {
			t.Errorf("%s: %d %s", path, resp.Code, resp.Body)
		}
	}
	kinds := map[string]bool{}
	for _, action := range fake.actions {
		kinds[action.Kind] = true
		if action.Connector != "orders sink" {
			t.Errorf("connector = %q", action.Connector)
		}
		if action.Kind == connect.ActionRestartTask && action.Task != 2 {
			t.Errorf("task = %d", action.Task)
		}
	}
	if len(kinds) != 4 {
		t.Fatalf("kinds = %v", kinds)
	}
}

func TestConnectActionRoutesMapErrors(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
		code   string
	}{
		{connect.ErrConnectNotConfigured, http.StatusNotFound, "connect_not_configured"},
		{connect.ErrInvalidConnectorName, http.StatusBadRequest, "invalid_connector"},
		{connect.ErrConnectorNotFound, http.StatusNotFound, "not_found"},
		{connect.ErrConnectRejected, http.StatusConflict, "connect_rejected"},
		{errors.Join(connect.ErrConnectActionUnknown, errors.New("reset")), http.StatusServiceUnavailable, "connect_action_unknown"},
		{connect.ErrConnectUnreachable, http.StatusServiceUnavailable, "connect_unreachable"},
	} {
		_, api := humatest.New(t)
		RegisterConnectorActions(api, &fakeManageConnector{err: tt.err})
		resp := api.Post("/api/v1/connect/connectors/sink/pause", strings.NewReader(""))
		if resp.Code != tt.status || !strings.Contains(resp.Body.String(), `"code":"`+tt.code+`"`) {
			t.Errorf("%v: %d %s", tt.err, resp.Code, resp.Body)
		}
	}
	_, api := humatest.New(t)
	fake := &fakeManageConnector{}
	RegisterConnectorActions(api, fake)
	if resp := api.Post("/api/v1/connect/connectors/sink/tasks/-1/restart", strings.NewReader("")); resp.Code != http.StatusUnprocessableEntity && resp.Code != http.StatusBadRequest {
		t.Fatalf("negative task: %d", resp.Code)
	}
	if len(fake.actions) != 0 {
		t.Fatalf("a negative task reached Connect: %+v", fake.actions)
	}
}
