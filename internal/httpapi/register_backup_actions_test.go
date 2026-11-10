package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2/humatest"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

type fakeManageBackup struct {
	err   error
	calls []string
}

func (f *fakeManageBackup) Execute(_ context.Context, topic, kind string) error {
	f.calls = append(f.calls, kind+" "+topic)
	return f.err
}

func TestBackupActionRoutes(t *testing.T) {
	_, api := humatest.New(t)
	fake := &fakeManageBackup{}
	RegisterBackupActions(api, fake)
	for _, resp := range []*httptest.ResponseRecorder{
		api.Post("/api/v1/backups/orders/pause", strings.NewReader("")),
		api.Post("/api/v1/backups/orders/resume", strings.NewReader("")),
		api.Delete("/api/v1/backups/orders"),
	} {
		if resp.Code != http.StatusAccepted {
			t.Fatalf("%d %s", resp.Code, resp.Body)
		}
	}
	if strings.Join(fake.calls, ",") != "pause orders,resume orders,delete orders" {
		t.Fatalf("calls = %v", fake.calls)
	}
	for _, tt := range []struct {
		err    error
		status int
		code   string
	}{
		{backup.ErrBackupNotFound, http.StatusNotFound, "not_found"},
		{backup.ErrBackupsNotConfigured, http.StatusNotFound, "backups_not_configured"},
		{connect.ErrConnectActionUnknown, http.StatusServiceUnavailable, "connect_action_unknown"},
	} {
		_, api := humatest.New(t)
		RegisterBackupActions(api, &fakeManageBackup{err: tt.err})
		resp := api.Delete("/api/v1/backups/orders")
		if resp.Code != tt.status || !strings.Contains(resp.Body.String(), `"code":"`+tt.code+`"`) {
			t.Errorf("%v: %d %s", tt.err, resp.Code, resp.Body)
		}
	}
}
