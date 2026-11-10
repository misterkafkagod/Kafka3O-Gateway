package httpapi

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2/humatest"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

type fakeListBackups struct {
	backups []backup.Backup
	err     error
}

func (f fakeListBackups) Execute(context.Context) ([]backup.Backup, error) { return f.backups, f.err }

func TestBackupListRouteReturnsBackups(t *testing.T) {
	last := time.Date(2026, 11, 10, 8, 1, 0, 0, time.UTC)
	_, api := humatest.New(t)
	RegisterListBackups(api, fakeListBackups{backups: []backup.Backup{{Topic: "orders", Connector: "kafka3o-backup-orders", Location: "s3://b/kafka-a/orders/",
		State: "RUNNING", Storage: &backup.StorageSummary{Objects: 2, StoredBytes: 385, LastWrite: &last}}}})
	resp := api.Get("/api/v1/backups")
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), `"lastWrite":"2026-11-10T08:01:00Z"`) || !strings.Contains(resp.Body.String(), `"storedBytes":385`) {
		t.Fatalf("%d %s", resp.Code, resp.Body)
	}
}

func TestBackupListRouteMapsErrors(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
		code   string
	}{
		{backup.ErrBackupsNotConfigured, http.StatusNotFound, "backups_not_configured"},
		{connect.ErrConnectUnreachable, http.StatusServiceUnavailable, "connect_unreachable"},
	} {
		_, api := humatest.New(t)
		RegisterListBackups(api, fakeListBackups{err: tt.err})
		resp := api.Get("/api/v1/backups")
		if resp.Code != tt.status || !strings.Contains(resp.Body.String(), `"code":"`+tt.code+`"`) {
			t.Errorf("%v: %d %s", tt.err, resp.Code, resp.Body)
		}
	}
}
