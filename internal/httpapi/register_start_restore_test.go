package httpapi

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2/humatest"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

type fakeStartRestore struct {
	err   error
	topic string
}

func (f *fakeStartRestore) Execute(_ context.Context, topic string) (backup.Restore, error) {
	f.topic = topic
	return backup.Restore{Topic: topic, RestoreID: "r-1", Objects: 2}, f.err
}

func TestRestoreStartRoute(t *testing.T) {
	_, api := humatest.New(t)
	fake := &fakeStartRestore{}
	RegisterStartRestore(api, fake)
	resp := api.Post("/api/v1/backups/orders/restore", strings.NewReader(""))
	if resp.Code != http.StatusAccepted || fake.topic != "orders" || !strings.Contains(resp.Body.String(), `"restoreId":"r-1"`) {
		t.Fatalf("%d %s", resp.Code, resp.Body)
	}
	for _, tt := range []struct {
		err    error
		status int
		code   string
	}{
		{backup.ErrRestoreInProgress, http.StatusConflict, "restore_in_progress"},
		{backup.ErrNothingToRestore, http.StatusNotFound, "nothing_to_restore"},
		{backup.ErrStorageUnavailable, http.StatusServiceUnavailable, "storage_unavailable"},
		{connect.ErrConnectActionUnknown, http.StatusServiceUnavailable, "connect_action_unknown"},
	} {
		_, api := humatest.New(t)
		RegisterStartRestore(api, &fakeStartRestore{err: tt.err})
		resp := api.Post("/api/v1/backups/orders/restore", strings.NewReader(""))
		if resp.Code != tt.status || !strings.Contains(resp.Body.String(), `"code":"`+tt.code+`"`) {
			t.Errorf("%v: %d %s", tt.err, resp.Code, resp.Body)
		}
	}
}
