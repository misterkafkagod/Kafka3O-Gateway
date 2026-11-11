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

type fakeStartRestore struct {
	err    error
	topic  string
	filter backup.RestoreFilter
	calls  int
}

func (f *fakeStartRestore) Execute(_ context.Context, topic string, filter backup.RestoreFilter) (backup.Restore, error) {
	f.topic, f.filter = topic, filter
	f.calls++
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
		{backup.ErrInvalidRestoreFilter, http.StatusBadRequest, "invalid_restore_filter"},
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

func TestRestoreStartRoutePassesTheSelection(t *testing.T) {
	_, api := humatest.New(t)
	fake := &fakeStartRestore{}
	RegisterStartRestore(api, fake)
	resp := api.Post("/api/v1/backups/orders/restore", strings.NewReader(`{"from":"2026-11-11T01:00:00Z","to":"2026-11-11T02:00:00.5Z","partitions":[0,2]}`))
	if resp.Code != http.StatusAccepted || fake.filter.From == nil || fake.filter.To == nil || len(fake.filter.Partitions) != 2 ||
		!fake.filter.From.Equal(time.Date(2026, 11, 11, 1, 0, 0, 0, time.UTC)) || fake.filter.To.UnixMilli() != time.Date(2026, 11, 11, 2, 0, 0, 500e6, time.UTC).UnixMilli() {
		t.Fatalf("%d %s %+v", resp.Code, resp.Body, fake.filter)
	}
	for _, body := range []string{"", "{}"} {
		fake := &fakeStartRestore{}
		_, api := humatest.New(t)
		RegisterStartRestore(api, fake)
		if resp := api.Post("/api/v1/backups/orders/restore", strings.NewReader(body)); resp.Code != http.StatusAccepted || fake.filter.From != nil || fake.filter.Partitions != nil {
			t.Fatalf("%q: %d %s %+v", body, resp.Code, resp.Body, fake.filter)
		}
	}
	fake = &fakeStartRestore{}
	_, api = humatest.New(t)
	RegisterStartRestore(api, fake)
	if resp := api.Post("/api/v1/backups/orders/restore", strings.NewReader(`{"partitions":[]}`)); resp.Code != http.StatusAccepted || fake.filter.Partitions == nil || len(fake.filter.Partitions) != 0 {
		t.Fatalf("an empty partition list must reach validation: %d %+v", resp.Code, fake.filter)
	}
}

func TestRestoreStartRouteRefusesMalformedSelections(t *testing.T) {
	for _, body := range []string{`{"from":"yesterday"}`, `{"partitions":["0"]}`, `{"topic":"other"}`} {
		fake := &fakeStartRestore{}
		_, api := humatest.New(t)
		RegisterStartRestore(api, fake)
		if resp := api.Post("/api/v1/backups/orders/restore", strings.NewReader(body)); resp.Code < 400 || resp.Code >= 500 || fake.calls != 0 {
			t.Errorf("%s: %d %s", body, resp.Code, resp.Body)
		}
	}
}
