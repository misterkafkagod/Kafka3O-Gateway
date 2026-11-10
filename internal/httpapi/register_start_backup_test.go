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

type fakeStartBackup struct {
	err   error
	topic string
}

func (f *fakeStartBackup) Execute(_ context.Context, topic string) (backup.Backup, error) {
	f.topic = topic
	return backup.Backup{Topic: topic, Connector: "kafka3o-backup-" + topic, Location: "s3://b/kafka-a/" + topic + "/"}, f.err
}

func TestBackupStartRouteCreatesTheBackup(t *testing.T) {
	_, api := humatest.New(t)
	fake := &fakeStartBackup{}
	RegisterStartBackup(api, fake)
	resp := api.Post("/api/v1/backups", strings.NewReader(`{"topic":"orders"}`))
	if resp.Code != http.StatusCreated || fake.topic != "orders" || !strings.Contains(resp.Body.String(), `"location":"s3://b/kafka-a/orders/"`) {
		t.Fatalf("%d %s", resp.Code, resp.Body)
	}
	if strings.Contains(strings.ToLower(resp.Body.String()), "secret") {
		t.Fatalf("response mentions a secret: %s", resp.Body)
	}
}

func TestBackupStartRouteMapsErrors(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
		code   string
	}{
		{backup.ErrBackupsNotConfigured, http.StatusNotFound, "backups_not_configured"},
		{backup.ErrInvalidTopic, http.StatusBadRequest, "invalid_topic"},
		{backup.ErrBackupExists, http.StatusConflict, "backup_exists"},
		{connect.ErrConnectActionUnknown, http.StatusServiceUnavailable, "connect_action_unknown"},
		{connect.ErrConnectUnreachable, http.StatusServiceUnavailable, "connect_unreachable"},
	} {
		_, api := humatest.New(t)
		RegisterStartBackup(api, &fakeStartBackup{err: tt.err})
		resp := api.Post("/api/v1/backups", strings.NewReader(`{"topic":"orders"}`))
		if resp.Code != tt.status || !strings.Contains(resp.Body.String(), `"code":"`+tt.code+`"`) {
			t.Errorf("%v: %d %s", tt.err, resp.Code, resp.Body)
		}
	}
	_, api := humatest.New(t)
	fake := &fakeStartBackup{}
	RegisterStartBackup(api, fake)
	if resp := api.Post("/api/v1/backups", strings.NewReader(`{"topic":""}`)); resp.Code != http.StatusUnprocessableEntity && resp.Code != http.StatusBadRequest {
		t.Fatalf("empty topic: %d", resp.Code)
	}
	if fake.topic != "" {
		t.Fatal("an empty topic reached the backup")
	}
}
