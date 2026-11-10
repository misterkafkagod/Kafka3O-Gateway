package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2/humatest"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

type fakePreviewOffsetReset struct {
	err        error
	gotGroup   string
	gotTopic   string
	gotTarget  groups.OffsetResetTarget
	calledWith bool
}

func (f *fakePreviewOffsetReset) Execute(_ context.Context, group, topic string, target groups.OffsetResetTarget) (groups.OffsetResetPreview, error) {
	f.calledWith, f.gotGroup, f.gotTopic, f.gotTarget = true, group, topic, target
	if f.err != nil {
		return groups.OffsetResetPreview{}, f.err
	}
	current := int64(9007199254740993)
	return groups.OffsetResetPreview{Group: group, Topic: topic, Partitions: []groups.PartitionOffsetReset{
		{Partition: 0, CurrentOffset: &current, TargetOffset: 9007199254740995},
		{Partition: 1, TargetOffset: 0},
	}}, nil
}

func TestOffsetResetPreviewRouteReturnsPreciseOffsets(t *testing.T) {
	_, api := humatest.New(t)
	fake := &fakePreviewOffsetReset{}
	RegisterPreviewOffsetReset(api, fake)
	resp := api.Get("/api/v1/consumer-groups/team%2Fbilling/offset-reset?topic=orders&target=timestamp&timestamp=1700000000000")
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.Code, resp.Body)
	}
	if fake.gotGroup != "team/billing" || fake.gotTopic != "orders" || fake.gotTarget != (groups.OffsetResetTarget{Kind: "timestamp", Timestamp: 1700000000000}) {
		t.Fatalf("called with %q %q %+v", fake.gotGroup, fake.gotTopic, fake.gotTarget)
	}
	var body struct {
		Group      string `json:"group"`
		Topic      string `json:"topic"`
		Partitions []struct {
			Partition     int32   `json:"partition"`
			CurrentOffset *string `json:"currentOffset"`
			TargetOffset  string  `json:"targetOffset"`
		} `json:"partitions"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Group != "team/billing" || len(body.Partitions) != 2 || *body.Partitions[0].CurrentOffset != "9007199254740993" ||
		body.Partitions[0].TargetOffset != "9007199254740995" || body.Partitions[1].CurrentOffset != nil || body.Partitions[1].TargetOffset != "0" {
		t.Fatalf("body = %s", resp.Body)
	}
}

func TestOffsetResetPreviewRouteValidatesTheTarget(t *testing.T) {
	for _, query := range []string{
		"topic=orders&target=middle", "topic=orders&target=timestamp", "topic=orders&target=timestamp&timestamp=-5",
		"topic=orders&target=timestamp&timestamp=soon", "topic=orders&target=latest&timestamp=1", "target=latest", "topic=orders",
	} {
		_, api := humatest.New(t)
		fake := &fakePreviewOffsetReset{}
		RegisterPreviewOffsetReset(api, fake)
		resp := api.Get("/api/v1/consumer-groups/billing/offset-reset?" + query)
		if resp.Code != http.StatusBadRequest && resp.Code != http.StatusUnprocessableEntity {
			t.Errorf("%s: status = %d", query, resp.Code)
		}
		if fake.calledWith {
			t.Errorf("%s: preview ran", query)
		}
	}
}

func TestOffsetResetPreviewRouteMapsErrors(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
		code   string
	}{
		{groups.ErrGroupActive, http.StatusConflict, "group_active"},
		{groups.ErrGroupNotFound, http.StatusNotFound, "not_found"},
		{groups.ErrTopicNotFound, http.StatusNotFound, "not_found"},
		{errors.New("broker down"), http.StatusServiceUnavailable, "cluster_unreachable"},
	} {
		_, api := humatest.New(t)
		RegisterPreviewOffsetReset(api, &fakePreviewOffsetReset{err: tt.err})
		resp := api.Get("/api/v1/consumer-groups/billing/offset-reset?topic=orders&target=latest")
		if resp.Code != tt.status || !strings.Contains(resp.Body.String(), `"code":"`+tt.code+`"`) {
			t.Errorf("%v: %d %s", tt.err, resp.Code, resp.Body)
		}
	}
	_, api := humatest.New(t)
	RegisterPreviewOffsetReset(api, &fakePreviewOffsetReset{err: groups.ErrGroupActive})
	if resp := api.Get("/api/v1/consumer-groups/billing/offset-reset?topic=orders&target=latest"); !strings.Contains(resp.Body.String(), "Stop the group's consumers first") {
		t.Fatalf("active group detail = %s", resp.Body)
	}
}
