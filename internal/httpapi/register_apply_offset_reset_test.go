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

type fakeApplyOffsetReset struct {
	err      error
	group    string
	topic    string
	requests []groups.OffsetResetRequest
	called   bool
}

func (f *fakeApplyOffsetReset) Execute(_ context.Context, group, topic string, requests []groups.OffsetResetRequest) (groups.OffsetResetResult, error) {
	f.called, f.group, f.topic, f.requests = true, group, topic, requests
	if f.err != nil {
		return groups.OffsetResetResult{}, f.err
	}
	result := groups.OffsetResetResult{Group: group, Topic: topic}
	for _, request := range requests {
		result.Partitions = append(result.Partitions, groups.PartitionOffsetReset{Partition: request.Partition, TargetOffset: request.TargetOffset})
	}
	return result, nil
}

const applyBody = `{"topic":"orders","partitions":[{"partition":0,"expectedOffset":"9007199254740993","targetOffset":"9007199254740995"},{"partition":1,"expectedOffset":null,"targetOffset":"0"}]}`

func TestOffsetResetApplyRouteCommitsPreciseOffsets(t *testing.T) {
	_, api := humatest.New(t)
	fake := &fakeApplyOffsetReset{}
	RegisterApplyOffsetReset(api, fake)
	resp := api.Post("/api/v1/consumer-groups/team%2Fbilling/offset-reset", strings.NewReader(applyBody))
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.Code, resp.Body)
	}
	if fake.group != "team/billing" || fake.topic != "orders" || len(fake.requests) != 2 ||
		*fake.requests[0].ExpectedOffset != 9007199254740993 || fake.requests[0].TargetOffset != 9007199254740995 ||
		fake.requests[1].ExpectedOffset != nil || fake.requests[1].TargetOffset != 0 {
		t.Fatalf("requests = %+v", fake.requests)
	}
	var body struct {
		Group      string `json:"group"`
		Partitions []struct {
			Partition       int32  `json:"partition"`
			CommittedOffset string `json:"committedOffset"`
		} `json:"partitions"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil || body.Group != "team/billing" || body.Partitions[0].CommittedOffset != "9007199254740995" {
		t.Fatalf("body = %s (%v)", resp.Body, err)
	}
}

func TestOffsetResetApplyRouteValidatesTheBody(t *testing.T) {
	for _, body := range []string{
		`{"topic":"orders","partitions":[]}`,
		`{"topic":"","partitions":[{"partition":0,"targetOffset":"1"}]}`,
		`{"topic":"orders","partitions":[{"partition":-1,"targetOffset":"1"}]}`,
		`{"topic":"orders","partitions":[{"partition":0,"targetOffset":"-1"}]}`,
		`{"topic":"orders","partitions":[{"partition":0,"targetOffset":"99999999999999999999"}]}`,
		`{"topic":"orders","partitions":[{"partition":0,"expectedOffset":"x","targetOffset":"1"}]}`,
		`{"topic":"orders","partitions":[{"partition":0,"targetOffset":1}]}`,
	} {
		_, api := humatest.New(t)
		fake := &fakeApplyOffsetReset{}
		RegisterApplyOffsetReset(api, fake)
		resp := api.Post("/api/v1/consumer-groups/billing/offset-reset", strings.NewReader(body))
		if resp.Code != http.StatusBadRequest && resp.Code != http.StatusUnprocessableEntity {
			t.Errorf("%s: status = %d", body, resp.Code)
		}
		if fake.called {
			t.Errorf("%s: reset ran", body)
		}
	}
}

func TestOffsetResetApplyRouteMapsErrors(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
		code   string
	}{
		{groups.ErrStalePreview, http.StatusConflict, "stale_preview"},
		{groups.ErrGroupActive, http.StatusConflict, "group_active"},
		{groups.ErrGroupNotFound, http.StatusNotFound, "not_found"},
		{groups.ErrTopicNotFound, http.StatusNotFound, "not_found"},
		{groups.ErrInvalidResetTarget, http.StatusBadRequest, "invalid_reset_target"},
		{errors.Join(groups.ErrOutcomeUnknown, errors.New("connection reset")), http.StatusServiceUnavailable, "offset_reset_unknown"},
		{errors.New("broker down"), http.StatusServiceUnavailable, "cluster_unreachable"},
	} {
		_, api := humatest.New(t)
		RegisterApplyOffsetReset(api, &fakeApplyOffsetReset{err: tt.err})
		resp := api.Post("/api/v1/consumer-groups/billing/offset-reset", strings.NewReader(applyBody))
		if resp.Code != tt.status || !strings.Contains(resp.Body.String(), `"code":"`+tt.code+`"`) {
			t.Errorf("%v: %d %s", tt.err, resp.Code, resp.Body)
		}
	}
}
