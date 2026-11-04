package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/humatest"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

type fakeListConsumerGroups struct {
	groups []groups.ConsumerGroup
	err    error
}

func (f fakeListConsumerGroups) Execute(context.Context) ([]groups.ConsumerGroup, error) {
	return f.groups, f.err
}

func TestRegisterListConsumerGroups_ReturnsPreciseOffsetsAndGroupErrors(t *testing.T) {
	_, api := humatest.New(t)
	RegisterListConsumerGroups(api, fakeListConsumerGroups{groups: []groups.ConsumerGroup{
		{
			ID: "orders-group", Protocol: "classic", State: "Stable",
			Members: []groups.ConsumerGroupMember{{ID: "member", ClientID: "client", ClientHost: "host"}},
			Topics:  []string{"orders"},
			Partitions: []groups.ConsumerGroupPartition{{
				Topic: "orders", Partition: 0, CommittedOffset: new("9007199254740993"),
				StartOffset: new("0"), EndOffset: new("9007199254740995"), Lag: new("2"), Status: "ok",
			}, {Topic: "orders", Partition: 1, Status: "unavailable", Error: "offset denied"}},
		},
		{ID: "failed", Members: []groups.ConsumerGroupMember{}, Topics: []string{},
			Partitions: []groups.ConsumerGroupPartition{}, Error: "group denied"},
	}})
	resp := api.Get("/api/v1/consumer-groups")
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.Code, resp.Body)
	}
	var body []struct {
		ID         string `json:"id"`
		Error      string `json:"error"`
		Partitions []struct {
			CommittedOffset *string `json:"committedOffset"`
			StartOffset     *string `json:"startOffset"`
			EndOffset       *string `json:"endOffset"`
			Lag             *string `json:"lag"`
			Status          string  `json:"status"`
			Error           string  `json:"error"`
		} `json:"partitions"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body) != 2 || len(body[0].Partitions) != 2 || body[1].Error != "group denied" {
		t.Fatalf("response = %#v", body)
	}
	first, second := body[0].Partitions[0], body[0].Partitions[1]
	if first.CommittedOffset == nil || *first.CommittedOffset != "9007199254740993" ||
		first.EndOffset == nil || *first.EndOffset != "9007199254740995" || first.Lag == nil || *first.Lag != "2" {
		t.Errorf("precise partition = %#v", first)
	}
	if second.CommittedOffset != nil || second.Lag != nil || second.Error != "offset denied" || second.Status != "unavailable" {
		t.Errorf("unavailable partition = %#v", second)
	}
}

func TestRegisterListConsumerGroups_EmptyAndUnavailable(t *testing.T) {
	_, api := humatest.New(t)
	RegisterListConsumerGroups(api, fakeListConsumerGroups{groups: []groups.ConsumerGroup{}})
	resp := api.Get("/api/v1/consumer-groups")
	if resp.Code != http.StatusOK || (resp.Body.String() != "[]" && resp.Body.String() != "[]\n") {
		t.Errorf("empty response: %d %s", resp.Code, resp.Body)
	}
	_, failedAPI := humatest.New(t)
	RegisterListConsumerGroups(failedAPI, fakeListConsumerGroups{err: errors.New("offline")})
	resp = failedAPI.Get("/api/v1/consumer-groups")
	var problem map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &problem); err != nil {
		t.Fatal(err)
	}
	if resp.Code != http.StatusServiceUnavailable || problem["code"] != "cluster_unreachable" {
		t.Errorf("failed response: %d %#v", resp.Code, problem)
	}
}

func TestRegisterListConsumerGroups_OpenAPIOffsetsAreNullableStrings(t *testing.T) {
	_, api := humatest.New(t)
	RegisterListConsumerGroups(api, fakeListConsumerGroups{})
	schema := api.OpenAPI().Components.Schemas.Map()["ConsumerGroupPartition"]
	if schema == nil {
		t.Fatal("consumer partition schema is missing")
	}
	for _, field := range []string{"committedOffset", "startOffset", "endOffset", "lag"} {
		property := schema.Properties[field]
		if property == nil || property.Type != huma.TypeString || !property.Nullable {
			t.Errorf("%s schema = %#v, want nullable string", field, property)
		}
	}
}
