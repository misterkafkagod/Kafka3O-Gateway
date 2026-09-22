package api_test

import (
	"net/http"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

func TestAPI_G1_PaginationEnvelope(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedGroup("g-alpha", nil)
	gw.Fake.SeedGroupMeta("g-alpha", "Stable", "consumer", 1)
	gw.Fake.SeedGroup("g-bravo", nil)
	gw.Fake.SeedGroupMeta("g-bravo", "Stable", "consumer", 1)
	gw.Fake.SeedGroup("g-charlie", nil)
	gw.Fake.SeedGroupMeta("g-charlie", "Empty", "consumer", 1)

	resp := gw.Get(t, "/v1/consumer-groups?page=1&pageSize=2")
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}

	items, _ := body["items"].([]any)
	page, _ := body["page"].(map[string]any)
	if len(items) != 2 {
		t.Errorf("items = %v, want 2 (pageSize=2)", items)
	}
	if page["page"] != 1.0 || page["pageSize"] != 2.0 || page["total"] != 3.0 {
		t.Errorf("page = %v, want {page:1 pageSize:2 total:3}", page)
	}

	first, ok := items[0].(map[string]any)
	if !ok || first["groupId"] != "g-alpha" || first["state"] != "Stable" {
		t.Errorf("items[0] = %v, want groupId g-alpha, state Stable", first)
	}
}

func TestAPI_G2_404Envelope(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)

	resp := gw.Get(t, "/v1/consumer-groups/nope")
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "NOT_FOUND" {
		t.Errorf("code = %v, want NOT_FOUND", errBody["code"])
	}
}

func TestAPI_G2_DescribeReturnsOffsetsAndTotalLag(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-demo", 1,
		kafka.Record{Value: []byte("a")}, kafka.Record{Value: []byte("b")}, kafka.Record{Value: []byte("c")})
	gw.Fake.SeedGroup("g1", map[kafka.TopicPartition]int64{{Topic: "t-demo", Partition: 0}: 1})
	gw.Fake.SeedGroupMeta("g1", "Stable", "consumer", 1)

	resp := gw.Get(t, "/v1/consumer-groups/g1")
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	if body["groupId"] != "g1" || body["totalLag"] != 2.0 {
		t.Errorf("body = %v, want groupId g1, totalLag 2", body)
	}
	offsets, _ := body["offsets"].([]any)
	if len(offsets) != 1 {
		t.Fatalf("offsets = %v, want 1 entry", offsets)
	}
}

func TestAPI_G3_GroupsForTopic(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-demo", 1, kafka.Record{Value: []byte("a")})
	gw.Fake.SeedGroup("g1", map[kafka.TopicPartition]int64{{Topic: "t-demo", Partition: 0}: 1})
	gw.Fake.SeedGroupMeta("g1", "Stable", "consumer", 1)

	resp := gw.Get(t, "/v1/topics/t-demo/consumer-groups")
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	if body["topic"] != "t-demo" {
		t.Errorf("topic = %v, want t-demo", body["topic"])
	}
	groups, _ := body["groups"].([]any)
	if len(groups) != 1 {
		t.Fatalf("groups = %v, want 1 entry", groups)
	}
	first, _ := groups[0].(map[string]any)
	if first["groupId"] != "g1" {
		t.Errorf("groups[0] = %v, want groupId g1", first)
	}
}

func TestAPI_Groups_NoGroupJoinNoCommits(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-demo", 1, kafka.Record{Value: []byte("a")})
	gw.Fake.SeedGroup("g1", map[kafka.TopicPartition]int64{{Topic: "t-demo", Partition: 0}: 1})
	gw.Fake.SeedGroupMeta("g1", "Stable", "consumer", 1)

	for _, path := range []string{
		"/v1/consumer-groups",
		"/v1/consumer-groups/g1",
		"/v1/topics/t-demo/consumer-groups",
	} {
		resp := gw.Get(t, path)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", path, resp.StatusCode)
		}
	}

	gw.Fake.AssertNoCommits(t)
	gw.Fake.AssertNoGroupJoin(t)
}
