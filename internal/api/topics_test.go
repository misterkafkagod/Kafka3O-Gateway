package api

import (
	"errors"
	"net/http"
	"slices"
	"testing"
)

func topicsFixture() *fakeCluster {
	return &fakeCluster{
		topics: []TopicPartitions{
			{Name: "users", Partitions: []PartitionInfo{
				{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
			}},
			{Name: "orders", Partitions: []PartitionInfo{
				{ID: 2, Leader: 3, Replicas: []int32{3, 1}, ISR: []int32{3}, StartOffset: 0, EndOffset: 2},
				{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}, StartOffset: 5, EndOffset: 9},
				{ID: 1, Leader: 2, Replicas: []int32{2, 3}, ISR: []int32{2, 3}, StartOffset: 10, EndOffset: 14},
			}},
			{Name: "__consumer_offsets", Partitions: []PartitionInfo{{ID: 0}}},
		},
		configs: map[string][]ConfigEntry{
			"orders": {
				{Key: "segment.bytes", Value: ptr("1073741824"), IsDefault: true},
				{Key: "retention.ms", Value: ptr("86400000"), IsDefault: false},
				{Key: "sasl.jaas.config", Value: ptr("secret"), Sensitive: true},
			},
		},
	}
}

func TestTopicsCountsSummedAndSorted(t *testing.T) {
	var body []topicSummary
	res := get(t, topicsFixture(), "/api/topics", &body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	want := []topicSummary{
		{Name: "__consumer_offsets", Partitions: 1, ApproxMessages: 0, Internal: true},
		{Name: "orders", Partitions: 3, ApproxMessages: 10},
		{Name: "users", Partitions: 1, ApproxMessages: 0},
	}
	if !slices.Equal(body, want) {
		t.Errorf("got  %+v\nwant %+v", body, want)
	}
}

func TestTopicsInternalByUnderscorePrefix(t *testing.T) {
	for name, want := range map[string]bool{"__consumer_offsets": true, "_schemas": true, "orders": false, "my_topic": false} {
		if got := isInternal(name); got != want {
			t.Errorf("isInternal(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestTopicsClusterError(t *testing.T) {
	var body errorBody
	res := get(t, &fakeCluster{topicsErr: errors.New("timeout")}, "/api/topics", &body)
	if res.StatusCode != http.StatusServiceUnavailable || body.Error.Code != CodeClusterUnreachable {
		t.Errorf("status %d body %+v", res.StatusCode, body)
	}
}

func TestTopicDetailPartitionsAndUnderReplicated(t *testing.T) {
	var body topicDetail
	res := get(t, topicsFixture(), "/api/topics/orders", &body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	if body.Name != "orders" || len(body.Partitions) != 3 {
		t.Fatalf("body = %+v", body)
	}
	for i, p := range body.Partitions {
		if p.ID != int32(i) {
			t.Errorf("partition %d has id %d, want sorted by id", i, p.ID)
		}
	}
	p0, p2 := body.Partitions[0], body.Partitions[2]
	if p0.Leader != 1 || !slices.Equal(p0.Replicas, []int32{1, 2}) || p0.StartOffset != 5 || p0.EndOffset != 9 || p0.UnderReplicated {
		t.Errorf("partition 0 = %+v", p0)
	}
	if !p2.UnderReplicated {
		t.Errorf("partition 2 (isr 1 of 2 replicas) not flagged under-replicated: %+v", p2)
	}
}

func TestTopicDetailOverrideVsDefault(t *testing.T) {
	var body topicDetail
	get(t, topicsFixture(), "/api/topics/orders", &body)
	byKey := map[string]configDetail{}
	for _, c := range body.Configs {
		byKey[c.Key] = c
	}
	if c := byKey["retention.ms"]; c.IsDefault || c.Value == nil || *c.Value != "86400000" {
		t.Errorf("retention.ms = %+v, want overridden 86400000", c)
	}
	if c := byKey["segment.bytes"]; !c.IsDefault {
		t.Errorf("segment.bytes = %+v, want default", c)
	}
}

func TestTopicDetailSensitiveValueOmitted(t *testing.T) {
	var body topicDetail
	get(t, topicsFixture(), "/api/topics/orders", &body)
	for _, c := range body.Configs {
		if c.Key == "sasl.jaas.config" {
			if !c.Sensitive || c.Value != nil {
				t.Errorf("sensitive config sent: %+v", c)
			}
			return
		}
	}
	t.Error("sasl.jaas.config missing from configs")
}

func TestTopicDetailMissing404(t *testing.T) {
	var body errorBody
	res := get(t, topicsFixture(), "/api/topics/missing", &body)
	if res.StatusCode != http.StatusNotFound || body.Error.Code != CodeTopicNotFound {
		t.Errorf("status %d body %+v", res.StatusCode, body)
	}
}
