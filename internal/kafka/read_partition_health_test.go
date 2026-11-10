package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

type fakePartitionHealthAdmin struct {
	details   kadm.TopicDetails
	err       error
	configs   kadm.ResourceConfigs
	configErr error
}

func (f fakePartitionHealthAdmin) ListTopicsWithInternal(context.Context, ...string) (kadm.TopicDetails, error) {
	return f.details, f.err
}

func (f fakePartitionHealthAdmin) DescribeTopicConfigs(context.Context, ...string) (kadm.ResourceConfigs, error) {
	return f.configs, f.configErr
}

func minISRConfig(topic, value string) kadm.ResourceConfig {
	return kadm.ResourceConfig{Name: topic, Configs: []kadm.Config{{Key: "min.insync.replicas", Value: &value}}}
}

var healthDetails = kadm.TopicDetails{
	"orders": {Topic: "orders", Partitions: kadm.PartitionDetails{
		0: {Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
		1: {Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1}},
		2: {Leader: -1, Replicas: []int32{1, 2, 3}, ISR: []int32{}},
	}},
	"__consumer_offsets": {Topic: "__consumer_offsets", IsInternal: true, Partitions: kadm.PartitionDetails{0: {Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}},
	"secret":             {Topic: "secret", Err: errors.New("TOPIC_AUTHORIZATION_FAILED")},
}

func TestReadPartitionHealth_CountsOfflineUnderReplicatedAndBelowMinISR(t *testing.T) {
	admin := fakePartitionHealthAdmin{details: healthDetails, configs: kadm.ResourceConfigs{minISRConfig("orders", "2"), minISRConfig("__consumer_offsets", "1")}}
	health, err := NewReadPartitionHealth(admin).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	byTopic := map[string]int{}
	for i, topic := range health {
		byTopic[topic.Topic] = i
	}
	orders := health[byTopic["orders"]]
	if orders.Partitions != 3 || orders.Offline != 1 || orders.UnderReplicated != 2 || orders.BelowMinISR == nil || *orders.BelowMinISR != 2 {
		t.Fatalf("orders = %+v", orders)
	}
	internal := health[byTopic["__consumer_offsets"]]
	if internal.Partitions != 1 || *internal.BelowMinISR != 0 {
		t.Fatalf("__consumer_offsets = %+v", internal)
	}
	if health[byTopic["secret"]].Error != "TOPIC_AUTHORIZATION_FAILED" {
		t.Fatalf("secret = %+v", health[byTopic["secret"]])
	}
}

func TestReadPartitionHealth_LeavesBelowMinISRUnknownWhenConfigurationIsUnreadable(t *testing.T) {
	admin := fakePartitionHealthAdmin{details: kadm.TopicDetails{"orders": healthDetails["orders"]}, configErr: errors.New("CLUSTER_AUTHORIZATION_FAILED")}
	health, err := NewReadPartitionHealth(admin).Execute(context.Background())
	if err != nil || health[0].BelowMinISR != nil || health[0].MinISRError != "read topic configuration: CLUSTER_AUTHORIZATION_FAILED" || health[0].Offline != 1 {
		t.Fatalf("health = %+v, err = %v", health, err)
	}
}

func TestReadPartitionHealth_ReportsAFailedMetadataRead(t *testing.T) {
	_, err := NewReadPartitionHealth(fakePartitionHealthAdmin{err: errors.New("dial tcp: refused")}).Execute(context.Background())
	if err == nil || err.Error() != "read topic metadata: dial tcp: refused" {
		t.Fatalf("err = %v", err)
	}
}
