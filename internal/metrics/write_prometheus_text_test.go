package metrics

import (
	"strings"
	"testing"
)

func TestWritePrometheusText_WritesEverySection(t *testing.T) {
	total, usable := int64(100), int64(60)
	var out strings.Builder
	err := WritePrometheusText(&out, ClusterMetrics{
		LogDirs: LogDirs{Dirs: []LogDir{
			{Broker: 1, Dir: `/data "a"`, TotalBytes: &total, UsableBytes: &usable, KafkaBytes: 7, TopicBytes: map[string]int64{"orders": 5, "__consumer_offsets": 2}},
			{Broker: 2, Dir: "/unknown", KafkaBytes: 3},
			{Broker: 3, Dir: "/broken", Error: "KAFKA_STORAGE_ERROR"},
		}},
		Partitions: Partitions{Topics: 2, Partitions: 4, UnderReplicated: 1, Offline: 0, BelowMinISR: new(int)},
		EndOffsets: EndOffsets{Topics: []TopicEndOffset{{Topic: "orders", EndOffset: "9007199254740993"}, {Topic: "failing", Error: "NOT_LEADER"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, line := range []string{
		"# TYPE kafka3o_metrics_section_up gauge",
		`kafka3o_metrics_section_up{section="logDirs"} 1`,
		`kafka3o_log_dir_total_bytes{broker="1",dir="/data \"a\""} 100`,
		`kafka3o_log_dir_usable_bytes{broker="1",dir="/data \"a\""} 60`,
		`kafka3o_log_dir_kafka_bytes{broker="1",dir="/data \"a\""} 7`,
		`kafka3o_log_dir_kafka_bytes{broker="2",dir="/unknown"} 3`,
		"kafka3o_topics 2",
		`kafka3o_partitions{state="all"} 4`,
		`kafka3o_partitions{state="under_replicated"} 1`,
		`kafka3o_partitions{state="below_min_isr"} 0`,
		`kafka3o_partitions{state="offline"} 0`,
		`kafka3o_topic_size_bytes{topic="orders"} 5`,
		`kafka3o_topic_size_bytes{topic="__consumer_offsets"} 2`,
		"# TYPE kafka3o_topic_end_offset_total counter",
		`kafka3o_topic_end_offset_total{topic="orders"} 9007199254740993`,
	} {
		if !strings.Contains(text, line+"\n") {
			t.Errorf("missing %q in:\n%s", line, text)
		}
	}
	for _, absent := range []string{`kafka3o_log_dir_total_bytes{broker="2"`, `broker="3"`, `topic="failing"`} {
		if strings.Contains(text, absent) {
			t.Errorf("unexpected %q in:\n%s", absent, text)
		}
	}
}

func TestWritePrometheusText_MarksUnavailableSectionsAndLeavesTheirValuesOut(t *testing.T) {
	var out strings.Builder
	if err := WritePrometheusText(&out, ClusterMetrics{
		LogDirs:    LogDirs{Error: "denied"},
		Partitions: Partitions{Error: "refused"},
		EndOffsets: EndOffsets{Error: "refused"},
	}); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, line := range []string{`kafka3o_metrics_section_up{section="logDirs"} 0`, `kafka3o_metrics_section_up{section="partitions"} 0`, `kafka3o_metrics_section_up{section="endOffsets"} 0`} {
		if !strings.Contains(text, line+"\n") {
			t.Errorf("missing %q", line)
		}
	}
	if strings.Contains(text, "kafka3o_topics ") || strings.Contains(text, "kafka3o_partitions{") {
		t.Errorf("unavailable partitions were written:\n%s", text)
	}
}

func TestWritePrometheusText_LeavesOutAnUnknownMinISRCount(t *testing.T) {
	var out strings.Builder
	_ = WritePrometheusText(&out, ClusterMetrics{Partitions: Partitions{Partitions: 1}})
	if strings.Contains(out.String(), "below_min_isr") {
		t.Fatalf("unknown below-min-ISR count was written:\n%s", out.String())
	}
}

func TestWritePrometheusText_Quorum(t *testing.T) {
	leader, zero, behind := int32(1), int64(0), int64(100)
	var out strings.Builder
	_ = WritePrometheusText(&out, ClusterMetrics{Quorum: Quorum{LeaderID: &leader, HighWatermark: 894587, Replicas: []QuorumReplica{
		{ID: 1, Role: "voter", Lag: &zero}, {ID: 4, Role: "observer", Lag: &behind}, {ID: 5, Role: "observer"}}}})
	text := out.String()
	for _, line := range []string{`kafka3o_metrics_section_up{section="quorum"} 1`, "kafka3o_quorum_leader 1", "kafka3o_quorum_high_watermark 894587",
		`kafka3o_quorum_replica_lag{replica="1",role="voter"} 0`, `kafka3o_quorum_replica_lag{replica="4",role="observer"} 100`} {
		if !strings.Contains(text, line+"\n") {
			t.Errorf("missing %q", line)
		}
	}
	if strings.Contains(text, `replica="5"`) {
		t.Error("an unknown lag was written")
	}
	out.Reset()
	_ = WritePrometheusText(&out, ClusterMetrics{Quorum: Quorum{Error: "describe quorum: CLUSTER_AUTHORIZATION_FAILED"}})
	if !strings.Contains(out.String(), `kafka3o_metrics_section_up{section="quorum"} 0`) || strings.Contains(out.String(), "kafka3o_quorum_") {
		t.Errorf("a failed quorum section wrote values:\n%s", out.String())
	}
}

func TestWritePrometheusText_Brokers(t *testing.T) {
	var out strings.Builder
	_ = WritePrometheusText(&out, ClusterMetrics{Brokers: Brokers{Brokers: []BrokerLeaders{{Broker: 1, Leaders: 39, Replicas: 40, NonPreferredLeaders: 2}}}})
	for _, line := range []string{`kafka3o_metrics_section_up{section="brokers"} 1`, `kafka3o_broker_leaders{broker="1"} 39`,
		`kafka3o_broker_replicas{broker="1"} 40`, `kafka3o_broker_non_preferred_leaders{broker="1"} 2`} {
		if !strings.Contains(out.String(), line+"\n") {
			t.Errorf("missing %q", line)
		}
	}
	out.Reset()
	_ = WritePrometheusText(&out, ClusterMetrics{Brokers: Brokers{Error: "read metadata: timeout"}})
	if !strings.Contains(out.String(), `kafka3o_metrics_section_up{section="brokers"} 0`) || strings.Contains(out.String(), "kafka3o_broker_") {
		t.Errorf("a failed brokers section wrote values:\n%s", out.String())
	}
}

func TestWritePrometheusText_ConsumerGroups(t *testing.T) {
	total, top, partial := "9007199254740993", "9007199254740990", "3"
	var out strings.Builder
	_ = WritePrometheusText(&out, ClusterMetrics{Groups: Groups{States: []StateCount{{State: "Stable", Count: 2}}, Groups: []GroupLag{
		{Group: "orders", State: "Stable", Members: 2, Lag: &total, MaxPartitionLag: &top},
		{Group: `odd"name`, State: "Stable", Lag: &partial, MaxPartitionLag: &partial, UnknownPartitions: 1},
	}}})
	text := out.String()
	for _, line := range []string{`kafka3o_metrics_section_up{section="consumerGroups"} 1`, `kafka3o_consumer_groups{state="Stable"} 2`,
		`kafka3o_consumer_group_members{group="orders"} 2`, `kafka3o_consumer_group_lag{group="orders"} 9007199254740993`,
		`kafka3o_consumer_group_max_partition_lag{group="orders"} 9007199254740990`, `kafka3o_consumer_group_unknown_lag_partitions{group="odd\"name"} 1`} {
		if !strings.Contains(text, line+"\n") {
			t.Errorf("missing %q", line)
		}
	}
	if strings.Contains(text, `kafka3o_consumer_group_lag{group="odd\"name"}`) {
		t.Error("a partly unknown lag was written")
	}
	out.Reset()
	_ = WritePrometheusText(&out, ClusterMetrics{Groups: Groups{Error: "list groups: timeout"}})
	if !strings.Contains(out.String(), `kafka3o_metrics_section_up{section="consumerGroups"} 0`) || strings.Contains(out.String(), "kafka3o_consumer_group") {
		t.Errorf("a failed groups section wrote values:\n%s", out.String())
	}
}
