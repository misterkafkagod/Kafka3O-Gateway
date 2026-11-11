package metrics

import (
	"bufio"
	"fmt"
	"io"
)

// WritePrometheusText writes the metrics in the Prometheus text exposition format 0.0.4. Values Kafka did not
// report are left out rather than written as zero; kafka3o_metrics_section_up says which sections answered.
func WritePrometheusText(w io.Writer, m ClusterMetrics) error {
	out := bufio.NewWriter(w)
	family := func(name, kind, help string) {
		fmt.Fprintf(out, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
	}
	up := func(failure string) int {
		if failure == "" {
			return 1
		}
		return 0
	}

	family("kafka3o_metrics_section_up", "gauge", "Whether Kafka answered this section of the metrics (1) or not (0).")
	fmt.Fprintf(out, "kafka3o_metrics_section_up{section=\"logDirs\"} %d\n", up(m.LogDirs.Error))
	fmt.Fprintf(out, "kafka3o_metrics_section_up{section=\"partitions\"} %d\n", up(m.Partitions.Error))
	fmt.Fprintf(out, "kafka3o_metrics_section_up{section=\"endOffsets\"} %d\n", up(m.EndOffsets.Error))
	fmt.Fprintf(out, "kafka3o_metrics_section_up{section=\"quorum\"} %d\n", up(m.Quorum.Error))
	fmt.Fprintf(out, "kafka3o_metrics_section_up{section=\"brokers\"} %d\n", up(m.Brokers.Error))
	fmt.Fprintf(out, "kafka3o_metrics_section_up{section=\"consumerGroups\"} %d\n", up(m.Groups.Error))

	for _, gauge := range []struct {
		name, help string
		value      func(LogDir) *int64
	}{
		{"kafka3o_log_dir_total_bytes", "Size of the volume holding the log directory.", func(d LogDir) *int64 { return d.TotalBytes }},
		{"kafka3o_log_dir_usable_bytes", "Free space on the volume holding the log directory.", func(d LogDir) *int64 { return d.UsableBytes }},
		{"kafka3o_log_dir_kafka_bytes", "Size of the partition replicas in the log directory.", func(d LogDir) *int64 { return &d.KafkaBytes }},
	} {
		family(gauge.name, "gauge", gauge.help)
		for _, dir := range m.LogDirs.Dirs {
			if value := gauge.value(dir); dir.Error == "" && value != nil {
				fmt.Fprintf(out, "%s{broker=\"%d\",dir=\"%s\"} %d\n", gauge.name, dir.Broker, EscapeLabelValue(dir.Dir), *value)
			}
		}
	}

	if m.Partitions.Error == "" {
		family("kafka3o_topics", "gauge", "Topics Kafka described, internal ones included.")
		fmt.Fprintf(out, "kafka3o_topics %d\n", m.Partitions.Topics)
		family("kafka3o_partitions", "gauge", "Partitions by state.")
		fmt.Fprintf(out, "kafka3o_partitions{state=\"all\"} %d\n", m.Partitions.Partitions)
		fmt.Fprintf(out, "kafka3o_partitions{state=\"under_replicated\"} %d\n", m.Partitions.UnderReplicated)
		if m.Partitions.BelowMinISR != nil {
			fmt.Fprintf(out, "kafka3o_partitions{state=\"below_min_isr\"} %d\n", *m.Partitions.BelowMinISR)
		}
		fmt.Fprintf(out, "kafka3o_partitions{state=\"offline\"} %d\n", m.Partitions.Offline)
	}

	family("kafka3o_topic_size_bytes", "gauge", "Size of the topic's replicas on disk.")
	for _, topic := range SumTopicSizes(m.LogDirs.Dirs) {
		fmt.Fprintf(out, "kafka3o_topic_size_bytes{topic=\"%s\"} %d\n", EscapeLabelValue(topic.Topic), topic.Bytes)
	}

	family("kafka3o_topic_end_offset_total", "counter", "Sum of the topic's partition end offsets; its rate is the topic's messages per second.")
	for _, topic := range m.EndOffsets.Topics {
		if topic.Error == "" {
			fmt.Fprintf(out, "kafka3o_topic_end_offset_total{topic=\"%s\"} %s\n", EscapeLabelValue(topic.Topic), topic.EndOffset)
		}
	}
	if m.Quorum.Error == "" {
		if m.Quorum.LeaderID != nil {
			family("kafka3o_quorum_leader", "gauge", "Node id of the KRaft controller quorum's leader.")
			fmt.Fprintf(out, "kafka3o_quorum_leader %d\n", *m.Quorum.LeaderID)
		}
		family("kafka3o_quorum_high_watermark", "gauge", "High watermark of the cluster metadata log.")
		fmt.Fprintf(out, "kafka3o_quorum_high_watermark %d\n", m.Quorum.HighWatermark)
		family("kafka3o_quorum_replica_lag", "gauge", "Records a quorum voter or observer is behind the leader's log end.")
		for _, replica := range m.Quorum.Replicas {
			if replica.Lag != nil {
				fmt.Fprintf(out, "kafka3o_quorum_replica_lag{replica=\"%d\",role=\"%s\"} %d\n", replica.ID, EscapeLabelValue(replica.Role), *replica.Lag)
			}
		}
	}
	for _, gauge := range []struct {
		name, help string
		value      func(BrokerLeaders) int
	}{
		{"kafka3o_broker_leaders", "Partitions the broker leads.", func(b BrokerLeaders) int { return b.Leaders }},
		{"kafka3o_broker_replicas", "Partition replicas the broker holds.", func(b BrokerLeaders) int { return b.Replicas }},
		{"kafka3o_broker_non_preferred_leaders", "Partitions the broker leads although it is not their preferred replica.", func(b BrokerLeaders) int { return b.NonPreferredLeaders }},
	} {
		if m.Brokers.Error != "" {
			break
		}
		family(gauge.name, "gauge", gauge.help)
		for _, broker := range m.Brokers.Brokers {
			fmt.Fprintf(out, "%s{broker=\"%d\"} %d\n", gauge.name, broker.Broker, gauge.value(broker))
		}
	}
	if m.Groups.Error == "" {
		family("kafka3o_consumer_groups", "gauge", "Consumer groups by state.")
		for _, state := range m.Groups.States {
			fmt.Fprintf(out, "kafka3o_consumer_groups{state=\"%s\"} %d\n", EscapeLabelValue(state.State), state.Count)
		}
		family("kafka3o_consumer_group_members", "gauge", "Members of the consumer group.")
		for _, group := range m.Groups.Groups {
			fmt.Fprintf(out, "kafka3o_consumer_group_members{group=\"%s\"} %d\n", EscapeLabelValue(group.Group), group.Members)
		}
		// Lag is written only when every partition's lag is known; a partial sum would read as a smaller lag.
		family("kafka3o_consumer_group_lag", "gauge", "Total lag of the consumer group over its partitions.")
		for _, group := range m.Groups.Groups {
			if group.Lag != nil && group.UnknownPartitions == 0 && group.Error == "" {
				fmt.Fprintf(out, "kafka3o_consumer_group_lag{group=\"%s\"} %s\n", EscapeLabelValue(group.Group), *group.Lag)
			}
		}
		family("kafka3o_consumer_group_max_partition_lag", "gauge", "Largest partition lag of the consumer group.")
		for _, group := range m.Groups.Groups {
			if group.MaxPartitionLag != nil && group.UnknownPartitions == 0 && group.Error == "" {
				fmt.Fprintf(out, "kafka3o_consumer_group_max_partition_lag{group=\"%s\"} %s\n", EscapeLabelValue(group.Group), *group.MaxPartitionLag)
			}
		}
		family("kafka3o_consumer_group_unknown_lag_partitions", "gauge", "Partitions of the consumer group whose lag Kafka did not report.")
		for _, group := range m.Groups.Groups {
			fmt.Fprintf(out, "kafka3o_consumer_group_unknown_lag_partitions{group=\"%s\"} %d\n", EscapeLabelValue(group.Group), group.UnknownPartitions)
		}
	}
	return out.Flush()
}
