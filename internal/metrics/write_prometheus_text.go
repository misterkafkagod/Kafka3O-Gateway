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
	return out.Flush()
}
