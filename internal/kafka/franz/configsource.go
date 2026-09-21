package franz

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// normalizeConfigSource maps every kmsg.ConfigSource value onto the port's
// three-way default/static/dynamic classification (FUNC-SPEC §5.1 T2). Any
// dynamic-flavoured source (topic, broker, cluster-default broker, or broker
// logger) normalises to dynamic; the static broker source to static; the
// default source, and any source this catalog does not otherwise classify
// (client-metrics, group configs — not topic or broker resources), to
// default.
func normalizeConfigSource(s kmsg.ConfigSource) kafka.ConfigSource {
	switch s {
	case kmsg.ConfigSourceStaticBrokerConfig:
		return kafka.SourceStatic
	case kmsg.ConfigSourceDynamicTopicConfig,
		kmsg.ConfigSourceDynamicBrokerConfig,
		kmsg.ConfigSourceDynamicDefaultBrokerConfig,
		kmsg.ConfigSourceDynamicBrokerLoggerConfig:
		return kafka.SourceDynamic
	case kmsg.ConfigSourceDefaultConfig,
		kmsg.ConfigSourceUnknown,
		kmsg.ConfigSourceClientMetricsConfig,
		kmsg.ConfigSourceGroupConfig:
		return kafka.SourceDefault
	default:
		return kafka.SourceDefault
	}
}
