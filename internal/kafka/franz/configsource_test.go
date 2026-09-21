package franz

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// TestFranz_ConfigSourceNormalisation covers every kmsg.ConfigSource value
// (TASKS.md Task 2.1 DoD): each maps onto exactly one of the port's
// default/static/dynamic values, with no panic for a source this catalog
// does not otherwise classify.
func TestFranz_ConfigSourceNormalisation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		src  kmsg.ConfigSource
		want kafka.ConfigSource
	}{
		{"unknown", kmsg.ConfigSourceUnknown, kafka.SourceDefault},
		{"dynamic topic config", kmsg.ConfigSourceDynamicTopicConfig, kafka.SourceDynamic},
		{"dynamic broker config", kmsg.ConfigSourceDynamicBrokerConfig, kafka.SourceDynamic},
		{"dynamic default broker config", kmsg.ConfigSourceDynamicDefaultBrokerConfig, kafka.SourceDynamic},
		{"static broker config", kmsg.ConfigSourceStaticBrokerConfig, kafka.SourceStatic},
		{"default config", kmsg.ConfigSourceDefaultConfig, kafka.SourceDefault},
		{"dynamic broker logger config", kmsg.ConfigSourceDynamicBrokerLoggerConfig, kafka.SourceDynamic},
		{"client metrics config", kmsg.ConfigSourceClientMetricsConfig, kafka.SourceDefault},
		{"group config", kmsg.ConfigSourceGroupConfig, kafka.SourceDefault},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeConfigSource(tc.src); got != tc.want {
				t.Errorf("normalizeConfigSource(%v) = %v, want %v", tc.src, got, tc.want)
			}
		})
	}
}
