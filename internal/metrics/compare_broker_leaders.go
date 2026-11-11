package metrics

import "cmp"

// CompareBrokerLeaders orders brokers by id.
func CompareBrokerLeaders(a, b BrokerLeaders) int {
	return cmp.Compare(a.Broker, b.Broker)
}
