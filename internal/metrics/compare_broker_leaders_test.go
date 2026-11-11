package metrics

import (
	"slices"
	"testing"
)

func TestCompareBrokerLeaders(t *testing.T) {
	brokers := []BrokerLeaders{{Broker: 3}, {Broker: 1}, {Broker: 2}}
	slices.SortFunc(brokers, CompareBrokerLeaders)
	if brokers[0].Broker != 1 || brokers[2].Broker != 3 {
		t.Fatalf("order = %+v", brokers)
	}
}
