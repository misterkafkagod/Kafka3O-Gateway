//go:build integration

package integration

import "testing"

// StartKafka runs a single-node Kafka container and returns its bootstrap address.
func StartKafka(t *testing.T) []string {
	t.Helper()
	brokers, _ := StartKafkaWithContainer(t)
	return brokers
}
