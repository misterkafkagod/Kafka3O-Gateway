//go:build integration

package integration

import (
	"os"
	"testing"
)

// StartKafka returns a fresh single-node broker for an integration test.
func StartKafka(t *testing.T) []string {
	t.Helper()
	if address, configured := os.LookupEnv("KAFKA3O_TEST_BROKER"); configured {
		return UseExternalTestBroker(t, address, os.Getenv("KAFKA3O_TEST_DISPOSABLE"))
	}
	brokers, _ := StartKafkaWithContainer(t)
	return brokers
}
