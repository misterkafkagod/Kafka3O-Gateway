//go:build integration

package integration

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func UseExternalTestBroker(t *testing.T, address, disposable string) []string {
	t.Helper()
	if err := ValidateExternalTestBroker(address, disposable); err != nil {
		t.Fatal(err)
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(address))
	if err != nil {
		t.Fatalf("create external Kafka client: %v", err)
	}
	defer client.Close()
	VerifyExternalTestBroker(t, client, address, disposable)
	return []string{address}
}
