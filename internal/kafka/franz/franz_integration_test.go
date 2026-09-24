//go:build integration

// The franz contract run (TECH-SPEC L1, §4.8): the same porttest suite the
// fake passes in CI, against a live cluster at KAFKA_BOOTSTRAP. Every
// resource a case creates is named acc-<runID>-porttest-... and removed in
// cleanup. Without KAFKA_BOOTSTRAP the tests skip, so `go vet -tags
// integration` and a cluster-less `go test -tags integration` stay green.
package franz

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/porttest"
)

// integrationConfig builds the adapter config from the same environment the
// acceptance suite reads; ok is false when no cluster is configured.
func integrationConfig() (cfg Config, ok bool) {
	bootstrap := os.Getenv("KAFKA_BOOTSTRAP")
	if bootstrap == "" {
		return Config{}, false
	}
	cfg = Config{Bootstrap: strings.Split(bootstrap, ","), RequestTimeout: 30 * time.Second}
	if os.Getenv("KAFKA_TLS_ENABLED") == "true" {
		cfg.TLS = TLSConfig{
			Enabled:  true,
			CAFile:   os.Getenv("KAFKA_TLS_CA_FILE"),
			CertFile: os.Getenv("KAFKA_TLS_CERT_FILE"),
			KeyFile:  os.Getenv("KAFKA_TLS_KEY_FILE"),
		}
	}
	cfg.SASL = SASLConfig{
		Mechanism: os.Getenv("KAFKA_SASL_MECHANISM"),
		Username:  os.Getenv("KAFKA_SASL_USERNAME"),
		Password:  os.Getenv("KAFKA_SASL_PASSWORD"),
	}
	return cfg, true
}

func integrationClient(t *testing.T) (*Client, Config) {
	t.Helper()
	cfg, ok := integrationConfig()
	if !ok {
		t.Skip("KAFKA_BOOTSTRAP is not set; the franz contract run needs a live cluster")
	}
	client, err := New(cfg)
	if err != nil {
		t.Fatalf("franz.New: %v", err)
	}
	t.Cleanup(client.Close)
	return client, cfg
}

func TestFranz_PortContract(t *testing.T) {
	client, _ := integrationClient(t)
	prefix := "acc-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-"
	t.Cleanup(func() { cleanupPrefix(t, client, prefix) })
	porttest.RunPrefixed(t, client, prefix)
}

// The consumer and producer suites need the fake's seeding capability for
// every case, so against a live cluster they skip; they run here anyway so
// the franz run reports the same case names as the fake run.
func TestFranz_ConsumerPortContract(t *testing.T) {
	client, cfg := integrationClient(t)
	consumer, err := NewConsumer(cfg, "read_committed")
	if err != nil {
		t.Fatalf("franz.NewConsumer: %v", err)
	}
	t.Cleanup(consumer.Close)
	porttest.RunConsumer(t, client, consumer)
}

func TestFranz_ProducerPortContract(t *testing.T) {
	client, _ := integrationClient(t)
	porttest.RunProducer(t, client, client)
}

// cleanupPrefix removes every topic, consumer group, SCRAM user, and user
// quota whose name starts with prefix. It reports failures but never fails
// the run: leftovers are prefixed, so they are safe to delete by hand.
func cleanupPrefix(t *testing.T, admin kafka.Admin, prefix string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if topics, err := admin.ListTopics(ctx); err == nil {
		var names []string
		for _, tp := range topics {
			if strings.HasPrefix(tp.Name, prefix) {
				names = append(names, tp.Name)
			}
		}
		if len(names) > 0 {
			if _, err := admin.DeleteTopics(ctx, names); err != nil {
				t.Logf("cleanup: delete topics %v: %v", names, err)
			}
		}
	}

	if groups, err := admin.ListGroups(ctx); err == nil {
		var ids []string
		for _, g := range groups {
			if strings.HasPrefix(g.ID, prefix) {
				ids = append(ids, g.ID)
			}
		}
		if len(ids) > 0 {
			if _, err := admin.DeleteGroups(ctx, ids); err != nil {
				t.Logf("cleanup: delete groups %v: %v", ids, err)
			}
		}
	}

	if users, err := admin.DescribeUserSCRAMs(ctx); err == nil {
		var deletes []kafka.ScramDelete
		for _, u := range users {
			if !strings.HasPrefix(u.Name, prefix) {
				continue
			}
			for _, c := range u.Credentials {
				deletes = append(deletes, kafka.ScramDelete{User: u.Name, Mechanism: c.Mechanism})
			}
		}
		if len(deletes) > 0 {
			if _, err := admin.AlterUserSCRAMs(ctx, nil, deletes); err != nil {
				t.Logf("cleanup: delete SCRAM credentials: %v", err)
			}
		}
	}

	if quotas, err := admin.DescribeClientQuotas(ctx, "user"); err == nil {
		var entries []kafka.QuotaAlterEntry
		for _, q := range quotas {
			if !entityHasPrefix(q.Entity, prefix) {
				continue
			}
			ops := make([]kafka.QuotaOp, len(q.Values))
			for i, v := range q.Values {
				ops[i] = kafka.QuotaOp{Key: v.Key, Remove: true}
			}
			entries = append(entries, kafka.QuotaAlterEntry{Entity: q.Entity, Ops: ops})
		}
		if len(entries) > 0 {
			if _, err := admin.AlterClientQuotas(ctx, entries); err != nil {
				t.Logf("cleanup: remove quotas: %v", err)
			}
		}
	}
}

func entityHasPrefix(e kafka.QuotaEntity, prefix string) bool {
	for _, c := range e {
		if c.Name != nil && strings.HasPrefix(*c.Name, prefix) {
			return true
		}
	}
	return false
}
