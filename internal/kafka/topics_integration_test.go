//go:build integration

package kafka

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/api"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka/kafkatest"
)

// ordersFixture creates "orders" (3 partitions, retention.ms overridden) with
// 10 records spread across its partitions.
func ordersFixture(t *testing.T, ctx context.Context, c *Client, brokers []string) {
	t.Helper()
	retention := "86400000"
	if _, err := c.adm.CreateTopic(ctx, 3, 1, map[string]*string{"retention.ms": &retention}, "orders"); err != nil {
		t.Fatalf("create orders: %v", err)
	}
	var recs []*kgo.Record
	for i := range 10 {
		recs = append(recs, &kgo.Record{Topic: "orders", Partition: int32(i % 3), Value: []byte(strconv.Itoa(i))})
	}
	p, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

func TestListTopics(t *testing.T) {
	brokers := kafkatest.Start(t)
	c := newClient(t, brokers)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ordersFixture(t, ctx, c, brokers)

	topics, err := c.ListTopics(ctx)
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	for _, tp := range topics {
		if tp.Name != "orders" {
			continue
		}
		var n int64
		for _, p := range tp.Partitions {
			n += p.EndOffset - p.StartOffset
		}
		if len(tp.Partitions) != 3 || n != 10 {
			t.Errorf("orders: %d partitions, %d messages; want 3 and 10", len(tp.Partitions), n)
		}
		return
	}
	t.Fatalf("orders not listed: %+v", topics)
}

func TestDescribeTopic(t *testing.T) {
	brokers := kafkatest.Start(t)
	c := newClient(t, brokers)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ordersFixture(t, ctx, c, brokers)

	tp, configs, err := c.DescribeTopic(ctx, "orders")
	if err != nil {
		t.Fatalf("DescribeTopic: %v", err)
	}
	if len(tp.Partitions) != 3 {
		t.Fatalf("got %d partitions, want 3", len(tp.Partitions))
	}
	for i, p := range tp.Partitions {
		if p.ID != int32(i) || p.Leader != 1 || len(p.Replicas) != 1 || len(p.ISR) != 1 {
			t.Errorf("partition %d = %+v", i, p)
		}
	}

	var sawRetention, sawDefault bool
	for _, cfg := range configs {
		switch {
		case cfg.Key == "retention.ms":
			sawRetention = true
			if cfg.IsDefault || cfg.Value == nil || *cfg.Value != "86400000" {
				t.Errorf("retention.ms = %+v, want overridden 86400000", cfg)
			}
		case cfg.IsDefault:
			sawDefault = true
		}
	}
	if !sawRetention || !sawDefault {
		t.Errorf("configs missing retention.ms override or defaults: %+v", configs)
	}

	if _, _, err := c.DescribeTopic(ctx, "missing"); !errors.Is(err, api.ErrTopicNotFound) {
		t.Errorf("DescribeTopic(missing) err = %v, want ErrTopicNotFound", err)
	}
}
