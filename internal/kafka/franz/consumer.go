package franz

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// Consumer is a dedicated, per-scan kgo.Client with manual partition
// assignment — no group, no commits (FUNC-SPEC O4; TECH-SPEC §2.3, C4). A
// scan's lifecycle never touches the shared admin/produce Client.
type Consumer struct {
	kgo *kgo.Client
}

// NewConsumer builds a Consumer over the same broker connection settings as
// a Client's Config, isolated in its own kgo.Client (TECH-SPEC §2.3).
// isolationLevel is config.Kafka.Consumer.IsolationLevel ("read_committed"
// or "read_uncommitted" — TECH-SPEC C4).
func NewConsumer(c Config, isolationLevel string) (*Consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Bootstrap...),
		kgo.WithHooks(kotelHooks()...),
		kgo.FetchIsolationLevel(isolationLevelOpt(isolationLevel)),
	}
	if c.RequestTimeout > 0 {
		opts = append(opts, kgo.RequestTimeoutOverhead(c.RequestTimeout))
	}
	if opt, err := tlsOpt(c.TLS); err != nil {
		return nil, err
	} else if opt != nil {
		opts = append(opts, opt)
	}
	if opt, err := saslOpt(c.SASL); err != nil {
		return nil, err
	} else if opt != nil {
		opts = append(opts, opt)
	}

	kc, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Consumer{kgo: kc}, nil
}

// isolationLevelOpt maps the configured level onto kgo's type, defaulting to
// ReadCommitted for anything but the literal "read_uncommitted" (TECH-SPEC
// C4: read_committed is the configured default).
func isolationLevelOpt(level string) kgo.IsolationLevel {
	if level == "read_uncommitted" {
		return kgo.ReadUncommitted()
	}
	return kgo.ReadCommitted()
}

// Assign begins consuming topic's partitions from startOffsets via
// AddConsumePartitions — no ConsumerGroup option is ever set on this client
// (FUNC-SPEC §9.2 Assign; O4).
func (c *Consumer) Assign(_ context.Context, topic string, partitions []int32, startOffsets map[int32]int64) error {
	offsets := make(map[int32]kgo.Offset, len(partitions))
	for _, p := range partitions {
		offsets[p] = kgo.NewOffset().At(startOffsets[p])
	}
	c.kgo.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets})
	return nil
}

// Poll returns whatever records arrived for the assigned partitions.
func (c *Consumer) Poll(ctx context.Context) ([]kafka.Record, error) {
	fetches := c.kgo.PollFetches(ctx)
	if err := fetches.Err(); err != nil {
		return nil, wrapErr("", err)
	}

	records := fetches.Records()
	out := make([]kafka.Record, len(records))
	for i, r := range records {
		out[i] = toDomainRecord(r)
	}
	return out, nil
}

// Close releases the dedicated client (TECH-SPEC §2.3).
func (c *Consumer) Close() { c.kgo.Close() }

// toDomainRecord converts one kgo.Record into the port's Record shape.
func toDomainRecord(r *kgo.Record) kafka.Record {
	headers := make([]kafka.Header, len(r.Headers))
	for i, h := range r.Headers {
		headers[i] = kafka.Header{Key: h.Key, Value: h.Value}
	}
	return kafka.Record{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp,
		Key:       r.Key,
		Value:     r.Value,
		Headers:   headers,
	}
}

// compile-time proof that Consumer satisfies kafka.Consumer.
var _ kafka.Consumer = (*Consumer)(nil)
