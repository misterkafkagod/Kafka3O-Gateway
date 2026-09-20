package fake

import "github.com/misterkafkagod/kafka3o/internal/kafka"

// SeedBroker adds one broker directly to the model, bypassing fault injection
// and call recording — seeding is test setup, not a port call (TECH-SPEC §4.3
// Seeding row). The first broker seeded becomes the controller; a way to
// choose a different controller is added by the task that needs one.
func (f *Fake) SeedBroker(id int32, host string, port int32, rack string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.model.brokers = append(f.model.brokers, kafka.Broker{ID: id, Host: host, Port: port, Rack: rack})
	if !f.model.controllerSet {
		f.model.controllerID = id
		f.model.controllerSet = true
	}
}

// SeedTopic creates a topic with the given partition count and appends
// records: a record's Partition field selects which partition it lands on
// (out-of-range or unset defaults to partition 0). The fake assigns offsets
// sequentially per partition and stamps the timestamp from the fake's clock
// when the caller leaves it zero (TECH-SPEC §4.3 Seeding, Time rows).
func (f *Fake) SeedTopic(name string, partitions int, records ...kafka.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()

	t := &fakeTopic{name: name, partitions: make([]fakePartition, partitions)}
	for _, r := range records {
		p := int(r.Partition)
		if p < 0 || p >= partitions {
			p = 0
		}
		if r.Timestamp.IsZero() {
			r.Timestamp = f.now()
		}
		r.Topic = name
		r.Partition = int32(p)
		r.Offset = t.partitions[p].beginOffset + int64(len(t.partitions[p].records))
		t.partitions[p].records = append(t.partitions[p].records, r)
	}

	if f.model.topics == nil {
		f.model.topics = map[string]*fakeTopic{}
	}
	f.model.topics[name] = t
}

// GroupOffset is one committed offset seeded onto a consumer group.
type GroupOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}

// SeedGroup creates a consumer group with the given committed offsets and no
// members (TECH-SPEC §4.3 Seeding row).
func (f *Fake) SeedGroup(id string, offsets ...GroupOffset) {
	f.mu.Lock()
	defer f.mu.Unlock()

	g := &fakeGroup{id: id, state: "Empty", offsets: map[kafka.TopicPartition]int64{}}
	for _, o := range offsets {
		g.offsets[kafka.TopicPartition{Topic: o.Topic, Partition: o.Partition}] = o.Offset
	}

	if f.model.groups == nil {
		f.model.groups = map[string]*fakeGroup{}
	}
	f.model.groups[id] = g
}
