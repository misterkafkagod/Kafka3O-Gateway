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

// SeedBrokerConfigs sets a broker's configuration properties, replacing any
// previously seeded for that broker id (Task 2.1 C2).
func (f *Fake) SeedBrokerConfigs(brokerID int32, configs ...kafka.ConfigEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.model.brokerConfigs == nil {
		f.model.brokerConfigs = map[int32][]kafka.ConfigEntry{}
	}
	f.model.brokerConfigs[brokerID] = append([]kafka.ConfigEntry(nil), configs...)
}

// SeedTopicMeta sets a topic's internal flag and replication factor. It must
// be called after SeedTopic creates the topic (Task 2.1 T1, T2).
func (f *Fake) SeedTopicMeta(name string, internal bool, replicationFactor int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.model.topics[name]
	if t == nil {
		return
	}
	t.internal = internal
	t.replicationFactor = replicationFactor
}

// SeedTopicConfigs sets a topic's configuration properties, replacing any
// previously seeded for that topic. It must be called after SeedTopic creates
// the topic (Task 2.1 T2).
func (f *Fake) SeedTopicConfigs(name string, configs ...kafka.ConfigEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.model.topics[name]
	if t == nil {
		return
	}
	t.configs = append([]kafka.ConfigEntry(nil), configs...)
}

// SeedPartitionMeta sets one partition's leader, replicas, and ISR. It must
// be called after SeedTopic creates the topic and partition (Task 2.1 T2).
func (f *Fake) SeedPartitionMeta(topic string, partition int32, leader int32, replicas, isr []int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.model.topics[topic]
	if t == nil || int(partition) < 0 || int(partition) >= len(t.partitions) {
		return
	}
	p := &t.partitions[partition]
	p.leader = leader
	p.replicas = append([]int32(nil), replicas...)
	p.isr = append([]int32(nil), isr...)
}

// SeedLogDir sets one partition replica's on-disk log directory and size. It
// must be called after SeedTopic creates the topic and partition (Task 2.1 T3).
func (f *Fake) SeedLogDir(topic string, partition int32, brokerID int32, dir string, bytes int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.model.topics[topic]
	if t == nil || int(partition) < 0 || int(partition) >= len(t.partitions) {
		return
	}
	p := &t.partitions[partition]
	if p.logDir == nil {
		p.logDir = map[int32]fakeLogDirEntry{}
	}
	p.logDir[brokerID] = fakeLogDirEntry{dir: dir, bytes: bytes}
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
