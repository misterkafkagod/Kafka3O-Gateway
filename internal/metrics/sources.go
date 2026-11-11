package metrics

// Sources are the reads GetMetrics combines, one per section.
type Sources struct {
	LogDirs ReadLogDirs
	Health  ReadPartitionHealth
	Offsets ReadEndOffsets
	Quorum  ReadQuorum
	Brokers ReadBrokerLeaders
}
