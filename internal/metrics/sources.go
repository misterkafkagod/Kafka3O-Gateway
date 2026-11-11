package metrics

import (
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

// Sources are the reads GetMetrics combines, one per section.
type Sources struct {
	LogDirs ReadLogDirs
	Health  ReadPartitionHealth
	Offsets ReadEndOffsets
	Quorum  ReadQuorum
	Brokers ReadBrokerLeaders
	Groups  groups.ListConsumerGroups
	Connect connect.ListConnectors
	// Backups must be built without the restore check, so reading metrics never finishes a restore.
	Backups backup.ListBackups
}
