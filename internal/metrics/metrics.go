package metrics

import "time"

// ClusterMetrics is what Kafka itself reports about the cluster at ReadAt; each section carries its own error
// instead of values when Kafka could not answer it.
type ClusterMetrics struct {
	ReadAt  time.Time `json:"readAt"`
	LogDirs LogDirs   `json:"logDirs"`
}

type LogDirs struct {
	Error string   `json:"error,omitempty"`
	Dirs  []LogDir `json:"dirs"`
}

// LogDir is one broker's log directory. TotalBytes and UsableBytes describe the volume holding it and are nil
// when Kafka does not report them; KafkaBytes is the size of the partition replicas stored in it.
type LogDir struct {
	Broker      int32  `json:"broker"`
	Dir         string `json:"dir"`
	TotalBytes  *int64 `json:"totalBytes"`
	UsableBytes *int64 `json:"usableBytes"`
	KafkaBytes  int64  `json:"kafkaBytes"`
	Error       string `json:"error,omitempty"`
}
