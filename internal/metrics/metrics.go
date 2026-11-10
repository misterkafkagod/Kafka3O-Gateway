package metrics

import "time"

// ClusterMetrics is what Kafka itself reports about the cluster at ReadAt; each section carries its own error
// instead of values when Kafka could not answer it.
type ClusterMetrics struct {
	ReadAt        time.Time  `json:"readAt"`
	LogDirs       LogDirs    `json:"logDirs"`
	Partitions    Partitions `json:"partitions"`
	LargestTopics TopicSizes `json:"largestTopics"`
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
	// TopicBytes is the replica size per topic in this directory; it feeds LargestTopics and is not sent.
	TopicBytes map[string]int64 `json:"-"`
}

// Partitions sums partition health over every topic, internal ones included. BelowMinISR is nil when a topic's
// min.insync.replicas could not be read; UnreadableTopics are left out of the counts.
type Partitions struct {
	Error            string   `json:"error,omitempty"`
	Topics           int      `json:"topics"`
	Partitions       int      `json:"partitions"`
	UnderReplicated  int      `json:"underReplicated"`
	Offline          int      `json:"offline"`
	BelowMinISR      *int     `json:"belowMinIsr"`
	MinISRError      string   `json:"minIsrError,omitempty"`
	UnreadableTopics []string `json:"unreadableTopics"`
}

// TopicHealth is one topic's partition health as read from Kafka.
type TopicHealth struct {
	Topic           string
	Error           string
	Partitions      int
	UnderReplicated int
	Offline         int
	BelowMinISR     *int
	MinISRError     string
}

type TopicSizes struct {
	Error  string      `json:"error,omitempty"`
	Topics []TopicSize `json:"topics"`
}

// TopicSize is a topic's size on disk: the sum of its replicas over every log directory.
type TopicSize struct {
	Topic string `json:"topic"`
	Bytes int64  `json:"bytes"`
}
