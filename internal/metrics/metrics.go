package metrics

import "time"

// ClusterMetrics is what Kafka itself reports about the cluster at ReadAt; each section carries its own error
// instead of values when Kafka could not answer it.
type ClusterMetrics struct {
	ReadAt        time.Time  `json:"readAt"`
	LogDirs       LogDirs    `json:"logDirs"`
	Partitions    Partitions `json:"partitions"`
	LargestTopics TopicSizes `json:"largestTopics"`
	EndOffsets    EndOffsets `json:"endOffsets"`
	Quorum        Quorum     `json:"quorum"`
	Brokers       Brokers    `json:"brokers"`
	Groups        Groups     `json:"consumerGroups"`
	Connect       Connect    `json:"connect"`
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

// EndOffsets are per-topic sums of partition end offsets; they only grow while a topic exists, so two samples
// give a topic's message rate.
type EndOffsets struct {
	Error  string           `json:"error,omitempty"`
	Topics []TopicEndOffset `json:"topics"`
}

// TopicEndOffset carries the sum as a decimal string, like every Kafka offset the Gateway returns, or an error.
type TopicEndOffset struct {
	Topic     string `json:"topic"`
	EndOffset string `json:"endOffset,omitempty"`
	Error     string `json:"error,omitempty"`
}

// Quorum is the KRaft controller quorum as DescribeQuorum reports it. LeaderID is nil when no leader is known.
type Quorum struct {
	Error         string          `json:"error,omitempty"`
	LeaderID      *int32          `json:"leaderId"`
	LeaderEpoch   int32           `json:"leaderEpoch"`
	HighWatermark int64           `json:"highWatermark"`
	Replicas      []QuorumReplica `json:"replicas"`
}

// QuorumReplica is one voter or observer. Lag is how far its log end is behind the leader's, nil when the leader's
// is unknown; the times are nil when Kafka reports -1.
type QuorumReplica struct {
	ID           int32      `json:"id"`
	Role         string     `json:"role"`
	LogEndOffset int64      `json:"logEndOffset"`
	Lag          *int64     `json:"lag"`
	LastFetch    *time.Time `json:"lastFetch"`
	LastCaughtUp *time.Time `json:"lastCaughtUp"`
}

// Brokers is how partition leadership and replicas are spread over the brokers.
type Brokers struct {
	Error   string          `json:"error,omitempty"`
	Brokers []BrokerLeaders `json:"brokers"`
}

// BrokerLeaders counts the partitions a broker leads, the replicas it holds and the partitions it leads although
// it is not their preferred (first) replica.
type BrokerLeaders struct {
	Broker              int32 `json:"broker"`
	Leaders             int   `json:"leaders"`
	Replicas            int   `json:"replicas"`
	NonPreferredLeaders int   `json:"nonPreferredLeaders"`
}

// Groups summarizes the cluster's consumer groups: how many are in each state and each group's lag.
type Groups struct {
	Error  string       `json:"error,omitempty"`
	States []StateCount `json:"states"`
	Groups []GroupLag   `json:"groups"`
}

type StateCount struct {
	State string `json:"state"`
	Count int    `json:"count"`
}

// GroupLag is one group's lag. Lag and MaxPartitionLag are exact decimal sums of the partitions whose lag is known;
// UnknownPartitions counts the others, so a non-zero count makes Lag a lower bound. Both are nil without known lag.
type GroupLag struct {
	Group             string  `json:"group"`
	State             string  `json:"state"`
	Members           int     `json:"members"`
	Lag               *string `json:"lag"`
	MaxPartitionLag   *string `json:"maxPartitionLag"`
	UnknownPartitions int     `json:"unknownPartitions"`
	Error             string  `json:"error,omitempty"`
}

// Connect counts the cluster's Kafka Connect connectors and tasks by state, as Connect reports them, and lists what
// failed. NotConfigured is true for a cluster without Kafka Connect.
type Connect struct {
	Error         string           `json:"error,omitempty"`
	NotConfigured bool             `json:"notConfigured,omitempty"`
	Connectors    []StateCount     `json:"connectors"`
	Tasks         []StateCount     `json:"tasks"`
	Failures      []ConnectFailure `json:"failures"`
}

// ConnectFailure is a failed connector (Task nil) or task, with Connect's error summary.
type ConnectFailure struct {
	Connector string `json:"connector"`
	Task      *int32 `json:"task"`
	Error     string `json:"error"`
}
