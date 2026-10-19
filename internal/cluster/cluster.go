package cluster

// Cluster is the identity of a Kafka cluster and its brokers.
type Cluster struct {
	ID           string   `json:"clusterId"`
	ControllerID int32    `json:"controllerId"`
	Brokers      []Broker `json:"brokers"`
}

type Broker struct {
	ID   int32   `json:"id"`
	Host string  `json:"host"`
	Port int32   `json:"port"`
	Rack *string `json:"rack,omitempty"`
}
