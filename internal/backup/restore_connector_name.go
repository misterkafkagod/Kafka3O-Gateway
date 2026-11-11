package backup

const restoreConnectorPrefix = "kafka3o-restore-"

// RestoreConnectorName names one restore's connector. Each restore gets its own name because Kafka Connect keeps a
// source connector's offsets after the connector is deleted: a reused name would make a later restore of the topic
// skip the objects an earlier one read. "~" never occurs in topic names, so the topic can be read back.
func RestoreConnectorName(topic, restoreID string) string {
	return restoreConnectorPrefix + topic + "~" + restoreID
}
