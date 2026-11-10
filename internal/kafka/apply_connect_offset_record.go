package kafka

import "encoding/json"

// ApplyConnectOffsetRecord folds one record of the Connect offsets topic into counts for one connector. Keys are
// ["<connector>", {"bucket":..., "objectKey":...}] and values {"recordCount": n}; a null value removes the object.
func ApplyConnectOffsetRecord(counts map[string]int64, connector string, key, value []byte) {
	var parsedKey []json.RawMessage
	if json.Unmarshal(key, &parsedKey) != nil || len(parsedKey) != 2 {
		return
	}
	var name string
	var partition struct {
		ObjectKey string `json:"objectKey"`
	}
	if json.Unmarshal(parsedKey[0], &name) != nil || name != connector || json.Unmarshal(parsedKey[1], &partition) != nil || partition.ObjectKey == "" {
		return
	}
	if value == nil {
		delete(counts, partition.ObjectKey)
		return
	}
	var offset struct {
		RecordCount *int64 `json:"recordCount"`
	}
	if json.Unmarshal(value, &offset) == nil && offset.RecordCount != nil {
		counts[partition.ObjectKey] = *offset.RecordCount
	}
}
