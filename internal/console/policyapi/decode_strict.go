package policyapi

import (
	"bytes"
	"encoding/json"
	"io"
)

// decodeStrict decodes exactly one JSON object with no unknown fields or trailing data.
func decodeStrict(data []byte, destination any) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var extra json.RawMessage
	return decoder.Decode(destination) == nil && decoder.Decode(&extra) == io.EOF
}
