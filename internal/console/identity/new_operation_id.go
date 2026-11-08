package identity

import "io"

// NewOperationID returns an opaque audit operation identifier for composition-root actions.
func NewOperationID(random io.Reader) (string, error) {
	return newOpaqueID(random)
}
