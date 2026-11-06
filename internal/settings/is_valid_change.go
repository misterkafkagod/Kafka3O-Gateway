package settings

import (
	"strings"
	"unicode/utf8"
)

func IsValidChange(change Change) bool {
	if change.Name == "" || len(change.Name) > 256 || strings.TrimSpace(change.Name) != change.Name {
		return false
	}
	switch change.Operation {
	case "set":
		return change.Value != nil && len(*change.Value) <= 32768 && utf8.ValidString(*change.Value)
	case "reset":
		return change.Value == nil
	default:
		return false
	}
}
