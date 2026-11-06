package httpapi

func (b *requiredBoolean) UnmarshalJSON(value []byte) error {
	if b.Present {
		return errInvalidRequest
	}
	switch string(value) {
	case "true":
		b.Value = true
	case "false":
		b.Value = false
	default:
		return errInvalidRequest
	}
	b.Present = true
	return nil
}
