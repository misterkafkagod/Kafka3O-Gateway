package identity

// ValidateChangeReason accepts a trimmed, printable change reason of 1–512 bytes.
func ValidateChangeReason(reason string) error {
	if !validChangeReason(reason) {
		return ErrChangeReasonRequired
	}
	return nil
}
