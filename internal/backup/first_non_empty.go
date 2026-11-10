package backup

// firstNonEmpty keeps an existing error message and otherwise uses the new one.
func firstNonEmpty(current, next string) string {
	if current != "" {
		return current
	}
	return next
}
