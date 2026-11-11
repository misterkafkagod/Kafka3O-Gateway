package backup

import "strings"

// ParseRestoreConnectorName returns the topic a restore connector restores; names from before per-restore names
// ("kafka3o-restore-<topic>") are read too.
func ParseRestoreConnectorName(name string) (string, bool) {
	rest, ok := strings.CutPrefix(name, restoreConnectorPrefix)
	if !ok || rest == "" {
		return "", false
	}
	if topic, id, found := strings.Cut(rest, "~"); found {
		return topic, topic != "" && id != ""
	}
	return rest, true
}
