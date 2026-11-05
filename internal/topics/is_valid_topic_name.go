package topics

import "regexp"

var topicNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

func IsValidTopicName(name string) bool {
	return name != "." && name != ".." && topicNamePattern.MatchString(name)
}
