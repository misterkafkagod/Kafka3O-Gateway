package topics

import "errors"

var ErrInvalidTopic = errors.New("invalid topic")
var ErrTopicAlreadyExists = errors.New("topic already exists")
