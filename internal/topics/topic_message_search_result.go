package topics

type TopicMessageSearchResult struct {
	Messages  []TopicMessage `json:"messages"`
	Scanned   int            `json:"scanned"`
	Truncated bool           `json:"truncated"`
}
