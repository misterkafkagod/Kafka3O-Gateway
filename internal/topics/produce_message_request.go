package topics

type ProduceMessageRequest struct {
	Value   *string                `json:"value" required:"true" nullable:"false" doc:"UTF-8 message value; empty is allowed"`
	Key     *string                `json:"key,omitempty" nullable:"false" doc:"Optional UTF-8 key; empty differs from absent"`
	Headers []ProduceMessageHeader `json:"headers,omitempty" nullable:"false" doc:"UTF-8 message headers"`
}

type ProduceMessageHeader struct {
	Key   *string `json:"key" required:"true" nullable:"false"`
	Value *string `json:"value" required:"true" nullable:"false"`
}

type ProduceMessageResult struct {
	Partition int32  `json:"partition"`
	Offset    string `json:"offset" doc:"Exact decimal Kafka offset"`
}
