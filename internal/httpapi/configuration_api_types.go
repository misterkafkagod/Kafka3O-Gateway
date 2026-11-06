package httpapi

import "github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"

type TopicConfigurationInput struct {
	Topic string `path:"topic" doc:"Topic name"`
}

type BrokerConfigurationInput struct {
	BrokerID int32 `path:"brokerId" doc:"Kafka broker ID" minimum:"0"`
}

type TopicConfigurationChangeInput struct {
	Topic string `path:"topic" doc:"Topic name"`
	Body  settings.Change
}

type BrokerConfigurationChangeInput struct {
	BrokerID int32 `path:"brokerId" doc:"Kafka broker ID" minimum:"0"`
	Body     settings.Change
}

type ConfigurationsOutput struct {
	Body []settings.Configuration
}

type ConfigurationChangeOutput struct {
	Body struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
}
