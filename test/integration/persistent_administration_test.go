//go:build integration

package integration

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
)

func TestPersistentKafka_AdministrationValidation(t *testing.T) {
	cfg, _, _ := loadPersistentKafkaConfig(t)
	reader, err := kafka.NewClient(cfg)
	if err != nil {
		t.Fatalf("create Kafka reader (error type %T)", err)
	}
	defer reader.Close()
	writer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("create Kafka validation client (error type %T)", err)
	}
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := kadm.NewClient(writer)

	resource := settings.Resource{Kind: settings.Broker, Name: "1"}
	describe := kafka.NewDescribeConfigurations(reader)
	before, err := describe.Execute(ctx, resource)
	if err != nil {
		t.Fatalf("read broker configuration (error type %T)", err)
	}
	beforeSetting := findPersistentSetting(before, "log.retention.ms")
	if beforeSetting == nil || beforeSetting.Sensitive || beforeSetting.ReadOnly || beforeSetting.Type != "LONG" {
		t.Fatal("expected an editable, non-sensitive log.retention.ms broker setting")
	}
	invalid := "not-a-valid-long"
	change := settings.Change{Name: "log.retention.ms", Operation: "set", Value: &invalid}
	err = kafka.NewAlterConfiguration(admin).Execute(ctx, resource, change, true)
	if !errors.Is(err, settings.ErrRejected) {
		t.Fatalf("Kafka must reject the invalid value during validate-only (error type %T)", err)
	}
	after, err := describe.Execute(ctx, resource)
	if err != nil {
		t.Fatalf("re-read broker configuration (error type %T)", err)
	}
	if !reflect.DeepEqual(beforeSetting, findPersistentSetting(after, change.Name)) {
		t.Fatal("broker configuration changed during validate-only")
	}

	t.Run("unchanged partition count", func(t *testing.T) {
		metadata, err := kafka.NewListTopicMetadata(reader).Execute(ctx)
		if err != nil {
			t.Fatalf("list topic metadata (error type %T)", err)
		}
		if len(metadata) == 0 {
			t.Skip("no existing topic is available for validate-only")
		}
		topic := metadata[0].Name
		placement := kafka.NewDescribePlacement(reader)
		before, err := placement.Execute(ctx, topic)
		if err != nil {
			t.Fatalf("read topic placement (error type %T)", err)
		}
		if len(before.Partitions) == 0 {
			t.Skip("the selected topic has no partitions")
		}
		err = kafka.NewUpdateKafkaPartitions(admin).Execute(ctx, topic, int32(len(before.Partitions)), true)
		if !errors.Is(err, partitions.ErrRejected) {
			t.Fatalf("Kafka must reject an unchanged partition count during validate-only (error type %T)", err)
		}
		after, err := placement.Execute(ctx, topic)
		if err != nil || !reflect.DeepEqual(before.Partitions, after.Partitions) {
			t.Fatalf("topic placement changed or could not be re-read (error type %T)", err)
		}
	})
}

func findPersistentSetting(configs []settings.Configuration, name string) *settings.Configuration {
	for _, entry := range configs {
		if entry.Name == name {
			return &entry
		}
	}
	return nil
}
