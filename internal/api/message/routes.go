package message

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	apierrors "github.com/misterkafkagod/kafka3o/internal/api/errors"
	"github.com/misterkafkagod/kafka3o/internal/scan"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/message"
)

// commandIDExtension is the x-command-id key every operation carries
// (TECH-SPEC O5).
const commandIDExtension = "x-command-id"

func commandExtension(id string) map[string]any {
	return map[string]any{commandIDExtension: id}
}

// Register wires the message commands onto humaAPI (FUNC-SPEC §8.7 M1, M2;
// TECH-SPEC §6.2).
func Register(humaAPI huma.API, svc *message.Service) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "message-read",
		Method:      http.MethodGet,
		Path:        "/v1/topics/{name}/messages",
		Summary:     "Read messages",
		Tags:        []string{"Messages"},
		Extensions:  commandExtension("M1"),
	}, readMessages(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "message-get",
		Method:      http.MethodGet,
		Path:        "/v1/topics/{name}/partitions/{partition}/messages/{offset}",
		Summary:     "Fetch a single message",
		Tags:        []string{"Messages"},
		Extensions:  commandExtension("M2"),
	}, getMessage(svc))
}

func readMessages(svc *message.Service) func(context.Context, *ReadMessagesInput) (*ReadMessagesOutput, error) {
	return func(ctx context.Context, in *ReadMessagesInput) (*ReadMessagesOutput, error) {
		requestID := apierrors.RequestIDFrom(ctx)

		from, err := ParseFrom(in.From)
		if err != nil {
			return nil, apierrors.Map(&core.PolicyError{Code: core.Validation, Message: err.Error()}, requestID)
		}
		var to *message.From
		if in.To != "" {
			t, err := ParseFrom(in.To)
			if err != nil {
				return nil, apierrors.Map(&core.PolicyError{Code: core.Validation, Message: err.Error()}, requestID)
			}
			to = &t
		}

		var partitions []int32
		if in.Partition >= 0 {
			partitions = []int32{in.Partition}
		}

		result, err := svc.Read(ctx, message.ReadParams{
			Topic:      in.Name,
			Partitions: partitions,
			From:       from,
			To:         to,
			Limit:      in.Limit,
			MaxBytes:   in.MaxBytes,
			MaxTimeMs:  in.MaxTimeMs,
			Format:     in.Format,
		})
		if err != nil {
			return nil, apierrors.Map(err, requestID)
		}
		return &ReadMessagesOutput{Body: ReadMessagesBody{
			Items: toRecordDTOs(result.Items),
			Scan:  toScanStatsDTO(result.Stats),
		}}, nil
	}
}

func getMessage(svc *message.Service) func(context.Context, *GetMessageInput) (*GetMessageOutput, error) {
	return func(ctx context.Context, in *GetMessageInput) (*GetMessageOutput, error) {
		r, err := svc.Get(ctx, in.Name, in.Partition, in.Offset)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &GetMessageOutput{Body: toRecordDTOs([]scan.Record{r})[0]}, nil
	}
}
