package message

import (
	"bytes"
	"context"
	"fmt"
	"mime"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	apierrors "github.com/misterkafkagod/kafka3o/internal/api/errors"
	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	"github.com/misterkafkagod/kafka3o/internal/scan"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/message"
)

// maxBulkRequestBytes bounds Huma's own raw-body read for M6 (a safety net
// against an unbounded read from a broken or malicious client): the
// service's own, configurable ceiling (FUNC-SPEC §8.8) is what actually
// produces the documented 413 PAYLOAD_TOO_LARGE response, so this is set
// generously above any reasonable configured value rather than tied to it.
const maxBulkRequestBytes = 64 * 1024 * 1024

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

	huma.Register(humaAPI, huma.Operation{
		OperationID: "message-search",
		Method:      http.MethodPost,
		Path:        "/v1/topics/{name}/messages/search",
		Summary:     "Search messages by regex",
		Tags:        []string{"Messages"},
		Extensions:  commandExtension("M3"),
	}, searchMessages(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "message-filter",
		Method:      http.MethodPost,
		Path:        "/v1/topics/{name}/messages/filter",
		Summary:     "Filter messages by JSONPath",
		Tags:        []string{"Messages"},
		Extensions:  commandExtension("M4"),
	}, filterMessages(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "message-produce",
		Method:      http.MethodPost,
		Path:        "/v1/topics/{name}/messages",
		Summary:     "Produce message(s)",
		Tags:        []string{"Messages"},
		Extensions:  commandExtension("M5"),
	}, produceMessages(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID:  "message-produce-bulk",
		Method:       http.MethodPost,
		Path:         "/v1/topics/{name}/messages/bulk",
		Summary:      "Bulk produce from an uploaded NDJSON or JSON-array body",
		Tags:         []string{"Messages"},
		Extensions:   commandExtension("M6"),
		MaxBodyBytes: maxBulkRequestBytes,
	}, produceBulk(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "message-tombstone",
		Method:      http.MethodPost,
		Path:        "/v1/topics/{name}/tombstones",
		Summary:     "Send a tombstone for a key",
		Tags:        []string{"Messages"},
		Extensions:  commandExtension("M7"),
	}, sendTombstone(svc))
}

func produceMessages(svc *message.Service) func(context.Context, *ProduceInput) (*ProduceOutput, error) {
	return func(ctx context.Context, in *ProduceInput) (*ProduceOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.Produce(ctx, caller, in.Name, in.Body.Records)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		body, status := toProduceResponseBody(result)
		return &ProduceOutput{Status: status, Body: body}, nil
	}
}

func produceBulk(svc *message.Service) func(context.Context, *ProduceBulkInput) (*ProduceOutput, error) {
	return func(ctx context.Context, in *ProduceBulkInput) (*ProduceOutput, error) {
		requestID := apierrors.RequestIDFrom(ctx)

		ndjson, err := parseBulkContentType(in.ContentType)
		if err != nil {
			return nil, apierrors.UnsupportedMediaType(requestID, in.ContentType)
		}

		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.ProduceBulk(ctx, caller, in.Name, bytes.NewReader(in.RawBody), ndjson)
		if err != nil {
			return nil, apierrors.Map(err, requestID)
		}
		body, status := toProduceResponseBody(result)
		return &ProduceOutput{Status: status, Body: body}, nil
	}
}

// parseBulkContentType reports whether contentType names NDJSON (true),
// the JSON-array default (false, including an absent Content-Type), or is
// unsupported (FUNC-SPEC §8.2: only application/json and
// application/x-ndjson are accepted).
func parseBulkContentType(contentType string) (ndjson bool, err error) {
	mt := contentType
	if parsed, _, perr := mime.ParseMediaType(contentType); perr == nil {
		mt = parsed
	}
	switch mt {
	case "application/x-ndjson":
		return true, nil
	case "application/json", "":
		return false, nil
	default:
		return false, fmt.Errorf("unsupported content type %q", contentType)
	}
}

func sendTombstone(svc *message.Service) func(context.Context, *TombstoneInput) (*TombstoneOutput, error) {
	return func(ctx context.Context, in *TombstoneInput) (*TombstoneOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.Tombstone(ctx, caller, in.Name, in.Body)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &TombstoneOutput{Body: TombstoneResponseBody{Partition: result.Partition, Offset: result.Offset}}, nil
	}
}

func searchMessages(svc *message.Service) func(context.Context, *SearchInput) (*ReadMessagesOutput, error) {
	return func(ctx context.Context, in *SearchInput) (*ReadMessagesOutput, error) {
		requestID := apierrors.RequestIDFrom(ctx)
		body := in.Body

		if body.Limit != nil {
			return nil, apierrors.Map(&core.PolicyError{Code: core.Validation, Message: "limit is not accepted on this endpoint"}, requestID)
		}

		from, to, err := parseFromTo(body.From, body.To, requestID)
		if err != nil {
			return nil, err
		}
		fields, ferr := parseRegexFields(body.Fields)
		if ferr != nil {
			return nil, apierrors.Map(&core.PolicyError{Code: core.Validation, Message: ferr.Error()}, requestID)
		}

		var partitions []int32
		if body.Partition != nil {
			partitions = []int32{*body.Partition}
		}

		result, err := svc.Search(ctx, message.SearchParams{
			Topic: in.Name, Partitions: partitions, From: from, To: to,
			Regex: body.Regex, Fields: fields, CaseInsensitive: body.CaseInsensitive,
			MaxScan: body.MaxScan, MaxMatches: body.MaxMatches,
			MaxBytes: body.MaxBytes, MaxTimeMs: body.MaxTimeMs, Format: body.Format,
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

func filterMessages(svc *message.Service) func(context.Context, *FilterInput) (*ReadMessagesOutput, error) {
	return func(ctx context.Context, in *FilterInput) (*ReadMessagesOutput, error) {
		requestID := apierrors.RequestIDFrom(ctx)
		body := in.Body

		if body.Limit != nil {
			return nil, apierrors.Map(&core.PolicyError{Code: core.Validation, Message: "limit is not accepted on this endpoint"}, requestID)
		}

		from, to, err := parseFromTo(body.From, body.To, requestID)
		if err != nil {
			return nil, err
		}
		op, operr := parseFilterOp(body.Filter.Op)
		if operr != nil {
			return nil, apierrors.Map(&core.PolicyError{Code: core.Validation, Message: operr.Error()}, requestID)
		}

		var partitions []int32
		if body.Partition != nil {
			partitions = []int32{*body.Partition}
		}

		result, err := svc.Filter(ctx, message.FilterParams{
			Topic: in.Name, Partitions: partitions, From: from, To: to,
			Path: body.Filter.Path, Op: op, Value: body.Filter.Value,
			MaxScan: body.MaxScan, MaxMatches: body.MaxMatches,
			MaxBytes: body.MaxBytes, MaxTimeMs: body.MaxTimeMs, Format: body.Format,
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

// parseFromTo parses M3/M4's shared from=/to= body fields, returning an
// already-mapped *apierrors.Envelope on failure so callers can return it
// directly.
func parseFromTo(fromRaw, toRaw, requestID string) (from message.From, to *message.From, err error) {
	from, ferr := ParseFrom(fromRaw)
	if ferr != nil {
		return message.From{}, nil, apierrors.Map(&core.PolicyError{Code: core.Validation, Message: ferr.Error()}, requestID)
	}
	if toRaw != "" {
		t, terr := ParseFrom(toRaw)
		if terr != nil {
			return message.From{}, nil, apierrors.Map(&core.PolicyError{Code: core.Validation, Message: terr.Error()}, requestID)
		}
		to = &t
	}
	return from, to, nil
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
