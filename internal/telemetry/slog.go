package telemetry

import (
	"context"
	"io"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
)

// NewLogger builds the gateway's structured logger (TECH-SPEC §1.1
// logging): every record is written as JSON to w (stdout in production)
// for operators, and the same record is bridged into the OTel Logs
// pipeline via otelslog for trace correlation — "logs via otelslog bridge
// until the logs SDK is stable in v1.47" (TECH-SPEC §1.0). Callers attach a
// request id to a line the same way as any other attribute, e.g.
// logger.InfoContext(ctx, "msg", "requestId", id) (FUNC-SPEC X4).
func NewLogger(w io.Writer, serviceName string) *slog.Logger {
	return slog.New(fanoutHandler{
		json:   slog.NewJSONHandler(w, nil),
		bridge: otelslog.NewHandler(serviceName),
	})
}

// fanoutHandler sends every record to both handlers (TECH-SPEC §1.1): json
// is the operator-facing sink, bridge forwards to OTel's Logs pipeline.
type fanoutHandler struct {
	json   slog.Handler
	bridge slog.Handler
}

func (h fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.json.Enabled(ctx, level) || h.bridge.Enabled(ctx, level)
}

func (h fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	if err := h.json.Handle(ctx, r.Clone()); err != nil {
		return err
	}
	return h.bridge.Handle(ctx, r.Clone())
}

func (h fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return fanoutHandler{json: h.json.WithAttrs(attrs), bridge: h.bridge.WithAttrs(attrs)}
}

func (h fanoutHandler) WithGroup(name string) slog.Handler {
	return fanoutHandler{json: h.json.WithGroup(name), bridge: h.bridge.WithGroup(name)}
}
