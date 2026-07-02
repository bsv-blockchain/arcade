package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Fields returns trace_id/span_id zap fields for the span carried by ctx.
// It returns nil when ctx carries no valid span context (telemetry disabled,
// no active span, or a remote context that was never sampled in).
func Fields(ctx context.Context) []zap.Field {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	return []zap.Field{
		zap.String("trace_id", sc.TraceID().String()),
		zap.String("span_id", sc.SpanID().String()),
	}
}

// LoggerWith returns base annotated with trace_id/span_id fields drawn from
// ctx's span context, so every log line emitted through the returned logger
// can be correlated to the trace in the downstream OTEL backend. base is
// returned unchanged when it is nil or ctx carries no valid span context.
func LoggerWith(ctx context.Context, base *zap.Logger) *zap.Logger {
	if base == nil {
		return base
	}
	fields := Fields(ctx)
	if len(fields) == 0 {
		return base
	}
	return base.With(fields...)
}
