package adapterr

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"goodkind.io/gklog/correlation"
	"google.golang.org/grpc/codes"
)

// Respond classifies err, logs it on the daemon side with the active
// correlation, and returns the gRPC code plus message for the
// boundary's [status.Error] call. A nil err returns ([codes.OK], "").
// Known classes return the safe-for-client message and hint; unknown
// classes return a sanitized message that references the daemon log
// by trace_id and job_id.
func Respond(ctx context.Context, err error) (codes.Code, string) {
	if err == nil {
		return codes.OK, ""
	}
	adapterErr := classify(err)
	logRespond(ctx, adapterErr)
	if adapterErr.SafeForClient {
		return CodeFor(adapterErr.Class), formatKnown(adapterErr, ctx)
	}
	return CodeFor(adapterErr.Class), formatUnknown(ctx)
}

// RespondMCP classifies err, logs it on the daemon side with the
// active correlation, and returns an [*MCPError] the MCP tool layer
// can surface to the client. A nil err returns nil.
func RespondMCP(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	adapterErr := classify(err)
	logRespond(ctx, adapterErr)
	corr := correlation.FromContext(ctx)
	message := formatUnknown(ctx)
	if adapterErr.SafeForClient {
		message = formatKnown(adapterErr, ctx)
	}
	return &MCPError{
		Class:   adapterErr.Class,
		Code:    adapterErr.Code,
		Message: message,
		TraceID: string(corr.TraceID),
		JobID:   corr.IdentityAttributeValue("job_id"),
	}
}

func classify(err error) *AdapterError {
	var adapterErr *AdapterError
	if errors.As(err, &adapterErr) {
		return adapterErr
	}
	return NewInternal(err.Error(), err)
}

func formatKnown(adapterErr *AdapterError, ctx context.Context) string {
	base := adapterErr.Message
	if adapterErr.Hint != "" {
		base = adapterErr.Message + "; " + adapterErr.Hint
	}
	if refs := formatDiagRefs(ctx); refs != "" {
		return base + " " + refs
	}
	return base
}

func formatUnknown(ctx context.Context) string {
	if refs := formatDiagRefs(ctx); refs != "" {
		return "internal error; see daemon logs " + refs
	}
	return "internal error"
}

// formatDiagRefs renders the correlation refs from ctx as "[trace_id=… job_id=…]"
// so error messages carry a greppable handle to the daemon log. Returns "" when
// no ids are available. The MCP client layer relies on the daemon embedding
// these in the message so it can stay a pure relay.
func formatDiagRefs(ctx context.Context) string {
	corr := correlation.FromContext(ctx)
	refs := make([]string, 0, 2)
	if corr.TraceID != "" {
		refs = append(refs, "trace_id="+string(corr.TraceID))
	}
	if jobID := corr.IdentityAttributeValue("job_id"); jobID != "" {
		refs = append(refs, "job_id="+jobID)
	}
	if len(refs) == 0 {
		return ""
	}
	return "[" + strings.Join(refs, " ") + "]"
}

func logRespond(ctx context.Context, adapterErr *AdapterError) {
	attrs := []slog.Attr{
		slog.String("class", string(adapterErr.Class)),
		slog.String("code", adapterErr.Code),
		slog.Bool("safe_for_client", adapterErr.SafeForClient),
	}
	if adapterErr.Hint != "" {
		attrs = append(attrs, slog.String("hint", adapterErr.Hint))
	}
	if adapterErr.Message != "" {
		attrs = append(attrs, slog.String("message", adapterErr.Message))
	}
	if adapterErr.Cause != nil {
		attrs = append(attrs, slog.String("cause", adapterErr.Cause.Error()))
	}
	level := slog.LevelWarn
	if adapterErr.Class == ClassInternal {
		level = slog.LevelError
	}
	slog.Default().LogAttrs(ctx, level, "adapter.error.responded", attrs...)
}
