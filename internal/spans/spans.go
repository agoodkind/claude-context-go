// Package spans opens daemon-side child spans on a [context.Context]
// that carries a [correlation.Context]. Each span emits a started/
// completed record pair tagged with span name, duration, and error
// class.
package spans

import (
	"context"
	"errors"
	"log/slog"

	"goodkind.io/claude-context-go/internal/adapterr"
	"goodkind.io/claude-context-go/internal/clock"
	"goodkind.io/gklog/correlation"
)

// Attach returns a child of ctx with the given correlation identity
// attributes added to the active [correlation.Context]. Use it at job
// creation sites to back-fill job_id and codebase_id onto the
// correlation context that downstream spans inherit.
func Attach(ctx context.Context, attrs ...correlation.IdentityAttribute) context.Context {
	return correlation.WithContext(ctx, correlation.FromContext(ctx).WithIdentityAttributes(attrs...))
}

// Open returns a child context plus a deferred completion function.
// The child correlation inherits the parent trace, gets a fresh span,
// and carries the supplied [correlation.IdentityAttribute] entries.
// Pair with a single defer at the call site:
//
//	ctx, done := spans.Open(ctx, "semantic.replace")
//	defer done(&err)
func Open(ctx context.Context, name string, attrs ...correlation.IdentityAttribute) (context.Context, func(*error)) {
	corr := correlation.FromContext(ctx).Child().WithIdentityAttributes(attrs...)
	ctx = correlation.WithContext(ctx, corr)
	started := clock.Now()
	slog.InfoContext(ctx, "daemon.span.started", "span", name)
	return ctx, func(errPtr *error) {
		var err error
		if errPtr != nil {
			err = *errPtr
		}
		level := slog.LevelInfo
		errorClass := ""
		if err != nil {
			level = slog.LevelWarn
			var adapterErr *adapterr.AdapterError
			if errors.As(err, &adapterErr) {
				errorClass = string(adapterErr.Class)
			}
		}
		slog.LogAttrs(ctx, level, "daemon.span.completed",
			slog.String("span", name),
			slog.Int64("duration_ms", clock.Now().Sub(started).Milliseconds()),
			slog.String("error_class", errorClass),
		)
	}
}
