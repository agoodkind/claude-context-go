package mcpserver

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"goodkind.io/claude-context-go/internal/adapterr"
	"goodkind.io/claude-context-go/internal/clock"
	"goodkind.io/gklog/correlation"
)

// wrapTool decorates a [server.ToolHandlerFunc] with correlation
// context, a started/completed log pair, panic recovery, and the
// [adapterr] error envelope. Failures route through
// [adapterr.RespondMCP] so the client sees a typed envelope and the
// operator finds the daemon-side detail by grepping trace_id.
func wrapTool(name string, inner server.ToolHandlerFunc) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (result *mcp.CallToolResult, outErr error) {
		ctx, _ = correlation.Ensure(ctx, "")
		corr := correlation.FromContext(ctx).WithIdentityAttributes(
			correlation.IdentityAttribute{Key: "mcp_tool", Value: name},
		)
		ctx = correlation.WithContext(ctx, corr)

		defer func() {
			if recovered := recover(); recovered != nil {
				mcpErr := adapterr.RespondMCP(ctx, adapterr.NewInternal("mcp tool panic", fmt.Errorf("panic: %v", recovered)))
				result = toolErrorResult(mcpErr.Error())
				outErr = nil
			}
		}()

		started := clock.Now()
		slog.InfoContext(ctx, "mcp.tool.started", "tool", name)
		result, innerErr := inner(ctx, req)
		elapsedMs := clock.Now().Sub(started).Milliseconds()
		if innerErr != nil {
			slog.WarnContext(ctx, "mcp.tool.completed", "tool", name, "duration_ms", elapsedMs, "err", innerErr.Error())
			mcpErr := adapterr.RespondMCP(ctx, innerErr)
			return toolErrorResult(mcpErr.Error()), nil
		}
		slog.InfoContext(ctx, "mcp.tool.completed", "tool", name, "duration_ms", elapsedMs)
		return result, nil
	}
}
