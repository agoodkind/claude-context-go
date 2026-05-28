// Command claude-context-mcp hosts the MCP adapter process.
//
// The adapter is launched by Claude, Cursor, and other MCP clients via stdio.
// Three independent defenses make sure the process exits with its parent and
// never accumulates into the orphan pile that hit the upstream TS adapter
// (199 orphan processes holding ~50-100MB of node memory each):
//   - stdin EOF unwinds the stdio read loop in mcpserver.Run.
//   - A PPID watcher cancels the run context when reparented to init.
//   - A panic recovery here forces [os.Exit](1) so a runtime panic in any
//     background goroutine takes the whole process down instead of leaving
//     a half-dead process that holds resources without doing work.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"goodkind.io/claude-context-go/internal/mcpserver"
	"goodkind.io/gklog/correlation"
)

func main() {
	rootContext := installCorrelationLogger("mcp-boot")
	slog.InfoContext(rootContext, "claude-context-mcp starting", "pid", os.Getpid(), "parent_pid", os.Getppid())
	exitCode := run(rootContext)
	slog.InfoContext(rootContext, "claude-context-mcp stopping", "exit_code", exitCode)
	os.Exit(exitCode)
}

// installCorrelationLogger wraps the default JSON slog handler with a
// correlation handler in strict mode and returns a root context that
// carries the given origin so boot records inherit a trace_id.
func installCorrelationLogger(origin string) context.Context {
	jsonHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	handler := correlation.SlogHandler(jsonHandler, correlation.HandlerOptions{
		Strict:   true,
		Required: []string{"trace_id", "span_id"},
	})
	slog.SetDefault(slog.New(handler))
	rootCorrelation := correlation.New("").WithIdentityAttributes(
		correlation.IdentityAttribute{Key: "origin", Value: origin},
	)
	return correlation.WithContext(context.Background(), rootCorrelation)
}

func run(rootContext context.Context) (exitCode int) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.ErrorContext(rootContext, "claude-context-mcp panicked", "err", fmt.Errorf("panic: %v", recovered))
			exitCode = 1
		}
	}()

	if err := mcpserver.Run(rootContext); err != nil {
		slog.ErrorContext(rootContext, "claude-context-mcp failed", "err", err)
		return 1
	}
	return 0
}
