// Package mcpserver exposes the daemon over the MCP stdio tool surface.
package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"syscall"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	pb "goodkind.io/claude-context-go/gen/go/claudecontext/v1"
	"goodkind.io/claude-context-go/internal/config"
	"goodkind.io/claude-context-go/internal/grpcutil"
	"goodkind.io/claude-context-go/internal/response"
	"goodkind.io/claude-context-go/internal/version"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const outputModeEnv = "CLAUDE_CONTEXT_MCP_OUTPUT"

// Run starts the MCP stdio server and blocks until the client disconnects.
func Run(ctx context.Context) error {
	slog.InfoContext(ctx, "start MCP server")

	cfg, err := config.Default()
	if err != nil {
		slog.ErrorContext(ctx, "load MCP config failed", "err", err)
		return fmt.Errorf("load MCP config: %w", err)
	}

	outputMode := response.ParseMode(os.Getenv(outputModeEnv))
	mcpServer := server.NewMCPServer("claude-context", version.Version)

	registerSemanticSearchResource(mcpServer)
	registerSemanticSearchPrompt(mcpServer)
	registerIndexTool(mcpServer, cfg.SocketPath, outputMode)
	registerClearTool(mcpServer, cfg.SocketPath, outputMode)
	registerStatusTool(mcpServer, cfg.SocketPath, outputMode)
	registerListIndexesTool(mcpServer, cfg.SocketPath, outputMode)
	registerListJobsTool(mcpServer, cfg.SocketPath, outputMode)
	registerGetJobTool(mcpServer, cfg.SocketPath, outputMode)
	registerDoctorTool(mcpServer, cfg.SocketPath, outputMode)
	registerSearchTool(mcpServer, cfg.SocketPath, outputMode)

	stdioServer := server.NewStdioServer(mcpServer)

	// Three lifecycle signals can shut the adapter down:
	//   1. The parent dies (PPID becomes init). Without this guard, orphans
	//      pile up in `S` state holding 50-100MB of memory each.
	//   2. The client closes stdin. The Listen read loop returns on EOF.
	//   3. SIGTERM/SIGINT.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(runCtx, "MCP signal watcher panicked", "err", fmt.Errorf("panic: %v", r))
			}
		}()
		select {
		case <-signals:
			slog.InfoContext(runCtx, "MCP server received shutdown signal")
			cancel()
		case <-runCtx.Done():
		}
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(runCtx, "MCP orphan guard panicked", "err", fmt.Errorf("panic: %v", r))
			}
		}()
		watchParentDeath(runCtx, cancel)
	}()

	if err := stdioServer.Listen(runCtx, os.Stdin, os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("serve MCP stdio: %w", err)
	}
	return nil
}

type daemonProtoCall func(context.Context, pb.ClaudeContextDaemonServiceClient) (proto.Message, error)

func registerIndexTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"index_codebase",
			mcp.WithDescription("Index a codebase directory for semantic search through the daemon"),
			mcp.WithString("path", mcp.Description("absolute path to the codebase directory")),
			mcp.WithBoolean("force", mcp.Description("force reindex even if already indexed")),
			mcp.WithString("splitter", mcp.Description("splitter type, typically ast")),
			mcp.WithArray("customExtensions", mcp.Description("extra file extensions to include"), mcp.WithStringItems()),
			mcp.WithArray("ignorePatterns", mcp.Description("extra ignore patterns to exclude"), mcp.WithStringItems()),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.StartIndex(ctx, &pb.StartIndexRequest{
					Path:             req.GetString("path", ""),
					Force:            req.GetBool("force", false),
					CustomExtensions: req.GetStringSlice("customExtensions", []string{}),
					IgnorePatterns:   req.GetStringSlice("ignorePatterns", []string{}),
					Splitter:         &pb.SplitterConfig{Type: req.GetString("splitter", "")},
					Client:           &pb.ClientInfo{Name: "mcp"},
				})
			})
		},
	)
}

func registerClearTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"clear_index",
			mcp.WithDescription("Clear a tracked codebase index through the daemon"),
			mcp.WithString("path", mcp.Description("absolute path to the codebase directory")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.ClearIndex(ctx, &pb.ClearIndexRequest{
					Path:   req.GetString("path", ""),
					Client: &pb.ClientInfo{Name: "mcp"},
				})
			})
		},
	)
}

func registerStatusTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"get_indexing_status",
			mcp.WithDescription("Get the current indexing status of one codebase path"),
			mcp.WithString("path", mcp.Description("absolute path to the codebase directory")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.GetIndex(ctx, &pb.GetIndexRequest{Path: req.GetString("path", "")})
			})
		},
	)
}

func registerListIndexesTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"list_indexing_statuses",
			mcp.WithDescription("List every tracked codebase and its current indexing status"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.ListIndexes(ctx, &pb.ListIndexesRequest{})
			})
		},
	)
}

func registerListJobsTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"list_indexing_jobs",
			mcp.WithDescription("List active and historical indexing jobs"),
			mcp.WithString("codebase_id", mcp.Description("optional codebase id to filter jobs")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.ListJobs(ctx, &pb.ListJobsRequest{CodebaseId: req.GetString("codebase_id", "")})
			})
		},
	)
}

func registerGetJobTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"get_indexing_job",
			mcp.WithDescription("Get one indexing job by id"),
			mcp.WithString("job_id", mcp.Description("job id to inspect")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.GetJob(ctx, &pb.GetJobRequest{JobId: req.GetString("job_id", "")})
			})
		},
	)
}

func registerDoctorTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"doctor_indexing",
			mcp.WithDescription("Inspect local daemon indexing health and diagnostics"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.Doctor(ctx, &pb.DoctorRequest{})
			})
		},
	)
}

func registerSearchTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"search_code",
			mcp.WithDescription("Search indexed code in the daemon"),
			mcp.WithString("path", mcp.Description("absolute path to the codebase directory")),
			mcp.WithString("query", mcp.Description("natural language code search query")),
			mcp.WithNumber("limit", mcp.Description("maximum number of results")),
			mcp.WithArray("extensionFilter", mcp.Description("optional file extensions filter"), mcp.WithStringItems()),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.SearchCode(ctx, &pb.SearchCodeRequest{
					Path:            req.GetString("path", ""),
					Query:           req.GetString("query", ""),
					Limit:           safeInt32(req.GetInt("limit", 10)),
					ExtensionFilter: req.GetStringSlice("extensionFilter", []string{}),
				})
			})
		},
	)
}

func callDaemonTool(ctx context.Context, socketPath string, outputMode response.Mode, call daemonProtoCall) (*mcp.CallToolResult, error) {
	connection, client, err := grpcutil.DialDaemon(ctx, socketPath)
	if err != nil {
		slog.ErrorContext(ctx, "dial daemon failed", "socket_path", socketPath, "err", err)
		return nil, fmt.Errorf("dial daemon: %w", err)
	}
	defer connection.Close()

	result, err := call(ctx, client)
	if err != nil {
		slog.ErrorContext(ctx, "daemon RPC failed", "socket_path", socketPath, "err", err)
		return toolErrorResult(rpcErrorText(err)), nil
	}

	return renderToolResponse(outputMode, result)
}

func renderToolResponse(outputMode response.Mode, message proto.Message) (*mcp.CallToolResult, error) {
	formatted, err := response.FormatProto(outputMode, message)
	if err != nil {
		slog.Error("format daemon response failed", "err", err)
		return nil, fmt.Errorf("format daemon response: %w", err)
	}
	return mcp.NewToolResultText(formatted), nil
}

func toolErrorResult(message string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{mcp.NewTextContent(message)},
	}
}

func rpcErrorText(err error) string {
	grpcStatus, ok := status.FromError(err)
	if ok && grpcStatus != nil {
		return grpcStatus.Message()
	}
	return err.Error()
}

func safeInt32(value int) int32 {
	if value < 0 {
		return 0
	}
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(value)
}
