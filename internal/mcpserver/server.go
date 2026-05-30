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
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	pb "goodkind.io/claude-context-go/gen/go/claudecontext/v1"
	"goodkind.io/claude-context-go/internal/config"
	"goodkind.io/claude-context-go/internal/grpcutil"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/response"
	"goodkind.io/gklog/version"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	outputModeEnv           = "CLAUDE_CONTEXT_MCP_OUTPUT"
	defaultIndexWaitSeconds = 300
	indexWaitPollInterval   = 1500 * time.Millisecond
)

// Run starts the MCP stdio server and blocks until the client disconnects.
func Run(ctx context.Context) error {
	slog.InfoContext(ctx, "start MCP server")

	cfg, err := config.Default()
	if err != nil {
		slog.ErrorContext(ctx, "load MCP config failed", "err", err)
		return fmt.Errorf("load MCP config: %w", err)
	}

	outputMode := response.ParseMode(os.Getenv(outputModeEnv))
	mcpServer := server.NewMCPServer("claude-context", version.String())

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
			mcp.WithBoolean("wait", mcp.Description("block this tool call until the indexing job reaches a terminal state (completed, failed, or cancelled)")),
			mcp.WithNumber("wait_timeout_seconds", mcp.Description("max seconds to wait when wait=true; on timeout the daemon job keeps running and the tool returns the current progress (default 300)")),
		),
		wrapTool("index_codebase", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			startRequest := &pb.StartIndexRequest{
				Path:             req.GetString("path", ""),
				Force:            req.GetBool("force", false),
				CustomExtensions: req.GetStringSlice("customExtensions", []string{}),
				IgnorePatterns:   req.GetStringSlice("ignorePatterns", []string{}),
				Splitter:         &pb.SplitterConfig{Type: req.GetString("splitter", "")},
				Client:           &pb.ClientInfo{Name: "mcp"},
			}
			if !req.GetBool("wait", false) {
				return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
					return client.StartIndex(ctx, startRequest)
				})
			}
			timeoutSeconds := req.GetInt("wait_timeout_seconds", defaultIndexWaitSeconds)
			if timeoutSeconds <= 0 {
				timeoutSeconds = defaultIndexWaitSeconds
			}
			return callDaemonIndexAndWait(ctx, socketPath, outputMode, startRequest, time.Duration(timeoutSeconds)*time.Second)
		}),
	)
}

func registerClearTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"clear_index",
			mcp.WithDescription("Clear a tracked codebase index through the daemon"),
			mcp.WithString("path", mcp.Description("absolute path to the codebase directory")),
		),
		wrapTool("clear_index", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.ClearIndex(ctx, &pb.ClearIndexRequest{
					Path:   req.GetString("path", ""),
					Client: &pb.ClientInfo{Name: "mcp"},
				})
			})
		}),
	)
}

func registerStatusTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"get_indexing_status",
			mcp.WithDescription("Get the current indexing status of one codebase path"),
			mcp.WithString("path", mcp.Description("absolute path to the codebase directory")),
		),
		wrapTool("get_indexing_status", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.GetIndex(ctx, &pb.GetIndexRequest{Path: req.GetString("path", "")})
			})
		}),
	)
}

func registerListIndexesTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"list_indexing_statuses",
			mcp.WithDescription("List every tracked codebase and its current indexing status"),
		),
		wrapTool("list_indexing_statuses", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.ListIndexes(ctx, &pb.ListIndexesRequest{})
			})
		}),
	)
}

func registerListJobsTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"list_indexing_jobs",
			mcp.WithDescription("List active and historical indexing jobs"),
			mcp.WithString("codebase_id", mcp.Description("optional codebase id to filter jobs")),
		),
		wrapTool("list_indexing_jobs", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.ListJobs(ctx, &pb.ListJobsRequest{CodebaseId: req.GetString("codebase_id", "")})
			})
		}),
	)
}

func registerGetJobTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"get_indexing_job",
			mcp.WithDescription("Get one indexing job by id"),
			mcp.WithString("job_id", mcp.Description("job id to inspect")),
		),
		wrapTool("get_indexing_job", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.GetJob(ctx, &pb.GetJobRequest{JobId: req.GetString("job_id", "")})
			})
		}),
	)
}

func registerDoctorTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"doctor_indexing",
			mcp.WithDescription("Inspect local daemon indexing health and diagnostics"),
		),
		wrapTool("doctor_indexing", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonDoctor(ctx, socketPath, outputMode)
		}),
	)
}

// callDaemonDoctor renders the daemon's own diagnostics and then appends a
// dropped-codebase section. A dropped codebase is a path that has a completed
// indexing job in the daemon's retained job history but no longer appears in
// the current registry, while its directory still exists on disk. Such a path
// was indexed before and silently fell out of tracking, so it would otherwise
// go stale forever without surfacing anywhere. A path that was never indexed is
// never reported, because the user intentionally leaves many repositories
// unindexed.
func callDaemonDoctor(ctx context.Context, socketPath string, outputMode response.Mode) (*mcp.CallToolResult, error) {
	connection, client, err := grpcutil.DialDaemon(ctx, socketPath)
	if err != nil {
		slog.ErrorContext(ctx, "dial daemon for doctor failed", "socket_path", socketPath, "err", err)
		return nil, fmt.Errorf("dial daemon: %w", err)
	}
	defer connection.Close()

	outgoingCtx := grpcutil.WithCorrelation(ctx)

	doctorResponse, err := client.Doctor(outgoingCtx, &pb.DoctorRequest{})
	if err != nil {
		slog.ErrorContext(ctx, "doctor RPC failed", "socket_path", socketPath, "err", err)
		return toolErrorResult(rpcErrorText(err)), nil
	}

	base, err := renderToolResponse(outputMode, doctorResponse)
	if err != nil {
		return nil, err
	}

	jobsResponse, err := client.ListJobs(outgoingCtx, &pb.ListJobsRequest{})
	if err != nil {
		slog.ErrorContext(ctx, "list jobs for doctor failed", "socket_path", socketPath, "err", err)
		return base, nil
	}
	indexesResponse, err := client.ListIndexes(outgoingCtx, &pb.ListIndexesRequest{})
	if err != nil {
		slog.ErrorContext(ctx, "list indexes for doctor failed", "socket_path", socketPath, "err", err)
		return base, nil
	}

	dropped := computeDroppedCodebases(jobsResponse.GetJobs(), indexesResponse.GetIndexes(), pathExists)
	section := renderDroppedSection(dropped)

	baseText := toolResultText(base)
	combined := baseText + "\n\n" + section
	return mcp.NewToolResultText(combined), nil
}

// computeDroppedCodebases returns the sorted canonical paths that have a
// completed indexing job but are absent from the current index set while their
// directory still exists on disk. The presence check runs through exists so
// the computation stays unit-testable without touching the filesystem.
func computeDroppedCodebases(jobs []*pb.Job, indexes []*pb.Codebase, exists func(string) bool) []string {
	tracked := make(map[string]struct{}, len(indexes))
	for _, codebase := range indexes {
		path := codebase.GetCanonicalPath()
		if path == "" {
			continue
		}
		tracked[path] = struct{}{}
	}

	indexedBefore := make(map[string]struct{})
	for _, job := range jobs {
		if model.JobState(job.GetState()) != model.JobStateCompleted {
			continue
		}
		path := job.GetCanonicalPath()
		if path == "" {
			continue
		}
		indexedBefore[path] = struct{}{}
	}

	dropped := make([]string, 0)
	for path := range indexedBefore {
		if _, stillTracked := tracked[path]; stillTracked {
			continue
		}
		if !exists(path) {
			continue
		}
		dropped = append(dropped, path)
	}
	sort.Strings(dropped)
	return dropped
}

// renderDroppedSection formats the dropped-codebase section for human-facing
// doctor output. With no dropped codebases it states that none exist so the
// section reads as a deliberate clean result rather than a missing check.
func renderDroppedSection(dropped []string) string {
	if len(dropped) == 0 {
		return "Dropped codebases (completed index, now untracked, still on disk): none"
	}

	lines := make([]string, 0, len(dropped)+1)
	lines = append(lines, fmt.Sprintf("Dropped codebases (completed index, now untracked, still on disk): %d", len(dropped)))
	for _, path := range dropped {
		lines = append(lines, "- "+path)
	}
	return strings.Join(lines, "\n")
}

// pathExists reports whether a directory or file exists at path. It treats any
// stat error other than non-existence as "present" so a transient permission
// error does not suppress a genuinely dropped codebase.
func pathExists(path string) bool {
	_, err := os.Stat(path)
	return !errors.Is(err, os.ErrNotExist)
}

// toolResultText extracts the single text payload from a rendered tool result.
// renderToolResponse always returns exactly one mcp.TextContent item, so the
// fallback empty string only guards against an unexpected content shape.
func toolResultText(result *mcp.CallToolResult) string {
	if result == nil || len(result.Content) == 0 {
		return ""
	}
	text, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		return ""
	}
	return text.Text
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
		wrapTool("search_code", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
				return client.SearchCode(ctx, &pb.SearchCodeRequest{
					Path:            req.GetString("path", ""),
					Query:           req.GetString("query", ""),
					Limit:           safeInt32(req.GetInt("limit", 10)),
					ExtensionFilter: req.GetStringSlice("extensionFilter", []string{}),
				})
			})
		}),
	)
}

// callDaemonIndexAndWait starts an index job through StartIndex and polls
// GetJob until the job reaches a terminal state (completed, failed,
// cancelled) or the supplied timeout elapses. When the timeout trips, the
// daemon job keeps running and the tool returns the latest GetIndex view so
// the caller can decide to poll again or move on. The poll cadence is short
// (~1.5s) so terminal events propagate to the caller with minimal delay.
//
// Concurrent waiters dedupe at the daemon (see Manager.dedupAgainstActiveJob)
// because StartIndex returns the existing job id when one is in flight, so
// every waiter ends up polling the same job and observing the same terminal
// event.
func callDaemonIndexAndWait(ctx context.Context, socketPath string, outputMode response.Mode, startRequest *pb.StartIndexRequest, timeout time.Duration) (*mcp.CallToolResult, error) {
	connection, client, err := grpcutil.DialDaemon(ctx, socketPath)
	if err != nil {
		slog.ErrorContext(ctx, "dial daemon for wait failed", "socket_path", socketPath, "err", err)
		return nil, fmt.Errorf("dial daemon: %w", err)
	}
	defer connection.Close()

	outgoingCtx := grpcutil.WithCorrelation(ctx)
	startResponse, err := client.StartIndex(outgoingCtx, startRequest)
	if err != nil {
		slog.ErrorContext(ctx, "start index for wait failed", "path", startRequest.GetPath(), "err", err)
		return toolErrorResult(rpcErrorText(err)), nil
	}

	jobID := startResponse.GetJobId()
	if jobID == "" {
		return renderToolResponse(outputMode, startResponse)
	}

	waitCtx, cancel := context.WithTimeout(outgoingCtx, timeout)
	defer cancel()
	ticker := time.NewTicker(indexWaitPollInterval)
	defer ticker.Stop()

	for {
		jobResponse, err := client.GetJob(waitCtx, &pb.GetJobRequest{JobId: jobID})
		if err == nil && isTerminalJobState(jobResponse.GetJob().GetState()) {
			indexResponse, err := client.GetIndex(outgoingCtx, &pb.GetIndexRequest{Path: startRequest.GetPath()})
			if err != nil {
				slog.ErrorContext(ctx, "get index after wait failed", "path", startRequest.GetPath(), "err", err)
				return renderToolResponse(outputMode, jobResponse)
			}
			return renderToolResponse(outputMode, indexResponse)
		}

		select {
		case <-waitCtx.Done():
			indexResponse, err := client.GetIndex(outgoingCtx, &pb.GetIndexRequest{Path: startRequest.GetPath()})
			if err != nil {
				slog.ErrorContext(ctx, "get index after wait timeout failed", "path", startRequest.GetPath(), "err", err)
				return renderToolResponse(outputMode, startResponse)
			}
			return renderToolResponse(outputMode, indexResponse)
		case <-ticker.C:
		}
	}
}

// isTerminalJobState reports whether a JobState value will not change again.
// The daemon emits JobState through the proto's `state` string field; the
// comparison goes through model.JobState so the switch stays on typed
// constants rather than bare string literals.
func isTerminalJobState(state string) bool {
	switch model.JobState(state) {
	case model.JobStateCompleted, model.JobStateFailed, model.JobStateCancelled:
		return true
	case model.JobStateQueued, model.JobStateRunning, model.JobStateCancelling:
		return false
	default:
		return false
	}
}

func callDaemonTool(ctx context.Context, socketPath string, outputMode response.Mode, call daemonProtoCall) (*mcp.CallToolResult, error) {
	connection, client, err := grpcutil.DialDaemon(ctx, socketPath)
	if err != nil {
		slog.ErrorContext(ctx, "dial daemon failed", "socket_path", socketPath, "err", err)
		return nil, fmt.Errorf("dial daemon: %w", err)
	}
	defer connection.Close()

	result, err := call(grpcutil.WithCorrelation(ctx), client)
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
