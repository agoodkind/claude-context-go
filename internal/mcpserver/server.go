// Package mcpserver exposes the daemon over the MCP stdio tool surface.
package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"goodkind.io/gklog/version"
	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/config"
	"goodkind.io/lm-semantic-search/internal/grpcutil"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/orphanguard"
	"goodkind.io/lm-semantic-search/internal/response"
	"goodkind.io/lm-semantic-search/internal/wait"
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
	// WithToolCapabilities advertises the tool set; WithInputSchemaValidation
	// makes the server reject a tool call that omits a Required argument at the
	// protocol layer, before the handler runs, so a missing argument fails
	// loudly instead of defaulting to an empty value.
	mcpServer := server.NewMCPServer(
		"lm-semantic-search",
		version.String(),
		server.WithToolCapabilities(true),
		server.WithInputSchemaValidation(),
	)

	registerSemanticSearchResource(mcpServer)
	registerSemanticSearchPrompt(mcpServer)
	registerIndexTool(mcpServer, cfg.SocketPath, outputMode)
	registerClearTool(mcpServer, cfg.SocketPath, outputMode)
	registerStatusTool(mcpServer, cfg.SocketPath, outputMode)
	registerWaitForIndexingTool(mcpServer, cfg.SocketPath, outputMode)
	registerListIndexesTool(mcpServer, cfg.SocketPath, outputMode)
	registerListJobsTool(mcpServer, cfg.SocketPath, outputMode)
	registerGetJobTool(mcpServer, cfg.SocketPath, outputMode)
	registerDoctorTool(mcpServer, cfg.SocketPath, outputMode)
	registerSearchTool(mcpServer, cfg.SocketPath, outputMode)
	registerQueryGraphTool(mcpServer, cfg.SocketPath, outputMode)
	registerTracePathTool(mcpServer, cfg.SocketPath, outputMode)
	registerGetArchitectureTool(mcpServer, cfg.SocketPath, outputMode)
	registerManageADRTool(mcpServer, cfg.SocketPath, outputMode)

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
		orphanguard.Watch(runCtx, cancel)
	}()

	if err := stdioServer.Listen(runCtx, os.Stdin, os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("serve MCP stdio: %w", err)
	}
	return nil
}

type daemonProtoCall func(context.Context, pb.SemanticSearchDaemonServiceClient) (proto.Message, error)

// mcpClientInfo identifies this adapter to the daemon. caller_cwd lets the
// daemon resolve a relative tool path against the adapter's working
// directory, which the editor sets to the project root.
func mcpClientInfo() *pb.ClientInfo {
	workingDir, err := os.Getwd()
	if err != nil {
		workingDir = ""
	}
	return &pb.ClientInfo{Name: "mcp", CallerCwd: workingDir}
}

func registerIndexTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"index_codebase",
			mcp.WithDescription("Index a codebase directory for semantic search through the daemon"),
			mcp.WithString("absolutePath", mcp.Required(), mcp.Description("absolute path to the codebase directory")),
			mcp.WithBoolean("force", mcp.Description("force reindex even if already indexed")),
			mcp.WithString("splitter", mcp.Description("splitter type, typically ast")),
			mcp.WithArray("ignorePatterns", mcp.Description("extra ignore patterns to exclude"), mcp.WithStringItems()),
			mcp.WithArray("includeSubmodules", mcp.Description("submodule names or paths to include"), mcp.WithStringItems()),
			mcp.WithBoolean("wait", mcp.Description("block this tool call until the indexing job reaches a terminal state (completed, failed, or canceled)")),
			mcp.WithNumber("wait_timeout_seconds", mcp.Description("max seconds to wait when wait=true; on timeout the daemon job keeps running and the tool returns the current progress (default 300)")),
		),
		wrapTool("index_codebase", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			absolutePath, errResult, ok := requireNonEmptyArg(req, "absolutePath")
			if !ok {
				return errResult, nil
			}
			startRequest := &pb.StartIndexRequest{
				Path:              absolutePath,
				Force:             req.GetBool("force", false),
				IgnorePatterns:    req.GetStringSlice("ignorePatterns", []string{}),
				IncludeSubmodules: req.GetStringSlice("includeSubmodules", []string{}),
				Splitter:          &pb.SplitterConfig{Type: req.GetString("splitter", "")},
				Client:            mcpClientInfo(),
			}
			if !req.GetBool("wait", false) {
				return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (proto.Message, error) {
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
			mcp.WithString("absolutePath", mcp.Required(), mcp.Description("absolute path to the codebase directory")),
		),
		wrapTool("clear_index", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			absolutePath, errResult, ok := requireNonEmptyArg(req, "absolutePath")
			if !ok {
				return errResult, nil
			}
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (proto.Message, error) {
				return client.ClearIndex(ctx, &pb.ClearIndexRequest{
					Path:   absolutePath,
					Client: mcpClientInfo(),
				})
			})
		}),
	)
}

func registerStatusTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"get_indexing_status",
			mcp.WithDescription("Get one codebase's indexing status, waiting up to 300 seconds while indexing progresses"),
			mcp.WithString("absolutePath", mcp.Required(), mcp.Description("absolute path to the codebase directory")),
		),
		wrapTool("get_indexing_status", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			absolutePath, errResult, ok := requireNonEmptyArg(req, "absolutePath")
			if !ok {
				return errResult, nil
			}
			timeout := time.Duration(defaultIndexWaitSeconds) * time.Second
			result, waitErr := wait.ForIndexStatusWithClientInfo(ctx, socketPath, absolutePath, timeout, mcpClientInfo())
			if result == nil {
				message := "get indexing status returned no status"
				if waitErr != nil {
					message = "get indexing status: " + waitErr.Error()
				}
				slog.ErrorContext(ctx, "get indexing status returned no status", "path", absolutePath, "err", waitErr)
				return toolErrorResult(message), nil
			}
			return renderToolResponse(outputMode, result)
		}),
	)
}

func registerWaitForIndexingTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"wait_for_indexing",
			mcp.WithDescription("Wait for a codebase to become searchable through the daemon"),
			mcp.WithString("absolutePath", mcp.Required(), mcp.Description("absolute path to the codebase directory")),
			mcp.WithNumber("wait_timeout_seconds", mcp.Description("max seconds to wait; on timeout the tool returns the current progress (default 300)")),
		),
		wrapTool("wait_for_indexing", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			absolutePath, errResult, ok := requireNonEmptyArg(req, "absolutePath")
			if !ok {
				return errResult, nil
			}
			timeoutSeconds := req.GetInt("wait_timeout_seconds", defaultIndexWaitSeconds)
			if timeoutSeconds <= 0 {
				timeoutSeconds = defaultIndexWaitSeconds
			}
			timeout := time.Duration(timeoutSeconds) * time.Second
			result, waitErr := wait.ForIndexStatusWithClientInfo(ctx, socketPath, absolutePath, timeout, mcpClientInfo())
			// A timeout or an interrupt still carries the last status the daemon
			// answered, and that progress is the useful reply, so it renders like
			// any other status. Only a wait that produced no status at all is
			// reported as a tool error.
			if result == nil {
				message := "wait for indexing returned no status"
				if waitErr != nil {
					message = "wait for indexing: " + waitErr.Error()
				}
				slog.ErrorContext(ctx, "wait for indexing returned no status", "path", absolutePath, "err", waitErr)
				return toolErrorResult(message), nil
			}
			return renderToolResponse(outputMode, result)
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
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (proto.Message, error) {
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
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (proto.Message, error) {
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
			mcp.WithString("job_id", mcp.Required(), mcp.Description("job id to inspect")),
		),
		wrapTool("get_indexing_job", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			jobID, errResult, ok := requireNonEmptyArg(req, "job_id")
			if !ok {
				return errResult, nil
			}
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (proto.Message, error) {
				return client.GetJob(ctx, &pb.GetJobRequest{JobId: jobID})
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

// callDaemonDoctor returns the daemon's doctor surface verbatim. The daemon's
// display_text already includes the dropped-codebases section (a completed
// index that fell out of tracking while its directory still exists on disk), so
// the adapter does not recompute it client-side.
func callDaemonDoctor(ctx context.Context, socketPath string, outputMode response.Mode) (*mcp.CallToolResult, error) {
	connection, client, err := grpcutil.DialDaemon(ctx, socketPath)
	if err != nil {
		slog.ErrorContext(ctx, "dial daemon for doctor failed", "socket_path", socketPath, "err", err)
		return nil, fmt.Errorf("dial daemon: %w", err)
	}
	defer func() { _ = connection.Close() }()

	outgoingCtx := grpcutil.WithCorrelation(ctx)

	doctorResponse, err := client.Doctor(outgoingCtx, &pb.DoctorRequest{})
	if err != nil {
		slog.ErrorContext(ctx, "doctor RPC failed", "socket_path", socketPath, "err", err)
		return toolErrorResult(rpcErrorText(err)), nil
	}

	return renderToolResponse(outputMode, doctorResponse)
}

func registerSearchTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"search_code",
			mcp.WithDescription("Search indexed code in the daemon"),
			mcp.WithString("absolutePath", mcp.Required(), mcp.Description("absolute path to the codebase directory")),
			mcp.WithString("query", mcp.Required(), mcp.Description("natural language code search query")),
			mcp.WithNumber("limit", mcp.Description("maximum number of results")),
			mcp.WithArray("extensionFilter", mcp.Description("optional file extensions filter"), mcp.WithStringItems()),
		),
		wrapTool("search_code", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			absolutePath, errResult, ok := requireNonEmptyArg(req, "absolutePath")
			if !ok {
				return errResult, nil
			}
			query, errResult, ok := requireNonEmptyArg(req, "query")
			if !ok {
				return errResult, nil
			}
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (proto.Message, error) {
				return client.SearchCode(ctx, &pb.SearchCodeRequest{
					Path:            absolutePath,
					Query:           query,
					Limit:           safeInt32(req.GetInt("limit", 10)),
					ExtensionFilter: req.GetStringSlice("extensionFilter", []string{}),
					Client:          mcpClientInfo(),
				})
			})
		}),
	)
}

func registerQueryGraphTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"query_graph",
			mcp.WithDescription("Query the indexed code graph through the daemon"),
			mcp.WithString("absolutePath", mcp.Required(), mcp.Description("absolute path to the codebase directory")),
			mcp.WithString("query", mcp.Required(), mcp.Description("Cypher graph query")),
		),
		wrapTool("query_graph", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			absolutePath, errResult, ok := requireNonEmptyArg(req, "absolutePath")
			if !ok {
				return errResult, nil
			}
			query, errResult, ok := requireNonEmptyArg(req, "query")
			if !ok {
				return errResult, nil
			}

			argsJSON, err := MarshalQueryGraphArguments(query)
			if err != nil {
				return nil, err
			}
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (proto.Message, error) {
				return client.GraphTool(ctx, &pb.GraphToolRequest{
					Path:     absolutePath,
					Client:   mcpClientInfo(),
					ToolName: "query_graph",
					ArgsJson: argsJSON,
				})
			})
		}),
	)
}

func registerTracePathTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"trace_path",
			mcp.WithDescription("Trace callers and callees of a function in the code graph"),
			mcp.WithString("absolutePath", mcp.Required(), mcp.Description("absolute path to the codebase directory")),
			mcp.WithString("functionName", mcp.Required(), mcp.Description("function name to trace")),
			mcp.WithString("direction", mcp.Description("trace direction"), mcp.Enum("inbound", "outbound", "both")),
			mcp.WithNumber("depth", mcp.Description("maximum traversal depth")),
		),
		wrapTool("trace_path", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			absolutePath, errResult, ok := requireNonEmptyArg(req, "absolutePath")
			if !ok {
				return errResult, nil
			}
			functionName, errResult, ok := requireNonEmptyArg(req, "functionName")
			if !ok {
				return errResult, nil
			}

			argsJSON, err := MarshalTracePathArguments(functionName, req.GetString("direction", ""), optionalIntArgument(req, "depth"))
			if err != nil {
				return nil, err
			}
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (proto.Message, error) {
				return client.GraphTool(ctx, &pb.GraphToolRequest{
					Path:     absolutePath,
					Client:   mcpClientInfo(),
					ToolName: "trace_path",
					ArgsJson: argsJSON,
				})
			})
		}),
	)
}

func registerGetArchitectureTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"get_architecture",
			mcp.WithDescription("Return a structural overview of the indexed codebase (languages, entry points, hotspots, clusters)"),
			mcp.WithString("absolutePath", mcp.Required(), mcp.Description("absolute path to the codebase directory")),
			mcp.WithString("path", mcp.Description("directory prefix to scope the summary")),
		),
		wrapTool("get_architecture", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			absolutePath, errResult, ok := requireNonEmptyArg(req, "absolutePath")
			if !ok {
				return errResult, nil
			}

			argsJSON, err := MarshalGetArchitectureArguments(req.GetString("path", ""))
			if err != nil {
				return nil, err
			}
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (proto.Message, error) {
				return client.GraphTool(ctx, &pb.GraphToolRequest{
					Path:     absolutePath,
					Client:   mcpClientInfo(),
					ToolName: "get_architecture",
					ArgsJson: argsJSON,
				})
			})
		}),
	)
}

func registerManageADRTool(mcpServer *server.MCPServer, socketPath string, outputMode response.Mode) {
	mcpServer.AddTool(
		mcp.NewTool(
			"manage_adr",
			mcp.WithDescription("Get or update the codebase Architecture Decision Record"),
			mcp.WithString("absolutePath", mcp.Required(), mcp.Description("absolute path to the codebase directory")),
			mcp.WithString("mode", mcp.Required(), mcp.Description("ADR operation mode"), mcp.Enum("get", "update", "sections")),
			mcp.WithString("content", mcp.Description("ADR content for update")),
			mcp.WithArray("sections", mcp.Description("ADR sections to inspect or update"), mcp.WithStringItems()),
		),
		wrapTool("manage_adr", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			absolutePath, errResult, ok := requireNonEmptyArg(req, "absolutePath")
			if !ok {
				return errResult, nil
			}
			mode, errResult, ok := requireNonEmptyArg(req, "mode")
			if !ok {
				return errResult, nil
			}

			argsJSON, err := MarshalManageADRArguments(mode, req.GetString("content", ""), req.GetStringSlice("sections", nil))
			if err != nil {
				return nil, err
			}
			return callDaemonTool(ctx, socketPath, outputMode, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (proto.Message, error) {
				return client.GraphTool(ctx, &pb.GraphToolRequest{
					Path:     absolutePath,
					Client:   mcpClientInfo(),
					ToolName: "manage_adr",
					ArgsJson: argsJSON,
				})
			})
		}),
	)
}

// MarshalQueryGraphArguments builds query_graph arguments for the daemon RPC.
func MarshalQueryGraphArguments(query string) (string, error) {
	args := struct {
		Query   string `json:"query"`
		Project string `json:"project"`
		MaxRows int    `json:"max_rows"`
	}{
		Query:   query,
		Project: "",
		MaxRows: 200,
	}
	data, err := json.Marshal(args)
	if err != nil {
		slog.Error("marshal query_graph arguments failed", "err", err)
		return "", fmt.Errorf("marshal query_graph arguments: %w", err)
	}
	return string(data), nil
}

// MarshalTracePathArguments builds trace_path arguments for the daemon RPC.
func MarshalTracePathArguments(functionName string, direction string, depth *int) (string, error) {
	args := struct {
		FunctionName string `json:"function_name"`
		Direction    string `json:"direction,omitempty"`
		Depth        *int   `json:"depth,omitempty"`
	}{
		FunctionName: functionName,
		Direction:    direction,
		Depth:        depth,
	}
	data, err := json.Marshal(args)
	if err != nil {
		slog.Error("marshal trace_path arguments failed", "err", err)
		return "", fmt.Errorf("marshal trace_path arguments: %w", err)
	}
	return string(data), nil
}

// MarshalGetArchitectureArguments builds get_architecture arguments for the daemon RPC.
func MarshalGetArchitectureArguments(path string) (string, error) {
	args := struct {
		Path string `json:"path,omitempty"`
	}{
		Path: path,
	}
	data, err := json.Marshal(args)
	if err != nil {
		slog.Error("marshal get_architecture arguments failed", "err", err)
		return "", fmt.Errorf("marshal get_architecture arguments: %w", err)
	}
	return string(data), nil
}

// MarshalManageADRArguments builds manage_adr arguments for the daemon RPC.
func MarshalManageADRArguments(mode string, content string, sections []string) (string, error) {
	args := struct {
		Mode     string   `json:"mode"`
		Content  string   `json:"content,omitempty"`
		Sections []string `json:"sections,omitempty"`
	}{
		Mode:     mode,
		Content:  content,
		Sections: sections,
	}
	data, err := json.Marshal(args)
	if err != nil {
		slog.Error("marshal manage_adr arguments failed", "err", err)
		return "", fmt.Errorf("marshal manage_adr arguments: %w", err)
	}
	return string(data), nil
}

func optionalIntArgument(req mcp.CallToolRequest, key string) *int {
	if _, found := req.GetArguments()[key]; !found {
		return nil
	}
	value := req.GetInt(key, 0)
	return &value
}

// callDaemonIndexAndWait starts an index job through StartIndex and polls
// GetJob until the job reaches a terminal state (completed, failed,
// canceled) or the supplied timeout elapses. When the timeout trips, the
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
			indexResponse, err := client.GetIndex(outgoingCtx, &pb.GetIndexRequest{Path: startRequest.GetPath(), Client: mcpClientInfo()})
			if err != nil {
				slog.ErrorContext(ctx, "get index after wait failed", "path", startRequest.GetPath(), "err", err)
				return renderToolResponse(outputMode, jobResponse)
			}
			return renderToolResponse(outputMode, indexResponse)
		}

		select {
		case <-waitCtx.Done():
			indexResponse, err := client.GetIndex(outgoingCtx, &pb.GetIndexRequest{Path: startRequest.GetPath(), Client: mcpClientInfo()})
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
	case model.JobStateQueued, model.JobStateRunning, model.JobStatePaused, model.JobStateCancelling:
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

// requireNonEmptyArg reads a required string argument and returns an error
// result when it is missing, empty, or only whitespace. The schema marks the
// argument Required, so a well-behaved client is rejected before the handler
// runs; this is the second line of defense for a client that sends the key with
// an empty value. The returned bool is false when the argument is unusable, and
// the caller returns the result immediately.
func requireNonEmptyArg(req mcp.CallToolRequest, name string) (string, *mcp.CallToolResult, bool) {
	value, err := req.RequireString(name)
	if err != nil {
		return "", toolErrorResult(name + " is required"), false
	}
	if strings.TrimSpace(value) == "" {
		return "", toolErrorResult(name + " is required"), false
	}
	return value, nil, true
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
