// Command claude-context is the operator CLI for the local daemon.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"

	pb "goodkind.io/claude-context-go/gen/go/claudecontext/v1"
	"goodkind.io/claude-context-go/internal/config"
	"goodkind.io/claude-context-go/internal/grpcutil"
	"goodkind.io/claude-context-go/internal/response"
	"goodkind.io/gklog/version"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type (
	command          string
	daemonSubcommand string
	rpcCall          func(context.Context, pb.ClaudeContextDaemonServiceClient) (proto.Message, error)
)

const (
	commandVersion command = "version"
	commandDaemon  command = "daemon"
	commandList    command = "list"
	commandJobs    command = "jobs"
	commandDoctor  command = "doctor"
	commandStatus  command = "status"
	commandJob     command = "job"
	commandIndex   command = "index"
	commandSync    command = "sync"
	commandSearch  command = "search"
	commandClear   command = "clear"
	commandCancel  command = "cancel"
)

type cliOptions struct {
	socketPath string
	outputMode response.Mode
}

type multiStringFlag []string

func (value *multiStringFlag) String() string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", []string(*value))
}

func (value *multiStringFlag) Set(entry string) error {
	*value = append(*value, entry)
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("cli failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Default()
	if err != nil {
		slog.Error("load config failed", "err", err)
		return fmt.Errorf("load config: %w", err)
	}

	socketPath := flag.String("socket", cfg.SocketPath, "unix socket path")
	jsonOutput := flag.Bool("json", false, "print compact JSON instead of human text")
	outputMode := flag.String("output", "human", "output mode: human, json, or single-line")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		return fmt.Errorf("command required: %s", usage())
	}

	mode := response.ParseMode(*outputMode)
	if *jsonOutput {
		mode = response.ModeJSON
	}

	return execute(command(args[0]), args[1:], cliOptions{
		socketPath: *socketPath,
		outputMode: mode,
	})
}

// blank reports whether a positional argument is missing or only whitespace,
// so a subcommand rejects `claude-context status ""` locally instead of sending
// an empty value to the daemon.
func blank(args []string, index int) bool {
	return index >= len(args) || strings.TrimSpace(args[index]) == ""
}

func execute(selected command, args []string, options cliOptions) error {
	switch selected {
	case commandVersion:
		fmt.Printf("version=%s commit=%s build_time=%s\n", version.String(), version.Commit, version.BuildTime)
		return nil
	case commandDaemon:
		return runDaemonSubcommand(args, options)
	case commandList:
		return callAndPrint(options, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.ListIndexes(ctx, &pb.ListIndexesRequest{})
		})
	case commandJobs:
		return callAndPrint(options, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			request := &pb.ListJobsRequest{}
			if len(args) > 0 {
				request.CodebaseId = args[0]
			}
			return client.ListJobs(ctx, request)
		})
	case commandDoctor:
		return callAndPrint(options, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.Doctor(ctx, &pb.DoctorRequest{})
		})
	case commandStatus:
		if blank(args, 0) {
			return fmt.Errorf("status requires a path")
		}
		return callAndPrint(options, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.GetIndex(ctx, &pb.GetIndexRequest{Path: args[0]})
		})
	case commandJob:
		if blank(args, 0) {
			return fmt.Errorf("job requires an id")
		}
		return callAndPrint(options, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.GetJob(ctx, &pb.GetJobRequest{JobId: args[0]})
		})
	case commandIndex:
		return runIndexCommand(args, options)
	case commandSync:
		if blank(args, 0) {
			return fmt.Errorf("sync requires a path")
		}
		clientInfo, err := currentClientInfo()
		if err != nil {
			return err
		}
		return callAndPrint(options, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.SyncIndex(ctx, &pb.SyncIndexRequest{Path: args[0], Client: clientInfo})
		})
	case commandSearch:
		return runSearchCommand(args, options)
	case commandClear:
		if blank(args, 0) {
			return fmt.Errorf("clear requires a path")
		}
		clientInfo, err := currentClientInfo()
		if err != nil {
			return err
		}
		return callAndPrint(options, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.ClearIndex(ctx, &pb.ClearIndexRequest{Path: args[0], Client: clientInfo})
		})
	case commandCancel:
		if blank(args, 0) {
			return fmt.Errorf("cancel requires a job id")
		}
		clientInfo, err := currentClientInfo()
		if err != nil {
			return err
		}
		return callAndPrint(options, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.CancelJob(ctx, &pb.CancelJobRequest{JobId: args[0], Client: clientInfo})
		})
	default:
		return fmt.Errorf("unsupported command %q: %s", selected, usage())
	}
}

func runIndexCommand(args []string, options cliOptions) error {
	flags := flag.NewFlagSet(string(commandIndex), flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	force := flags.Bool("force", false, "force reindex even if already indexed")
	splitterType := flags.String("splitter", "", "splitter type")
	var customExtensions multiStringFlag
	var ignorePatterns multiStringFlag
	flags.Var(&customExtensions, "extension", "custom file extension to include")
	flags.Var(&ignorePatterns, "ignore", "ignore pattern to exclude")

	if err := flags.Parse(args); err != nil {
		slog.Error("parse index flags failed", "err", err)
		return fmt.Errorf("parse index flags: %w", err)
	}
	remaining := flags.Args()
	if blank(remaining, 0) {
		return fmt.Errorf("index requires a path")
	}

	clientInfo, err := currentClientInfo()
	if err != nil {
		return err
	}

	request := &pb.StartIndexRequest{
		Path:             remaining[0],
		Force:            *force,
		CustomExtensions: []string(customExtensions),
		IgnorePatterns:   []string(ignorePatterns),
		Client:           clientInfo,
	}
	if *splitterType != "" {
		request.Splitter = &pb.SplitterConfig{Type: *splitterType}
	}

	return callAndPrint(options, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
		return client.StartIndex(ctx, request)
	})
}

func runSearchCommand(args []string, options cliOptions) error {
	flags := flag.NewFlagSet(string(commandSearch), flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	limit := flags.Int("limit", 10, "maximum number of results")
	var extensions multiStringFlag
	flags.Var(&extensions, "extension", "file extension filter")

	if err := flags.Parse(args); err != nil {
		slog.Error("parse search flags failed", "err", err)
		return fmt.Errorf("parse search flags: %w", err)
	}
	remaining := flags.Args()
	if blank(remaining, 0) || blank(remaining, 1) {
		return fmt.Errorf("search requires a path and query")
	}

	return callAndPrint(options, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
		searchLimit, err := safeSearchLimit(*limit)
		if err != nil {
			return nil, err
		}
		return client.SearchCode(ctx, &pb.SearchCodeRequest{
			Path:            remaining[0],
			Query:           remaining[1],
			Limit:           searchLimit,
			ExtensionFilter: []string(extensions),
		})
	})
}

func runDaemonSubcommand(args []string, options cliOptions) error {
	if len(args) == 0 {
		return fmt.Errorf("daemon subcommand required")
	}

	switch daemonSubcommand(args[0]) {
	case daemonSubcommand("status"):
		return callAndPrint(options, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.Version(ctx, &pb.VersionRequest{})
		})
	case daemonSubcommand("stop"):
		return callAndPrint(options, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.Shutdown(ctx, &pb.ShutdownRequest{})
		})
	default:
		return fmt.Errorf("unsupported daemon subcommand %q", args[0])
	}
}

func usage() string {
	return "usage: claude-context [--socket PATH] [--json|--output MODE] <version|daemon|list|jobs|doctor|status|job|index|sync|search|clear|cancel> [arg]"
}

func currentClientInfo() (*pb.ClientInfo, error) {
	pid := os.Getpid()
	if pid < 0 || pid > math.MaxInt32 {
		return nil, fmt.Errorf("process id %d does not fit in int32", pid)
	}
	return &pb.ClientInfo{
		Name: "cli",
		Pid:  int32(pid),
	}, nil
}

func callAndPrint(options cliOptions, call rpcCall) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	connection, client, err := grpcutil.DialDaemon(ctx, options.socketPath)
	if err != nil {
		slog.Error("dial daemon failed", "socket_path", options.socketPath, "err", err)
		return fmt.Errorf("dial daemon: %w", err)
	}
	defer connection.Close()

	result, err := call(ctx, client)
	if err != nil {
		slog.Error("daemon RPC failed", "socket_path", options.socketPath, "err", err)
		return formatCallError(err)
	}

	formatted, err := response.FormatProto(options.outputMode, result)
	if err != nil {
		slog.Error("format response failed", "err", err)
		return fmt.Errorf("format response: %w", err)
	}
	fmt.Printf("%s\n", formatted)
	return nil
}

func formatCallError(err error) error {
	grpcErr, ok := grpcstatus.FromError(err)
	if ok {
		return errors.New(grpcErr.Message())
	}
	return errors.New(err.Error())
}

func safeSearchLimit(limit int) (int32, error) {
	if limit < 0 {
		return 0, fmt.Errorf("limit must be non-negative")
	}
	if limit > math.MaxInt32 {
		return 0, fmt.Errorf("limit %d exceeds int32", limit)
	}
	return int32(limit), nil
}
