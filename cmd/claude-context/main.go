// Command claude-context is the operator CLI for the local daemon.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"time"

	pb "github.com/zilliztech/claude-context-go/gen/go/claudecontext/v1"
	"github.com/zilliztech/claude-context-go/internal/config"
	"github.com/zilliztech/claude-context-go/internal/grpcutil"
	"github.com/zilliztech/claude-context-go/internal/version"
	"google.golang.org/protobuf/encoding/protojson"
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
	commandClear   command = "clear"
	commandCancel  command = "cancel"
)

func main() {
	if err := run(); err != nil {
		slog.Error("cli failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	slog.Info("start cli")

	cfg, err := config.Default()
	if err != nil {
		slog.Error("load config failed", "err", err)
		return fmt.Errorf("load config: %w", err)
	}

	socketPath := flag.String("socket", cfg.SocketPath, "unix socket path")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		return fmt.Errorf("command required: %s", usage())
	}

	return execute(command(args[0]), args[1:], *socketPath)
}

func execute(selected command, args []string, socketPath string) error {
	switch selected {
	case commandVersion:
		fmt.Printf("version=%s commit=%s build_time=%s\n", version.Version, version.Commit, version.BuildTime)
		return nil
	case commandDaemon:
		return runDaemonSubcommand(args, socketPath)
	case commandList:
		return callAndPrint(socketPath, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.ListIndexes(ctx, &pb.ListIndexesRequest{})
		})
	case commandJobs:
		return callAndPrint(socketPath, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			request := &pb.ListJobsRequest{}
			if len(args) > 0 {
				request.CodebaseId = args[0]
			}
			return client.ListJobs(ctx, request)
		})
	case commandDoctor:
		return callAndPrint(socketPath, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.Doctor(ctx, &pb.DoctorRequest{})
		})
	case commandStatus:
		if len(args) == 0 {
			return fmt.Errorf("status requires a path")
		}
		return callAndPrint(socketPath, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.GetIndex(ctx, &pb.GetIndexRequest{Path: args[0]})
		})
	case commandJob:
		if len(args) == 0 {
			return fmt.Errorf("job requires an id")
		}
		return callAndPrint(socketPath, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.GetJob(ctx, &pb.GetJobRequest{JobId: args[0]})
		})
	case commandIndex:
		if len(args) == 0 {
			return fmt.Errorf("index requires a path")
		}
		clientInfo, err := currentClientInfo()
		if err != nil {
			return err
		}
		return callAndPrint(socketPath, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.StartIndex(ctx, &pb.StartIndexRequest{Path: args[0], Client: clientInfo})
		})
	case commandClear:
		if len(args) == 0 {
			return fmt.Errorf("clear requires a path")
		}
		clientInfo, err := currentClientInfo()
		if err != nil {
			return err
		}
		return callAndPrint(socketPath, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.ClearIndex(ctx, &pb.ClearIndexRequest{Path: args[0], Client: clientInfo})
		})
	case commandCancel:
		if len(args) == 0 {
			return fmt.Errorf("cancel requires a job id")
		}
		clientInfo, err := currentClientInfo()
		if err != nil {
			return err
		}
		return callAndPrint(socketPath, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.CancelJob(ctx, &pb.CancelJobRequest{JobId: args[0], Client: clientInfo})
		})
	default:
		return fmt.Errorf("unsupported command %q: %s", selected, usage())
	}
}

func runDaemonSubcommand(args []string, socketPath string) error {
	if len(args) == 0 {
		return fmt.Errorf("daemon subcommand required")
	}

	switch daemonSubcommand(args[0]) {
	case daemonSubcommand("status"):
		return callAndPrint(socketPath, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.Version(ctx, &pb.VersionRequest{})
		})
	case daemonSubcommand("stop"):
		return callAndPrint(socketPath, func(ctx context.Context, client pb.ClaudeContextDaemonServiceClient) (proto.Message, error) {
			return client.Shutdown(ctx, &pb.ShutdownRequest{})
		})
	default:
		return fmt.Errorf("unsupported daemon subcommand %q", args[0])
	}
}

func usage() string {
	return "usage: claude-context [--socket PATH] <version|daemon|list|jobs|doctor|status|job|index|clear|cancel> [arg]"
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

func callAndPrint(socketPath string, call rpcCall) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	connection, client, err := grpcutil.DialDaemon(ctx, socketPath)
	if err != nil {
		slog.Error("dial daemon failed", "socket_path", socketPath, "err", err)
		return fmt.Errorf("dial daemon: %w", err)
	}
	defer connection.Close()

	result, err := call(ctx, client)
	if err != nil {
		slog.Error("daemon RPC failed", "socket_path", socketPath, "err", err)
		return fmt.Errorf("call daemon: %w", err)
	}

	marshaler := protojson.MarshalOptions{
		Multiline: true,
		Indent:    "  ",
	}
	bytes, err := marshaler.Marshal(result)
	if err != nil {
		slog.Error("marshal response failed", "err", err)
		return fmt.Errorf("marshal response: %w", err)
	}
	fmt.Printf("%s\n", bytes)
	return nil
}
