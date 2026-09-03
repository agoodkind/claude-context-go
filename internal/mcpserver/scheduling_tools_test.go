package mcpserver

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/server"
	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/response"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type mcpPolicyServer struct {
	pb.UnimplementedSemanticSearchDaemonServiceServer
	startRequests chan *pb.StartIndexRequest
}

func (policyServer *mcpPolicyServer) StartIndex(
	_ context.Context,
	request *pb.StartIndexRequest,
) (*pb.StartIndexResponse, error) {
	policyServer.startRequests <- proto.Clone(request).(*pb.StartIndexRequest)
	return &pb.StartIndexResponse{
		JobId:       "job-index",
		CodebaseId:  "codebase-index",
		State:       "queued",
		DisplayText: "index queued",
	}, nil
}

func TestIndexSchedulingArguments(t *testing.T) {
	testCases := []struct {
		name      string
		arguments map[string]any
		want      *pb.SchedulingPolicyPatch
	}{
		{
			name:      "omitted",
			arguments: map[string]any{"absolutePath": "/tmp/project"},
			want:      nil,
		},
		{
			name: "priority",
			arguments: map[string]any{
				"absolutePath": "/tmp/project",
				"priority":     "high",
			},
			want: &pb.SchedulingPolicyPatch{
				Priority: pb.SchedulingPriority_SCHEDULING_PRIORITY_HIGH.Enum(),
			},
		},
		{
			name: "explicit quiet false",
			arguments: map[string]any{
				"absolutePath": "/tmp/project",
				"quiet":        false,
			},
			want: &pb.SchedulingPolicyPatch{Quiet: mcpBoolPointer(false)},
		},
		{
			name: "idle seconds",
			arguments: map[string]any{
				"absolutePath":       "/tmp/project",
				"idle_after_seconds": float64(300),
			},
			want: &pb.SchedulingPolicyPatch{IdleAfterSeconds: mcpInt32Pointer(300)},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			policyServer := &mcpPolicyServer{
				startRequests: make(chan *pb.StartIndexRequest, 1),
			}
			socketPath := startMCPPolicyGRPCServer(t, policyServer)
			mcpServer := server.NewMCPServer("test", "test")
			registerIndexTool(mcpServer, socketPath, response.ModeHuman)

			result, err := mcpServer.GetTool("index_codebase").Handler(
				context.Background(),
				callRequest(testCase.arguments),
			)
			if err != nil {
				t.Fatalf("index handler returned error: %v", err)
			}
			if result == nil || result.IsError {
				t.Fatalf("index handler result = %#v, want success", result)
			}

			request := <-policyServer.startRequests
			if !proto.Equal(request.GetSchedulingPolicy(), testCase.want) {
				t.Fatalf(
					"scheduling policy = %v, want %v",
					request.GetSchedulingPolicy(),
					testCase.want,
				)
			}
		})
	}
}

func TestIndexSchedulingArgumentsRejectExplicitZeroAndFraction(t *testing.T) {
	testCases := []struct {
		name      string
		value     float64
		wantError string
	}{
		{
			name:      "zero",
			value:     0,
			wantError: "idle after seconds must be positive",
		},
		{
			name:      "fraction",
			value:     1.5,
			wantError: "idle_after_seconds must be a whole number",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			policyServer := &mcpPolicyServer{
				startRequests: make(chan *pb.StartIndexRequest, 1),
			}
			socketPath := startMCPPolicyGRPCServer(t, policyServer)
			mcpServer := server.NewMCPServer("test", "test")
			registerIndexTool(mcpServer, socketPath, response.ModeHuman)

			result, err := mcpServer.GetTool("index_codebase").Handler(
				context.Background(),
				callRequest(map[string]any{
					"absolutePath":       "/tmp/project",
					"idle_after_seconds": testCase.value,
				}),
			)
			if err != nil {
				t.Fatalf("index handler returned transport error: %v", err)
			}
			if result == nil || !result.IsError {
				t.Fatalf("index handler result = %#v, want tool error", result)
			}
			if got := errorText(t, result); !strings.Contains(got, testCase.wantError) {
				t.Fatalf("error text = %q, want %q", got, testCase.wantError)
			}
			select {
			case request := <-policyServer.startRequests:
				t.Fatalf("invalid request reached daemon: %v", request)
			default:
			}
		})
	}
}

func TestIndexSchedulingPolicyPatchLogsInvalidArgumentsAtDebug(t *testing.T) {
	previousLogger := slog.Default()
	var output bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	_, err := indexSchedulingPolicyPatch(
		context.Background(),
		callRequest(map[string]any{"priority": "unsupported"}),
	)
	if err == nil {
		t.Fatal("indexSchedulingPolicyPatch accepted an unsupported priority")
	}
	if got := output.String(); !strings.Contains(got, "level=DEBUG") ||
		strings.Contains(got, "level=WARN") {
		t.Fatalf("validation log = %q, want debug without warning", got)
	}
}

func TestIndexSchedulingArgumentsAreRegistered(t *testing.T) {
	mcpServer := newMCPServer("/tmp/daemon.sock", response.ModeHuman)
	indexTool := mcpServer.GetTool("index_codebase")
	if indexTool == nil {
		t.Fatal("index_codebase is not registered")
	}
	for _, name := range []string{"priority", "quiet", "idle_after_seconds"} {
		if _, found := indexTool.Tool.InputSchema.Properties[name]; !found {
			t.Fatalf("index_codebase schema missing %q", name)
		}
	}
}

func TestNoSyncTool(t *testing.T) {
	mcpServer := newMCPServer("/tmp/daemon.sock", response.ModeHuman)
	if _, found := mcpServer.ListTools()["sync_codebase"]; found {
		t.Fatal("sync_codebase must not be registered")
	}
}

func startMCPPolicyGRPCServer(t *testing.T, policyServer *mcpPolicyServer) string {
	t.Helper()
	socketDirectory, err := os.MkdirTemp("", "lms-mcp-")
	if err != nil {
		t.Fatalf("create daemon socket directory: %v", err)
	}
	socketPath := filepath.Join(socketDirectory, "daemon.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen on daemon socket: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterSemanticSearchDaemonServiceServer(grpcServer, policyServer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		_ = os.RemoveAll(socketDirectory)
	})
	return socketPath
}

func mcpBoolPointer(value bool) *bool {
	return &value
}

func mcpInt32Pointer(value int32) *int32 {
	return &value
}
