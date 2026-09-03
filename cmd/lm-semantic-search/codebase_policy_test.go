package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type policyCommandServer struct {
	pb.UnimplementedSemanticSearchDaemonServiceServer
	startRequests  chan *pb.StartIndexRequest
	syncRequests   chan *pb.SyncIndexRequest
	updateRequests chan *pb.UpdateCodebasePolicyRequest
}

func newPolicyCommandServer() *policyCommandServer {
	return &policyCommandServer{
		startRequests:  make(chan *pb.StartIndexRequest, 1),
		syncRequests:   make(chan *pb.SyncIndexRequest, 1),
		updateRequests: make(chan *pb.UpdateCodebasePolicyRequest, 1),
	}
}

func (server *policyCommandServer) StartIndex(
	_ context.Context,
	request *pb.StartIndexRequest,
) (*pb.StartIndexResponse, error) {
	server.startRequests <- proto.Clone(request).(*pb.StartIndexRequest)
	return &pb.StartIndexResponse{
		JobId:       "job-index",
		CodebaseId:  "codebase-index",
		State:       "queued",
		DisplayText: "index queued",
	}, nil
}

func (server *policyCommandServer) SyncIndex(
	_ context.Context,
	request *pb.SyncIndexRequest,
) (*pb.SyncIndexResponse, error) {
	server.syncRequests <- proto.Clone(request).(*pb.SyncIndexRequest)
	return &pb.SyncIndexResponse{
		JobId:       "job-sync",
		CodebaseId:  "codebase-sync",
		State:       "queued",
		DisplayText: "sync queued",
	}, nil
}

func (server *policyCommandServer) UpdateCodebasePolicy(
	_ context.Context,
	request *pb.UpdateCodebasePolicyRequest,
) (*pb.UpdateCodebasePolicyResponse, error) {
	server.updateRequests <- proto.Clone(request).(*pb.UpdateCodebasePolicyRequest)
	return &pb.UpdateCodebasePolicyResponse{DisplayText: "policy updated"}, nil
}

func TestIndexSchedulingPolicyArguments(t *testing.T) {
	testCases := []struct {
		name      string
		arguments []string
		want      *pb.SchedulingPolicyPatch
	}{
		{
			name:      "omitted",
			arguments: nil,
			want:      nil,
		},
		{
			name:      "priority",
			arguments: []string{"--priority=high"},
			want: &pb.SchedulingPolicyPatch{
				Priority: pb.SchedulingPriority_SCHEDULING_PRIORITY_HIGH.Enum(),
			},
		},
		{
			name:      "quiet on",
			arguments: []string{"--quiet"},
			want:      &pb.SchedulingPolicyPatch{Quiet: boolPointer(true)},
		},
		{
			name:      "quiet explicit false",
			arguments: []string{"--quiet=false"},
			want:      &pb.SchedulingPolicyPatch{Quiet: boolPointer(false)},
		},
		{
			name:      "idle duration",
			arguments: []string{"--idle-after=5m"},
			want:      &pb.SchedulingPolicyPatch{IdleAfterSeconds: int32Pointer(300)},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := newPolicyCommandServer()
			socketPath := startPolicyCommandGRPCServer(t, server)
			arguments := []string{"codebase", "index", "/tmp/project"}
			arguments = append(arguments, testCase.arguments...)

			if err := executePolicyCommand(socketPath, arguments); err != nil {
				t.Fatalf("execute codebase index: %v", err)
			}

			request := <-server.startRequests
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

func TestSyncSchedulingPolicyArguments(t *testing.T) {
	server := newPolicyCommandServer()
	socketPath := startPolicyCommandGRPCServer(t, server)
	arguments := []string{
		"codebase",
		"sync",
		"/tmp/project",
		"--priority=low",
		"--quiet=false",
		"--idle-after=5m",
	}

	if err := executePolicyCommand(socketPath, arguments); err != nil {
		t.Fatalf("execute codebase sync: %v", err)
	}

	request := <-server.syncRequests
	want := &pb.SchedulingPolicyPatch{
		Priority:         pb.SchedulingPriority_SCHEDULING_PRIORITY_LOW.Enum(),
		Quiet:            boolPointer(false),
		IdleAfterSeconds: int32Pointer(300),
	}
	if !proto.Equal(request.GetSchedulingPolicy(), want) {
		t.Fatalf("scheduling policy = %v, want %v", request.GetSchedulingPolicy(), want)
	}
}

func TestIndexSchedulingPolicyRejectsInvalidIdleAfter(t *testing.T) {
	testCases := []struct {
		name      string
		argument  string
		wantError string
	}{
		{
			name:      "explicit zero",
			argument:  "--idle-after=0s",
			wantError: "idle after seconds must be positive",
		},
		{
			name:      "fractional second",
			argument:  "--idle-after=1500ms",
			wantError: "idle after must resolve to whole seconds",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := newPolicyCommandServer()
			socketPath := startPolicyCommandGRPCServer(t, server)
			err := executePolicyCommand(
				socketPath,
				[]string{"codebase", "index", "/tmp/project", testCase.argument},
			)
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("error = %v, want %q", err, testCase.wantError)
			}
			select {
			case request := <-server.startRequests:
				t.Fatalf("invalid request reached daemon: %v", request)
			default:
			}
		})
	}
}

func TestCodebasePriorityCommand(t *testing.T) {
	server := newPolicyCommandServer()
	socketPath := startPolicyCommandGRPCServer(t, server)
	if err := executePolicyCommand(
		socketPath,
		[]string{"codebase", "priority", "/tmp/project", "high"},
	); err != nil {
		t.Fatalf("execute codebase priority: %v", err)
	}

	request := <-server.updateRequests
	want := &pb.SchedulingPolicyPatch{
		Priority: pb.SchedulingPriority_SCHEDULING_PRIORITY_HIGH.Enum(),
	}
	if request.GetPath() != "/tmp/project" || !proto.Equal(request.GetPatch(), want) {
		t.Fatalf("update request = %v, want path and priority-only patch", request)
	}
}

func TestCodebasePriorityRejectsInvalidValue(t *testing.T) {
	server := newPolicyCommandServer()
	socketPath := startPolicyCommandGRPCServer(t, server)
	err := executePolicyCommand(
		socketPath,
		[]string{"codebase", "priority", "/tmp/project", "urgent"},
	)
	if err == nil || !strings.Contains(err.Error(), "priority must be high, normal, or low") {
		t.Fatalf("error = %v, want priority choices", err)
	}
	select {
	case request := <-server.updateRequests:
		t.Fatalf("invalid request reached daemon: %v", request)
	default:
	}
}

func TestCodebaseQuietCommand(t *testing.T) {
	testCases := []struct {
		name      string
		arguments []string
		want      *pb.SchedulingPolicyPatch
	}{
		{
			name:      "on",
			arguments: []string{"codebase", "quiet", "/tmp/project", "on"},
			want:      &pb.SchedulingPolicyPatch{Quiet: boolPointer(true)},
		},
		{
			name: "off with idle threshold",
			arguments: []string{
				"codebase",
				"quiet",
				"/tmp/project",
				"off",
				"--idle-after=5m",
			},
			want: &pb.SchedulingPolicyPatch{
				Quiet:            boolPointer(false),
				IdleAfterSeconds: int32Pointer(300),
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := newPolicyCommandServer()
			socketPath := startPolicyCommandGRPCServer(t, server)
			if err := executePolicyCommand(socketPath, testCase.arguments); err != nil {
				t.Fatalf("execute codebase quiet: %v", err)
			}

			request := <-server.updateRequests
			if request.GetPath() != "/tmp/project" || !proto.Equal(request.GetPatch(), testCase.want) {
				t.Fatalf("update request = %v, want path and quiet patch %v", request, testCase.want)
			}
		})
	}
}

func TestCodebaseQuietRejectsInvalidValue(t *testing.T) {
	server := newPolicyCommandServer()
	socketPath := startPolicyCommandGRPCServer(t, server)
	err := executePolicyCommand(
		socketPath,
		[]string{"codebase", "quiet", "/tmp/project", "sometimes"},
	)
	if err == nil || !strings.Contains(err.Error(), "quiet must be on or off") {
		t.Fatalf("error = %v, want quiet choices", err)
	}
	select {
	case request := <-server.updateRequests:
		t.Fatalf("invalid request reached daemon: %v", request)
	default:
	}
}

func startPolicyCommandGRPCServer(
	t *testing.T,
	policyServer *policyCommandServer,
) string {
	t.Helper()
	socketDirectory, err := os.MkdirTemp("", "lms-cli-")
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

func executePolicyCommand(socketPath string, arguments []string) error {
	root := newRoot(socketPath)
	root.SetArgs(arguments)
	return root.Execute()
}

func boolPointer(value bool) *bool {
	return &value
}

func int32Pointer(value int32) *int32 {
	return &value
}
