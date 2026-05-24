package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	pb "github.com/zilliztech/claude-context-go/gen/go/claudecontext/v1"
	"github.com/zilliztech/claude-context-go/internal/model"
	"github.com/zilliztech/claude-context-go/internal/pbconv"
	"github.com/zilliztech/claude-context-go/internal/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// GRPCServer exposes the daemon manager through the generated gRPC service.
type GRPCServer struct {
	manager  *Manager
	shutdown func()
}

// NewGRPCServer builds the daemon's gRPC service implementation.
func NewGRPCServer(manager *Manager, shutdown func()) *GRPCServer {
	return &GRPCServer{
		manager:  manager,
		shutdown: shutdown,
	}
}

// Version reports daemon build metadata.
func (server *GRPCServer) Version(ctx context.Context, request *pb.VersionRequest) (*pb.VersionResponse, error) {
	_ = ctx
	_ = request
	return &pb.VersionResponse{
		Version:   version.Version,
		Commit:    version.Commit,
		BuildTime: version.BuildTime,
	}, nil
}

// StartIndex registers a new indexing request with the daemon.
func (server *GRPCServer) StartIndex(ctx context.Context, request *pb.StartIndexRequest) (*pb.StartIndexResponse, error) {
	job, codebase, deduplicated, err := server.manager.StartIndex(ctx, request.GetPath(), pbClient(request.GetClient()), pbconv.FromStartIndexConfig(request), request.GetForce())
	if err != nil {
		if strings.Contains(err.Error(), "conflicting active job") {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.StartIndexResponse{
		JobId:         job.ID,
		CodebaseId:    codebase.ID,
		State:         string(job.State),
		Deduplicated:  deduplicated,
		CanonicalPath: codebase.CanonicalPath,
	}, nil
}

// ClearIndex removes a tracked codebase from daemon state.
func (server *GRPCServer) ClearIndex(ctx context.Context, request *pb.ClearIndexRequest) (*pb.ClearIndexResponse, error) {
	_ = ctx
	codebase, err := server.manager.ClearIndex(request.GetPath(), pbClient(request.GetClient()))
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &pb.ClearIndexResponse{
		CodebaseId: codebase.ID,
		Cleared:    true,
	}, nil
}

// CancelJob cancels a tracked daemon job.
func (server *GRPCServer) CancelJob(ctx context.Context, request *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	_ = ctx
	job, err := server.manager.CancelJob(request.GetJobId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &pb.CancelJobResponse{
		JobId:     job.ID,
		Cancelled: job.State == "cancelled",
	}, nil
}

// SyncIndex registers a sync request against an existing codebase.
func (server *GRPCServer) SyncIndex(ctx context.Context, request *pb.SyncIndexRequest) (*pb.SyncIndexResponse, error) {
	job, codebase, deduplicated, err := server.manager.StartIndex(ctx, request.GetPath(), pbClient(request.GetClient()), pbconv.FromStartIndexConfig(&pb.StartIndexRequest{Path: request.GetPath()}), false)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	operation := "sync"
	if deduplicated {
		operation = job.Operation
	}
	if operation == "sync" {
		job.Operation = operation
	}
	return &pb.SyncIndexResponse{
		JobId:      job.ID,
		CodebaseId: codebase.ID,
		State:      string(job.State),
	}, nil
}

// GetIndex resolves one tracked codebase by alias or canonical path.
func (server *GRPCServer) GetIndex(ctx context.Context, request *pb.GetIndexRequest) (*pb.GetIndexResponse, error) {
	_ = ctx
	codebase, found, err := server.manager.GetIndex(request.GetPath())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !found {
		return nil, status.Error(codes.NotFound, "codebase not tracked: "+request.GetPath())
	}
	return &pb.GetIndexResponse{Codebase: pbconv.ToCodebase(codebase)}, nil
}

// ListIndexes returns all tracked codebases.
func (server *GRPCServer) ListIndexes(ctx context.Context, request *pb.ListIndexesRequest) (*pb.ListIndexesResponse, error) {
	_ = ctx
	_ = request
	codebases := server.manager.ListIndexes()
	response := &pb.ListIndexesResponse{
		Indexes: make([]*pb.Codebase, 0, len(codebases)),
	}
	for _, codebase := range codebases {
		response.Indexes = append(response.Indexes, pbconv.ToCodebase(codebase))
	}
	return response, nil
}

// GetJob resolves one tracked job by id.
func (server *GRPCServer) GetJob(ctx context.Context, request *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	_ = ctx
	job, found := server.manager.GetJob(request.GetJobId())
	if !found {
		return nil, status.Error(codes.NotFound, "job not found: "+request.GetJobId())
	}
	return &pb.GetJobResponse{Job: pbconv.ToJob(job)}, nil
}

// ListJobs returns all tracked jobs, optionally filtered by codebase id.
func (server *GRPCServer) ListJobs(ctx context.Context, request *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	_ = ctx
	jobs := server.manager.ListJobs(request.GetCodebaseId())
	response := &pb.ListJobsResponse{
		Jobs: make([]*pb.Job, 0, len(jobs)),
	}
	for _, job := range jobs {
		response.Jobs = append(response.Jobs, pbconv.ToJob(job))
	}
	return response, nil
}

// WatchJobs streams the latest visible state for requested jobs.
func (server *GRPCServer) WatchJobs(request *pb.WatchJobsRequest, stream pb.ClaudeContextDaemonService_WatchJobsServer) error {
	for _, jobID := range request.GetJobIds() {
		job, found := server.manager.GetJob(jobID)
		if !found {
			continue
		}
		if err := stream.Send(&pb.WatchJobsResponse{Job: pbconv.ToJob(job)}); err != nil {
			slog.Error("send watch jobs event failed", "job_id", jobID, "err", err)
			return fmt.Errorf("send watch jobs event for %s: %w", jobID, err)
		}
	}
	return nil
}

// SearchCode is the future search RPC surface for semantic lookups.
func (server *GRPCServer) SearchCode(ctx context.Context, request *pb.SearchCodeRequest) (*pb.SearchCodeResponse, error) {
	results, err := server.manager.SearchCode(request.GetPath(), request.GetQuery(), request.GetLimit(), request.GetExtensionFilter())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	response := &pb.SearchCodeResponse{
		Results: make([]*pb.SearchResult, 0, len(results)),
	}
	for _, result := range results {
		response.Results = append(response.Results, &pb.SearchResult{
			RelativePath: result.RelativePath,
			StartLine:    result.StartLine,
			EndLine:      result.EndLine,
			Language:     result.Language,
			Score:        0,
			Content:      result.Content,
		})
	}
	return response, nil
}

// Doctor reports daemon-local diagnostics.
func (server *GRPCServer) Doctor(ctx context.Context, request *pb.DoctorRequest) (*pb.DoctorResponse, error) {
	_ = ctx
	_ = request
	diagnostics := server.manager.Doctor()
	response := &pb.DoctorResponse{
		Diagnostics: make([]*pb.Diagnostic, 0, len(diagnostics)),
	}
	for _, diagnostic := range diagnostics {
		response.Diagnostics = append(response.Diagnostics, &pb.Diagnostic{
			Severity: "warning",
			Code:     "path_check",
			Summary:  diagnostic,
			Detail:   diagnostic,
		})
	}
	return response, nil
}

// Shutdown requests a graceful daemon shutdown.
func (server *GRPCServer) Shutdown(ctx context.Context, request *pb.ShutdownRequest) (*pb.ShutdownResponse, error) {
	_ = ctx
	_ = request
	peerInfo, found := peer.FromContext(ctx)
	if found && peerInfo.Addr != nil {
		slog.InfoContext(ctx, "shutdown requested", "peer", peerInfo.Addr.String())
	} else {
		slog.InfoContext(ctx, "shutdown requested")
	}
	if server.shutdown != nil {
		go func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					slog.ErrorContext(ctx, "shutdown callback panic", "err", fmt.Errorf("panic: %v", recovered))
				}
			}()
			server.shutdown()
		}()
	}
	return &pb.ShutdownResponse{Accepted: true}, nil
}

func pbClient(client *pb.ClientInfo) model.ClientInfo {
	if client == nil {
		return model.ClientInfo{}
	}
	return model.ClientInfo{
		Name: client.GetName(),
		PID:  client.GetPid(),
	}
}
