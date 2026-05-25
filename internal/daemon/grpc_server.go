package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	pb "goodkind.io/claude-context-go/gen/go/claudecontext/v1"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/pbconv"
	"goodkind.io/claude-context-go/internal/version"
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
		DisplayText:   renderStartIndex(request.GetPath(), codebase, job, deduplicated),
	}, nil
}

// ClearIndex removes a tracked codebase from daemon state.
func (server *GRPCServer) ClearIndex(ctx context.Context, request *pb.ClearIndexRequest) (*pb.ClearIndexResponse, error) {
	codebase, err := server.manager.ClearIndex(ctx, request.GetPath(), pbClient(request.GetClient()))
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	indexedCount, indexingCount := countCodebaseStates(server.manager.ListIndexes(ctx))
	return &pb.ClearIndexResponse{
		CodebaseId:  codebase.ID,
		Cleared:     true,
		DisplayText: renderClearIndex(codebase, indexedCount, indexingCount),
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
		JobId:       job.ID,
		Cancelled:   job.State == "cancelled",
		DisplayText: renderCancelJob(job),
	}, nil
}

// SyncIndex registers a sync request against an existing codebase.
func (server *GRPCServer) SyncIndex(ctx context.Context, request *pb.SyncIndexRequest) (*pb.SyncIndexResponse, error) {
	job, codebase, deduplicated, err := server.manager.SyncIndex(ctx, request.GetPath(), pbClient(request.GetClient()))
	if err != nil {
		if strings.Contains(err.Error(), "conflicting active job") {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		if strings.Contains(err.Error(), "codebase not tracked") {
			return nil, status.Error(codes.NotFound, err.Error())
		}
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
		JobId:       job.ID,
		CodebaseId:  codebase.ID,
		State:       string(job.State),
		DisplayText: renderSyncIndex(codebase, job, deduplicated),
	}, nil
}

// GetIndex resolves one tracked codebase by alias or canonical path.
func (server *GRPCServer) GetIndex(ctx context.Context, request *pb.GetIndexRequest) (*pb.GetIndexResponse, error) {
	codebase, activeJob, found, err := server.manager.GetIndex(ctx, request.GetPath())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	response := &pb.GetIndexResponse{
		Tracked:     found,
		DisplayText: renderGetIndex(request.GetPath(), found, codebasePointer(found, codebase), activeJob),
	}
	if found {
		response.Codebase = pbconv.ToCodebase(codebase)
		response.ActiveJob = pbconv.ToJobPointer(activeJob)
	}
	return response, nil
}

// ListIndexes returns all tracked codebases.
func (server *GRPCServer) ListIndexes(ctx context.Context, request *pb.ListIndexesRequest) (*pb.ListIndexesResponse, error) {
	_ = ctx
	_ = request
	codebases := server.manager.ListIndexes(ctx)
	response := &pb.ListIndexesResponse{
		Indexes: make([]*pb.Codebase, 0, len(codebases)),
	}
	for _, codebase := range codebases {
		response.Indexes = append(response.Indexes, pbconv.ToCodebase(codebase))
	}
	response.DisplayText = renderListIndexes(codebases)
	return response, nil
}

// GetJob resolves one tracked job by id.
func (server *GRPCServer) GetJob(ctx context.Context, request *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	_ = ctx
	job, found := server.manager.GetJob(request.GetJobId())
	if !found {
		return nil, status.Error(codes.NotFound, "job not found: "+request.GetJobId())
	}
	return &pb.GetJobResponse{
		Job:         pbconv.ToJob(job),
		DisplayText: renderGetJob(&job),
	}, nil
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
	response.DisplayText = renderListJobs(jobs)
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
	outcome, err := server.manager.SearchCode(ctx, request.GetPath(), request.GetQuery(), request.GetLimit(), request.GetExtensionFilter())
	if err != nil {
		if strings.Contains(err.Error(), "codebase not tracked") {
			return nil, status.Error(codes.NotFound, fmt.Sprintf("Error: Codebase '%s' is not indexed. Please index it first using the index_codebase tool.", request.GetPath()))
		}
		if strings.Contains(err.Error(), "invalid file extensions in extensionFilter") {
			return nil, status.Error(codes.InvalidArgument, "Error: "+err.Error())
		}
		if strings.Contains(err.Error(), "index data for '") {
			return nil, status.Error(codes.InvalidArgument, "Error: "+err.Error())
		}
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	response := &pb.SearchCodeResponse{
		Results:   make([]*pb.SearchResult, 0, len(outcome.Results)),
		Codebase:  pbconv.ToCodebase(outcome.Codebase),
		ActiveJob: pbconv.ToJobPointer(outcome.ActiveJob),
		DisplayText: renderSearch(searchView{
			RequestedPath: request.GetPath(),
			Query:         request.GetQuery(),
			Codebase:      outcome.Codebase,
			ActiveJob:     outcome.ActiveJob,
			Results:       outcome.Results,
		}),
	}
	for _, result := range outcome.Results {
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
	response.DisplayText = renderDoctor(diagnostics)
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
		return model.ClientInfo{Name: "", PID: 0}
	}
	return model.ClientInfo{
		Name: client.GetName(),
		PID:  client.GetPid(),
	}
}

func codebasePointer(found bool, codebase model.Codebase) *model.Codebase {
	if !found {
		return nil
	}
	return &codebase
}

func countCodebaseStates(codebases []model.Codebase) (int, int) {
	indexedCount := 0
	indexingCount := 0
	for _, codebase := range codebases {
		switch codebase.Status {
		case model.CodebaseStatusIndexed:
			indexedCount++
		case model.CodebaseStatusIndexing:
			indexingCount++
		case model.CodebaseStatusNotIndexed, model.CodebaseStatusFailed, model.CodebaseStatusStale:
		default:
		}
	}
	return indexedCount, indexingCount
}
