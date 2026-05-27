package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	pb "goodkind.io/claude-context-go/gen/go/claudecontext/v1"
	"goodkind.io/claude-context-go/internal/adapterr"
	"goodkind.io/claude-context-go/internal/clock"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/pbconv"
	"goodkind.io/claude-context-go/internal/version"
	"goodkind.io/gklog/correlation"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// beginRPC opens the per-RPC correlation span, emits daemon.rpc.started,
// and returns the derived context plus a deferred completion function
// that recovers panics through [adapterr.Respond] and emits
// daemon.rpc.completed with status code and duration.
func beginRPC(ctx context.Context, method string) (context.Context, func(*error)) {
	corr := correlation.FromIncomingMetadata(ctx).Child()
	ctx = correlation.WithContext(ctx, corr)
	started := clock.Now()
	slog.InfoContext(ctx, "daemon.rpc.started", "method", method)
	return ctx, func(errPtr *error) {
		if recovered := recover(); recovered != nil {
			*errPtr = status.Error(adapterr.Respond(ctx, adapterr.NewInternal("daemon panic", fmt.Errorf("panic: %v", recovered))))
		}
		logRPCDone(ctx, method, started, *errPtr)
	}
}

func logRPCDone(ctx context.Context, method string, started time.Time, err error) {
	level := slog.LevelInfo
	code := "OK"
	if err != nil {
		level = slog.LevelWarn
		code = status.Code(err).String()
	}
	slog.LogAttrs(ctx, level, "daemon.rpc.completed",
		slog.String("method", method),
		slog.String("status", code),
		slog.Int64("duration_ms", clock.Now().Sub(started).Milliseconds()),
	)
}

// classifyManagerError promotes a raw manager error into a typed
// [adapterr.AdapterError] based on the message substrings the
// manager layer uses today. Unrecognized errors pass through; the
// boundary wraps them as [adapterr.ClassInternal].
func classifyManagerError(path string, err error) error {
	if err == nil {
		return nil
	}
	var adapterErr *adapterr.AdapterError
	if errors.As(err, &adapterErr) {
		return err
	}
	text := err.Error()
	switch {
	case strings.Contains(text, "conflicting active job"):
		return adapterr.NewConflictingJob(text, err)
	case strings.Contains(text, "codebase not tracked"):
		return adapterr.NewNotIndexed(path, err)
	case strings.Contains(text, "invalid file extensions in extensionFilter"):
		return adapterr.NewInvalidPath(text, err)
	case strings.Contains(text, "index data for '"):
		return adapterr.NewInvalidPath(text, err)
	}
	return err
}

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
func (server *GRPCServer) Version(ctx context.Context, request *pb.VersionRequest) (resp *pb.VersionResponse, err error) {
	ctx, done := beginRPC(ctx, "Version")
	defer done(&err)
	_ = request
	_ = ctx
	return &pb.VersionResponse{
		Version:   version.Version,
		Commit:    version.Commit,
		BuildTime: version.BuildTime,
	}, nil
}

// StartIndex registers a new indexing request with the daemon.
func (server *GRPCServer) StartIndex(ctx context.Context, request *pb.StartIndexRequest) (resp *pb.StartIndexResponse, err error) {
	ctx, done := beginRPC(ctx, "StartIndex")
	defer done(&err)
	job, codebase, deduplicated, callErr := server.manager.StartIndex(ctx, request.GetPath(), pbClient(request.GetClient()), pbconv.FromStartIndexConfig(request), request.GetForce())
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(request.GetPath(), callErr)))
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
func (server *GRPCServer) ClearIndex(ctx context.Context, request *pb.ClearIndexRequest) (resp *pb.ClearIndexResponse, err error) {
	ctx, done := beginRPC(ctx, "ClearIndex")
	defer done(&err)
	codebase, callErr := server.manager.ClearIndex(ctx, request.GetPath(), pbClient(request.GetClient()))
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(request.GetPath(), callErr)))
	}
	indexedCount, indexingCount := countCodebaseStates(server.manager.ListIndexes(ctx))
	return &pb.ClearIndexResponse{
		CodebaseId:  codebase.ID,
		Cleared:     true,
		DisplayText: renderClearIndex(codebase, indexedCount, indexingCount),
	}, nil
}

// CancelJob cancels a tracked daemon job.
func (server *GRPCServer) CancelJob(ctx context.Context, request *pb.CancelJobRequest) (resp *pb.CancelJobResponse, err error) {
	ctx, done := beginRPC(ctx, "CancelJob")
	defer done(&err)
	_ = ctx
	job, callErr := server.manager.CancelJob(request.GetJobId())
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, adapterr.NewJobNotFound(request.GetJobId())))
	}
	return &pb.CancelJobResponse{
		JobId:       job.ID,
		Cancelled:   job.State == "cancelled",
		DisplayText: renderCancelJob(job),
	}, nil
}

// SyncIndex registers a sync request against an existing codebase.
func (server *GRPCServer) SyncIndex(ctx context.Context, request *pb.SyncIndexRequest) (resp *pb.SyncIndexResponse, err error) {
	ctx, done := beginRPC(ctx, "SyncIndex")
	defer done(&err)
	job, codebase, deduplicated, callErr := server.manager.SyncIndex(ctx, request.GetPath(), pbClient(request.GetClient()))
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(request.GetPath(), callErr)))
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
func (server *GRPCServer) GetIndex(ctx context.Context, request *pb.GetIndexRequest) (resp *pb.GetIndexResponse, err error) {
	ctx, done := beginRPC(ctx, "GetIndex")
	defer done(&err)
	codebase, activeJob, found, callErr := server.manager.GetIndex(ctx, request.GetPath())
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(request.GetPath(), callErr)))
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
func (server *GRPCServer) ListIndexes(ctx context.Context, request *pb.ListIndexesRequest) (resp *pb.ListIndexesResponse, err error) {
	ctx, done := beginRPC(ctx, "ListIndexes")
	defer done(&err)
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
func (server *GRPCServer) GetJob(ctx context.Context, request *pb.GetJobRequest) (resp *pb.GetJobResponse, err error) {
	ctx, done := beginRPC(ctx, "GetJob")
	defer done(&err)
	_ = ctx
	job, found := server.manager.GetJob(request.GetJobId())
	if !found {
		return nil, status.Error(adapterr.Respond(ctx, adapterr.NewJobNotFound(request.GetJobId())))
	}
	return &pb.GetJobResponse{
		Job:         pbconv.ToJob(job),
		DisplayText: renderGetJob(&job),
	}, nil
}

// ListJobs returns all tracked jobs, optionally filtered by codebase id.
func (server *GRPCServer) ListJobs(ctx context.Context, request *pb.ListJobsRequest) (resp *pb.ListJobsResponse, err error) {
	ctx, done := beginRPC(ctx, "ListJobs")
	defer done(&err)
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
func (server *GRPCServer) WatchJobs(request *pb.WatchJobsRequest, stream pb.ClaudeContextDaemonService_WatchJobsServer) (err error) {
	ctx, done := beginRPC(stream.Context(), "WatchJobs")
	defer done(&err)
	for _, jobID := range request.GetJobIds() {
		job, found := server.manager.GetJob(jobID)
		if !found {
			continue
		}
		if sendErr := stream.Send(&pb.WatchJobsResponse{Job: pbconv.ToJob(job)}); sendErr != nil {
			slog.ErrorContext(ctx, "send watch jobs event failed", "job_id", jobID, "err", sendErr)
			return fmt.Errorf("send watch jobs event for %s: %w", jobID, sendErr)
		}
	}
	return nil
}

// SearchCode is the future search RPC surface for semantic lookups.
func (server *GRPCServer) SearchCode(ctx context.Context, request *pb.SearchCodeRequest) (resp *pb.SearchCodeResponse, err error) {
	ctx, done := beginRPC(ctx, "SearchCode")
	defer done(&err)
	outcome, callErr := server.manager.SearchCode(ctx, request.GetPath(), request.GetQuery(), request.GetLimit(), request.GetExtensionFilter())
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(request.GetPath(), callErr)))
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
func (server *GRPCServer) Doctor(ctx context.Context, request *pb.DoctorRequest) (resp *pb.DoctorResponse, err error) {
	ctx, done := beginRPC(ctx, "Doctor")
	defer done(&err)
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
func (server *GRPCServer) Shutdown(ctx context.Context, request *pb.ShutdownRequest) (resp *pb.ShutdownResponse, err error) {
	ctx, done := beginRPC(ctx, "Shutdown")
	defer done(&err)
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
