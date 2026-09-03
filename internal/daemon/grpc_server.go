package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"goodkind.io/gklog/correlation"
	"goodkind.io/gklog/version"
	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/adapterr"
	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/pbconv"
	render "goodkind.io/lm-semantic-search/internal/render"
	"goodkind.io/lm-semantic-search/internal/view"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// toDependencyHealth converts the daemon's cached shared-dependency health into
// its protobuf form for JSON consumers. Timestamps stay UTC on the wire and are
// omitted while zero, so a never-degraded record carries no since stamp.
//
// last_healthy_at carries the last time the dependency named by mode answered,
// the same value the human banner shows, so the two surfaces cannot disagree
// about the dependency they are both describing. A dependency that has not
// answered in this daemon's lifetime is omitted rather than filled in from the
// other dependency's evidence. A healthy record names no single dependency, so
// it carries whichever of the two answered most recently.
func toDependencyHealth(health dependencyHealth) *pb.DependencyHealth {
	result := &pb.DependencyHealth{
		Degraded:      health.Degraded(),
		Mode:          string(health.Mode),
		Since:         nil,
		LastHealthyAt: nil,
	}
	if !health.Since.IsZero() {
		result.Since = timestamppb.New(health.Since)
	}
	if reachableAt := health.lastReachableAt(); !reachableAt.IsZero() {
		result.LastHealthyAt = timestamppb.New(reachableAt)
	}
	return result
}

// appendCorrelationRef prefixes one compact diagnostics line to a display
// text so every successful response starts with a greppable correlation
// header. Extras are key/value pairs for ids the trace context does not
// already carry, such as codebase_id and job_id.
func appendCorrelationRef(displayText string, ctx context.Context, extras ...string) string {
	corr := correlation.FromContext(ctx)
	line := correlation.HeaderLine(corr, extras...)
	if line == "" {
		return displayText
	}
	if strings.TrimSpace(displayText) == "" {
		return line
	}
	return line + "\n" + displayText
}

// envelopeText composes the human-facing display text for a read surface as the
// shared envelope: the dependency-health banner (only when a shared dependency is
// degraded), then the correlation header, then the body, joined with single
// newlines. It is the one place the banner is prepended, so every surface shows
// exactly one banner and the body renderers never carry it. The caller passes the
// health snapshot it already read so the banner and the body agree.
func (server *GRPCServer) envelopeText(ctx context.Context, health dependencyHealth, body string, extras ...string) string {
	withHeader := appendCorrelationRef(body, ctx, extras...)
	banner := render.HealthBanner(resolveBannerView(health, server.manager.config))
	if banner == "" {
		return withHeader
	}
	if strings.TrimSpace(withHeader) == "" {
		return banner
	}
	return banner + "\n" + withHeader
}

// jobIDOf returns the id of job or "" when job is nil, so callers can fold
// optional job ids into appendCorrelationRef without nil checks.
func jobIDOf(job *model.Job) string {
	if job == nil {
		return ""
	}
	return job.ID
}

// codebaseIDOf returns the id of codebase or "" when found is false.
func codebaseIDOf(found bool, codebase model.Codebase) string {
	if !found {
		return ""
	}
	return codebase.ID
}

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
	slog.LogAttrs(
		ctx, level, "daemon.rpc.completed",
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
	// Path-guard refusals: the reason must reach the client, not just the
	// daemon log, so the operator can correct the argument.
	case strings.Contains(text, "looks like a URI"):
		return adapterr.NewInvalidPath(text, err)
	case strings.Contains(text, "is relative"):
		return adapterr.NewInvalidPath(text, err)
	case strings.Contains(text, "refusing to index filesystem root"):
		return adapterr.NewInvalidPath(text, err)
	case strings.Contains(text, "is not a directory"):
		return adapterr.NewInvalidPath(text, err)
	case strings.Contains(text, "covers daemon state root"):
		return adapterr.NewInvalidPath(text, err)
	}
	return err
}

// requireNonEmpty reports an InvalidArgument error when a required request
// field is empty or whitespace, so the daemon rejects a missing path, query, or
// job id loudly instead of treating it as a normal not-found. argument names the
// field for the error envelope; pathLike picks the path-specific hint over the
// generic missing-argument hint.
func requireNonEmpty(ctx context.Context, value string, argument string, pathLike bool) error {
	if strings.TrimSpace(value) != "" {
		return nil
	}
	if pathLike {
		return status.Error(adapterr.Respond(ctx, adapterr.NewInvalidPath("codebase path is required", nil)))
	}
	return status.Error(adapterr.Respond(ctx, adapterr.NewMissingArgument(argument)))
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
		Version:   version.String(),
		Commit:    version.Commit,
		BuildTime: version.BuildTime,
	}, nil
}

// StartIndex registers a new indexing request with the daemon.
func (server *GRPCServer) StartIndex(ctx context.Context, request *pb.StartIndexRequest) (resp *pb.StartIndexResponse, err error) {
	ctx, done := beginRPC(ctx, "StartIndex")
	defer done(&err)
	policyPatch, policyErr := pbconv.FromSchedulingPolicyPatch(request.GetSchedulingPolicy())
	if policyErr != nil {
		return nil, status.Error(codes.InvalidArgument, policyErr.Error())
	}
	if argErr := requireNonEmpty(ctx, request.GetPath(), "absolutePath", true); argErr != nil {
		return nil, argErr
	}
	requestedPath, pathErr := resolveRequestPath(request.GetPath(), request.GetClient().GetCallerCwd())
	if pathErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, adapterr.NewInvalidPath(pathErr.Error(), pathErr)))
	}
	job, codebase, deduplicated, overlapsCodebaseID, callErr := server.manager.StartIndexWithPolicy(ctx, requestedPath, pbClient(request.GetClient()), pbconv.FromStartIndexConfig(request), request.GetForce(), pbconv.FromStartIndexBudget(request), policyPatch)
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(requestedPath, callErr)))
	}
	startIndexView := server.resolveStartIndexView(requestedPath, codebase, job, deduplicated, overlapsCodebaseID)
	health := server.manager.DependencyHealth()
	return &pb.StartIndexResponse{
		JobId:              job.ID,
		CodebaseId:         codebase.ID,
		State:              string(job.State),
		Deduplicated:       deduplicated,
		CanonicalPath:      codebase.CanonicalPath,
		OverlapsCodebaseId: overlapsCodebaseID,
		DisplayText:        server.envelopeText(ctx, health, render.StartIndex(startIndexView), "codebase_id", codebase.ID, "job_id", job.ID),
	}, nil
}

// startIndexMergeNote describes a containment relationship the StartIndex call
// resolved: a merge-up redirect when the requested path is covered by the
// returned (larger) codebase, or a merge-down reuse note when the requested
// path roots above already-indexed sub-folders. It returns an empty string for
// an ordinary, non-overlapping index.
func (server *GRPCServer) startIndexMergeNote(requestedPath string, codebase model.Codebase) string {
	if canonical, err := canonicalizePath(requestedPath); err == nil && codebase.CanonicalPath != "" {
		coveringRoot := filepath.Clean(codebase.CanonicalPath)
		if canonical != coveringRoot && pathCovers(coveringRoot, canonical) {
			return fmt.Sprintf("🔁 '%s' is covered by the larger index '%s'; syncing that index to include this subtree instead of building a separate one.", requestedPath, codebase.CanonicalPath)
		}
	}
	descendants := server.manager.IndexedDescendants(requestedPath)
	if len(descendants) == 0 {
		return ""
	}
	names := make([]string, 0, len(descendants))
	for _, child := range descendants {
		names = append(names, child.CanonicalPath)
	}
	return "🔗 Reusing already-indexed " + plural("sub-folder", len(names)) + ": " + strings.Join(names, ", ") + " (their embeddings are merged in, not re-embedded)."
}

// ClearIndex removes a tracked codebase from daemon state.
func (server *GRPCServer) ClearIndex(ctx context.Context, request *pb.ClearIndexRequest) (resp *pb.ClearIndexResponse, err error) {
	ctx, done := beginRPC(ctx, "ClearIndex")
	defer done(&err)
	if argErr := requireNonEmpty(ctx, request.GetPath(), "absolutePath", true); argErr != nil {
		return nil, argErr
	}
	requestedPath, pathErr := resolveRequestPath(request.GetPath(), request.GetClient().GetCallerCwd())
	if pathErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, adapterr.NewInvalidPath(pathErr.Error(), pathErr)))
	}
	codebase, callErr := server.manager.ClearIndex(ctx, requestedPath, pbClient(request.GetClient()))
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(requestedPath, callErr)))
	}
	ack := view.MutationAckView{
		Kind:            view.AckClear,
		Path:            codebase.CanonicalPath,
		JobID:           "",
		StateLabel:      "",
		AlreadyTerminal: false,
		Deduplicated:    false,
		CollectionID:    "",
		CollectionName:  "",
		CodebaseID:      codebase.ID,
		ConversationID:  "",
		DocumentCount:   0,
		NeededCount:     0,
		TotalCount:      0,
	}
	health := server.manager.DependencyHealth()
	return &pb.ClearIndexResponse{
		CodebaseId:  codebase.ID,
		Cleared:     true,
		DisplayText: server.envelopeText(ctx, health, render.MutationAck(ack), "codebase_id", codebase.ID),
	}, nil
}

// CancelJob cancels a tracked daemon job.
func (server *GRPCServer) CancelJob(ctx context.Context, request *pb.CancelJobRequest) (resp *pb.CancelJobResponse, err error) {
	ctx, done := beginRPC(ctx, "CancelJob")
	defer done(&err)
	if argErr := requireNonEmpty(ctx, request.GetJobId(), "job_id", false); argErr != nil {
		return nil, argErr
	}
	job, callErr := server.manager.CancelJob(ctx, request.GetJobId())
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, adapterr.NewJobNotFound(request.GetJobId())))
	}
	ack := resolveCancelJobAck(job)
	health := server.manager.DependencyHealth()
	return &pb.CancelJobResponse{
		JobId:       job.ID,
		Cancelled:   job.State == model.JobStateCancelled,
		DisplayText: server.envelopeText(ctx, health, render.MutationAck(ack), "job_id", job.ID, "codebase_id", job.CodebaseID),
	}, nil
}

// SyncIndex registers a sync request against an existing codebase.
func (server *GRPCServer) SyncIndex(ctx context.Context, request *pb.SyncIndexRequest) (resp *pb.SyncIndexResponse, err error) {
	ctx, done := beginRPC(ctx, "SyncIndex")
	defer done(&err)
	policyPatch, policyErr := pbconv.FromSchedulingPolicyPatch(request.GetSchedulingPolicy())
	if policyErr != nil {
		return nil, status.Error(codes.InvalidArgument, policyErr.Error())
	}
	if argErr := requireNonEmpty(ctx, request.GetPath(), "absolutePath", true); argErr != nil {
		return nil, argErr
	}
	requestedPath, pathErr := resolveRequestPath(request.GetPath(), request.GetClient().GetCallerCwd())
	if pathErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, adapterr.NewInvalidPath(pathErr.Error(), pathErr)))
	}
	job, codebase, deduplicated, callErr := server.manager.SyncIndexWithPolicy(ctx, requestedPath, pbClient(request.GetClient()), policyPatch)
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(requestedPath, callErr)))
	}
	operation := "sync"
	if deduplicated {
		operation = job.Operation
	}
	if operation == "sync" {
		job.Operation = operation
	}
	ack := view.MutationAckView{
		Kind:            view.AckSync,
		Path:            codebase.CanonicalPath,
		JobID:           job.ID,
		StateLabel:      "",
		AlreadyTerminal: false,
		Deduplicated:    deduplicated,
		CollectionID:    "",
		CollectionName:  "",
		CodebaseID:      codebase.ID,
		ConversationID:  "",
		DocumentCount:   0,
		NeededCount:     0,
		TotalCount:      0,
	}
	health := server.manager.DependencyHealth()
	return &pb.SyncIndexResponse{
		JobId:       job.ID,
		CodebaseId:  codebase.ID,
		State:       string(job.State),
		DisplayText: server.envelopeText(ctx, health, render.MutationAck(ack), "codebase_id", codebase.ID, "job_id", job.ID),
	}, nil
}

// applyDisplayTokens sets the display status plus its glyph and label on a
// protobuf codebase, so the three stay in sync from the daemon's single
// vocabulary. pbconv cannot import the daemon vocab, so the tokens are applied
// here at the boundary.
func applyDisplayTokens(pbCodebase *pb.Codebase, display displayStatus) {
	pbCodebase.DisplayStatus = string(display)
	pbCodebase.GlyphToken = glyphForDisplay(display)
	pbCodebase.StatusLabel = labelForDisplay(display)
}

// applyReuseForecast sets the discovered-worktree reuse forecast on the wire
// codebase so a list client can show that a pending build is cheap. It is zero
// for every non-discovered codebase. pbconv cannot compute it, so the boundary
// applies it here from the manager.
func applyReuseForecast(pbCodebase *pb.Codebase, reuseSiblingCount int32) {
	pbCodebase.ReuseSiblingCount = reuseSiblingCount
}

// applyJobDisplayTokens sets the resolved presentation fields on a protobuf job
// from the daemon's single status vocabulary, so a machine consumer reads the
// same folded status the human surfaces do instead of re-deriving it from the
// raw state and error. The raw state and error fields stay for debugging.
// pbconv cannot compute the degraded fold, so the tokens are applied here at the
// boundary, mirroring applyDisplayTokens for codebases.
func applyJobDisplayTokens(pbJob *pb.Job, job model.Job, pipelineDegraded bool, supersededByJobID string) {
	if pbJob == nil {
		return
	}
	surface := resolveJobSurface(job, pipelineDegraded, supersededByJobID)
	pbJob.DisplayState = surface.StateLabel
	pbJob.DisplayError = surface.ErrorLine
	pbJob.Superseded = surface.Superseded
	pbJob.SupersededByJobId = surface.SupersededByJobID
}

// toJobWithTokens converts a job to protobuf and applies its resolved display
// tokens, so every emit site produces a job carrying the authoritative status.
func toJobWithTokens(job model.Job, pipelineDegraded bool, supersededByJobID string) *pb.Job {
	pbJob := pbconv.ToJob(job)
	applyJobDisplayTokens(pbJob, job, pipelineDegraded, supersededByJobID)
	return pbJob
}

// toJobPointerWithTokens is the optional-job variant for the active-job fields,
// returning nil when there is no job so the response stays canonical.
func toJobPointerWithTokens(job *model.Job, pipelineDegraded bool, supersededByJobID string) *pb.Job {
	if job == nil {
		return nil
	}
	return toJobWithTokens(*job, pipelineDegraded, supersededByJobID)
}

// GetIndex resolves one tracked codebase whose canonical path covers the
// queried path.
func (server *GRPCServer) GetIndex(ctx context.Context, request *pb.GetIndexRequest) (resp *pb.GetIndexResponse, err error) {
	ctx, done := beginRPC(ctx, "GetIndex")
	defer done(&err)
	if argErr := requireNonEmpty(ctx, request.GetPath(), "absolutePath", true); argErr != nil {
		return nil, argErr
	}
	requestedPath, pathErr := resolveRequestPath(request.GetPath(), request.GetClient().GetCallerCwd())
	if pathErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, adapterr.NewInvalidPath(pathErr.Error(), pathErr)))
	}
	codebase, activeJob, found, classification, callErr := server.manager.GetIndex(ctx, requestedPath)
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(requestedPath, callErr)))
	}
	var indexedDescendants []model.Codebase
	if !found {
		indexedDescendants = server.manager.IndexedDescendants(requestedPath)
	}
	// Global dependency health owns the banner. Per-path readiness remains
	// separate, so one idle or loading collection never raises a store outage.
	searchableEligible := classification != nil && classification.Kind == model.PathClassificationInScopeIndexed
	server.manager.refreshDependencyHealth(ctx)
	health := server.manager.DependencyHealth()
	readiness, observedRows := server.manager.pathCollectionObservation(
		ctx,
		codebase.CanonicalPath,
		searchableEligible,
	)
	if activeJob != nil && observedRows != nil {
		activeJob.Progress.ChunksTotal = *observedRows
	}
	getIndexView := server.manager.resolveGetIndexView(ctx, requestedPath, found, codebasePointer(found, codebase), activeJob, health, readiness, observedRows, classification, indexedDescendants)
	getIndexView.PathErrorLine = mcpMissingPathError(request.GetClient().GetName(), requestedPath)
	// The daemon always answers Searchable with a definite true or false.
	//
	// The local variable exists so the address is taken from a plain bool. A
	// new(expr) form compiles under this module's go directive, but it reads as
	// an error to anyone who knows the older builtin, and it needs the newer
	// language version to build at all.
	searchable := computeSearchable(searchableEligible, health.Mode, readiness)
	response := &pb.GetIndexResponse{
		Tracked:             found,
		Classification:      pbconv.ToPathClassification(classification),
		DependencyHealth:    toDependencyHealth(health),
		Searchable:          &searchable,
		CollectionReadiness: string(readiness),
		DisplayText:         server.envelopeText(ctx, health, render.GetIndex(getIndexView), "codebase_id", codebaseIDOf(found, codebase), "job_id", jobIDOf(activeJob)),
	}
	if found {
		pbCodebase := pbconv.ToCodebase(codebase)
		display := computeDisplayStatus(codebase, activeJob, health.Mode, readiness)
		applyDisplayTokens(pbCodebase, display)
		if display == displayDiscovered {
			applyReuseForecast(pbCodebase, server.manager.worktreeReuseForecast(codebase))
		}
		response.Codebase = pbCodebase
		response.ActiveJob = toJobPointerWithTokens(activeJob, health.Degraded(), "")
	}
	return response, nil
}

// GetJob resolves one tracked job by id.
func (server *GRPCServer) GetJob(ctx context.Context, request *pb.GetJobRequest) (resp *pb.GetJobResponse, err error) {
	ctx, done := beginRPC(ctx, "GetJob")
	defer done(&err)
	if argErr := requireNonEmpty(ctx, request.GetJobId(), "job_id", false); argErr != nil {
		return nil, argErr
	}
	job, found := server.manager.GetJob(request.GetJobId())
	if !found {
		return nil, status.Error(adapterr.Respond(ctx, adapterr.NewJobNotFound(request.GetJobId())))
	}
	health := server.manager.DependencyHealth()
	successorID := server.manager.JobSuccessorID(job)
	entry := resolveJobEntry(job, health.Degraded(), successorID)
	return &pb.GetJobResponse{
		Job:              toJobWithTokens(job, health.Degraded(), successorID),
		DependencyHealth: toDependencyHealth(health),
		DisplayText:      server.envelopeText(ctx, health, render.GetJob(entry, true), "job_id", job.ID, "codebase_id", job.CodebaseID),
	}, nil
}

// ListJobs returns all tracked jobs, optionally filtered by codebase id.
func (server *GRPCServer) ListJobs(ctx context.Context, request *pb.ListJobsRequest) (resp *pb.ListJobsResponse, err error) {
	ctx, done := beginRPC(ctx, "ListJobs")
	defer done(&err)
	jobs := server.manager.ListJobs(request.GetCodebaseId())
	health := server.manager.DependencyHealth()
	successors := buildJobSuccessors(jobs)
	summary := resolveListSummary(jobs, health.Degraded())
	activeEntries := make([]view.JobEntryView, 0, len(jobs))
	terminalEntries := make([]view.JobEntryView, 0, len(jobs))
	response := &pb.ListJobsResponse{Jobs: make([]*pb.Job, 0, len(jobs))}
	for _, job := range jobs {
		response.Jobs = append(response.Jobs, toJobWithTokens(job, health.Degraded(), successors[job.ID]))
		entry := resolveJobEntry(job, health.Degraded(), successors[job.ID])
		if isTerminalJobState(job.State) {
			terminalEntries = append(terminalEntries, entry)
		} else {
			activeEntries = append(activeEntries, entry)
		}
	}
	response.DependencyHealth = toDependencyHealth(health)
	response.DisplayText = server.envelopeText(ctx, health, render.ListJobs(summary, activeEntries, terminalEntries), "codebase_id", request.GetCodebaseId())
	return response, nil
}

// WatchJobs streams the latest visible state for requested jobs.
func (server *GRPCServer) WatchJobs(request *pb.WatchJobsRequest, stream pb.SemanticSearchDaemonService_WatchJobsServer) (err error) {
	ctx, done := beginRPC(stream.Context(), "WatchJobs")
	defer done(&err)
	degraded := server.manager.DependencyHealth().Degraded()
	for _, jobID := range request.GetJobIds() {
		job, found := server.manager.GetJob(jobID)
		if !found {
			continue
		}
		if sendErr := stream.Send(&pb.WatchJobsResponse{Job: toJobWithTokens(job, degraded, server.manager.JobSuccessorID(job))}); sendErr != nil {
			slog.ErrorContext(ctx, "send watch jobs event failed", "job_id", jobID, "err", sendErr)
			return status.Error(adapterr.Respond(ctx, adapterr.NewInternal("send watch jobs event for "+jobID, sendErr)))
		}
	}
	return nil
}

// SearchCode is the future search RPC surface for semantic lookups.
func (server *GRPCServer) SearchCode(ctx context.Context, request *pb.SearchCodeRequest) (resp *pb.SearchCodeResponse, err error) {
	ctx, done := beginRPC(ctx, "SearchCode")
	defer done(&err)
	if argErr := requireNonEmpty(ctx, request.GetPath(), "absolutePath", true); argErr != nil {
		return nil, argErr
	}
	if argErr := requireNonEmpty(ctx, request.GetQuery(), "query", false); argErr != nil {
		return nil, argErr
	}
	requestedPath, pathErr := resolveRequestPath(request.GetPath(), request.GetClient().GetCallerCwd())
	if pathErr != nil {
		return nil, adapterr.RespondGRPC(ctx, adapterr.NewInvalidPath(pathErr.Error(), pathErr))
	}
	outcome, callErr := server.manager.SearchCode(ctx, requestedPath, request.GetQuery(), request.GetLimit(), request.GetExtensionFilter())
	if callErr != nil {
		return nil, adapterr.RespondGRPC(ctx, classifyManagerError(requestedPath, callErr))
	}
	server.manager.fillLiveChunkTotal(ctx, outcome.Codebase, outcome.ActiveJob)
	health := server.manager.DependencyHealth()
	inFlightStatus, inFlightTemplateName, inFlightBackgroundSync := resolveSearchStatusView(outcome.Codebase, outcome.ActiveJob, health, server.manager.graphIndexing(outcome.Codebase.ID))
	searchView := view.SearchView{
		RequestedPath:          requestedPath,
		Query:                  request.GetQuery(),
		CodebaseName:           filepath.Base(outcome.Codebase.CanonicalPath),
		CodebasePath:           outcome.Codebase.CanonicalPath,
		Results:                resolveSearchResults(outcome.Results),
		StateNote:              outcome.StateNote,
		InFlight:               outcome.ActiveJob != nil,
		InFlightStatus:         inFlightStatus,
		InFlightTemplateName:   inFlightTemplateName,
		InFlightPercent:        0,
		InFlightBackgroundSync: inFlightBackgroundSync,
		Degraded:               health.Degraded(),
		ResolutionLines:        pathResolutionLines(requestedPath),
	}
	response := &pb.SearchCodeResponse{
		Results:          make([]*pb.SearchResult, 0, len(outcome.Results)),
		Codebase:         pbconv.ToCodebase(outcome.Codebase),
		ActiveJob:        toJobPointerWithTokens(outcome.ActiveJob, health.Degraded(), ""),
		DependencyHealth: toDependencyHealth(health),
		DisplayText:      server.envelopeText(ctx, health, render.Search(searchView), "codebase_id", outcome.Codebase.ID, "job_id", jobIDOf(outcome.ActiveJob)),
	}
	for _, result := range outcome.Results {
		response.Results = append(response.Results, &pb.SearchResult{
			RelativePath: result.RelativePath,
			StartLine:    result.StartLine,
			EndLine:      result.EndLine,
			Language:     result.Language,
			Score:        result.Score,
			Content:      result.Content,
		})
	}
	return response, nil
}

// RegisterConversationCollection reserves the conversation collection RPC surface.
func (server *GRPCServer) RegisterConversationCollection(ctx context.Context, request *pb.RegisterConversationCollectionRequest) (resp *pb.RegisterConversationCollectionResponse, err error) {
	ctx, done := beginRPC(ctx, "RegisterConversationCollection")
	defer done(&err)
	if argErr := requireNonEmpty(ctx, request.GetCollectionId(), "collection_id", false); argErr != nil {
		return nil, argErr
	}
	codebase, callErr := server.manager.RegisterConversationCollection(ctx, request.GetCollectionId())
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, callErr))
	}
	ack := view.MutationAckView{
		Kind:            view.AckRegisterConversation,
		Path:            "",
		JobID:           "",
		StateLabel:      "",
		AlreadyTerminal: false,
		Deduplicated:    false,
		CollectionID:    request.GetCollectionId(),
		CollectionName:  codebase.CollectionName,
		CodebaseID:      codebase.ID,
		ConversationID:  "",
		DocumentCount:   0,
		NeededCount:     0,
		TotalCount:      0,
	}
	health := server.manager.DependencyHealth()
	return &pb.RegisterConversationCollectionResponse{
		CodebaseId:     codebase.ID,
		CollectionName: codebase.CollectionName,
		DisplayText: server.envelopeText(
			ctx,
			health,
			render.MutationAck(ack),
			"codebase_id",
			codebase.ID,
		),
	}, nil
}

// SyncConversationManifest diffs clyde's full conversation manifest against the
// engine checkpoint and returns the conversation ids the engine needs.
func (server *GRPCServer) SyncConversationManifest(ctx context.Context, request *pb.SyncConversationManifestRequest) (resp *pb.SyncConversationManifestResponse, err error) {
	ctx, done := beginRPC(ctx, "SyncConversationManifest")
	defer done(&err)
	if argErr := requireNonEmpty(ctx, request.GetCollectionId(), "collection_id", false); argErr != nil {
		return nil, argErr
	}
	needed, callErr := server.manager.SyncConversationManifest(ctx, request.GetCollectionId(), pbConversationManifest(request.GetManifest()))
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(request.GetCollectionId(), callErr)))
	}
	ack := view.MutationAckView{
		Kind:            view.AckManifest,
		Path:            "",
		JobID:           "",
		StateLabel:      "",
		AlreadyTerminal: false,
		Deduplicated:    false,
		CollectionID:    request.GetCollectionId(),
		CollectionName:  "",
		CodebaseID:      request.GetCollectionId(),
		ConversationID:  "",
		DocumentCount:   0,
		NeededCount:     len(needed),
		TotalCount:      len(request.GetManifest()),
	}
	health := server.manager.DependencyHealth()
	return &pb.SyncConversationManifestResponse{
		NeededConversationIds: needed,
		DisplayText: server.envelopeText(
			ctx,
			health,
			render.MutationAck(ack),
			"codebase_id",
			request.GetCollectionId(),
		),
	}, nil
}

// DeleteConversation reserves the conversation deletion RPC surface.
func (server *GRPCServer) DeleteConversation(ctx context.Context, request *pb.DeleteConversationRequest) (resp *pb.DeleteConversationResponse, err error) {
	ctx, done := beginRPC(ctx, "DeleteConversation")
	defer done(&err)
	if argErr := requireNonEmpty(ctx, request.GetCollectionId(), "collection_id", false); argErr != nil {
		return nil, argErr
	}
	if argErr := requireNonEmpty(ctx, request.GetConversationId(), "conversation_id", false); argErr != nil {
		return nil, argErr
	}
	job, callErr := server.manager.deleteConversation(
		ctx,
		request.GetCollectionId(),
		request.GetConversationId(),
		pbClient(request.GetClient()),
	)
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(request.GetCollectionId(), callErr)))
	}
	ack := view.MutationAckView{
		Kind:            view.AckDeleteConversation,
		Path:            "",
		JobID:           job.ID,
		StateLabel:      "",
		AlreadyTerminal: false,
		Deduplicated:    false,
		CollectionID:    request.GetCollectionId(),
		CollectionName:  "",
		CodebaseID:      job.CodebaseID,
		ConversationID:  request.GetConversationId(),
		DocumentCount:   0,
		NeededCount:     0,
		TotalCount:      0,
	}
	health := server.manager.DependencyHealth()
	return &pb.DeleteConversationResponse{
		JobId: job.ID,
		DisplayText: server.envelopeText(
			ctx,
			health,
			render.MutationAck(ack),
			"codebase_id",
			job.CodebaseID,
			"job_id",
			job.ID,
		),
	}, nil
}

// SearchConversations reserves the conversation search RPC surface.
func (server *GRPCServer) SearchConversations(ctx context.Context, request *pb.SearchConversationsRequest) (resp *pb.SearchConversationsResponse, err error) {
	ctx, done := beginRPC(ctx, "SearchConversations")
	defer done(&err)
	if argErr := requireNonEmpty(ctx, request.GetCollectionId(), "collection_id", false); argErr != nil {
		return nil, argErr
	}
	if argErr := requireNonEmpty(ctx, request.GetQuery(), "query", false); argErr != nil {
		return nil, argErr
	}
	results, callErr := server.manager.SearchConversations(ctx, request.GetCollectionId(), request.GetQuery(), request.GetLimit(), pbConversationSearchFilter(request.GetFilter()), request.GetPerConversationLimit())
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(request.GetCollectionId(), callErr)))
	}
	health := server.manager.DependencyHealth()
	conversationView := view.ConversationSearchView{
		CollectionID: request.GetCollectionId(),
		Query:        request.GetQuery(),
		Results:      resolveConversationSearchResults(results),
		StateNote:    "",
	}
	response := &pb.SearchConversationsResponse{
		Results:          conversationSearchResults(results),
		DependencyHealth: toDependencyHealth(health),
		DisplayText:      server.envelopeText(ctx, health, render.ConversationSearch(conversationView)),
	}
	return response, nil
}

// SearchWithinConversation retrieves one conversation's matching rows plus the
// fingerprint the engine has embedded for it, so the caller can tell complete
// results from ones that trail the live transcript.
func (server *GRPCServer) SearchWithinConversation(ctx context.Context, request *pb.SearchWithinConversationRequest) (resp *pb.SearchWithinConversationResponse, err error) {
	ctx, done := beginRPC(ctx, "SearchWithinConversation")
	defer done(&err)
	if argErr := requireNonEmpty(ctx, request.GetCollectionId(), "collection_id", false); argErr != nil {
		return nil, argErr
	}
	if argErr := requireNonEmpty(ctx, request.GetConversationId(), "conversation_id", false); argErr != nil {
		return nil, argErr
	}
	if argErr := requireNonEmpty(ctx, request.GetQuery(), "query", false); argErr != nil {
		return nil, argErr
	}
	results, indexedFingerprint, callErr := server.manager.SearchWithinConversation(ctx, request.GetCollectionId(), request.GetConversationId(), request.GetQuery(), request.GetLimit(), pbConversationSearchFilter(request.GetFilter()))
	if callErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, classifyManagerError(request.GetCollectionId(), callErr)))
	}
	health := server.manager.DependencyHealth()
	conversationView := view.ConversationSearchView{
		CollectionID: request.GetCollectionId(),
		Query:        request.GetQuery(),
		Results:      resolveConversationSearchResults(results),
		StateNote:    "",
	}
	return &pb.SearchWithinConversationResponse{
		Results:            conversationSearchResults(results),
		IndexedFingerprint: indexedFingerprint,
		DependencyHealth:   toDependencyHealth(health),
		DisplayText:        server.envelopeText(ctx, health, render.ConversationSearch(conversationView)),
	}, nil
}

// conversationSearchResults converts retrieved chunks to wire results,
// carrying the retrieval score.
func conversationSearchResults(results []model.StoredChunk) []*pb.ConversationSearchResult {
	out := make([]*pb.ConversationSearchResult, 0, len(results))
	for _, result := range results {
		out = append(out, &pb.ConversationSearchResult{
			ConversationId:       result.ConversationID,
			ParentConversationId: result.ParentConversationID,
			MessageIndex:         result.MessageIndex,
			Role:                 result.Role,
			TimestampUnix:        result.TimestampUnix,
			Score:                result.Score,
			Content:              result.Content,
			LoadRules:            result.LoadRules,
		})
	}
	return out
}

// pbConversationSearchFilter converts the wire filter to the manager's typed
// filter. A nil filter matches everything.
func pbConversationSearchFilter(filter *pb.ConversationSearchFilter) conversationSearchFilter {
	if filter == nil {
		return conversationSearchFilter{
			Providers:            nil,
			WorkspaceRoots:       nil,
			Roles:                nil,
			FromUnix:             0,
			UntilUnix:            0,
			ConversationIDs:      nil,
			ParentConversationID: "",
			MinScore:             0,
			MessageIndexFrom:     0,
			MessageIndexUntil:    0,
			Archived:             nil,
		}
	}
	return conversationSearchFilter{
		Providers:            filter.GetProviders(),
		WorkspaceRoots:       filter.GetWorkspaceRoots(),
		Roles:                filter.GetRoles(),
		FromUnix:             filter.GetFromUnix(),
		UntilUnix:            filter.GetUntilUnix(),
		ConversationIDs:      filter.GetConversationIds(),
		ParentConversationID: filter.GetParentConversationId(),
		MinScore:             filter.GetMinScore(),
		MessageIndexFrom:     filter.GetMessageIndexFrom(),
		MessageIndexUntil:    filter.GetMessageIndexUntil(),
		Archived:             cloneOptionalBool(filter.Archived),
	}
}

// cloneOptionalBool copies a proto3 optional bool into a freshly allocated
// pointer so the manager filter does not alias the wire message's memory. A nil
// input (the field was unset) stays nil, meaning no archived filter.
func cloneOptionalBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

// Doctor reports daemon-local diagnostics.
func (server *GRPCServer) Doctor(ctx context.Context, request *pb.DoctorRequest) (resp *pb.DoctorResponse, err error) {
	ctx, done := beginRPC(ctx, "Doctor")
	defer done(&err)
	_ = request
	// Doctor exists to answer whether the system is healthy right now, so it
	// probes rather than reporting whatever the last caller happened to record.
	server.manager.refreshDependencyHealth(ctx)
	diagnostics := server.manager.Doctor(ctx)
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
	health := server.manager.DependencyHealth()
	doctorView := view.DoctorView{
		Diagnostics: diagnostics,
		Dropped:     server.manager.DroppedCodebases(),
		Quarantined: server.manager.quarantinedCodebases(),
	}
	response.DisplayText = server.envelopeText(ctx, health, render.Doctor(doctorView))
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

func pbConversationDocuments(documents []*pb.ConversationDocument) []model.ConversationDocument {
	result := make([]model.ConversationDocument, 0, len(documents))
	for _, document := range documents {
		if document == nil {
			continue
		}
		result = append(result, model.ConversationDocument{
			ConversationID:       document.GetConversationId(),
			ParentConversationID: document.GetParentConversationId(),
			MessageIndex:         document.GetMessageIndex(),
			Role:                 document.GetRole(),
			TimestampUnix:        document.GetTimestampUnix(),
			Text:                 document.GetText(),
			Tools:                pbConversationToolCalls(document.GetTools()),
			Thinking:             document.GetThinking(),
			WorkspaceRoot:        document.GetWorkspaceRoot(),
			Archived:             document.GetArchived(),
			LoadRules:            document.GetLoadRules(),
		})
	}
	return result
}

func pbConversationToolCalls(tools []*pb.ConversationToolCall) []model.ConversationToolCall {
	if len(tools) == 0 {
		return nil
	}
	result := make([]model.ConversationToolCall, 0, len(tools))
	for _, tool := range tools {
		if tool == nil {
			continue
		}
		result = append(result, model.ConversationToolCall{
			Name:     tool.GetName(),
			Display:  tool.GetDisplay(),
			LangHint: tool.GetLangHint(),
			Output:   tool.GetOutput(),
			IsError:  tool.GetIsError(),
		})
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// pbConversationManifest converts the wire fingerprint list to the id ->
// fingerprint map the manager diffs against its checkpoint. A nil or empty list
// returns nil so the manager derives the manifest from the delivered documents.
func pbConversationManifest(fingerprints []*pb.ConversationFingerprint) map[string]string {
	if len(fingerprints) == 0 {
		return nil
	}
	manifest := make(map[string]string, len(fingerprints))
	for _, fingerprint := range fingerprints {
		if fingerprint == nil {
			continue
		}
		conversationID := strings.TrimSpace(fingerprint.GetConversationId())
		if conversationID == "" {
			continue
		}
		manifest[conversationID] = fingerprint.GetFingerprint()
	}
	return manifest
}

func codebasePointer(found bool, codebase model.Codebase) *model.Codebase {
	if !found {
		return nil
	}
	return &codebase
}
