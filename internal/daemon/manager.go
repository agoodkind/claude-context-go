// Package daemon owns persisted daemon state and request coordination.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"sort"
	"sync"
	"time"

	"goodkind.io/claude-context-go/internal/clock"
	"goodkind.io/claude-context-go/internal/config"
	"goodkind.io/claude-context-go/internal/discovery"
	"goodkind.io/claude-context-go/internal/indexer"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/semantic"
	"goodkind.io/claude-context-go/internal/spans"
	"goodkind.io/claude-context-go/internal/store"
	"goodkind.io/gklog/correlation"
)

// jobOperation tags one daemon job so runJob can route it to the right
// execution path. The model.Job.Operation field is a plain string for wire
// compatibility, but the daemon's internal switch uses this named type so
// staticcheck can verify the dispatch covers every case.
type jobOperation string

const (
	// jobOperationIndex runs a full Replace against an empty or
	// previously-cleared collection.
	jobOperationIndex jobOperation = "index"
	// jobOperationSync runs an incremental delta against the existing
	// merkle snapshot and falls back to full Replace when no snapshot exists.
	jobOperationSync jobOperation = "sync"
	// jobOperationStreamingReindex re-walks the entire codebase and
	// replaces chunks file by file through semantic.Reindex, so the existing
	// Milvus collection stays searchable across the upgrade.
	jobOperationStreamingReindex jobOperation = "streaming_reindex"
)

// CodebaseLifecycleHook is the watcher-side interface the manager calls so
// new codebases start receiving filesystem events without a restart. The
// hook is plugged in via SetCodebaseLifecycleHook; until then the manager
// is a no-op for these callbacks.
type CodebaseLifecycleHook interface {
	AddCodebase(ctx context.Context, codebase model.Codebase)
	RemoveCodebase(ctx context.Context, codebaseID string)
}

// Manager coordinates persisted codebase and job state for the daemon.
type Manager struct {
	config         config.Config
	mu             sync.Mutex
	codebases      map[string]model.Codebase
	jobs           map[string]model.Job
	cancels        map[string]context.CancelFunc
	done           map[string]chan struct{}
	runner         indexingRunner
	semantic       *semantic.Service
	lifecycleHook  CodebaseLifecycleHook
	lifecycleMutex sync.Mutex
}

// SearchOutcome carries search results plus current indexing context.
type SearchOutcome struct {
	Codebase  model.Codebase
	ActiveJob *model.Job
	Results   []model.StoredChunk
}

type indexingRunner interface {
	Index(context.Context, string, model.IndexConfig, func(indexer.Progress)) (indexer.Result, error)
	IndexFiles(context.Context, string, []string, model.IndexConfig, func(indexer.Progress)) (indexer.Result, error)
	IndexOne(context.Context, string, string, model.IndexConfig) (indexer.OneFileResult, error)
}

// NewManager loads persisted daemon state from disk.
func NewManager(ctx context.Context, cfg config.Config) (*Manager, error) {
	manager := &Manager{
		config:         cfg,
		mu:             sync.Mutex{},
		codebases:      map[string]model.Codebase{},
		jobs:           map[string]model.Job{},
		cancels:        map[string]context.CancelFunc{},
		done:           map[string]chan struct{}{},
		runner:         indexer.NewRunner(),
		semantic:       nil,
		lifecycleHook:  nil,
		lifecycleMutex: sync.Mutex{},
	}
	semanticService, err := semantic.NewService(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create semantic service: %w", err)
	}
	manager.semantic = semanticService
	if err := manager.load(ctx); err != nil {
		slog.ErrorContext(ctx, "load daemon state failed", "state_root", cfg.StateRoot, "err", err)
		return nil, fmt.Errorf("load daemon state: %w", err)
	}
	return manager, nil
}

func (manager *Manager) load(ctx context.Context) error {
	registry, err := store.ReadRegistry(manager.config.RegistryPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.ErrorContext(ctx, "read registry failed", "path", manager.config.RegistryPath, "err", err)
		return fmt.Errorf("read registry: %w", err)
	}
	for _, codebase := range registry.Codebases {
		manager.codebases[codebase.ID] = codebase
	}

	jobs, err := store.ReadJobEvents(manager.config.JobsPath)
	if err != nil {
		slog.ErrorContext(ctx, "read jobs failed", "path", manager.config.JobsPath, "err", err)
		return fmt.Errorf("read jobs: %w", err)
	}
	maps.Copy(manager.jobs, jobs)
	manager.reconcileJournalOnStartLocked()
	return nil
}

// reconcileJournalOnStartLocked sanitizes the job journal after the previous
// daemon process exited. Any queued, running, or cancelling job becomes
// cancelled in the journal because its goroutine is gone. Codebase records
// keep Status=Indexing when they were mid-flight so ResumeOrphanedJobs can
// pick them back up with a fresh streaming reindex; the registry already
// holds the canonical path and effective config that resume needs.
func (manager *Manager) reconcileJournalOnStartLocked() {
	now := clock.Now()
	for id, job := range manager.jobs {
		switch job.State {
		case model.JobStateQueued, model.JobStateRunning, model.JobStateCancelling:
		case model.JobStateCompleted, model.JobStateFailed, model.JobStateCancelled:
			continue
		default:
			continue
		}
		job.State = model.JobStateCancelled
		job.UpdatedAt = now
		completedAt := now
		job.CompletedAt = &completedAt
		job.Progress.Phase = "cancelled"
		job.Progress.LastEventAt = now
		job.Progress.HeartbeatAt = now
		manager.jobs[id] = job
		if err := store.AppendJobEvent(manager.config.JobsPath, model.JobEvent{
			Event:      "job_orphan_recovered",
			OccurredAt: now,
			Job:        job,
		}); err != nil {
			slog.Error("append orphan recovery event failed", "job_id", id, "err", err)
		}
		slog.Warn("orphan job sanitized in journal after restart", "job_id", id, "codebase_id", job.CodebaseID)
	}
}

// ResumeOrphanedJobs re-queues a streaming reindex for every codebase whose
// previous indexing job was still running when the daemon exited. The
// streaming path's per-file delete-then-upsert keeps the run idempotent, so
// resuming is safe even though no mid-job state is persisted. Call this
// once after NewManager returns and before the daemon advertises ready.
func (manager *Manager) ResumeOrphanedJobs(ctx context.Context) {
	manager.mu.Lock()
	type resumePlan struct {
		canonicalPath string
		config        model.IndexConfig
		codebaseID    string
	}
	plans := make([]resumePlan, 0)
	for _, codebase := range manager.codebases {
		if codebase.Status != model.CodebaseStatusIndexing {
			continue
		}
		plans = append(plans, resumePlan{
			canonicalPath: codebase.CanonicalPath,
			config:        codebase.EffectiveConfig,
			codebaseID:    codebase.ID,
		})
	}
	manager.mu.Unlock()

	if len(plans) > 0 {
		paths := make([]string, 0, len(plans))
		for _, plan := range plans {
			paths = append(paths, plan.canonicalPath)
		}
		slog.InfoContext(ctx, "resuming orphaned indexing jobs", "count", len(plans), "paths", paths)
	}
	for _, plan := range plans {
		client := model.ClientInfo{Name: "daemon-resume", PID: 0}
		_, _, _, _, err := manager.StartIndex(ctx, plan.canonicalPath, client, plan.config, false)
		if err != nil {
			slog.ErrorContext(ctx, "resume orphaned job failed", "codebase_id", plan.codebaseID, "path", plan.canonicalPath, "err", err)
		}
	}
}

func (manager *Manager) saveLocked() error {
	codebases := make([]model.Codebase, 0, len(manager.codebases))
	for _, codebase := range manager.codebases {
		codebases = append(codebases, codebase)
	}
	sort.Slice(codebases, func(i int, j int) bool {
		return codebases[i].CanonicalPath < codebases[j].CanonicalPath
	})
	registry := model.RegistryFile{
		Codebases: codebases,
		UpdatedAt: clock.Now(),
	}
	if err := store.WriteRegistry(manager.config.RegistryPath, registry); err != nil {
		slog.Error("write registry failed", "path", manager.config.RegistryPath, "err", err)
		return fmt.Errorf("write registry %s: %w", manager.config.RegistryPath, err)
	}
	return nil
}

func (manager *Manager) appendJobLocked(event string, job model.Job) error {
	manager.jobs[job.ID] = job
	jobEvent := model.JobEvent{
		Event:      event,
		OccurredAt: clock.Now(),
		Job:        job,
	}
	if err := store.AppendJobEvent(manager.config.JobsPath, jobEvent); err != nil {
		slog.Error("append jobs journal failed", "path", manager.config.JobsPath, "err", err)
		return fmt.Errorf("append jobs journal %s: %w", manager.config.JobsPath, err)
	}
	return nil
}

// Version returns daemon runtime path details.
func (manager *Manager) Version() map[string]string {
	return map[string]string{
		"state_root":  manager.config.StateRoot,
		"socket_path": manager.config.SocketPath,
	}
}

func (manager *Manager) reconcileIndexedCodebases(ctx context.Context) {
	if manager.semantic == nil || !manager.semantic.Available() {
		return
	}

	collections, err := manager.semantic.ListCollections(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "reconcile indexed codebases failed", "err", err)
		return
	}

	collectionSet := make(map[string]struct{}, len(collections))
	for _, collectionName := range collections {
		collectionSet[collectionName] = struct{}{}
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	changed := false
	for codebaseID, codebase := range manager.codebases {
		if codebase.Status != model.CodebaseStatusIndexed {
			continue
		}
		expectedCollectionName := codebase.CollectionName
		if expectedCollectionName == "" && manager.semantic != nil {
			expectedCollectionName = manager.semantic.CollectionName(codebase.CanonicalPath)
			codebase.CollectionName = expectedCollectionName
			manager.codebases[codebaseID] = codebase
			changed = true
		}
		if expectedCollectionName == "" {
			continue
		}
		if _, found := collectionSet[expectedCollectionName]; found {
			continue
		}
		delete(manager.codebases, codebaseID)
		changed = true
	}
	if changed {
		if err := manager.saveLocked(); err != nil {
			slog.ErrorContext(ctx, "persist reconciled codebases failed", "err", err)
		}
	}
}

func newCodebaseRecord(canonicalPath string) model.Codebase {
	return model.Codebase{
		ID:                newID("cb"),
		CanonicalPath:     canonicalPath,
		Status:            model.CodebaseStatusNotIndexed,
		ActiveJobID:       "",
		LastSuccessfulRun: nil,
		LastFailedRun:     nil,
		EffectiveConfig: model.IndexConfig{
			SplitterType:       "",
			SplitterChunkSize:  0,
			SplitterOverlap:    0,
			Extensions:         nil,
			IgnorePatterns:     nil,
			IgnoreDigest:       "",
			EmbeddingProvider:  "",
			EmbeddingModel:     "",
			EmbeddingDimension: 0,
			VectorBackend:      "",
			Hybrid:             false,
		},
		CollectionName:        "",
		LegacyCollectionNames: nil,
		MerkleSnapshotPath:    "",
		InodeTrackingDisabled: false,
		ResolvedIgnoreRules:   discovery.IgnoreRules{Nodes: nil},
		UpdatedAt:             clock.Now(),
	}
}

func newQueuedJob(
	codebaseID string,
	requestedPath string,
	canonicalPath string,
	client model.ClientInfo,
	operation string,
	indexConfig model.IndexConfig,
	now time.Time,
) model.Job {
	return model.Job{
		ID:            newID("job"),
		CodebaseID:    codebaseID,
		RequestedPath: requestedPath,
		CanonicalPath: canonicalPath,
		Client:        client,
		Operation:     operation,
		State:         model.JobStateQueued,
		Progress: model.Progress{
			Phase:                     "queued",
			PhasePercent:              0,
			OverallPercent:            0,
			FilesTotal:                0,
			FilesProcessed:            0,
			ChunksGenerated:           0,
			EmbeddingBatchesTotal:     0,
			EmbeddingBatchesCompleted: 0,
			CollectionRowsWritten:     0,
			LastEventAt:               now,
			HeartbeatAt:               now,
		},
		Config:      indexConfig,
		StartedAt:   now,
		UpdatedAt:   now,
		CompletedAt: nil,
		Error:       nil,
	}
}

// startIndexDecision captures one StartIndex call's resolved codebase plus
// the routing decision derived from the current registry state.
type startIndexDecision struct {
	codebase         model.Codebase
	activeJob        model.Job
	dedup            bool
	streamingReindex bool
	alreadyIndexed   bool
}

// decideStartIndexLocked resolves the codebase record and routing decision
// from the registry plus the caller-provided Milvus collection state. A
// registry miss with hasCollection=true produces an Indexed codebase that
// streams its next reindex into the existing collection. A Failed status
// always allows retry: streaming when the collection exists, full bootstrap
// otherwise. Caller must hold manager.mu.
func (manager *Manager) decideStartIndexLocked(canonicalPath string, indexConfig model.IndexConfig, force bool, hasCollection bool) (startIndexDecision, error) {
	var emptyJob model.Job
	codebase, found := manager.findCodebaseByExactRoot(canonicalPath)
	if !found {
		fresh := newCodebaseRecord(canonicalPath)
		if hasCollection {
			fresh.Status = model.CodebaseStatusIndexed
			return startIndexDecision{
				codebase:         fresh,
				activeJob:        emptyJob,
				dedup:            false,
				streamingReindex: true,
				alreadyIndexed:   false,
			}, nil
		}
		return startIndexDecision{
			codebase:         fresh,
			activeJob:        emptyJob,
			dedup:            false,
			streamingReindex: false,
			alreadyIndexed:   false,
		}, nil
	}
	activeJob, deduplicated, err := manager.activeJobLocked(codebase, canonicalPath, indexConfig)
	if err != nil {
		return startIndexDecision{}, err
	}
	if deduplicated {
		return startIndexDecision{
			codebase:         codebase,
			activeJob:        activeJob,
			dedup:            true,
			streamingReindex: false,
			alreadyIndexed:   false,
		}, nil
	}
	// Failed, Stale, or Indexing-without-an-active-job all allow a new
	// indexing pass. The Indexing case is the daemon-restart resume path:
	// the codebase was mid-flight when the previous process exited, so
	// the resumed run streams into the existing collection (or bootstraps
	// when Milvus is empty).
	switch codebase.Status {
	case model.CodebaseStatusFailed, model.CodebaseStatusStale, model.CodebaseStatusIndexing:
		return startIndexDecision{
			codebase:         codebase,
			activeJob:        emptyJob,
			dedup:            false,
			streamingReindex: hasCollection,
			alreadyIndexed:   false,
		}, nil
	case model.CodebaseStatusIndexed, model.CodebaseStatusNotIndexed:
	}
	indexed := codebase.Status == model.CodebaseStatusIndexed || hasCollection
	if !indexed {
		return startIndexDecision{
			codebase:         codebase,
			activeJob:        emptyJob,
			dedup:            false,
			streamingReindex: false,
			alreadyIndexed:   false,
		}, nil
	}
	// Matching config with force=false maps to a no-op "already indexed"
	// reply. Every other re-call streams into the existing collection so
	// search keeps working across the upgrade.
	if !force && codebase.EffectiveConfig.IgnoreDigest == indexConfig.IgnoreDigest {
		return startIndexDecision{
			codebase:         codebase,
			activeJob:        emptyJob,
			dedup:            false,
			streamingReindex: false,
			alreadyIndexed:   true,
		}, nil
	}
	return startIndexDecision{
		codebase:         codebase,
		activeJob:        emptyJob,
		dedup:            false,
		streamingReindex: true,
		alreadyIndexed:   false,
	}, nil
}

// StartIndex registers a new indexing job or deduplicates an existing one.
//
// Returns the queued or in-flight job, the resolved codebase record, a
// deduplicated flag (true when the call matched an in-flight job), and the
// id of any existing codebase whose canonical path strictly prefix-covers
// the new registration (empty when no overlap).
func (manager *Manager) StartIndex(ctx context.Context, requestedPath string, client model.ClientInfo, indexConfig model.IndexConfig, force bool) (model.Job, model.Codebase, bool, string, error) {
	var emptyJob model.Job
	var emptyCodebase model.Codebase

	canonicalPath, err := canonicalizePath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		return emptyJob, emptyCodebase, false, "", fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	if err := manager.guardStateRoot(canonicalPath); err != nil {
		return emptyJob, emptyCodebase, false, "", err
	}
	if err := manager.guardDirectory(canonicalPath); err != nil {
		return emptyJob, emptyCodebase, false, "", err
	}

	indexConfig = manager.enrichIndexConfig(indexConfig)
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)

	if dedupedJob, dedupedCodebase, deduped := manager.dedupAgainstActiveJob(canonicalPath, indexConfig); deduped {
		return dedupedJob, dedupedCodebase, true, "", nil
	}

	if force {
		if err := manager.cancelActiveJobForPath(ctx, canonicalPath); err != nil {
			return emptyJob, emptyCodebase, false, "", err
		}
	}

	hasCollection := manager.probeCollectionForPath(ctx, canonicalPath)

	job, codebase, deduped, overlapsCodebaseID, err := manager.commitStartIndexLocked(ctx, canonicalPath, requestedPath, client, indexConfig, force, hasCollection)
	if err != nil || deduped {
		return job, codebase, deduped, overlapsCodebaseID, err
	}
	if job.ID == "" {
		return emptyJob, codebase, false, overlapsCodebaseID, nil
	}
	manager.notifyCodebaseAdded(ctx, codebase)
	ctx = spans.Attach(ctx, correlation.IdentityAttribute{Key: "job_id", Value: job.ID}, correlation.IdentityAttribute{Key: "codebase_id", Value: codebase.ID})
	manager.runJobAsync(ctx, job.ID)
	return job, codebase, false, overlapsCodebaseID, nil
}

// probeCollectionForPath asks Milvus whether canonicalPath already has a
// collection so commitStartIndexLocked can decide between bootstrap and
// streaming-reindex. Returns false when the semantic service is unavailable
// or the check fails; both cases route to bootstrap which is correct.
func (manager *Manager) probeCollectionForPath(ctx context.Context, canonicalPath string) bool {
	if manager.semantic == nil || !manager.semantic.Available() {
		return false
	}
	present, hasErr := manager.semantic.HasCollectionForPath(ctx, canonicalPath)
	if hasErr != nil {
		slog.WarnContext(ctx, "Milvus HasCollection failed during StartIndex", "path", canonicalPath, "err", hasErr)
		return false
	}
	return present
}

// commitStartIndexLocked acquires the registry lock, runs the decision
// table, applies the resulting codebase mutation, persists the registry,
// and queues the job event. The returned job has an empty ID when the
// decision resolved as already-indexed; the caller treats that as a no-op
// success.
func (manager *Manager) commitStartIndexLocked(ctx context.Context, canonicalPath string, requestedPath string, client model.ClientInfo, indexConfig model.IndexConfig, force bool, hasCollection bool) (model.Job, model.Codebase, bool, string, error) {
	var emptyJob model.Job
	var emptyCodebase model.Codebase
	manager.mu.Lock()
	defer manager.mu.Unlock()

	decision, err := manager.decideStartIndexLocked(canonicalPath, indexConfig, force, hasCollection)
	if err != nil {
		slog.ErrorContext(ctx, "resolve active job failed", "canonical_path", canonicalPath, "err", err)
		return emptyJob, emptyCodebase, false, "", err
	}
	if decision.dedup {
		return decision.activeJob, decision.codebase, true, "", nil
	}
	overlapsCodebaseID := ""
	if ancestor, found := manager.findStrictAncestor(canonicalPath); found {
		overlapsCodebaseID = ancestor.ID
	}
	if decision.alreadyIndexed {
		return emptyJob, decision.codebase, false, overlapsCodebaseID, nil
	}

	codebase := decision.codebase
	codebase.Status = model.CodebaseStatusIndexing
	codebase.EffectiveConfig = indexConfig
	codebase.InodeTrackingDisabled = detectInodeTrackingDisabled(ctx, canonicalPath)
	codebase.ResolvedIgnoreRules = resolveIgnoreRulesOrLog(ctx, canonicalPath, indexConfig.IgnorePatterns)
	if manager.semantic != nil && manager.semantic.Available() {
		codebase.CollectionName = manager.semantic.CollectionName(canonicalPath)
	}
	codebase.UpdatedAt = clock.Now()

	operation := jobOperationIndex
	if decision.streamingReindex {
		operation = jobOperationStreamingReindex
	}
	job := newQueuedJob(codebase.ID, requestedPath, canonicalPath, client, string(operation), indexConfig, clock.Now())
	codebase.ActiveJobID = job.ID
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		return emptyJob, emptyCodebase, false, "", err
	}
	if err := manager.appendJobLocked("start_index", job); err != nil {
		return emptyJob, emptyCodebase, false, "", err
	}
	return job, codebase, false, overlapsCodebaseID, nil
}

// SyncIndex registers a new sync job for an existing tracked codebase.
func (manager *Manager) SyncIndex(ctx context.Context, requestedPath string, client model.ClientInfo) (model.Job, model.Codebase, bool, error) {
	canonicalPath, err := canonicalizePath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false, fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	manager.mu.Lock()

	matches := manager.findCodebasesByCoverage(canonicalPath)
	if len(matches) == 0 {
		manager.mu.Unlock()
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false, errors.New("codebase not tracked: " + requestedPath)
	}
	codebase := matches[0]

	indexConfig := manager.enrichIndexConfig(codebase.EffectiveConfig)
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)

	activeJob, deduplicated, err := manager.activeJobLocked(codebase, canonicalPath, indexConfig)
	if err != nil {
		slog.ErrorContext(ctx, "resolve active sync job failed", "canonical_path", canonicalPath, "err", err)
		manager.mu.Unlock()
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false, err
	}
	if deduplicated {
		manager.mu.Unlock()
		return activeJob, codebase, true, nil
	}

	codebase.Status = model.CodebaseStatusIndexing
	codebase.EffectiveConfig = indexConfig
	codebase.ResolvedIgnoreRules = resolveIgnoreRulesOrLog(ctx, canonicalPath, indexConfig.IgnorePatterns)
	if manager.semantic != nil && manager.semantic.Available() {
		codebase.CollectionName = manager.semantic.CollectionName(canonicalPath)
	}
	codebase.UpdatedAt = clock.Now()

	now := clock.Now()
	job := newQueuedJob(codebase.ID, requestedPath, canonicalPath, client, string(jobOperationSync), indexConfig, now)

	codebase.ActiveJobID = job.ID
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false, err
	}
	if err := manager.appendJobLocked("start_sync", job); err != nil {
		manager.mu.Unlock()
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false, err
	}
	manager.mu.Unlock()
	ctx = spans.Attach(ctx, correlation.IdentityAttribute{Key: "job_id", Value: job.ID}, correlation.IdentityAttribute{Key: "codebase_id", Value: codebase.ID})
	manager.runJobAsync(ctx, job.ID)
	return job, codebase, false, nil
}

// ClearIndex removes a tracked codebase from daemon state.
func (manager *Manager) ClearIndex(ctx context.Context, requestedPath string, client model.ClientInfo) (model.Codebase, error) {
	_ = client

	canonicalPath, err := canonicalizePath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		return model.Codebase{}, fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	manager.mu.Lock()
	matches := manager.findCodebasesByCoverage(canonicalPath)
	if len(matches) == 0 {
		manager.mu.Unlock()
		return model.Codebase{}, errors.New("codebase not tracked: " + requestedPath)
	}
	codebase := matches[0]
	jobDone, cancel := manager.beginActiveJobCancellationLocked(codebase)
	manager.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if err := waitForJobDone(ctx, jobDone); err != nil {
		return model.Codebase{}, err
	}

	if err := store.RemoveFile(manager.chunkPath(codebase.ID)); err != nil {
		return model.Codebase{}, fmt.Errorf("remove chunk cache for %s: %w", codebase.ID, err)
	}
	if err := store.RemoveFile(manager.merklePath(codebase.ID)); err != nil {
		return model.Codebase{}, fmt.Errorf("remove Merkle snapshot for %s: %w", codebase.ID, err)
	}
	if manager.semantic != nil {
		if err := manager.semantic.Drop(ctx, codebase.CanonicalPath); err != nil && !errors.Is(err, semantic.ErrUnavailable) {
			return model.Codebase{}, fmt.Errorf("drop semantic index for %s: %w", codebase.CanonicalPath, err)
		}
	}

	manager.mu.Lock()

	clearedCodebase := codebase
	current, found := manager.codebases[codebase.ID]
	if !found {
		manager.mu.Unlock()
		manager.notifyCodebaseRemoved(ctx, codebase.ID)
		return clearedCodebase, nil
	}
	delete(manager.codebases, current.ID)
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		return model.Codebase{}, err
	}
	manager.mu.Unlock()
	manager.notifyCodebaseRemoved(ctx, current.ID)
	return current, nil
}

// CancelJob marks a tracked job as cancelled.
func (manager *Manager) CancelJob(jobID string) (model.Job, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return model.Job{}, fmt.Errorf("job not found: %s", jobID)
	}
	if job.State == model.JobStateCompleted || job.State == model.JobStateFailed || job.State == model.JobStateCancelled {
		return job, nil
	}

	cancel, found := manager.cancels[jobID]
	if found {
		cancel()
		delete(manager.cancels, jobID)
	}

	now := clock.Now()
	job.State = model.JobStateCancelled
	job.UpdatedAt = now
	job.CompletedAt = &now
	job.Progress.Phase = "cancelled"
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	if err := manager.appendJobLocked("cancel_job", job); err != nil {
		return model.Job{}, err
	}

	codebase, found := manager.codebases[job.CodebaseID]
	if found && codebase.ActiveJobID == job.ID {
		codebase.ActiveJobID = ""
		codebase.Status = model.CodebaseStatusFailed
		codebase.LastFailedRun = &model.IndexRunFailure{
			Message:                 "job cancelled",
			LastAttemptedPercentage: 0,
			FailedAt:                now,
			TraceID:                 "",
			JobID:                   jobID,
		}
		codebase.UpdatedAt = now
		manager.codebases[codebase.ID] = codebase
		if err := manager.saveLocked(); err != nil {
			return model.Job{}, err
		}
	}

	return job, nil
}

// Codebase lifecycle hook plumbing lives in manager_lifecycle.go.

// resolveIgnoreRulesOrLog computes the discovery rule tree for a codebase
// at registration or sync time. A failure is downgraded to an empty rule
// set so the codebase keeps running with the built-in defaults; the
// underlying error is logged with the path so the operator can act.
func resolveIgnoreRulesOrLog(ctx context.Context, canonicalPath string, overrides []string) discovery.IgnoreRules {
	rules, err := discovery.EffectiveIgnorePatterns(ctx, canonicalPath, overrides)
	if err != nil {
		slog.ErrorContext(ctx, "resolve effective ignore rules failed; falling back to empty tree", "path", canonicalPath, "err", err)
		return discovery.IgnoreRules{Nodes: nil}
	}
	return rules
}

// GetIndex / classification / synthesis helpers live in manager_status.go.

// ListIndexes returns every tracked codebase in canonical path order.
func (manager *Manager) ListIndexes(ctx context.Context) []model.Codebase {
	manager.reconcileIndexedCodebases(ctx)

	manager.mu.Lock()
	defer manager.mu.Unlock()

	codebases := make([]model.Codebase, 0, len(manager.codebases))
	for _, codebase := range manager.codebases {
		codebases = append(codebases, codebase)
	}
	sort.Slice(codebases, func(i int, j int) bool {
		return codebases[i].CanonicalPath < codebases[j].CanonicalPath
	})
	return codebases
}

// GetJob resolves one tracked job by id.
func (manager *Manager) GetJob(jobID string) (model.Job, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	job, found := manager.jobs[jobID]
	return job, found
}

// ListJobs returns tracked jobs, optionally filtered by codebase id.
func (manager *Manager) ListJobs(codebaseID string) []model.Job {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	jobs := make([]model.Job, 0, len(manager.jobs))
	for _, job := range manager.jobs {
		if codebaseID == "" || job.CodebaseID == codebaseID {
			jobs = append(jobs, job)
		}
	}
	sort.Slice(jobs, func(i int, j int) bool {
		return jobs[i].StartedAt.After(jobs[j].StartedAt)
	})
	return jobs
}

// Doctor reports basic local state-path diagnostics.
func (manager *Manager) Doctor() []string {
	diagnostics := []string{}
	for _, path := range []string{
		manager.config.StateRoot,
		manager.config.SocketsDir,
		manager.config.LogsDir,
	} {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			diagnostics = append(diagnostics, "missing path: "+path)
		}
	}

	manager.mu.Lock()
	codebases := make([]model.Codebase, 0, len(manager.codebases))
	for _, codebase := range manager.codebases {
		codebases = append(codebases, codebase)
	}
	manager.mu.Unlock()
	sort.Slice(codebases, func(i int, j int) bool {
		return codebases[i].CanonicalPath < codebases[j].CanonicalPath
	})
	for _, codebase := range codebases {
		if codebase.LastSuccessfulRun == nil {
			continue
		}
		skipped := len(codebase.LastSuccessfulRun.SkippedFiles)
		if skipped == 0 {
			continue
		}
		diagnostics = append(diagnostics, fmt.Sprintf(
			"%s: %d non-UTF-8 file(s) skipped during last indexing run",
			codebase.CanonicalPath,
			skipped,
		))
	}
	return diagnostics
}

// dedupAgainstActiveJob returns an existing in-flight job that matches the
// caller's effective config so concurrent MCP requests (including
// force-reindex requests) collapse into a single embedding pass.
func (manager *Manager) dedupAgainstActiveJob(canonicalPath string, indexConfig model.IndexConfig) (model.Job, model.Codebase, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	existingCodebase, codebaseFound := manager.findCodebaseByExactRoot(canonicalPath)
	if !codebaseFound {
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false
	}
	activeJob, deduplicated, err := manager.activeJobLocked(existingCodebase, canonicalPath, indexConfig)
	if err != nil || !deduplicated {
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false
	}
	return activeJob, existingCodebase, true
}

func (manager *Manager) activeJobLocked(codebase model.Codebase, canonicalPath string, indexConfig model.IndexConfig) (model.Job, bool, error) {
	if codebase.ActiveJobID == "" {
		var emptyJob model.Job
		return emptyJob, false, nil
	}

	activeJob, found := manager.jobs[codebase.ActiveJobID]
	if !found {
		var emptyJob model.Job
		return emptyJob, false, nil
	}

	switch activeJob.State {
	case model.JobStateCompleted, model.JobStateFailed, model.JobStateCancelled:
		var emptyJob model.Job
		return emptyJob, false, nil
	case model.JobStateQueued, model.JobStateRunning, model.JobStateCancelling:
	default:
		var emptyJob model.Job
		return emptyJob, false, fmt.Errorf("unknown job state %s for active job %s", activeJob.State, activeJob.ID)
	}

	if activeJob.Config.IgnoreDigest == indexConfig.IgnoreDigest && activeJob.Config.SplitterType == indexConfig.SplitterType {
		return activeJob, true, nil
	}

	var emptyJob model.Job
	return emptyJob, false, fmt.Errorf("conflicting active job %s for canonical path %s", activeJob.ID, canonicalPath)
}

func (manager *Manager) activeJobSnapshotLocked(codebase model.Codebase) *model.Job {
	if codebase.ActiveJobID == "" {
		return nil
	}

	job, found := manager.jobs[codebase.ActiveJobID]
	if !found {
		return nil
	}
	switch job.State {
	case model.JobStateQueued, model.JobStateRunning, model.JobStateCancelling:
		jobCopy := job
		return &jobCopy
	case model.JobStateCompleted, model.JobStateFailed, model.JobStateCancelled:
		return nil
	default:
		return nil
	}
}

func (manager *Manager) cancelActiveJobForPath(ctx context.Context, canonicalPath string) error {
	manager.mu.Lock()
	codebase, found := manager.findCodebaseByExactRoot(canonicalPath)
	if !found {
		manager.mu.Unlock()
		return nil
	}
	jobDone, cancel := manager.beginActiveJobCancellationLocked(codebase)
	manager.mu.Unlock()

	if cancel == nil {
		return nil
	}

	cancel()
	if err := waitForJobDone(ctx, jobDone); err != nil {
		return err
	}
	return nil
}

func (manager *Manager) beginActiveJobCancellationLocked(codebase model.Codebase) (chan struct{}, context.CancelFunc) {
	if codebase.ActiveJobID == "" {
		return nil, nil
	}

	job, found := manager.jobs[codebase.ActiveJobID]
	if !found {
		return nil, nil
	}
	if job.State == model.JobStateCompleted || job.State == model.JobStateFailed || job.State == model.JobStateCancelled {
		return nil, nil
	}

	now := clock.Now()
	job.State = model.JobStateCancelling
	job.UpdatedAt = now
	job.Progress.Phase = "cancelling"
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	manager.jobs[job.ID] = job
	cancel := manager.cancels[job.ID]
	jobDone := manager.done[job.ID]
	return jobDone, cancel
}

func waitForJobDone(ctx context.Context, jobDone chan struct{}) error {
	if jobDone == nil {
		return nil
	}

	select {
	case <-jobDone:
		return nil
	case <-ctx.Done():
		slog.ErrorContext(ctx, "wait for active job cancellation failed", "err", ctx.Err())
		return fmt.Errorf("wait for active job cancellation: %w", ctx.Err())
	}
}

// Delta sync helpers live in manager_delta.go.
// Job state mutators live in manager_jobs_state.go.
// SearchCode and rankChunks live in manager_search.go.
// Path helpers live in manager_paths.go.
// Config helpers and id helpers live in manager_config.go.
// Boundary guards (StateRoot, directory, inode-stability) live in
// manager_guards.go.
