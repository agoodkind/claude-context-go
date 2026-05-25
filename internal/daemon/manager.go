// Package daemon owns persisted daemon state and request coordination.
package daemon

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"goodkind.io/claude-context-go/internal/clock"
	"goodkind.io/claude-context-go/internal/config"
	"goodkind.io/claude-context-go/internal/indexer"
	"goodkind.io/claude-context-go/internal/merkle"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/semantic"
	"goodkind.io/claude-context-go/internal/store"
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

// Manager coordinates persisted codebase and job state for the daemon.
type Manager struct {
	config    config.Config
	mu        sync.Mutex
	codebases map[string]model.Codebase
	jobs      map[string]model.Job
	cancels   map[string]context.CancelFunc
	done      map[string]chan struct{}
	runner    indexingRunner
	semantic  *semantic.Service
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
}

// NewManager loads persisted daemon state from disk.
func NewManager(cfg config.Config) (*Manager, error) {
	manager := &Manager{
		config:    cfg,
		mu:        sync.Mutex{},
		codebases: map[string]model.Codebase{},
		jobs:      map[string]model.Job{},
		cancels:   map[string]context.CancelFunc{},
		done:      map[string]chan struct{}{},
		runner:    indexer.NewRunner(),
		semantic:  nil,
	}
	semanticService, err := semantic.NewService(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("create semantic service: %w", err)
	}
	manager.semantic = semanticService
	if err := manager.load(); err != nil {
		slog.Error("load daemon state failed", "state_root", cfg.StateRoot, "err", err)
		return nil, fmt.Errorf("load daemon state: %w", err)
	}
	return manager, nil
}

func (manager *Manager) load() error {
	registry, err := store.ReadRegistry(manager.config.RegistryPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Error("read registry failed", "path", manager.config.RegistryPath, "err", err)
		return fmt.Errorf("read registry: %w", err)
	}
	for _, codebase := range registry.Codebases {
		manager.codebases[codebase.ID] = codebase
	}

	jobs, err := store.ReadJobEvents(manager.config.JobsPath)
	if err != nil {
		slog.Error("read jobs failed", "path", manager.config.JobsPath, "err", err)
		return fmt.Errorf("read jobs: %w", err)
	}
	maps.Copy(manager.jobs, jobs)
	return nil
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
		Aliases:           nil,
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

// decideStartIndexLocked resolves the codebase record for a StartIndex call
// and decides whether to dedupe, refuse with "already indexed", or run a
// streaming reindex against the existing collection. Caller must hold
// manager.mu.
// decideStartIndexLocked resolves the codebase record and routing decision
// using the registry plus the caller-provided Milvus collection state.
// Milvus is the source of truth for indexed state, so a registry miss with
// hasCollection=true promotes the path to an indexed codebase that streams
// the next reindex into the existing collection. Caller must hold
// manager.mu.
func (manager *Manager) decideStartIndexLocked(canonicalPath string, aliasPath string, indexConfig model.IndexConfig, force bool, hasCollection bool) (startIndexDecision, error) {
	var emptyJob model.Job
	codebase, found := manager.findCodebaseByPathLocked(canonicalPath, aliasPath)
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
func (manager *Manager) StartIndex(ctx context.Context, requestedPath string, client model.ClientInfo, indexConfig model.IndexConfig, force bool) (model.Job, model.Codebase, bool, error) {
	var emptyJob model.Job
	var emptyCodebase model.Codebase

	canonicalPath, aliasPath, err := canonicalizePath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		return emptyJob, emptyCodebase, false, fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	indexConfig = manager.enrichIndexConfig(indexConfig)
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)

	if dedupedJob, dedupedCodebase, deduped := manager.dedupAgainstActiveJob(canonicalPath, aliasPath, indexConfig); deduped {
		return dedupedJob, dedupedCodebase, true, nil
	}

	if force {
		if err := manager.cancelActiveJobForPath(ctx, canonicalPath, aliasPath); err != nil {
			return emptyJob, emptyCodebase, false, err
		}
	}

	hasCollection := false
	if manager.semantic != nil && manager.semantic.Available() {
		present, hasErr := manager.semantic.HasCollectionForPath(ctx, canonicalPath)
		if hasErr != nil {
			slog.WarnContext(ctx, "Milvus HasCollection failed during StartIndex", "path", canonicalPath, "err", hasErr)
		} else {
			hasCollection = present
		}
	}

	manager.mu.Lock()

	decision, err := manager.decideStartIndexLocked(canonicalPath, aliasPath, indexConfig, force, hasCollection)
	if err != nil {
		manager.mu.Unlock()
		slog.ErrorContext(ctx, "resolve active job failed", "canonical_path", canonicalPath, "err", err)
		return emptyJob, emptyCodebase, false, err
	}
	if decision.dedup {
		manager.mu.Unlock()
		return decision.activeJob, decision.codebase, true, nil
	}
	if decision.alreadyIndexed {
		manager.mu.Unlock()
		return emptyJob, emptyCodebase, false, errors.New("codebase already indexed: " + canonicalPath)
	}

	codebase := decision.codebase
	codebase.Aliases = mergeAliases(codebase.Aliases, aliasPath, requestedPath, canonicalPath)
	codebase.Status = model.CodebaseStatusIndexing
	codebase.EffectiveConfig = indexConfig
	if manager.semantic != nil && manager.semantic.Available() {
		codebase.CollectionName = manager.semantic.CollectionName(canonicalPath)
	}
	codebase.UpdatedAt = clock.Now()

	now := clock.Now()
	operation := jobOperationIndex
	if decision.streamingReindex {
		operation = jobOperationStreamingReindex
	}
	job := newQueuedJob(codebase.ID, requestedPath, canonicalPath, client, string(operation), indexConfig, now)

	codebase.ActiveJobID = job.ID
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		return emptyJob, emptyCodebase, false, err
	}
	if err := manager.appendJobLocked("start_index", job); err != nil {
		manager.mu.Unlock()
		return emptyJob, emptyCodebase, false, err
	}
	manager.mu.Unlock()
	manager.runJobAsync(ctx, job.ID)
	return job, codebase, false, nil
}

// SyncIndex registers a new sync job for an existing tracked codebase.
func (manager *Manager) SyncIndex(ctx context.Context, requestedPath string, client model.ClientInfo) (model.Job, model.Codebase, bool, error) {
	canonicalPath, aliasPath, err := canonicalizePath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false, fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	manager.mu.Lock()

	codebase, found := manager.findCodebaseByPathLocked(canonicalPath, aliasPath)
	if !found {
		manager.mu.Unlock()
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false, errors.New("codebase not tracked: " + requestedPath)
	}

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

	codebase.Aliases = mergeAliases(codebase.Aliases, aliasPath, requestedPath, canonicalPath)
	codebase.Status = model.CodebaseStatusIndexing
	codebase.EffectiveConfig = indexConfig
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
	manager.runJobAsync(ctx, job.ID)
	return job, codebase, false, nil
}

// ClearIndex removes a tracked codebase from daemon state.
func (manager *Manager) ClearIndex(ctx context.Context, requestedPath string, client model.ClientInfo) (model.Codebase, error) {
	_ = client

	canonicalPath, aliasPath, err := canonicalizePath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		return model.Codebase{}, fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	manager.mu.Lock()
	codebase, found := manager.findCodebaseByPathLocked(canonicalPath, aliasPath)
	if !found {
		manager.mu.Unlock()
		return model.Codebase{}, errors.New("codebase not tracked: " + requestedPath)
	}
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
		if err := manager.semantic.Drop(ctx, canonicalPath); err != nil && !errors.Is(err, semantic.ErrUnavailable) {
			return model.Codebase{}, fmt.Errorf("drop semantic index for %s: %w", canonicalPath, err)
		}
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	clearedCodebase := codebase
	codebase, found = manager.findCodebaseByPathLocked(canonicalPath, aliasPath)
	if !found {
		return clearedCodebase, nil
	}
	delete(manager.codebases, codebase.ID)
	if err := manager.saveLocked(); err != nil {
		return model.Codebase{}, err
	}
	return codebase, nil
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
		}
		codebase.UpdatedAt = now
		manager.codebases[codebase.ID] = codebase
		if err := manager.saveLocked(); err != nil {
			return model.Job{}, err
		}
	}

	return job, nil
}

// GetIndex resolves one tracked codebase by canonical path or alias. Milvus
// is the source of truth for indexed state, so GetIndex returns Indexed for
// any path whose Milvus collection exists, synthesizing a record when the
// registry has no entry for it.
func (manager *Manager) GetIndex(ctx context.Context, requestedPath string) (model.Codebase, *model.Job, bool, error) {
	manager.reconcileIndexedCodebases(ctx)

	canonicalPath, aliasPath, err := canonicalizePath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		return model.Codebase{}, nil, false, fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	manager.mu.Lock()
	codebase, found := manager.findCodebaseByPathLocked(canonicalPath, aliasPath)
	if found {
		activeJob := manager.activeJobSnapshotLocked(codebase)
		manager.mu.Unlock()
		return codebase, activeJob, true, nil
	}
	manager.mu.Unlock()

	if manager.semantic != nil && manager.semantic.Available() {
		hasCollection, hasErr := manager.semantic.HasCollectionForPath(ctx, canonicalPath)
		if hasErr == nil && hasCollection {
			return manager.synthesizeUnregisteredCodebase(canonicalPath), nil, true, nil
		}
		if hasErr != nil {
			slog.WarnContext(ctx, "Milvus HasCollection failed during GetIndex", "path", canonicalPath, "err", hasErr)
		}
	}

	var emptyCodebase model.Codebase
	return emptyCodebase, nil, false, nil
}

// synthesizeUnregisteredCodebase builds an in-memory codebase record from
// the Milvus collection state for a path with no registry entry. The record
// stays in-memory and lets GetIndex answer "indexed" from the shared data
// store alone.
func (manager *Manager) synthesizeUnregisteredCodebase(canonicalPath string) model.Codebase {
	collectionName := ""
	if manager.semantic != nil {
		collectionName = manager.semantic.CollectionName(canonicalPath)
	}
	codebase := newCodebaseRecord(canonicalPath)
	codebase.Status = model.CodebaseStatusIndexed
	codebase.CollectionName = collectionName
	codebase.EffectiveConfig.Hybrid = manager.config.HybridMode
	return codebase
}

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
func (manager *Manager) dedupAgainstActiveJob(canonicalPath string, aliasPath string, indexConfig model.IndexConfig) (model.Job, model.Codebase, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	existingCodebase, codebaseFound := manager.findCodebaseByPathLocked(canonicalPath, aliasPath)
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

func (manager *Manager) cancelActiveJobForPath(ctx context.Context, canonicalPath string, aliasPath string) error {
	manager.mu.Lock()
	codebase, found := manager.findCodebaseByPathLocked(canonicalPath, aliasPath)
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

func (manager *Manager) runJobAsync(ctx context.Context, jobID string) {
	backgroundContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})

	manager.mu.Lock()
	manager.cancels[jobID] = cancel
	manager.done[jobID] = done
	manager.mu.Unlock()

	go func() {
		defer func() {
			cancel()
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(ctx, "indexing goroutine panic", "err", fmt.Errorf("panic: %v", recovered), "job_id", jobID)
			}
			manager.mu.Lock()
			delete(manager.cancels, jobID)
			delete(manager.done, jobID)
			manager.mu.Unlock()
			close(done)
		}()
		manager.runJob(backgroundContext, jobID)
	}()
}

func (manager *Manager) runJob(ctx context.Context, jobID string) {
	manager.mu.Lock()
	job, found := manager.jobs[jobID]
	manager.mu.Unlock()
	if !found {
		return
	}

	manager.updateJobRunning(job)

	switch jobOperation(job.Operation) {
	case jobOperationSync:
		if manager.runDeltaSync(ctx, job) {
			return
		}
	case jobOperationStreamingReindex:
		manager.runDeltaSync(ctx, job)
		return
	case jobOperationIndex:
	}

	result, err := manager.runner.Index(ctx, job.CanonicalPath, job.Config, func(progress indexer.Progress) {
		manager.updateJobProgress(job.ID, progress)
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			manager.updateJobCancelled(job.ID)
			return
		}
		manager.updateJobFailed(job.ID, err)
		return
	}
	if manager.semantic != nil && manager.semantic.Available() {
		err = manager.semantic.Replace(ctx, job.CanonicalPath, result.Chunks, func(progress semantic.Progress) {
			manager.updateJobSemanticProgress(job.ID, progress)
		})
		if err != nil {
			manager.updateJobFailed(job.ID, err)
			return
		}
	}
	manager.updateJobCompleted(job.ID, result)
}

// deltaPlan packages the file-set decision for one runDeltaSync invocation.
// fallback=true signals "no usable previous snapshot, route through full
// Replace instead". handled=true signals the helper already terminated the
// job (cancellation, snapshot-capture failure, or a no-op completion).
type deltaPlan struct {
	diff            merkle.Diff
	currentSnapshot merkle.Snapshot
	fallback        bool
	handled         bool
}

// planStreamingReindex captures a fresh merkle snapshot and synthesizes a
// diff where every discovered file counts as "modified". This is the
// "upgrade splitter granularity without dropping the collection" path.
func (manager *Manager) planStreamingReindex(ctx context.Context, job model.Job) deltaPlan {
	captured, err := merkle.Capture(ctx, job.CanonicalPath, job.Config)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			manager.updateJobCancelled(job.ID)
		} else {
			manager.updateJobFailed(job.ID, fmt.Errorf("capture reindex snapshot: %w", err))
		}
		return deltaPlan{
			diff:            merkle.Diff{Added: nil, Modified: nil, Removed: nil},
			currentSnapshot: merkle.Snapshot{Files: nil},
			fallback:        false,
			handled:         true,
		}
	}
	modifiedFiles := make([]string, 0, len(captured.Files))
	for relativePath := range captured.Files {
		modifiedFiles = append(modifiedFiles, relativePath)
	}
	sort.Strings(modifiedFiles)
	return deltaPlan{
		diff:            merkle.Diff{Added: nil, Modified: modifiedFiles, Removed: nil},
		currentSnapshot: captured,
		fallback:        false,
		handled:         false,
	}
}

// planSyncDiff reads the previous snapshot, captures the current one, and
// returns the diff. An empty diff completes the job here as a no-op tagged
// "already up to date".
func (manager *Manager) planSyncDiff(ctx context.Context, job model.Job, codebaseID string) deltaPlan {
	snapshotPath := manager.merklePath(codebaseID)
	previousSnapshot, snapshotErr := merkle.ReadSnapshot(snapshotPath)
	if snapshotErr != nil {
		slog.WarnContext(ctx, "no previous merkle snapshot for sync; falling back to full reindex", "path", snapshotPath, "err", snapshotErr)
		return deltaPlan{
			diff:            merkle.Diff{Added: nil, Modified: nil, Removed: nil},
			currentSnapshot: merkle.Snapshot{Files: nil},
			fallback:        true,
			handled:         false,
		}
	}
	captured, err := merkle.Capture(ctx, job.CanonicalPath, job.Config)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			manager.updateJobCancelled(job.ID)
		} else {
			manager.updateJobFailed(job.ID, fmt.Errorf("capture sync snapshot: %w", err))
		}
		return deltaPlan{
			diff:            merkle.Diff{Added: nil, Modified: nil, Removed: nil},
			currentSnapshot: merkle.Snapshot{Files: nil},
			fallback:        false,
			handled:         true,
		}
	}
	diff := merkle.DiffSnapshots(previousSnapshot, captured)
	if diff.Empty() {
		manager.updateJobCompleted(job.ID, indexer.Result{
			IndexedFiles: 0,
			TotalChunks:  0,
			Chunks:       nil,
			FileHashes:   captured.Files,
			SkippedFiles: nil,
		})
		return deltaPlan{
			diff:            diff,
			currentSnapshot: captured,
			fallback:        false,
			handled:         true,
		}
	}
	return deltaPlan{
		diff:            diff,
		currentSnapshot: captured,
		fallback:        false,
		handled:         false,
	}
}

// runDeltaSync attempts the incremental sync path and returns true when it
// fully handled the job (success, failure, no-op, or cancellation). It
// returns false to fall back to the full Replace path when there is no
// previous snapshot or the semantic collection is gone.
//
// Two operations route here. "sync" computes the merkle diff against the
// previous snapshot and processes only added and modified files; this is the
// background-sync hot path. "streaming_reindex" skips the diff entirely and
// treats every discovered file as modified so the agent can upgrade the
// splitter (for example coarse one-chunk-per-file to ast) without dropping
// the Milvus collection; the existing index stays searchable file by file as
// semantic.Reindex deletes and upserts each row.
func (manager *Manager) runDeltaSync(ctx context.Context, job model.Job) bool {
	manager.mu.Lock()
	codebase, codebaseFound := manager.codebases[job.CodebaseID]
	manager.mu.Unlock()
	if !codebaseFound {
		return false
	}

	var plan deltaPlan
	if jobOperation(job.Operation) == jobOperationStreamingReindex {
		plan = manager.planStreamingReindex(ctx, job)
	} else {
		plan = manager.planSyncDiff(ctx, job, codebase.ID)
	}
	if plan.fallback {
		return false
	}
	if plan.handled {
		return true
	}
	diff := plan.diff
	currentSnapshot := plan.currentSnapshot

	changedFiles := append([]string{}, diff.Added...)
	changedFiles = append(changedFiles, diff.Modified...)

	result, err := manager.runner.IndexFiles(ctx, job.CanonicalPath, changedFiles, job.Config, func(progress indexer.Progress) {
		manager.updateJobProgress(job.ID, progress)
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			manager.updateJobCancelled(job.ID)
			return true
		}
		manager.updateJobFailed(job.ID, err)
		return true
	}

	if manager.semantic != nil && manager.semantic.Available() {
		removedOrModified := append([]string{}, diff.Removed...)
		removedOrModified = append(removedOrModified, diff.Modified...)

		err = manager.semantic.Reindex(ctx, job.CanonicalPath, result.Chunks, removedOrModified, func(progress semantic.Progress) {
			manager.updateJobSemanticProgress(job.ID, progress)
		})
		switch {
		case errors.Is(err, semantic.ErrCollectionMissing):
			slog.WarnContext(ctx, "semantic collection missing during delta sync; falling back to full reindex", "job_id", job.ID)
			return false
		case errors.Is(err, context.Canceled):
			manager.updateJobCancelled(job.ID)
			return true
		case err != nil:
			manager.updateJobFailed(job.ID, err)
			return true
		}
	}

	mergedHashes := make(map[string]string, len(currentSnapshot.Files))
	maps.Copy(mergedHashes, currentSnapshot.Files)
	result.FileHashes = mergedHashes
	manager.updateJobCompleted(job.ID, result)
	return true
}

func (manager *Manager) updateJobRunning(job model.Job) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	currentJob, found := manager.jobs[job.ID]
	if !found {
		return
	}
	now := clock.Now()
	currentJob.State = model.JobStateRunning
	currentJob.UpdatedAt = now
	currentJob.Progress.Phase = "Preparing and scanning files..."
	currentJob.Progress.LastEventAt = now
	currentJob.Progress.HeartbeatAt = now
	currentJob.Progress.OverallPercent = 0
	_ = manager.appendJobLocked("job_running", currentJob)
}

func (manager *Manager) updateJobProgress(jobID string, progress indexer.Progress) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}
	if job.State != model.JobStateQueued && job.State != model.JobStateRunning && job.State != model.JobStateCancelling {
		return
	}

	now := clock.Now()
	job.State = model.JobStateRunning
	job.UpdatedAt = now
	job.Progress.Phase = progress.Phase
	job.Progress.OverallPercent = progress.OverallPercent
	job.Progress.FilesTotal = progress.FilesTotal
	job.Progress.FilesProcessed = progress.FilesProcessed
	job.Progress.ChunksGenerated = progress.ChunksGenerated
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	manager.jobs[jobID] = job
}

func (manager *Manager) updateJobSemanticProgress(jobID string, progress semantic.Progress) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}
	if job.State != model.JobStateQueued && job.State != model.JobStateRunning && job.State != model.JobStateCancelling {
		return
	}

	now := clock.Now()
	job.State = model.JobStateRunning
	job.UpdatedAt = now
	job.Progress.Phase = progress.Phase
	job.Progress.OverallPercent = progress.OverallPercent
	job.Progress.EmbeddingBatchesTotal = progress.EmbeddingBatchesTotal
	job.Progress.EmbeddingBatchesCompleted = progress.EmbeddingBatchesCompleted
	job.Progress.CollectionRowsWritten = progress.CollectionRowsWritten
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	manager.jobs[jobID] = job
}

func (manager *Manager) updateJobCompleted(jobID string, result indexer.Result) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}

	now := clock.Now()
	job.State = model.JobStateCompleted
	job.UpdatedAt = now
	job.CompletedAt = &now
	job.Progress.Phase = "completed"
	job.Progress.OverallPercent = 100
	job.Progress.FilesProcessed = result.IndexedFiles
	job.Progress.FilesTotal = result.IndexedFiles
	job.Progress.ChunksGenerated = result.TotalChunks
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	if err := manager.appendJobLocked("job_completed", job); err != nil {
		slog.Error("append completed job event failed", "job_id", jobID, "err", err)
	}

	codebase, found := manager.codebases[job.CodebaseID]
	if !found {
		return
	}
	codebase.Status = model.CodebaseStatusIndexed
	codebase.ActiveJobID = ""
	codebase.LastSuccessfulRun = &model.IndexRunSummary{
		IndexedFiles: result.IndexedFiles,
		TotalChunks:  result.TotalChunks,
		Status:       "completed",
		CompletedAt:  now,
		SkippedFiles: result.SkippedFiles,
	}
	codebase.MerkleSnapshotPath = manager.merklePath(codebase.ID)
	codebase.UpdatedAt = now
	manager.codebases[codebase.ID] = codebase
	chunkPath := manager.chunkPath(codebase.ID)
	if err := store.WriteChunks(chunkPath, result.Chunks); err != nil {
		slog.Error("write chunk cache failed", "job_id", jobID, "err", err)
	}
	if len(result.FileHashes) != 0 {
		snapshot := merkle.Snapshot{Files: result.FileHashes}
		if err := merkle.WriteSnapshot(codebase.MerkleSnapshotPath, snapshot); err != nil {
			slog.Error("write Merkle snapshot failed", "job_id", jobID, "err", err)
		}
	}
	if err := manager.saveLocked(); err != nil {
		slog.Error("write registry after completed job failed", "job_id", jobID, "err", err)
	}
}

func (manager *Manager) updateJobFailed(jobID string, runErr error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}

	now := clock.Now()
	job.State = model.JobStateFailed
	job.UpdatedAt = now
	job.CompletedAt = &now
	job.Progress.Phase = "failed"
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	job.Error = &model.JobError{
		Message:   runErr.Error(),
		Retryable: false,
	}
	if err := manager.appendJobLocked("job_failed", job); err != nil {
		slog.Error("append failed job event failed", "job_id", jobID, "err", err)
	}

	codebase, found := manager.codebases[job.CodebaseID]
	if !found {
		return
	}
	codebase.Status = model.CodebaseStatusFailed
	codebase.ActiveJobID = ""
	codebase.LastFailedRun = &model.IndexRunFailure{
		Message:                 runErr.Error(),
		LastAttemptedPercentage: 0,
		FailedAt:                now,
	}
	codebase.UpdatedAt = now
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.Error("write registry after failed job failed", "job_id", jobID, "err", err)
	}
}

func (manager *Manager) updateJobCancelled(jobID string) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}

	now := clock.Now()
	job.State = model.JobStateCancelled
	job.UpdatedAt = now
	job.CompletedAt = &now
	job.Progress.Phase = "cancelled"
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	if err := manager.appendJobLocked("job_cancelled", job); err != nil {
		slog.Error("append cancelled job event failed", "job_id", jobID, "err", err)
	}

	codebase, found := manager.codebases[job.CodebaseID]
	if !found {
		return
	}
	codebase.Status = model.CodebaseStatusFailed
	codebase.ActiveJobID = ""
	codebase.LastFailedRun = &model.IndexRunFailure{
		Message:                 "job cancelled",
		LastAttemptedPercentage: 0,
		FailedAt:                now,
	}
	codebase.UpdatedAt = now
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.Error("write registry after cancelled job failed", "job_id", jobID, "err", err)
	}
}

// SearchCode performs a local ranked search over persisted chunk content.
func (manager *Manager) SearchCode(ctx context.Context, requestedPath string, query string, limit int32, extensionFilter []string) (SearchOutcome, error) {
	normalizedExtensions, err := semantic.ValidateExtensionFilter(extensionFilter)
	if err != nil {
		return SearchOutcome{}, fmt.Errorf("validate extension filter: %w", err)
	}

	codebase, activeJob, found, err := manager.GetIndex(ctx, requestedPath)
	if err != nil {
		return SearchOutcome{}, err
	}
	if !found {
		return SearchOutcome{}, errors.New("codebase not tracked: " + requestedPath)
	}

	if manager.semantic != nil && manager.semantic.Available() {
		chunks, semanticErr := manager.semantic.Search(ctx, codebase.CanonicalPath, query, limit, normalizedExtensions)
		switch {
		case semanticErr == nil:
			return SearchOutcome{
				Codebase:  codebase,
				ActiveJob: activeJob,
				Results:   semantic.DeduplicateChunks(chunks),
			}, nil
		case (errors.Is(semanticErr, semantic.ErrCollectionMissing) ||
			errors.Is(semanticErr, semantic.ErrCollectionNotReady) ||
			errors.Is(semanticErr, semantic.ErrSearchResultIncomplete)) &&
			codebase.Status == model.CodebaseStatusIndexing:
			return SearchOutcome{Codebase: codebase, ActiveJob: activeJob, Results: []model.StoredChunk{}}, nil
		case errors.Is(semanticErr, semantic.ErrCollectionMissing):
			return SearchOutcome{}, fmt.Errorf("index data for '%s' has been lost (collection not found in Milvus). Please re-index using index_codebase with force=true", codebase.CanonicalPath)
		case errors.Is(semanticErr, semantic.ErrUnavailable):
		default:
			return SearchOutcome{}, fmt.Errorf("semantic search for %s: %w", codebase.CanonicalPath, semanticErr)
		}
	}

	chunks, err := store.ReadChunks(manager.chunkPath(codebase.ID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && codebase.Status == model.CodebaseStatusIndexing {
			return SearchOutcome{Codebase: codebase, ActiveJob: activeJob, Results: []model.StoredChunk{}}, nil
		}
		slog.ErrorContext(ctx, "read chunk cache failed", "codebase_id", codebase.ID, "err", err)
		return SearchOutcome{}, fmt.Errorf("read chunk cache for %s: %w", codebase.ID, err)
	}
	return SearchOutcome{Codebase: codebase, ActiveJob: activeJob, Results: rankChunks(chunks, query, limit, normalizedExtensions)}, nil
}

func (manager *Manager) findCodebaseByPathLocked(canonicalPath string, aliasPath string) (model.Codebase, bool) {
	var bestMatch model.Codebase
	bestMatchLength := -1

	for _, codebase := range manager.codebases {
		if codebase.CanonicalPath == canonicalPath {
			return codebase, true
		}
		if pathCovers(codebase.CanonicalPath, canonicalPath) && len(codebase.CanonicalPath) > bestMatchLength {
			bestMatch = codebase
			bestMatchLength = len(codebase.CanonicalPath)
		}
		for _, alias := range codebase.Aliases {
			if alias == aliasPath || alias == canonicalPath {
				return codebase, true
			}
			if pathCovers(alias, aliasPath) && len(alias) > bestMatchLength {
				bestMatch = codebase
				bestMatchLength = len(alias)
			}
		}
	}
	if bestMatchLength >= 0 {
		return bestMatch, true
	}
	var emptyCodebase model.Codebase
	return emptyCodebase, false
}

func canonicalizePath(requestedPath string) (string, string, error) {
	absolutePath, err := filepath.Abs(requestedPath)
	if err != nil {
		slog.Error("resolve absolute path failed", "path", requestedPath, "err", err)
		return "", "", fmt.Errorf("resolve absolute path for %s: %w", requestedPath, err)
	}
	canonicalPath, err := filepath.EvalSymlinks(absolutePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return absolutePath, absolutePath, nil
		}
		slog.Error("resolve symlinks failed", "path", absolutePath, "err", err)
		return "", "", fmt.Errorf("resolve symlinks for %s: %w", absolutePath, err)
	}
	return canonicalPath, absolutePath, nil
}

func mergeAliases(existing []string, aliases ...string) []string {
	seen := map[string]struct{}{}
	merged := make([]string, 0, len(existing)+len(aliases))
	for _, alias := range existing {
		if alias == "" {
			continue
		}
		if _, found := seen[alias]; found {
			continue
		}
		seen[alias] = struct{}{}
		merged = append(merged, alias)
	}
	for _, alias := range aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		if _, found := seen[alias]; found {
			continue
		}
		seen[alias] = struct{}{}
		merged = append(merged, alias)
	}
	sort.Strings(merged)
	return merged
}

func pathCovers(rootPath string, targetPath string) bool {
	rootPath = filepath.Clean(rootPath)
	targetPath = filepath.Clean(targetPath)
	if rootPath == targetPath {
		return true
	}
	prefixWithSeparator := rootPath + string(filepath.Separator)
	return strings.HasPrefix(targetPath, prefixWithSeparator)
}

func digestIndexConfig(indexConfig model.IndexConfig) string {
	digestBytes, err := json.Marshal(indexConfig)
	if err != nil {
		digest := sha256.Sum256([]byte(indexConfig.SplitterType + indexConfig.IgnoreDigest))
		return "sha256:" + hex.EncodeToString(digest[:])
	}
	digest := sha256.Sum256(digestBytes)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func (manager *Manager) enrichIndexConfig(indexConfig model.IndexConfig) model.IndexConfig {
	if strings.TrimSpace(indexConfig.SplitterType) == "" {
		indexConfig.SplitterType = "ast"
	}
	if indexConfig.SplitterChunkSize == 0 {
		indexConfig.SplitterChunkSize = 2500
	}
	if indexConfig.SplitterOverlap == 0 {
		indexConfig.SplitterOverlap = 300
	}
	indexConfig.EmbeddingProvider = manager.config.EmbeddingProvider
	indexConfig.EmbeddingModel = manager.config.EmbeddingModel
	if manager.config.EmbeddingDimension > 0 {
		indexConfig.EmbeddingDimension = manager.config.EmbeddingDimension
	}
	indexConfig.VectorBackend = "milvus"
	indexConfig.Hybrid = manager.config.HybridMode
	indexConfig.Extensions = mergeDistinct(indexConfig.Extensions, manager.config.CustomExtensions)
	indexConfig.IgnorePatterns = mergeDistinct(indexConfig.IgnorePatterns, manager.config.CustomIgnorePatterns)
	return indexConfig
}

// mergeDistinct returns base + extras with duplicates removed and original
// ordering preserved.
func mergeDistinct(base []string, extras []string) []string {
	if len(extras) == 0 {
		return base
	}
	seen := make(map[string]struct{}, len(base)+len(extras))
	out := make([]string, 0, len(base)+len(extras))
	for _, value := range base {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	for _, value := range extras {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (manager *Manager) chunkPath(codebaseID string) string {
	return filepath.Join(manager.config.ChunksDir, codebaseID+".json")
}

func (manager *Manager) merklePath(codebaseID string) string {
	return filepath.Join(manager.config.MerkleDir, codebaseID+".json")
}

func newID(prefix string) string {
	randomBytes := make([]byte, 6)
	if _, err := rand.Read(randomBytes); err != nil {
		return fmt.Sprintf("%s_%d", prefix, clock.Now().UnixNano())
	}
	return fmt.Sprintf("%s_%d_%s", prefix, clock.Now().Unix(), hex.EncodeToString(randomBytes))
}

func rankChunks(chunks []model.StoredChunk, query string, limit int32, extensionFilter []string) []model.StoredChunk {
	filteredChunks := make([]model.StoredChunk, 0, len(chunks))
	filterSet := map[string]struct{}{}
	for _, extension := range extensionFilter {
		filterSet[extension] = struct{}{}
	}

	queryLower := strings.ToLower(query)
	queryTerms := strings.Fields(queryLower)
	type scoredChunk struct {
		chunk model.StoredChunk
		score int
	}
	scored := make([]scoredChunk, 0, len(chunks))
	for _, chunk := range chunks {
		if len(filterSet) > 0 {
			if _, found := filterSet[chunk.FileExtension]; !found {
				continue
			}
		}

		contentLower := strings.ToLower(chunk.Content)
		score := 0
		if strings.Contains(contentLower, queryLower) {
			score += 100
		}
		for _, term := range queryTerms {
			if strings.Contains(contentLower, term) {
				score++
			}
		}
		if score == 0 {
			continue
		}
		scored = append(scored, scoredChunk{chunk: chunk, score: score})
	}

	sort.SliceStable(scored, func(i int, j int) bool {
		if scored[i].score == scored[j].score {
			if scored[i].chunk.RelativePath == scored[j].chunk.RelativePath {
				return scored[i].chunk.StartLine < scored[j].chunk.StartLine
			}
			return scored[i].chunk.RelativePath < scored[j].chunk.RelativePath
		}
		return scored[i].score > scored[j].score
	})

	maxResults := int(limit)
	if maxResults <= 0 {
		maxResults = 10
	}
	if maxResults > len(scored) {
		maxResults = len(scored)
	}
	for _, item := range scored[:maxResults] {
		filteredChunks = append(filteredChunks, item.chunk)
	}
	return filteredChunks
}
