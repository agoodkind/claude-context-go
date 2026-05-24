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

	"github.com/zilliztech/claude-context-go/internal/clock"
	"github.com/zilliztech/claude-context-go/internal/config"
	"github.com/zilliztech/claude-context-go/internal/indexer"
	"github.com/zilliztech/claude-context-go/internal/migrate"
	"github.com/zilliztech/claude-context-go/internal/model"
	"github.com/zilliztech/claude-context-go/internal/store"
)

// Manager coordinates persisted codebase and job state for the daemon.
type Manager struct {
	config    config.Config
	mu        sync.Mutex
	codebases map[string]model.Codebase
	jobs      map[string]model.Job
	runner    *indexer.Runner
}

// NewManager loads persisted daemon state from disk.
func NewManager(cfg config.Config) (*Manager, error) {
	manager := &Manager{
		config:    cfg,
		codebases: map[string]model.Codebase{},
		jobs:      map[string]model.Job{},
		runner:    indexer.NewRunner(),
	}
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
	if errors.Is(err, os.ErrNotExist) {
		if migrationErr := manager.importLegacySnapshot(); migrationErr != nil {
			return migrationErr
		}
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

func (manager *Manager) importLegacySnapshot() error {
	snapshotPath, err := migrate.LegacySnapshotPath()
	if err != nil {
		return fmt.Errorf("resolve legacy snapshot path: %w", err)
	}
	if !migrate.SnapshotExists(snapshotPath) {
		return nil
	}

	codebases, jobs, err := migrate.ImportLegacySnapshot(snapshotPath)
	if err != nil {
		slog.Error("import legacy snapshot failed", "path", snapshotPath, "err", err)
		return fmt.Errorf("import legacy snapshot %s: %w", snapshotPath, err)
	}
	for _, codebase := range codebases {
		manager.codebases[codebase.ID] = codebase
	}
	for _, job := range jobs {
		manager.jobs[job.ID] = job
	}
	if len(codebases) == 0 && len(jobs) == 0 {
		return nil
	}
	if err := manager.saveLocked(); err != nil {
		return err
	}
	for _, job := range jobs {
		if appendErr := manager.appendJobLocked("legacy_migration", job); appendErr != nil {
			slog.Error("append migrated job failed", "job_id", job.ID, "err", appendErr)
			return appendErr
		}
	}
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

// StartIndex registers a new indexing job or deduplicates an existing one.
func (manager *Manager) StartIndex(ctx context.Context, requestedPath string, client model.ClientInfo, indexConfig model.IndexConfig, force bool) (model.Job, model.Codebase, bool, error) {
	_ = ctx

	canonicalPath, aliasPath, err := canonicalizePath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		return model.Job{}, model.Codebase{}, false, fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)

	manager.mu.Lock()
	defer manager.mu.Unlock()

	codebase, found := manager.findCodebaseByPathLocked(canonicalPath, aliasPath)
	if found {
		activeJob, deduplicated, err := manager.activeJobLocked(codebase, canonicalPath, indexConfig)
		if err != nil {
			slog.ErrorContext(ctx, "resolve active job failed", "canonical_path", canonicalPath, "err", err)
			return model.Job{}, model.Codebase{}, false, err
		}
		if deduplicated {
			return activeJob, codebase, true, nil
		}
		if !force && codebase.Status == model.CodebaseStatusIndexed {
			return model.Job{}, model.Codebase{}, false, errors.New("codebase already indexed: " + canonicalPath)
		}
	} else {
		codebase = model.Codebase{
			ID:            newID("cb"),
			CanonicalPath: canonicalPath,
			Status:        model.CodebaseStatusNotIndexed,
		}
	}

	codebase.Aliases = mergeAliases(codebase.Aliases, aliasPath, requestedPath, canonicalPath)
	codebase.Status = model.CodebaseStatusIndexing
	codebase.EffectiveConfig = indexConfig
	codebase.UpdatedAt = clock.Now()

	now := clock.Now()
	job := model.Job{
		ID:            newID("job"),
		CodebaseID:    codebase.ID,
		RequestedPath: requestedPath,
		CanonicalPath: canonicalPath,
		Client:        client,
		Operation:     "index",
		State:         model.JobStateQueued,
		Progress: model.Progress{
			Phase:       "queued",
			LastEventAt: now,
			HeartbeatAt: now,
		},
		Config:    indexConfig,
		StartedAt: now,
		UpdatedAt: now,
	}

	codebase.ActiveJobID = job.ID
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		return model.Job{}, model.Codebase{}, false, err
	}
	if err := manager.appendJobLocked("start_index", job); err != nil {
		return model.Job{}, model.Codebase{}, false, err
	}
	manager.runJobAsync(ctx, job.ID)
	return job, codebase, false, nil
}

// ClearIndex removes a tracked codebase from daemon state.
func (manager *Manager) ClearIndex(requestedPath string, client model.ClientInfo) (model.Codebase, error) {
	_ = client

	canonicalPath, aliasPath, err := canonicalizePath(requestedPath)
	if err != nil {
		slog.Error("canonicalize path failed", "path", requestedPath, "err", err)
		return model.Codebase{}, fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	codebase, found := manager.findCodebaseByPathLocked(canonicalPath, aliasPath)
	if !found {
		return model.Codebase{}, errors.New("codebase not tracked: " + requestedPath)
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
			Message:  "job cancelled",
			FailedAt: now,
		}
		codebase.UpdatedAt = now
		manager.codebases[codebase.ID] = codebase
		if err := manager.saveLocked(); err != nil {
			return model.Job{}, err
		}
	}

	return job, nil
}

// GetIndex resolves one tracked codebase by canonical path or alias.
func (manager *Manager) GetIndex(requestedPath string) (model.Codebase, bool, error) {
	canonicalPath, aliasPath, err := canonicalizePath(requestedPath)
	if err != nil {
		slog.Error("canonicalize path failed", "path", requestedPath, "err", err)
		return model.Codebase{}, false, fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	codebase, found := manager.findCodebaseByPathLocked(canonicalPath, aliasPath)
	return codebase, found, nil
}

// ListIndexes returns every tracked codebase in canonical path order.
func (manager *Manager) ListIndexes() []model.Codebase {
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
	return diagnostics
}

func (manager *Manager) activeJobLocked(codebase model.Codebase, canonicalPath string, indexConfig model.IndexConfig) (model.Job, bool, error) {
	if codebase.ActiveJobID == "" {
		return model.Job{}, false, nil
	}

	activeJob, found := manager.jobs[codebase.ActiveJobID]
	if !found {
		return model.Job{}, false, nil
	}

	switch activeJob.State {
	case model.JobStateCompleted, model.JobStateFailed, model.JobStateCancelled:
		return model.Job{}, false, nil
	case model.JobStateQueued, model.JobStateRunning, model.JobStateCancelling:
	default:
		return model.Job{}, false, fmt.Errorf("unknown job state %s for active job %s", activeJob.State, activeJob.ID)
	}

	if activeJob.Config.IgnoreDigest == indexConfig.IgnoreDigest && activeJob.Config.SplitterType == indexConfig.SplitterType {
		return activeJob, true, nil
	}

	return model.Job{}, false, fmt.Errorf("conflicting active job %s for canonical path %s", activeJob.ID, canonicalPath)
}

func (manager *Manager) runJobAsync(ctx context.Context, jobID string) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(ctx, "indexing goroutine panic", "err", fmt.Errorf("panic: %v", recovered), "job_id", jobID)
			}
		}()
		manager.runJob(ctx, jobID)
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

	result, err := manager.runner.Index(ctx, job.CanonicalPath, job.Config)
	if err != nil {
		manager.updateJobFailed(job.ID, err)
		return
	}
	manager.updateJobCompleted(job.ID, result)
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
	currentJob.Progress.Phase = "scanning"
	currentJob.Progress.LastEventAt = now
	currentJob.Progress.HeartbeatAt = now
	_ = manager.appendJobLocked("job_running", currentJob)
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
	}
	codebase.UpdatedAt = now
	manager.codebases[codebase.ID] = codebase
	chunkPath := manager.chunkPath(codebase.ID)
	if err := store.WriteChunks(chunkPath, result.Chunks); err != nil {
		slog.Error("write chunk cache failed", "job_id", jobID, "err", err)
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
		Message:  runErr.Error(),
		FailedAt: now,
	}
	codebase.UpdatedAt = now
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.Error("write registry after failed job failed", "job_id", jobID, "err", err)
	}
}

// SearchCode performs a local ranked search over persisted chunk content.
func (manager *Manager) SearchCode(requestedPath string, query string, limit int32, extensionFilter []string) ([]model.StoredChunk, error) {
	codebase, found, err := manager.GetIndex(requestedPath)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("codebase not tracked: " + requestedPath)
	}

	chunks, err := store.ReadChunks(manager.chunkPath(codebase.ID))
	if err != nil {
		slog.Error("read chunk cache failed", "codebase_id", codebase.ID, "err", err)
		return nil, fmt.Errorf("read chunk cache for %s: %w", codebase.ID, err)
	}
	return rankChunks(chunks, query, limit, extensionFilter), nil
}

func (manager *Manager) findCodebaseByPathLocked(canonicalPath string, aliasPath string) (model.Codebase, bool) {
	for _, codebase := range manager.codebases {
		if codebase.CanonicalPath == canonicalPath {
			return codebase, true
		}
		for _, alias := range codebase.Aliases {
			if alias == aliasPath || alias == canonicalPath {
				return codebase, true
			}
		}
	}
	return model.Codebase{}, false
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

func digestIndexConfig(indexConfig model.IndexConfig) string {
	digestBytes, err := json.Marshal(indexConfig)
	if err != nil {
		digest := sha256.Sum256([]byte(indexConfig.SplitterType + indexConfig.IgnoreDigest))
		return "sha256:" + hex.EncodeToString(digest[:])
	}
	digest := sha256.Sum256(digestBytes)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func (manager *Manager) chunkPath(codebaseID string) string {
	return filepath.Join(manager.config.ChunksDir, codebaseID+".json")
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
