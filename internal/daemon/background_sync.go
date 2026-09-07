package daemon

import (
	"context"
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

	"goodkind.io/gklog/correlation"
	"goodkind.io/lm-semantic-search/internal/adapterr"
	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/config"
	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/merkle"
	"goodkind.io/lm-semantic-search/internal/metrics"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/store"
)

const (
	defaultInitialSyncDelay  = 5 * time.Second
	defaultTriggerPollPeriod = 1 * time.Second
	defaultTriggerDebounce   = 2 * time.Second
	minimumSyncIntervalMS    = 1000
)

// BackgroundSync owns daemon-driven file-watch, periodic, and trigger-based
// sync. The file watcher is the steady-state driver: it converges changed
// paths within the debounce window. The periodic sweep is the anti-entropy
// backstop that repairs drift from missed events or downtime.
//
// Watcher converges run through the manager's index-slot semaphore, so several
// codebases converge at once up to the cap and a single heavily-edited
// repository never blocks the others. Per-codebase serialization keeps two
// converges of the same codebase from racing, and the sync lock held for the
// embed window keeps a second process on this machine off that window.
type BackgroundSync struct {
	cfg     config.Config
	manager *Manager

	mu           sync.Mutex
	triggerTimer *time.Timer
	lastTrigger  time.Time

	convergeMu sync.Mutex
	// converging maps a codebase id to when its admitted converge began, so a
	// status read can distinguish capacity waits from running work.
	converging map[string]time.Time

	deferredWatcherMu    sync.Mutex
	deferredWatcherPaths map[string]map[string]struct{}

	queue   *EventQueue
	watcher *Watcher
}

// NewBackgroundSync constructs the daemon background sync coordinator.
func NewBackgroundSync(cfg config.Config, manager *Manager) *BackgroundSync {
	return &BackgroundSync{
		cfg:                  cfg,
		manager:              manager,
		mu:                   sync.Mutex{},
		triggerTimer:         nil,
		lastTrigger:          time.Time{},
		convergeMu:           sync.Mutex{},
		converging:           make(map[string]time.Time),
		deferredWatcherMu:    sync.Mutex{},
		deferredWatcherPaths: make(map[string]map[string]struct{}),
		queue:                nil,
		watcher:              nil,
	}
}

// Start launches the file watcher plus the periodic and trigger-driven sync
// loops.
func (syncer *BackgroundSync) Start(ctx context.Context) {
	if syncer.cfg.FileWatcherEnabled {
		syncer.queue = NewEventQueue(defaultTriggerDebounce, func(codebaseID string, relativePaths []string) {
			syncer.convergeViaWatcher(ctx, codebaseID, relativePaths)
		})
		syncer.watcher = NewWatcher(syncer.manager, syncer.queue)
		syncer.manager.SetCodebaseLifecycleHook(syncer)
		syncer.manager.SetWatcherActivityReporter(syncer)
		go func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					slog.ErrorContext(ctx, "background sync loop panic", "loop", "watcher", "err", recovered)
				}
			}()
			syncer.watcher.Run(ctx)
		}()
	}
	if syncer.cfg.TriggerWatcherEnabled {
		go func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					slog.ErrorContext(ctx, "background sync loop panic", "loop", "watchTrigger", "err", recovered)
				}
			}()
			syncer.watchTrigger(ctx)
		}()
	}
	if syncer.cfg.BackgroundSyncEnabled {
		syncer.startLoop(ctx, "runPeriodicSync", syncer.runPeriodicSync)
		syncer.startLoop(ctx, "runPeriodicMaintenance", syncer.runPeriodicMaintenance)
	}
}

func (syncer *BackgroundSync) startLoop(ctx context.Context, name string, loop func(context.Context)) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(ctx, "background sync loop panic", "loop", name, "err", recovered)
			}
		}()
		loop(ctx)
	}()
}

// AddCodebase forwards a newly registered codebase to the underlying watcher so
// its file events are tracked.
func (syncer *BackgroundSync) AddCodebase(ctx context.Context, codebase model.Codebase) {
	if syncer.watcher == nil {
		return
	}
	syncer.watcher.AddCodebase(ctx, codebase)
}

// RemoveCodebase drops any deferred first-build watcher paths for the codebase
// and stops the underlying watcher from tracking it.
func (syncer *BackgroundSync) RemoveCodebase(ctx context.Context, codebaseID string) {
	syncer.IndexStopped(ctx, codebaseID)
	if syncer.watcher == nil {
		return
	}
	syncer.watcher.RemoveCodebase(ctx, codebaseID)
}

// IndexReady flushes the watcher paths deferred during a codebase's first build
// once that build has promoted, so edits made while the live collection did not
// yet exist converge exactly once.
func (syncer *BackgroundSync) IndexReady(ctx context.Context, codebase model.Codebase) {
	relativePaths := syncer.takeDeferredWatcherPaths(codebase.ID)
	if len(relativePaths) == 0 {
		return
	}
	flushCtx := context.WithoutCancel(ctx)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(flushCtx, "background sync loop panic", "loop", "indexReadyFlush", "err", recovered)
			}
		}()
		syncer.convergeViaWatcher(flushCtx, codebase.ID, relativePaths)
	}()
}

// IndexStopped drops any watcher paths deferred for a codebase whose first build
// failed or was cancelled, so a failed build does not later replay churn.
func (syncer *BackgroundSync) IndexStopped(_ context.Context, codebaseID string) {
	syncer.deferredWatcherMu.Lock()
	defer syncer.deferredWatcherMu.Unlock()
	delete(syncer.deferredWatcherPaths, codebaseID)
}

func (syncer *BackgroundSync) runPeriodicSync(ctx context.Context) {
	syncer.runPeriodicLoop(ctx, func() { syncer.runSyncAll(ctx, "interval") })
}

func (syncer *BackgroundSync) runPeriodicMaintenance(ctx context.Context) {
	syncer.runPeriodicLoop(ctx, func() { syncer.runPeriodicMaintenanceOnce(ctx) })
}

func (syncer *BackgroundSync) runPeriodicMaintenanceOnce(ctx context.Context) {
	syncer.ensureMmapEnabled(ctx)
	syncer.backfillConversationColumns(ctx)
}

func (syncer *BackgroundSync) runPeriodicLoop(ctx context.Context, action func()) {
	initialTimer := time.NewTimer(defaultInitialSyncDelay)
	defer initialTimer.Stop()
	interval := time.Duration(syncer.cfg.SyncIntervalMS) * time.Millisecond
	if syncer.cfg.SyncIntervalMS < minimumSyncIntervalMS {
		interval = time.Duration(minimumSyncIntervalMS) * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-initialTimer.C:
			action()
		case <-ticker.C:
			action()
		}
	}
}

// ensureMmapEnabled drives
// collections once per periodic tick. It is a no-op when Milvus is unavailable
// and near-free after the first successful sweep (already-migrated collections
// are in-memory guard hits), so it is safe to run on every tick. Running it from
// the periodic loop gives the migration convergence and self-heal without putting
// migration policy in the semantic connection layer.
func (syncer *BackgroundSync) ensureMmapEnabled(ctx context.Context) {
	if syncer.manager == nil || syncer.manager.semantic == nil {
		return
	}
	syncer.manager.semantic.EnsureMmapEnabledAllCollections(ctx)
}

// backfillConversationColumns drives the metadata-only conversation scalar-column
// backfill once per conversation collection per process. It is a no-op when
// Milvus is unavailable and a guard hit after the first successful run per
// collection, so it is safe to run on every tick. It preserves each dense vector,
// so no chunk is re-embedded.
func (syncer *BackgroundSync) backfillConversationColumns(ctx context.Context) {
	if syncer.manager == nil || syncer.manager.semantic == nil {
		return
	}
	syncer.manager.semantic.BackfillConversationCollectionsOnce(ctx)
}

func (syncer *BackgroundSync) watchTrigger(ctx context.Context) {
	if err := store.EnsureDir(syncer.cfg.ContextRoot); err != nil {
		slog.ErrorContext(ctx, "ensure legacy context directory failed", "path", syncer.cfg.ContextRoot, "err", err)
		return
	}

	triggerPath := filepath.Join(syncer.cfg.ContextRoot, ".sync-trigger")
	if info, err := os.Stat(triggerPath); err == nil {
		syncer.lastTrigger = info.ModTime()
	}

	ticker := time.NewTicker(defaultTriggerPollPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			info, err := os.Stat(triggerPath)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					slog.ErrorContext(ctx, "stat sync trigger failed", "path", triggerPath, "err", err)
				}
				continue
			}
			if !info.ModTime().After(syncer.lastTrigger) {
				continue
			}
			syncer.lastTrigger = info.ModTime()
			syncer.scheduleTriggerSync(ctx)
		}
	}
}

func (syncer *BackgroundSync) scheduleTriggerSync(ctx context.Context) {
	syncer.mu.Lock()
	defer syncer.mu.Unlock()

	if syncer.triggerTimer != nil {
		syncer.triggerTimer.Stop()
	}
	syncer.triggerTimer = time.AfterFunc(defaultTriggerDebounce, func() {
		syncer.runSyncAll(ctx, "trigger")
	})
}

func (syncer *BackgroundSync) runSyncAll(ctx context.Context, source string) {
	if ctx.Err() != nil {
		return
	}

	rootCorr := correlation.New("").WithIdentityAttributes(
		correlation.IdentityAttribute{Key: "origin", Value: "sync-" + source},
	)
	ctx = correlation.WithContext(ctx, rootCorr)

	defer func() {
		if recovered := recover(); recovered != nil {
			_, _ = adapterr.Respond(ctx, adapterr.NewInternal("background sync panic", fmt.Errorf("panic: %v", recovered)))
		}
	}()

	// Skip the sweep whenever the embedder is already recorded unreachable. That
	// one fact is recorded reactively by whatever first fails an embed against the
	// down endpoint (a search, an index job, or an earlier sweep that attempted
	// before the fact was set). Once it is set, every sweep reads it here and
	// enqueues nothing, so a sustained outage stops re-recording itself as a fresh
	// failed and superseded job per codebase per interval. The fact clears through
	// the existing embed-success signals (a search query embed or a real embed),
	// after which the next sweep resumes. This only reads the shared health owner;
	// it adds no probe and no second source of truth.
	if syncer.manager.DependencyHealth().Mode == dependencyEmbedderUnreachable {
		return
	}

	syncer.manager.RepairMissingCollections(ctx)

	codebases := syncer.manager.ListIndexes(ctx)
	for _, codebase := range codebases {
		if codebase.Kind == model.CodebaseKindDocument {
			continue
		}
		if codebase.Status == model.CodebaseStatusQuarantined {
			syncer.handleQuarantinedCodebase(ctx, codebase)
			continue
		}
		if _, err := os.Stat(codebase.CanonicalPath); errors.Is(err, os.ErrNotExist) {
			continue
		}
		hasActiveJob := syncer.hasActiveJob(codebase)
		if shouldSkipForActiveFirstBuildStaging(codebase, hasActiveJob) {
			continue
		}
		// A discovered worktree whose deferred build never ran (for example the
		// daemon restarted before the short timer fired) is built here as the
		// backstop. StartIndex deduplicates, so this never double-starts a build
		// that the timer already kicked off.
		if codebase.Status == model.CodebaseStatusDiscovered {
			discoverCtx := correlation.WithContext(ctx, correlation.FromContext(ctx).Child().WithIdentityAttributes(
				correlation.IdentityAttribute{Key: "codebase_id", Value: codebase.ID},
			))
			syncer.manager.startDeferredBuild(discoverCtx, codebase.CanonicalPath)
			continue
		}
		if codebase.Status == model.CodebaseStatusFailed {
			retryCtx := correlation.WithContext(ctx, correlation.FromContext(ctx).Child().WithIdentityAttributes(
				correlation.IdentityAttribute{Key: "codebase_id", Value: codebase.ID},
			))
			syncer.manager.retryFailedBuild(retryCtx, codebase)
			continue
		}
		if codebase.Status != model.CodebaseStatusIndexed {
			continue
		}

		syntheticJobID := fmt.Sprintf("sync-%s-%d", codebase.ID, clock.Now().Unix())
		iterCtx := correlation.WithContext(ctx, correlation.FromContext(ctx).Child().WithIdentityAttributes(
			correlation.IdentityAttribute{Key: "codebase_id", Value: codebase.ID},
			correlation.IdentityAttribute{Key: "job_id", Value: syntheticJobID},
		))

		// Keep this codebase's ignore rules fresh independent of the file watcher.
		// CheckSources stats the codebase's ignore sources and invalidates the
		// resolver when any changed, so an edit to a non-indexed source or any edit
		// made while the watcher is disabled is caught on this sweep. It runs before
		// and independent of the change detection below.
		syncer.manager.observer.CheckSources(iterCtx, codebase.ID, codebase.CanonicalPath)

		changed, err := syncer.codebaseChanged(iterCtx, codebase)
		if err != nil {
			slog.ErrorContext(iterCtx, "check sync state failed", "path", codebase.CanonicalPath, "err", err)
			continue
		}
		metrics.SweepRan(changed)
		if !changed {
			continue
		}

		_, _, _, err = syncer.manager.SyncIndex(
			iterCtx,
			codebase.CanonicalPath,
			model.ClientInfo{Name: "daemon-sync", PID: 0},
		)
		if err != nil {
			if syncConflictError(err) {
				continue
			}
			slog.ErrorContext(iterCtx, "start sync job failed", "path", codebase.CanonicalPath, "err", err)
		}
	}
}

func (syncer *BackgroundSync) handleQuarantinedCodebase(ctx context.Context, codebase model.Codebase) {
	if sourceDirMissing(codebase.CanonicalPath) {
		syncer.manager.markCodebaseMissing(ctx, codebase.ID)
		return
	}

	// Never advance toward destructive sync or clear quarantine while a git
	// operation is mid-flight: tracked files legitimately vanish during a
	// checkout, rebase, or merge and reappear when it finishes. Hold the
	// quarantine and re-evaluate on a later sweep once the tree settles.
	if vcsOperationInProgress(codebase.CanonicalPath) {
		slog.WarnContext(ctx, "quarantine held during vcs operation", "codebase_id", codebase.ID, "path", codebase.CanonicalPath)
		return
	}

	snapshot := syncer.manager.loadLiveCheckpoint(ctx, codebase, codebase.EffectiveConfig.IgnoreDigest).snapshot
	currentSnapshot, err := merkle.Capture(ctx, syncer.manager.indexability, codebase.ID, codebase.CanonicalPath, codebase.EffectiveConfig)
	if err != nil {
		slog.ErrorContext(ctx, "quarantine capture failed", "codebase_id", codebase.ID, "path", codebase.CanonicalPath, "err", err)
		return
	}
	diff := merkle.DiffSnapshots(snapshot, currentSnapshot)
	signal, suspicious := assessDeltaDeleteWave(codebase, diff, snapshot, codebase.CanonicalPath)
	if !suspicious {
		syncer.manager.clearCodebaseQuarantine(ctx, codebase.ID, model.CodebaseStatusIndexed)
		if diff.Empty() {
			return
		}
		_, _, _, err = syncer.manager.SyncIndex(
			ctx,
			codebase.CanonicalPath,
			model.ClientInfo{Name: "daemon-quarantine-release", PID: 0},
		)
		if err != nil && !syncConflictError(err) {
			slog.ErrorContext(ctx, "start sync job after clearing quarantine failed", "codebase_id", codebase.ID, "path", codebase.CanonicalPath, "err", err)
		}
		return
	}

	observations := syncer.manager.quarantineCodebase(ctx, codebase.ID, signal)
	if observations < quarantineConfirmationObservations {
		slog.WarnContext(ctx, "quarantine held after corroborating full scan", "codebase_id", codebase.ID, "missing_count", signal.missingCount, "total_count", signal.totalCount, "observations", observations)
		return
	}

	job, codebase, deduplicated, err := syncer.manager.SyncIndex(
		ctx,
		codebase.CanonicalPath,
		model.ClientInfo{Name: "daemon-quarantine-release", PID: 0},
	)
	_ = job
	_ = codebase
	_ = deduplicated
	if err != nil && !syncConflictError(err) {
		slog.ErrorContext(ctx, "start destructive sync after quarantine confirmation failed", "codebase_id", codebase.ID, "path", codebase.CanonicalPath, "err", err)
	}
}

// convergeViaWatcher runs the debounced path set for one codebase through the
// manager's index-slot semaphore, so several codebases converge at once up to
// the cap and a heavily-edited repository never blocks the others. A second
// converge of the same codebase, or one that finds the sync lock held by
// another process, requeues its paths so no change is lost.
func (syncer *BackgroundSync) convergeViaWatcher(ctx context.Context, codebaseID string, relativePaths []string) {
	if ctx.Err() != nil {
		return
	}

	corr := correlation.New("").WithIdentityAttributes(
		correlation.IdentityAttribute{Key: "origin", Value: "watcher"},
		correlation.IdentityAttribute{Key: "codebase_id", Value: codebaseID},
	)
	ctx = correlation.WithContext(ctx, corr)

	codebase, found := syncer.watcherCodebase(codebaseID)
	if !found {
		return
	}
	if shouldDeferWatcherConvergeForFirstBuild(codebase) {
		syncer.deferWatcherPaths(codebaseID, relativePaths)
		return
	}
	if syncer.hasActiveJob(codebase) {
		metrics.SyncSkippedInflight()
		syncer.requeuePaths(codebaseID, relativePaths)
		return
	}

	// Serialize converges of the same codebase so two never race on its
	// snapshot; a concurrent one requeues rather than waits.
	if !syncer.beginConverge(codebaseID) {
		metrics.SyncSkippedInflight()
		syncer.requeuePaths(codebaseID, relativePaths)
		return
	}
	defer syncer.endConverge(codebaseID)

	// Bound concurrency across codebases through the shared index-slot
	// semaphore that user index jobs also use.
	select {
	case syncer.manager.indexSlots <- struct{}{}:
		defer func() { <-syncer.manager.indexSlots }()
	case <-ctx.Done():
		return
	}

	// Hold the sync lock for the embed window. A lock another process holds means
	// someone else owns the window, so defer and requeue. A
	// permanent lock failure is reported and dropped instead of requeued,
	// because every redelivery would hit the same error and no converge can
	// embed until the machine's configuration changes.
	lease, outcome, lockErr := syncer.manager.syncLock.acquire(ctx)
	switch outcome {
	case syncLockAcquired:
		defer lease.release(ctx)
	case syncLockBusy, syncLockCancelled:
		metrics.SyncSkippedInflight()
		syncer.requeuePaths(codebaseID, relativePaths)
		return
	case syncLockFailed:
		slog.ErrorContext(ctx, "watcher.sync_lock_failed", "component", "daemon", "subcomponent", "watcher", "codebase_id", codebaseID, "err", lockErr)
		return
	default:
		// An outcome this switch does not name skips the embed rather than
		// converging with no lock held, which is the safe direction to fall in.
		slog.ErrorContext(ctx, "watcher.sync_lock_unknown_outcome", "component", "daemon", "subcomponent", "watcher", "codebase_id", codebaseID, "outcome", string(outcome), "err", errSyncLockUnavailable)
		return
	}

	// Both waits are behind us, so the converge is genuinely running rather than
	// queued behind capacity. A status read reports the difference, and the
	// start time is stamped here so a row's age measures work rather than wait.
	codebase, found = syncer.watcherCodebase(codebaseID)
	if !found {
		return
	}
	if shouldDeferWatcherConvergeForFirstBuild(codebase) {
		syncer.deferWatcherPaths(codebaseID, relativePaths)
		return
	}
	syncer.markConvergeRunning(codebaseID)

	registration, err := syncer.registerConvergeJob(ctx, codebase, relativePaths)
	if err != nil {
		slog.ErrorContext(ctx, "register converge job failed", "codebase_id", codebaseID, "err", err)
		return
	}
	defer registration.release()

	syncer.runDetachedWatcherConverge(codebaseID, relativePaths, registration)
}

func (syncer *BackgroundSync) runDetachedWatcherConverge(
	codebaseID string,
	relativePaths []string,
	registration convergeJobRegistration,
) {
	registration.withContext(func(runCtx context.Context) {
		lastReportedPaths := int32(-1)
		outcome, runErr := syncer.manager.ConvergePaths(runCtx, codebaseID, relativePaths, func(progress ConvergeOutcome) {
			if progress.PathsProcessed == lastReportedPaths {
				syncer.manager.updateDetachedJobHeartbeat(registration.job.ID)
				return
			}
			percent := 100.0
			if progress.PathsGiven > 0 {
				percent = float64(progress.PathsProcessed) / float64(progress.PathsGiven) * 100
			}
			syncer.manager.updateDetachedJobProgress(registration.job.ID, indexer.Progress{
				Phase:                  "Converging changed paths...",
				OverallPercent:         percent,
				FilesTotal:             progress.PathsGiven,
				FilesProcessed:         progress.PathsProcessed,
				FilesEmbedded:          0,
				FilesSkippedOversize:   0,
				FilesSkippedUnreadable: 0,
				FilesPending:           0,
				ChunksProcessed:        0,
				ChunksReused:           0,
				ChunksEmbedded:         0,
				ChunksGenerated:        0,
				ChunksDropped:          0,
				ReuseVectorsLoaded:     0,
			}, "path")
			lastReportedPaths = progress.PathsProcessed
		})
		terminalCtx := context.WithoutCancel(runCtx)
		switch {
		case runCtx.Err() != nil:
			syncer.manager.updateDetachedJobCancelled(terminalCtx, registration.job.ID)
		case runErr != nil:
			syncer.manager.updateDetachedJobFailed(terminalCtx, registration.job.ID, runErr)
		default:
			syncer.manager.updateDetachedJobCompleted(terminalCtx, registration.job.ID, indexer.Result{
				IndexedFiles:      outcome.PathsProcessed,
				TotalChunks:       0,
				TotalBytes:        0,
				Chunks:            nil,
				FileHashes:        nil,
				SkippedFiles:      nil,
				SkippedOversize:   0,
				SkippedUnreadable: 0,
				SkippedPending:    0,
			})
		}
	})
}

type convergeJobRegistration struct {
	job         model.Job
	withContext func(func(context.Context))
	release     func()
}

func (syncer *BackgroundSync) registerConvergeJob(
	ctx context.Context,
	codebase model.Codebase,
	relativePaths []string,
) (convergeJobRegistration, error) {
	now := clock.Now()
	job := newQueuedJob(
		codebase.ID,
		codebase.CanonicalPath,
		codebase.CanonicalPath,
		model.ClientInfo{Name: "daemon-watcher", PID: 0},
		"converge",
		false,
		codebase.EffectiveConfig,
		emptyAdmissionBudget,
		now,
	)
	job.Progress.FilesTotal = safeInt32(len(relativePaths))
	job.Progress.Unit = "path"

	jobCorr := correlation.FromContext(ctx).WithIdentityAttributes(
		correlation.IdentityAttribute{Key: "job_id", Value: job.ID},
	)
	jobCtx, cancel := context.WithCancel(correlation.WithContext(ctx, jobCorr))

	syncer.manager.mu.Lock()
	current, found := syncer.manager.codebases[codebase.ID]
	if !found || current.ActiveJobID != "" || current.Status != codebase.Status || current.ActiveJobID != codebase.ActiveJobID || !current.UpdatedAt.Equal(codebase.UpdatedAt) {
		syncer.manager.mu.Unlock()
		cancel()
		syncer.requeuePaths(codebase.ID, relativePaths)
		return convergeJobRegistration{}, fmt.Errorf("start converge job: codebase ownership changed")
	}
	syncer.manager.cancels[job.ID] = cancel
	_, effectivePolicy, policyErr := syncer.manager.resolveIndexPolicyLocked(current, indexPolicyIntent{
		Patch:      model.SchedulingPolicyPatch{Priority: nil, Quiet: nil, IdleAfterSeconds: nil},
		Initialize: false,
	})
	if policyErr != nil {
		delete(syncer.manager.cancels, job.ID)
		syncer.manager.mu.Unlock()
		cancel()
		syncer.requeuePaths(codebase.ID, relativePaths)
		return convergeJobRegistration{}, policyErr
	}
	applyJobSchedulingPolicy(&job, effectivePolicy, model.SchedulingPolicyPatch{Priority: nil, Quiet: nil, IdleAfterSeconds: nil}, syncer.manager.nextQueueSequenceLocked())
	// A converge does not claim codebase.ActiveJobID, so
	// beginActiveJobCancellationLocked cannot route a waiter to it.
	// waitForJobDone accepts a nil channel, so manager.done needs no entry.
	job.State = model.JobStateRunning
	job.UpdatedAt = now
	job.Progress.Phase = "Preparing and scanning files..."
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	job.Progress.OverallPercent = 0
	if err := syncer.manager.appendJobLocked("job_running", job); err != nil {
		delete(syncer.manager.jobs, job.ID)
		delete(syncer.manager.cancels, job.ID)
		syncer.manager.mu.Unlock()
		cancel()
		syncer.requeuePaths(codebase.ID, relativePaths)
		wrapped := fmt.Errorf("append running converge job event: %w", err)
		slog.ErrorContext(jobCtx, "append running converge job event failed", "job_id", job.ID, "err", wrapped)
		return convergeJobRegistration{}, wrapped
	}
	syncer.manager.mu.Unlock()

	return convergeJobRegistration{
		job: job,
		withContext: func(run func(context.Context)) {
			run(jobCtx)
		},
		release: func() {
			cancel()
			syncer.manager.mu.Lock()
			delete(syncer.manager.cancels, job.ID)
			syncer.manager.mu.Unlock()
		},
	}, nil
}

func (syncer *BackgroundSync) watcherCodebase(codebaseID string) (model.Codebase, bool) {
	syncer.manager.mu.Lock()
	defer syncer.manager.mu.Unlock()
	codebase, found := syncer.manager.codebases[codebaseID]
	return codebase, found
}

func (syncer *BackgroundSync) hasActiveJob(codebase model.Codebase) bool {
	syncer.manager.mu.Lock()
	defer syncer.manager.mu.Unlock()
	current, found := syncer.manager.codebases[codebase.ID]
	if !found {
		return false
	}
	return syncer.manager.activeJobSnapshotLocked(current) != nil
}

func (syncer *BackgroundSync) deferWatcherPaths(codebaseID string, relativePaths []string) {
	if len(relativePaths) == 0 {
		return
	}
	syncer.deferredWatcherMu.Lock()
	defer syncer.deferredWatcherMu.Unlock()
	buffered, found := syncer.deferredWatcherPaths[codebaseID]
	if !found {
		buffered = make(map[string]struct{}, len(relativePaths))
		syncer.deferredWatcherPaths[codebaseID] = buffered
	}
	for _, relativePath := range relativePaths {
		buffered[relativePath] = struct{}{}
	}
}

// deferredPathCounts reports how many changed paths each codebase has buffered
// while its first build runs. That work is real and waiting: it converges the
// moment the build promotes, so a status read that omitted it would report a
// quiet system while edits piled up. The returned map is a copy.
func (syncer *BackgroundSync) deferredPathCounts() map[string]int {
	syncer.deferredWatcherMu.Lock()
	defer syncer.deferredWatcherMu.Unlock()

	counts := make(map[string]int, len(syncer.deferredWatcherPaths))
	for codebaseID, paths := range syncer.deferredWatcherPaths {
		counts[codebaseID] = len(paths)
	}
	return counts
}

func (syncer *BackgroundSync) takeDeferredWatcherPaths(codebaseID string) []string {
	syncer.deferredWatcherMu.Lock()
	defer syncer.deferredWatcherMu.Unlock()
	buffered := syncer.deferredWatcherPaths[codebaseID]
	if len(buffered) == 0 {
		return nil
	}
	relativePaths := make([]string, 0, len(buffered))
	for relativePath := range buffered {
		relativePaths = append(relativePaths, relativePath)
	}
	sort.Strings(relativePaths)
	delete(syncer.deferredWatcherPaths, codebaseID)
	return relativePaths
}

// beginConverge claims the per-codebase converge slot, returning false when a
// converge for that codebase is already running.
func (syncer *BackgroundSync) beginConverge(codebaseID string) bool {
	syncer.convergeMu.Lock()
	defer syncer.convergeMu.Unlock()
	if _, admitted := syncer.converging[codebaseID]; admitted {
		return false
	}
	// A zero start time means admitted but not yet running: the converge still
	// has to win an index slot and the shared advisory lock. markConvergeRunning
	// stamps the real time once both waits are behind it, so a status read never
	// reports a queued converge as running work whose age keeps growing.
	syncer.converging[codebaseID] = time.Time{}
	return true
}

// markConvergeRunning records that an admitted converge has taken its index slot
// and the shared advisory lock, so it is doing work rather than waiting for
// capacity.
func (syncer *BackgroundSync) markConvergeRunning(codebaseID string) {
	syncer.convergeMu.Lock()
	defer syncer.convergeMu.Unlock()
	if _, admitted := syncer.converging[codebaseID]; !admitted {
		return
	}
	syncer.converging[codebaseID] = clock.Now()
}

// endConverge releases the per-codebase converge slot.
func (syncer *BackgroundSync) endConverge(codebaseID string) {
	syncer.convergeMu.Lock()
	defer syncer.convergeMu.Unlock()
	delete(syncer.converging, codebaseID)
}

// requeuePaths re-enqueues a deferred converge's paths so the change is picked
// up on the next debounce rather than dropped.
func (syncer *BackgroundSync) requeuePaths(codebaseID string, relativePaths []string) {
	for _, relativePath := range relativePaths {
		syncer.queue.Enqueue(codebaseID, relativePath)
	}
}

func (syncer *BackgroundSync) codebaseChanged(ctx context.Context, codebase model.Codebase) (bool, error) {
	if codebase.Kind == model.CodebaseKindDocument {
		return false, nil
	}

	// The checkpoint read goes through the manager's one live-checkpoint reader,
	// so an empty codebase that never wrote a file is not reported as damaged on
	// every sweep. Its absent checkpoint reads as the empty snapshot it would
	// have written, which matches the empty capture below, so the sweep also
	// stops enqueuing a sync for a codebase where nothing changed. A codebase
	// that did index files and lost its checkpoint still reports the loss, and
	// its empty snapshot differs from a non-empty capture, so the resync it
	// needs still starts.
	existingSnapshot := syncer.manager.loadLiveCheckpoint(ctx, codebase, codebase.EffectiveConfig.IgnoreDigest).snapshot

	currentSnapshot, err := merkle.Capture(
		ctx,
		syncer.manager.indexability,
		codebase.ID,
		codebase.CanonicalPath,
		codebase.EffectiveConfig,
	)
	if err != nil {
		slog.ErrorContext(ctx, "capture Merkle snapshot failed", "path", codebase.CanonicalPath, "err", err)
		return false, fmt.Errorf("capture Merkle snapshot for %s: %w", codebase.CanonicalPath, err)
	}
	if !merkle.Equal(existingSnapshot, currentSnapshot) {
		return true, nil
	}
	presence := syncer.manager.probeCollectionEvidence(ctx, codebase.CanonicalPath, "backgroundSync").presence
	currentSnapshotHash := snapshotHashForGraph(currentSnapshot, codebase.EffectiveConfig.IgnoreDigest)
	return syncer.manager.shouldReconcileGraph(codebase.ID, currentSnapshotHash, presence), nil
}

func syncConflictError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "conflicting active job") ||
		strings.Contains(message, "codebase not tracked")
}

// Watcher activity states describe file-change work before job registration and
// paths waiting behind a running converge.
const (
	WatcherStateRunning = "running"
	WatcherStateQueued  = "queued"
)

// WatcherActivity is one unit of admission or queued-path state the background
// syncer owns. StatusSnapshot removes an admitted entry once its converge job is
// registered, so a status reply reports the work once through the job store.
type WatcherActivity struct {
	CodebaseID   string
	State        string
	PendingPaths int
	StartedAt    time.Time
}

// WatcherActivityReporter is the seam the manager holds so a status read
// reaches file-change work without the manager depending on the syncer.
type WatcherActivityReporter interface {
	WatcherActivity() []WatcherActivity
}

// WatcherActivity reports every admitted converge and every codebase whose
// changed paths are waiting on a debounce timer.
//
// An admitted converge with no start time is still waiting for an index slot or
// the shared advisory lock, so it reports queued rather than running: reporting
// it as running would show an age that measures the wait, not the work. A
// codebase that is converging while new paths accumulate reports once, carrying
// the paths that drain after it. Rows sort by codebase id, so two reads of an
// unchanged daemon render identically.
func (syncer *BackgroundSync) WatcherActivity() []WatcherActivity {
	syncer.convergeMu.Lock()
	admitted := make(map[string]time.Time, len(syncer.converging))
	maps.Copy(admitted, syncer.converging)
	syncer.convergeMu.Unlock()

	// Waiting paths come from two places. The event queue holds those inside a
	// debounce window; the deferred buffer holds those a first build is standing
	// in front of, which converge the moment that build promotes. Both are work
	// the operator is waiting on, so both count toward one figure.
	pending := map[string]int{}
	if syncer.queue != nil {
		pending = syncer.queue.PendingCounts()
	}
	for codebaseID, count := range syncer.deferredPathCounts() {
		pending[codebaseID] += count
	}

	activity := make([]WatcherActivity, 0, len(admitted)+len(pending))
	for codebaseID, startedAt := range admitted {
		state := WatcherStateRunning
		if startedAt.IsZero() {
			state = WatcherStateQueued
		}
		activity = append(activity, WatcherActivity{
			CodebaseID:   codebaseID,
			State:        state,
			PendingPaths: pending[codebaseID],
			StartedAt:    startedAt,
		})
	}
	for codebaseID, count := range pending {
		if _, alreadyAdmitted := admitted[codebaseID]; alreadyAdmitted {
			continue
		}
		activity = append(activity, WatcherActivity{
			CodebaseID:   codebaseID,
			State:        WatcherStateQueued,
			PendingPaths: count,
			StartedAt:    time.Time{},
		})
	}
	sort.Slice(activity, func(first int, second int) bool {
		return activity[first].CodebaseID < activity[second].CodebaseID
	})
	return activity
}
