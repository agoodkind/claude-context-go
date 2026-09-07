// Package daemon owns persisted daemon state and request coordination.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"goodkind.io/gklog/correlation"
	"goodkind.io/lm-semantic-search/internal/cbm"
	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/config"
	"goodkind.io/lm-semantic-search/internal/gitworktree"
	"goodkind.io/lm-semantic-search/internal/indexability"
	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/semantic"
	"goodkind.io/lm-semantic-search/internal/spans"
	"goodkind.io/lm-semantic-search/internal/store"
)

// jobOperation tags one daemon job so runJob can route it to the right
// execution path. The model.Job.Operation field is a plain string for wire
// compatibility, but the daemon's internal switch uses this named type so
// staticcheck can verify the dispatch covers every case.
type jobOperation string

type appendJobEventFunc func(string, model.JobEvent) error

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
	// jobOperationConversationIngest upserts or deletes virtual conversation
	// documents in a document collection.
	jobOperationConversationIngest jobOperation = "conversation_ingest"
)

// CodebaseLifecycleHook is the watcher-side interface the manager calls so a
// codebase's background wiring stays in sync with its lifecycle without a
// restart. AddCodebase and RemoveCodebase start and stop filesystem-event
// watching. IndexReady and IndexStopped report a codebase's first build
// promoting or ending (failed or cancelled), so a deferred-converge implementer
// can flush or drop the watcher paths it buffered during that first build. The
// hook is plugged in via SetCodebaseLifecycleHook; until then the manager is a
// no-op for these callbacks.
type CodebaseLifecycleHook interface {
	AddCodebase(ctx context.Context, codebase model.Codebase)
	RemoveCodebase(ctx context.Context, codebaseID string)
	IndexReady(ctx context.Context, codebase model.Codebase)
	IndexStopped(ctx context.Context, codebaseID string)
}

// Manager coordinates persisted codebase and job state for the daemon.
type Manager struct {
	config config.Config
	// conversationChunkByteBudget is this manager's immutable byte cap for
	// splitting conversation text, derived once from config at construction. It
	// keeps a conversation chunk within the embedder's token limit. Held per
	// manager (not a package global) so managers with different configs, and
	// concurrent construction, never contaminate or race each other.
	conversationChunkByteBudget int
	mu                          sync.Mutex
	codebases                   map[string]model.Codebase
	jobs                        map[string]model.Job
	conversationJobs            map[string]conversationJobPayload
	conversationSyncCursors     map[string]string
	// pendingConversationJobs holds at most one coalesced conversation upsert
	// payload per codebase (depth 1). An upsert that arrives while that codebase
	// has an active job merges into this slot instead of refusing; the slot drains
	// into a fresh job when the active job reaches a terminal state. Guarded by mu.
	pendingConversationJobs map[string]conversationJobPayload
	// pendingCodeJobs holds at most one coalesced code sync request per codebase
	// (depth 1), admitted when a non-matching-config index or sync request arrives
	// while a code job is active and drained on terminal. Guarded by mu.
	pendingCodeJobs         map[string]pendingCodeRequest
	interruptedConvergeJobs map[string]model.Job
	cancels                 map[string]context.CancelFunc
	done                    map[string]chan struct{}
	// failedBuildRetries caps automatic retries for terminal failed builds per daemon lifetime; not persisted, guarded by mu.
	failedBuildRetries map[string]int
	// lastJobJournalAt throttles periodic job-progress journaling; not persisted, guarded by mu.
	lastJobJournalAt map[string]time.Time
	appendJobEvent   appendJobEventFunc
	jobJournal       *jobJournalWriter
	runner           indexingRunner
	semantic         semanticIndex
	graphEngines     map[string]*cbm.Engine
	graphLifecycle   map[string]*graphLifecycleState
	graphMutex       sync.Mutex
	graphIndex       func(context.Context, *cbm.Engine, string, string) error
	graphIndexHook   func()
	lifecycleHook    CodebaseLifecycleHook
	lifecycleMutex   sync.Mutex
	// startedAt is when this daemon process built its manager. A status read
	// reports uptime from it, so the number comes from the process rather than
	// from a caller's clock.
	startedAt time.Time
	// watcherActivity reports file-change admission and queued paths. The
	// background syncer installs itself here at start; StatusSnapshot filters
	// entries that already have a converge job.
	watcherActivity      WatcherActivityReporter
	watcherActivityMutex sync.Mutex
	// indexSlots caps concurrently running index jobs. Each runJob holds one
	// buffered slot for its duration; jobs that cannot acquire a slot stay
	// queued until one frees.
	indexSlots chan struct{}
	// jobCapacityTimings bounds how long a read may stall before the job frees
	// its slot and the sync lock, and how long it may then wait to resume. They
	// are settable so the public-boundary contention tests do not pay the
	// production waits.
	jobCapacityTimings jobCapacityTimings
	// syncLock is the process-wide refcounted hold of the daemon's kernel file
	// lock. Index jobs and background converges take a reference for their embed,
	// so any other process holding the same lock file backs off while a daemon
	// embed runs. The upstream TypeScript tool does not take this lock.
	syncLock *syncLock
	// health is the daemon's view of shared-infrastructure health (the embedding
	// pipeline and the vector store). It is global, not per-codebase, observed
	// from job outcomes, and drives the status banner. Guarded by mu.
	health dependencyHealth
	// dependencyFailureGeneration advances for every dependency failure that
	// reached the record, including a repeat of the mode already on it. A
	// background success compares its starting generation before clearing newer
	// evidence. Guarded by mu.
	dependencyFailureGeneration uint64
	// healthObservations numbers dependency evidence in the order it is gathered.
	// Every observation takes the next number under mu at the moment it starts
	// looking at a dependency, so a probe's number is older than anything recorded
	// while it was in flight. Guarded by mu.
	healthObservations healthObservation
	// appliedStoreObservation and appliedEmbedderObservation are the numbers of
	// the newest evidence already applied for each dependency. An observation
	// numbered below one of them lost the race and its verdict is dropped, in
	// both directions: a slow passing probe cannot erase a newer failure, and a
	// slow failing probe cannot erase a newer success. Guarded by mu.
	appliedStoreObservation    healthObservation
	appliedEmbedderObservation healthObservation
	// lastDepProbeAt debounces refreshDependencyHealth's backend probe. Guarded by mu.
	lastDepProbeAt time.Time
	// deferredBuildDelay is the post-discovery wait before a worktree build starts; settable so a test can keep the timer from firing mid-test.
	deferredBuildDelay time.Duration
	// bootSelfCheckDelay is the wait before the first end-to-end self-check and
	// between retries; settable so tests do not pay the real delay.
	bootSelfCheckDelay time.Duration
	// indexability resolves whether a path should be indexed, caching one
	// git-style ignore matcher per codebase id. Converge and the watcher both
	// route their ignore and scope decisions through it. It reads each codebase's
	// custom ignore patterns straight from the registry source of truth through
	// ignoreOverridesFromRegistry, so per-codebase ignore state lives only in the
	// registry and the matcher cache, never in a third copy.
	indexability *indexability.Resolver
	// observer is the single owner of resolver-cache invalidation. Every path that
	// changes a codebase's ignore rules (a committed config, an adopted or
	// discovered codebase, a live ignore-source edit, or a backstop sweep) routes
	// through it instead of calling the resolver's invalidate directly, so
	// invalidation has exactly one home.
	observer *ignoreObserver
}

// SearchOutcome carries search results plus current indexing context.
// StateNote adds read-only repair or availability context to the rendered
// response when results alone are not enough to explain the current state.
type SearchOutcome struct {
	Codebase  model.Codebase
	ActiveJob *model.Job
	Results   []model.StoredChunk
	StateNote string
}

type indexingRunner interface {
	Index(context.Context, *indexability.Resolver, string, string, model.IndexConfig, func(indexer.Progress)) (indexer.Result, error)
	IndexFiles(context.Context, *indexability.Resolver, string, string, []string, model.IndexConfig, func(indexer.Progress)) (indexer.Result, error)
	IndexOne(context.Context, *indexability.Resolver, string, string, string, model.IndexConfig) (indexer.OneFileResult, error)
}

// NewManager loads persisted daemon state from disk.
func NewManager(ctx context.Context, cfg config.Config) (*Manager, error) {
	return newManagerWithSemanticFactory(ctx, cfg, newSemanticIndex)
}

func newManagerWithSemanticFactory(
	ctx context.Context,
	cfg config.Config,
	semanticFactory func(context.Context, config.Config) (semanticIndex, error),
) (*Manager, error) {
	manager := &Manager{
		config:                      cfg,
		conversationChunkByteBudget: conversationChunkMaxBytes,
		mu:                          sync.Mutex{},
		codebases:                   map[string]model.Codebase{},
		jobs:                        map[string]model.Job{},
		conversationJobs:            map[string]conversationJobPayload{},
		conversationSyncCursors:     map[string]string{},
		pendingConversationJobs:     map[string]conversationJobPayload{},
		pendingCodeJobs:             map[string]pendingCodeRequest{},
		interruptedConvergeJobs:     map[string]model.Job{},
		cancels:                     map[string]context.CancelFunc{},
		done:                        map[string]chan struct{}{},
		failedBuildRetries:          map[string]int{},
		lastJobJournalAt:            map[string]time.Time{},
		appendJobEvent:              store.AppendJobEvent,
		jobJournal:                  nil,
		runner:                      indexer.NewRunner(),
		semantic:                    nil,
		graphEngines:                map[string]*cbm.Engine{},
		graphLifecycle:              map[string]*graphLifecycleState{},
		graphMutex:                  sync.Mutex{},
		graphIndex:                  defaultGraphIndex,
		graphIndexHook:              nil,
		lifecycleHook:               nil,
		lifecycleMutex:              sync.Mutex{},
		startedAt:                   clock.Now(),
		watcherActivity:             nil,
		watcherActivityMutex:        sync.Mutex{},
		indexSlots:                  make(chan struct{}, max(1, cfg.MaxConcurrentIndexJobs)),
		jobCapacityTimings:          defaultJobCapacityTimings(),
		syncLock:                    newSyncLock(filepath.Join(cfg.ContextRoot, "mcp-sync.flock"), cfg.ContextRoot),
		health:                      dependencyHealth{Mode: dependencyHealthy, Since: time.Time{}, StoreReachableAt: time.Time{}, EmbedderReachableAt: time.Time{}},
		dependencyFailureGeneration: 0,
		healthObservations:          0,
		appliedStoreObservation:     0,
		appliedEmbedderObservation:  0,
		lastDepProbeAt:              time.Time{},
		deferredBuildDelay:          defaultDeferredBuildDelay,
		bootSelfCheckDelay:          defaultBootSelfCheckDelay,
		indexability:                nil,
		observer:                    nil,
	}
	// Drop this manager's conversation chunk byte budget from the varchar-safe
	// default (set in the literal above) to the embedding token budget when
	// EmbeddingMaxTokens is set, so a conversation chunk stays within the model's
	// input limit instead of being silently truncated. Stored on the manager, not
	// a package global, so managers never contaminate or race each other.
	if budget := config.EmbedChunkByteBudget(cfg.EmbeddingMaxTokens); budget > 0 && budget < manager.conversationChunkByteBudget {
		manager.conversationChunkByteBudget = budget
	}
	if err := store.EnsureDir(cfg.GraphDir); err != nil {
		slog.ErrorContext(ctx, "create graph cache directory failed", "path", cfg.GraphDir, "err", err)
		return nil, fmt.Errorf("create graph cache directory: %w", err)
	}
	// Force the cbm engine to a single worker. Its parallel extraction worker
	// pool aborts inside cgo when driven repeatedly from this long-lived daemon
	// process, so the graph engine runs sequentially. Graph indexing is a
	// background, non-fatal pass, so the lost parallelism is acceptable.
	if err := os.Setenv("CBM_WORKERS", "1"); err != nil {
		slog.ErrorContext(ctx, "set cbm worker count failed", "err", err)
		return nil, fmt.Errorf("set cbm worker count: %w", err)
	}
	// The resolver reads each codebase's custom ignore patterns straight from the
	// registry source of truth at build time, so per-codebase ignore state has no
	// second copy and never drifts from the registry.
	manager.indexability = indexability.NewResolver(manager.ignoreOverridesFromRegistry, manager.submoduleAllowlistFromRegistry)
	// The observer is the sole caller of the resolver's invalidate, so every
	// consumer signals it instead of invalidating the cache itself.
	manager.observer = newIgnoreObserver(manager.indexability)
	semanticBackend, err := semanticFactory(ctx, cfg)
	if err != nil {
		return nil, err
	}
	manager.semantic = semanticBackend
	if svc, ok := semanticBackend.(*semantic.Service); ok && svc.Degraded() {
		manager.health = dependencyHealth{Mode: dependencyStoreUnavailable, Since: clock.Now(), StoreReachableAt: time.Time{}, EmbedderReachableAt: time.Time{}}
	}
	if err := manager.load(ctx); err != nil {
		slog.ErrorContext(ctx, "load daemon state failed", "state_root", cfg.StateRoot, "err", err)
		loadErr := fmt.Errorf("load daemon state: %w", err)
		if closeErr := manager.Close(context.WithoutCancel(ctx)); closeErr != nil {
			slog.ErrorContext(ctx, "close manager after state load failure", "err", closeErr)
			return nil, errors.Join(loadErr, fmt.Errorf("close manager after state load failure: %w", closeErr))
		}
		return nil, loadErr
	}
	manager.jobJournal = newJobJournalWriter(
		cfg.JobsPath,
		func(path string, event model.JobEvent) error {
			return manager.appendJobEvent(path, event)
		},
		jobJournalQueueCapacity,
	)
	return manager, nil
}

// dropGhostURICodebases removes code-kind records whose canonical path is a
// filesystem-mangled URI, which the previous boot resume path could create by
// running [filepath.Abs] on a chat URI. A legitimate conversation codebase keeps
// its scheme intact and its kind set to document, so it never matches.
func dropGhostURICodebases(codebases map[string]model.Codebase) {
	for id, codebase := range codebases {
		if codebase.Kind == model.CodebaseKindDocument {
			continue
		}
		segments := strings.SplitN(strings.TrimPrefix(codebase.CanonicalPath, "/"), "/", 2)
		if len(segments) > 0 && strings.HasSuffix(segments[0], ":") {
			slog.Warn("dropping ghost URI codebase record", "codebase_id", id, "path", codebase.CanonicalPath)
			delete(codebases, id)
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
	if manager.jobJournal != nil {
		return manager.jobJournal.enqueue(jobEvent)
	}
	return appendJobJournalEvent(manager.config.JobsPath, manager.appendJobEvent, jobEvent)
}

// Version returns daemon runtime path details.
func (manager *Manager) Version() map[string]string {
	return map[string]string{
		"state_root":  manager.config.StateRoot,
		"socket_path": manager.config.SocketPath,
	}
}

func newCodebaseRecord(canonicalPath string) model.Codebase {
	return model.Codebase{
		ID:                newID("cb"),
		Kind:              model.CodebaseKindCode,
		CanonicalPath:     canonicalPath,
		Status:            model.CodebaseStatusNotIndexed,
		ActiveJobID:       "",
		LastSuccessfulRun: nil,
		LastFailedRun:     nil,
		LiveFileTotal:     0,
		LiveChunkTotal:    0,
		EffectiveConfig: model.IndexConfig{
			SplitterType:       "",
			SplitterChunkSize:  0,
			SplitterOverlap:    0,
			IgnorePatterns:     nil,
			IncludeSubmodules:  nil,
			IgnoreDigest:       "",
			EmbeddingProvider:  "",
			EmbeddingModel:     "",
			EmbeddingDimension: 0,
			VectorBackend:      "",
			Hybrid:             false,
		},
		SchedulingPolicy:            model.DefaultSchedulingPolicy(),
		PolicyPendingInitialization: false,
		CollectionName:              "",
		LegacyCollectionNames:       nil,
		MerkleSnapshotPath:          "",
		GraphState:                  "",
		GraphSnapshotHash:           "",
		GraphUpdatedAt:              time.Time{},
		Quarantine:                  nil,
		WorktreeCommonDir:           "",
		InodeTrackingDisabled:       false,
		UpdatedAt:                   clock.Now(),
	}
}

// startIndexDecision captures one StartIndex call's resolved codebase plus
// the routing decision derived from the current registry state.
type startIndexDecision struct {
	codebase  model.Codebase
	activeJob model.Job
	dedup     bool
	// coalesce is set when an in-flight job carries a different config: the commit
	// path records a pending code request and returns the active job rather than
	// refusing with a conflict error.
	coalesce bool
	mode     startIndexMode
}

// decideStartIndexLocked resolves the codebase record and routing mode from
// the registry plus the caller-provided collection presence. The shared
// collection-state policy decides whether this call is already indexed, should
// queue an incremental run, or must bootstrap. Caller must hold manager.mu.
func (manager *Manager) decideStartIndexLocked(canonicalPath string, indexConfig model.IndexConfig, force bool, presence collectionPresence) (startIndexDecision, error) {
	var emptyJob model.Job
	codebase, found := manager.findCodebaseByExactRoot(canonicalPath)
	if !found {
		fresh := newCodebaseRecord(canonicalPath)
		if presence == collectionPresencePresent {
			fresh.Status = model.CodebaseStatusIndexed
		}
		return startIndexDecision{
			codebase:  fresh,
			activeJob: emptyJob,
			dedup:     false,
			coalesce:  false,
			mode:      decideStartIndexMode(false, fresh.Status, false, force, presence),
		}, nil
	}
	activeJob, resolution, err := manager.activeJobLocked(codebase, indexConfig)
	if err != nil {
		return startIndexDecision{}, err
	}
	switch resolution {
	case activeJobDedup:
		return startIndexDecision{
			codebase:  codebase,
			activeJob: activeJob,
			dedup:     true,
			coalesce:  false,
			mode:      startIndexModeBootstrap,
		}, nil
	case activeJobConflict:
		return startIndexDecision{
			codebase:  codebase,
			activeJob: activeJob,
			dedup:     false,
			coalesce:  true,
			mode:      startIndexModeBootstrap,
		}, nil
	case activeJobNone:
	}
	return startIndexDecision{
		codebase:  codebase,
		activeJob: emptyJob,
		dedup:     false,
		coalesce:  false,
		mode: decideStartIndexMode(
			true,
			codebase.Status,
			codebase.EffectiveConfig.IgnoreDigest == indexConfig.IgnoreDigest,
			force,
			presence,
		),
	}, nil
}

// StartIndex registers a new indexing job or deduplicates an existing one.
//
// Returns the queued or in-flight job, the resolved codebase record, a
// deduplicated flag (true when the call matched an in-flight job), and the
// id of any existing codebase whose canonical path strictly prefix-covers
// the new registration (empty when no overlap).
func (manager *Manager) StartIndex(ctx context.Context, requestedPath string, client model.ClientInfo, indexConfig model.IndexConfig, force bool, budget model.AdmissionBudget) (model.Job, model.Codebase, bool, string, error) {
	return manager.StartIndexWithPolicy(ctx, requestedPath, client, indexConfig, force, budget, model.SchedulingPolicyPatch{Priority: nil, Quiet: nil, IdleAfterSeconds: nil})
}

// StartIndexWithPolicy starts an index with a per-run scheduling-policy patch.
func (manager *Manager) StartIndexWithPolicy(ctx context.Context, requestedPath string, client model.ClientInfo, indexConfig model.IndexConfig, force bool, budget model.AdmissionBudget, policyPatch model.SchedulingPolicyPatch) (model.Job, model.Codebase, bool, string, error) {
	return manager.startIndexWithIntent(ctx, requestedPath, client, indexConfig, force, budget, indexPolicyIntent{
		Patch:      policyPatch,
		Initialize: true,
	})
}

func (manager *Manager) startIndexWithIntent(ctx context.Context, requestedPath string, client model.ClientInfo, indexConfig model.IndexConfig, force bool, budget model.AdmissionBudget, policyIntent indexPolicyIntent) (model.Job, model.Codebase, bool, string, error) {
	return manager.startIndexWithRecovery(ctx, requestedPath, client, indexConfig, force, budget, policyIntent, nil)
}

func (manager *Manager) startIndexWithRecovery(ctx context.Context, requestedPath string, client model.ClientInfo, indexConfig model.IndexConfig, force bool, budget model.AdmissionBudget, policyIntent indexPolicyIntent, recoveredPlan *resumePlan) (model.Job, model.Codebase, bool, string, error) {
	var emptyJob model.Job
	var emptyCodebase model.Codebase

	canonicalPath, err := manager.resolveCanonicalPath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		return emptyJob, emptyCodebase, false, "", fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	if err := manager.guardStateRoot(canonicalPath); err != nil {
		return emptyJob, emptyCodebase, false, "", err
	}
	if err := manager.guardFilesystemRoot(canonicalPath); err != nil {
		return emptyJob, emptyCodebase, false, "", err
	}
	if err := manager.guardDirectory(canonicalPath); err != nil {
		return emptyJob, emptyCodebase, false, "", err
	}

	// Merge-up: a nested path already covered by an indexed parent does not get
	// its own redundant index. Resolve to the covering parent and sync it so the
	// requested subtree is current, rather than building a second overlapping
	// collection over the shared files. A git worktree root is exempt: it shares
	// the parent's repo group but holds a different branch, so it stays its own
	// codebase rather than merging into a sibling worktree.
	if ancestor, found := manager.mergeUpTarget(canonicalPath); found && !manager.isWorktreeBoundary(canonicalPath, ancestor) {
		return manager.redirectIndexToAncestor(ctx, requestedPath, ancestor, client, policyIntent.Patch)
	}

	indexConfig = manager.enrichIndexConfig(indexConfig)
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)

	if dedupedJob, dedupedCodebase, deduped := manager.dedupAgainstActiveJob(canonicalPath, indexConfig); deduped {
		resolvedCodebase, resolveErr := manager.resolveAndPersistIndexPolicy(dedupedCodebase.ID, policyIntent)
		if resolveErr != nil {
			return emptyJob, emptyCodebase, false, "", resolveErr
		}
		return dedupedJob, resolvedCodebase, true, "", nil
	}

	if force {
		if err := manager.cancelActiveJobForPath(ctx, canonicalPath); err != nil {
			return emptyJob, emptyCodebase, false, "", err
		}
	}

	evidence := manager.probeCollectionEvidence(ctx, canonicalPath, "StartIndex")

	job, codebase, deduped, overlapsCodebaseID, err := manager.commitStartIndexLocked(ctx, canonicalPath, requestedPath, client, indexConfig, force, evidence.presence, budget, policyIntent, recoveredPlan)
	if err != nil || deduped {
		return job, codebase, deduped, overlapsCodebaseID, err
	}
	if job.ID == "" {
		return emptyJob, codebase, false, overlapsCodebaseID, nil
	}
	notifyCtx := correlation.WithContext(context.WithoutCancel(ctx), correlation.FromContext(ctx).Child())
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(notifyCtx, "notify codebase added panic", "codebase_id", codebase.ID, "err", recovered)
			}
		}()
		manager.notifyCodebaseAdded(notifyCtx, codebase)
	}()
	ctx = spans.Attach(ctx, correlation.IdentityAttribute{Key: "job_id", Value: job.ID}, correlation.IdentityAttribute{Key: "codebase_id", Value: codebase.ID})
	manager.runJobAsync(ctx, job.ID)
	return job, codebase, false, overlapsCodebaseID, nil
}

// commitStartIndexLocked acquires the registry lock, runs the decision
// table, applies the resulting codebase mutation, persists the registry,
// and queues the job event. The returned job has an empty ID when the
// decision resolved as already-indexed; the caller treats that as a no-op
// success.
func (manager *Manager) commitStartIndexLocked(ctx context.Context, canonicalPath string, requestedPath string, client model.ClientInfo, indexConfig model.IndexConfig, force bool, presence collectionPresence, budget model.AdmissionBudget, policyIntent indexPolicyIntent, recoveredPlan *resumePlan) (model.Job, model.Codebase, bool, string, error) {
	var emptyJob model.Job
	var emptyCodebase model.Codebase
	manager.mu.Lock()
	defer manager.mu.Unlock()

	decision, err := manager.decideStartIndexLocked(canonicalPath, indexConfig, force, presence)
	if err != nil {
		slog.ErrorContext(ctx, "resolve active job failed", "canonical_path", canonicalPath, "err", err)
		return emptyJob, emptyCodebase, false, "", err
	}
	resolvedCodebase, effectivePolicy, err := manager.resolveIndexPolicyLocked(decision.codebase, policyIntent)
	if err != nil {
		return emptyJob, emptyCodebase, false, "", err
	}
	originalCodebase := decision.codebase
	decision.codebase = resolvedCodebase
	if decision.dedup {
		if err := manager.persistResolvedIndexPolicyLocked(originalCodebase, resolvedCodebase); err != nil {
			return emptyJob, emptyCodebase, false, "", err
		}
		return decision.activeJob, decision.codebase, true, "", nil
	}
	if decision.coalesce {
		// A non-matching-config request arriving while a code job is active coalesces
		// onto the depth-1 pending slot and returns the active job as deduplicated, so
		// the caller treats it as success rather than a conflict. The slot drains into
		// a fresh sync when the active job reaches a terminal state.
		manager.mergePendingCodeRequestLocked(decision.codebase.ID, pendingCodeRequest{
			requestedPath: requestedPath,
			canonicalPath: canonicalPath,
			client:        client,
			indexConfig:   indexConfig,
			force:         force,
			policyPatch:   policyIntent.Patch,
		})
		return decision.activeJob, decision.codebase, true, "", nil
	}
	overlapsCodebaseID := ""
	if ancestor, found := manager.findStrictAncestor(canonicalPath); found {
		overlapsCodebaseID = ancestor.ID
	}
	if decision.mode == startIndexModeAlreadyIndexed {
		if err := manager.persistResolvedIndexPolicyLocked(originalCodebase, resolvedCodebase); err != nil {
			return emptyJob, emptyCodebase, false, "", err
		}
		return emptyJob, decision.codebase, false, overlapsCodebaseID, nil
	}

	codebase := decision.codebase
	// A first build is pending until its queued job acquires a slot; updateJobRunning
	// flips it to indexing. A rebuild of an already-indexed codebase stays indexing so
	// it keeps reading as active work over the prior index, not as a fresh request.
	codebase.Status = model.CodebaseStatusPending
	if codebase.LastSuccessfulRun != nil {
		codebase.Status = model.CodebaseStatusIndexing
	}
	codebase.EffectiveConfig = indexConfig
	codebase.InodeTrackingDisabled = detectInodeTrackingDisabled(ctx, canonicalPath)
	if manager.semantic != nil && manager.semantic.Available() {
		codebase.CollectionName = manager.semantic.CollectionName(canonicalPath)
	}
	if info, ok := gitworktree.Resolve(canonicalPath); ok && info.Linked {
		codebase.WorktreeCommonDir = info.CommonDir
	}
	codebase.UpdatedAt = clock.Now()

	operation := jobOperationIndex
	if decision.mode == startIndexModeIncremental {
		operation = jobOperationStreamingReindex
	}
	job := newQueuedJob(codebase.ID, requestedPath, canonicalPath, client, string(operation), force, indexConfig, budget, clock.Now())
	if recoveredPlan != nil {
		queueSequence := recoveredPlan.queueSequence
		if queueSequence == 0 {
			queueSequence = manager.nextQueueSequenceLocked()
		}
		applyJobSchedulingPolicy(&job, recoveredPlan.effectiveSchedulingPolicy, recoveredPlan.schedulingOverride, queueSequence)
	} else {
		applyJobSchedulingPolicy(&job, effectivePolicy, policyIntent.Patch, manager.nextQueueSequenceLocked())
	}
	codebase.ActiveJobID = job.ID
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		return emptyJob, emptyCodebase, false, "", err
	}
	if err := manager.appendJobLocked("start_index", job); err != nil {
		return emptyJob, emptyCodebase, false, "", err
	}
	// The committed EffectiveConfig may carry new custom ignore patterns, so signal
	// the observer to invalidate the cached matcher; the next live decision rebuilds
	// it from the registry source of truth.
	manager.observer.Invalidate(codebase.ID)
	return job, codebase, false, overlapsCodebaseID, nil
}

// SyncIndex registers a new sync job for an existing tracked codebase.
func (manager *Manager) SyncIndex(ctx context.Context, requestedPath string, client model.ClientInfo) (model.Job, model.Codebase, bool, error) {
	return manager.SyncIndexWithPolicy(ctx, requestedPath, client, model.SchedulingPolicyPatch{Priority: nil, Quiet: nil, IdleAfterSeconds: nil})
}

// SyncIndexWithPolicy starts a sync with a per-run scheduling-policy patch.
func (manager *Manager) SyncIndexWithPolicy(ctx context.Context, requestedPath string, client model.ClientInfo, policyPatch model.SchedulingPolicyPatch) (model.Job, model.Codebase, bool, error) {
	canonicalPath, err := manager.resolveCanonicalPath(requestedPath)
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
	resolvedCodebase, _, policyErr := manager.resolveIndexPolicyLocked(codebase, indexPolicyIntent{
		Patch:      policyPatch,
		Initialize: false,
	})
	if policyErr != nil {
		manager.mu.Unlock()
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false, policyErr
	}
	codebase = resolvedCodebase

	indexConfig := manager.enrichIndexConfig(codebase.EffectiveConfig)
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)

	activeJob, resolution, err := manager.activeJobLocked(codebase, indexConfig)
	if err != nil {
		slog.ErrorContext(ctx, "resolve active sync job failed", "canonical_path", canonicalPath, "err", err)
		manager.mu.Unlock()
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false, err
	}
	if resolution == activeJobDedup {
		manager.mu.Unlock()
		return activeJob, codebase, true, nil
	}
	if resolution == activeJobConflict {
		// A non-matching-config sync arriving while a code job is active coalesces
		// onto the depth-1 pending slot and returns the active job as deduplicated, so
		// the caller treats it as success. The slot drains on the active job's
		// terminal transition.
		manager.mergePendingCodeRequestLocked(codebase.ID, pendingCodeRequest{
			requestedPath: requestedPath,
			canonicalPath: canonicalPath,
			client:        client,
			indexConfig:   indexConfig,
			force:         false,
			policyPatch:   policyPatch,
		})
		manager.mu.Unlock()
		return activeJob, codebase, true, nil
	}

	job, err := manager.enqueueCodeSyncJobLocked(codebase, pendingCodeRequest{
		requestedPath: requestedPath,
		canonicalPath: canonicalPath,
		client:        client,
		indexConfig:   indexConfig,
		force:         false,
		policyPatch:   policyPatch,
	})
	if err != nil {
		manager.mu.Unlock()
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false, err
	}
	updatedCodebase := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	ctx = spans.Attach(ctx, correlation.IdentityAttribute{Key: "job_id", Value: job.ID}, correlation.IdentityAttribute{Key: "codebase_id", Value: codebase.ID})
	manager.runJobAsync(ctx, job.ID)
	return job, updatedCodebase, false, nil
}

// ClearIndex removes a tracked codebase from daemon state.
func (manager *Manager) ClearIndex(ctx context.Context, requestedPath string, client model.ClientInfo) (model.Codebase, error) {
	_ = client

	canonicalPath, err := manager.resolveCanonicalPath(requestedPath)
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
	// Drop any coalesced pending work before cancelling the active job, so the
	// cancel's terminal transition does not drain a successor into a codebase this
	// call is about to remove.
	delete(manager.pendingConversationJobs, codebase.ID)
	delete(manager.pendingCodeJobs, codebase.ID)
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
	if err := manager.clearGraphCache(ctx, codebase.ID); err != nil {
		return model.Codebase{}, fmt.Errorf("remove graph cache for %s: %w", codebase.ID, err)
	}
	if err := store.RemoveFile(manager.stagingMerklePath(codebase.ID)); err != nil {
		return model.Codebase{}, fmt.Errorf("remove staging Merkle snapshot for %s: %w", codebase.ID, err)
	}
	if manager.semantic != nil {
		if err := manager.semantic.Drop(ctx, codebase.CanonicalPath); err != nil && !errors.Is(err, semantic.ErrUnavailable) {
			return model.Codebase{}, fmt.Errorf("drop semantic index for %s: %w", codebase.CanonicalPath, err)
		}
		if err := manager.semantic.DropStaging(ctx, codebase.CanonicalPath); err != nil && !errors.Is(err, semantic.ErrUnavailable) {
			return model.Codebase{}, fmt.Errorf("drop semantic staging for %s: %w", codebase.CanonicalPath, err)
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
func (manager *Manager) CancelJob(ctx context.Context, jobID string) (model.Job, error) {
	manager.mu.Lock()

	job, found := manager.jobs[jobID]
	if !found {
		manager.mu.Unlock()
		return model.Job{}, fmt.Errorf("job not found: %s", jobID)
	}
	if job.State == model.JobStateCompleted || job.State == model.JobStateFailed || job.State == model.JobStateCancelled {
		manager.mu.Unlock()
		return job, nil
	}
	delete(manager.conversationJobs, jobID)

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
		manager.mu.Unlock()
		return model.Job{}, err
	}

	codebaseID := job.CodebaseID
	var saveErr error
	drainedJobID := ""
	drained := false
	codebase, found := manager.codebases[job.CodebaseID]
	if found && codebase.ActiveJobID == job.ID {
		// A cancellation is not a failure: leave the codebase at its last-good
		// state so a status check reflects the current usable state.
		codebase.ActiveJobID = ""
		codebase.UpdatedAt = now
		manager.codebases[codebase.ID] = codebase
		if err := manager.saveLocked(); err != nil {
			saveErr = err
		}
		// A submission coalesced onto this now-cancelled job still runs: drain the
		// depth-1 pending slot into a fresh successor.
		drainedJobID, drained = manager.drainPendingJobLocked(ctx, codebase.ID)
	}

	manager.mu.Unlock()
	manager.notifyIndexStopped(ctx, codebaseID)
	if drained {
		manager.runDrainedJob(ctx, codebaseID, drainedJobID)
	}
	if saveErr != nil {
		return model.Job{}, saveErr
	}
	return job, nil
}

// Codebase lifecycle hook plumbing lives in manager_lifecycle.go.

// GetIndex / classification / synthesis helpers live in manager_status.go.

// Codebase listing lives in manager_list.go.

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
func (manager *Manager) Doctor(ctx context.Context) []string {
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
		if diagnostic := manager.graphDiagnostic(ctx, codebase); diagnostic != "" {
			diagnostics = append(diagnostics, diagnostic)
		}
		if codebase.LastSuccessfulRun == nil {
			continue
		}
		skipped := len(codebase.LastSuccessfulRun.SkippedFiles)
		if skipped == 0 {
			continue
		}
		diagnostics = append(diagnostics, fmt.Sprintf(
			"%s: %d non-UTF-8 %s skipped during last indexing run",
			codebase.CanonicalPath,
			skipped,
			plural("file", skipped),
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
	// The early dedup runs before StartIndex's force-cancel, so it only collapses a
	// matching-config request; a conflict is left for the commit path to coalesce
	// after any force-cancel has cleared the active job.
	activeJob, resolution, err := manager.activeJobLocked(existingCodebase, indexConfig)
	if err != nil || resolution != activeJobDedup {
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false
	}
	return activeJob, existingCodebase, true
}

// Delta sync helpers live in manager_delta.go. Job state mutators live in
// manager_jobs_state.go. SearchCode lives in manager_search.go. Path helpers
// live in manager_paths.go. Config helpers and id helpers live in
// manager_config.go. Boundary guards (StateRoot, directory, inode-stability)
// live in manager_guards.go. Active-job snapshot and cancellation helpers live
// in manager_active_job.go.
