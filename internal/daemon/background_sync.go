package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"goodkind.io/claude-context-go/internal/adapterr"
	"goodkind.io/claude-context-go/internal/clock"
	"goodkind.io/claude-context-go/internal/config"
	"goodkind.io/claude-context-go/internal/merkle"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/store"
	"goodkind.io/gklog/correlation"
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
type BackgroundSync struct {
	cfg     config.Config
	manager *Manager

	mu           sync.Mutex
	syncing      bool
	triggerTimer *time.Timer
	lastTrigger  time.Time

	queue   *EventQueue
	watcher *Watcher
}

// NewBackgroundSync constructs the daemon background sync coordinator.
func NewBackgroundSync(cfg config.Config, manager *Manager) *BackgroundSync {
	return &BackgroundSync{
		cfg:          cfg,
		manager:      manager,
		mu:           sync.Mutex{},
		syncing:      false,
		triggerTimer: nil,
		lastTrigger:  time.Time{},
		queue:        nil,
		watcher:      nil,
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
		syncer.manager.SetCodebaseLifecycleHook(syncer.watcher)
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
		go func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					slog.ErrorContext(ctx, "background sync loop panic", "loop", "runPeriodicSync", "err", recovered)
				}
			}()
			syncer.runPeriodicSync(ctx)
		}()
	}
}

func (syncer *BackgroundSync) runPeriodicSync(ctx context.Context) {
	initialTimer := time.NewTimer(defaultInitialSyncDelay)
	defer initialTimer.Stop()

	syncInterval := time.Duration(syncer.cfg.SyncIntervalMS) * time.Millisecond
	if syncer.cfg.SyncIntervalMS < minimumSyncIntervalMS {
		syncInterval = time.Duration(minimumSyncIntervalMS) * time.Millisecond
	}
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-initialTimer.C:
			syncer.runSyncAll(ctx, "startup")
		case <-ticker.C:
			syncer.runSyncAll(ctx, "interval")
		}
	}
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

	if !syncer.beginSync(ctx, source) {
		return
	}
	defer syncer.endSync(ctx, source)

	codebases := syncer.manager.ListIndexes(ctx)
	for _, codebase := range codebases {
		if codebase.Status != model.CodebaseStatusIndexed {
			continue
		}
		if _, err := os.Stat(codebase.CanonicalPath); errors.Is(err, os.ErrNotExist) {
			continue
		}

		syntheticJobID := fmt.Sprintf("sync-%s-%d", codebase.ID, clock.Now().Unix())
		iterCtx := correlation.WithContext(ctx, correlation.FromContext(ctx).Child().WithIdentityAttributes(
			correlation.IdentityAttribute{Key: "codebase_id", Value: codebase.ID},
			correlation.IdentityAttribute{Key: "job_id", Value: syntheticJobID},
		))

		changed, err := syncer.codebaseChanged(iterCtx, codebase)
		if err != nil {
			slog.ErrorContext(iterCtx, "check sync state failed", "path", codebase.CanonicalPath, "err", err)
			continue
		}
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

// convergeViaWatcher runs a per-path converge for the debounced path set from
// the file watcher, serialized against the periodic sweep through the same
// single-flight guard. When a sweep already holds the guard, the paths are
// requeued so the change is not lost.
func (syncer *BackgroundSync) convergeViaWatcher(ctx context.Context, codebaseID string, relativePaths []string) {
	if ctx.Err() != nil {
		return
	}

	corr := correlation.New("").WithIdentityAttributes(
		correlation.IdentityAttribute{Key: "origin", Value: "watcher"},
		correlation.IdentityAttribute{Key: "codebase_id", Value: codebaseID},
		correlation.IdentityAttribute{Key: "job_id", Value: fmt.Sprintf("watch-%s-%d", codebaseID, clock.Now().Unix())},
	)
	ctx = correlation.WithContext(ctx, corr)

	if !syncer.beginSync(ctx, "watcher") {
		for _, relativePath := range relativePaths {
			syncer.queue.Enqueue(codebaseID, relativePath)
		}
		return
	}
	defer syncer.endSync(ctx, "watcher")

	if err := syncer.manager.ConvergePaths(ctx, codebaseID, relativePaths); err != nil {
		slog.ErrorContext(ctx, "watcher.converge_failed", "component", "daemon", "subcomponent", "watcher", "codebase_id", codebaseID, "err", err)
	}
}

func (syncer *BackgroundSync) codebaseChanged(ctx context.Context, codebase model.Codebase) (bool, error) {
	snapshotPath := codebase.MerkleSnapshotPath
	if strings.TrimSpace(snapshotPath) == "" {
		snapshotPath = syncer.manager.merklePath(codebase.ID)
	}

	existingSnapshot, err := merkle.ReadSnapshot(snapshotPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		slog.ErrorContext(ctx, "read Merkle snapshot failed", "path", snapshotPath, "err", err)
		return false, fmt.Errorf("read Merkle snapshot %s: %w", snapshotPath, err)
	}

	currentSnapshot, err := merkle.Capture(
		ctx,
		codebase.CanonicalPath,
		codebase.EffectiveConfig,
	)
	if err != nil {
		slog.ErrorContext(ctx, "capture Merkle snapshot failed", "path", codebase.CanonicalPath, "err", err)
		return false, fmt.Errorf("capture Merkle snapshot for %s: %w", codebase.CanonicalPath, err)
	}
	return !merkle.Equal(existingSnapshot, currentSnapshot), nil
}

func (syncer *BackgroundSync) beginSync(ctx context.Context, source string) bool {
	syncer.mu.Lock()
	if syncer.syncing {
		syncer.mu.Unlock()
		return false
	}
	syncer.syncing = true
	syncer.mu.Unlock()

	if ok := syncer.acquireGlobalLock(ctx, source); ok {
		return true
	}

	syncer.mu.Lock()
	syncer.syncing = false
	syncer.mu.Unlock()
	return false
}

func (syncer *BackgroundSync) endSync(ctx context.Context, source string) {
	syncer.releaseGlobalLock(ctx, source)
	syncer.mu.Lock()
	syncer.syncing = false
	syncer.mu.Unlock()
}

func (syncer *BackgroundSync) acquireGlobalLock(ctx context.Context, source string) bool {
	lockPath := filepath.Join(syncer.cfg.ContextRoot, "mcp-sync.lock")
	if err := store.EnsureDir(syncer.cfg.ContextRoot); err != nil {
		slog.ErrorContext(ctx, "ensure sync root failed", "path", syncer.cfg.ContextRoot, "err", err)
		return false
	}

	if err := os.Mkdir(lockPath, 0o755); err == nil {
		return true
	} else if !errors.Is(err, os.ErrExist) {
		slog.ErrorContext(ctx, "acquire sync lock failed", "path", lockPath, "source", source, "err", err)
		return false
	}

	info, err := os.Stat(lockPath)
	if err != nil {
		slog.ErrorContext(ctx, "inspect sync lock failed", "path", lockPath, "source", source, "err", err)
		return false
	}
	staleAge := time.Duration(syncer.cfg.SyncLockStaleMS) * time.Millisecond
	if syncer.cfg.SyncLockStaleMS <= 0 {
		staleAge = 10 * time.Minute
	}
	if clock.Now().Sub(info.ModTime()) <= staleAge {
		return false
	}

	if err := os.RemoveAll(lockPath); err != nil {
		slog.ErrorContext(ctx, "remove stale sync lock failed", "path", lockPath, "source", source, "err", err)
		return false
	}
	if err := os.Mkdir(lockPath, 0o755); err != nil {
		slog.ErrorContext(ctx, "reacquire sync lock failed", "path", lockPath, "source", source, "err", err)
		return false
	}
	return true
}

func (syncer *BackgroundSync) releaseGlobalLock(ctx context.Context, source string) {
	lockPath := filepath.Join(syncer.cfg.ContextRoot, "mcp-sync.lock")
	if err := os.RemoveAll(lockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.ErrorContext(ctx, "release sync lock failed", "path", lockPath, "source", source, "err", err)
	}
}

func syncConflictError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "conflicting active job") ||
		strings.Contains(message, "codebase not tracked")
}
