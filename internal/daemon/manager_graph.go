package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"goodkind.io/lm-semantic-search/internal/adapterr"
	"goodkind.io/lm-semantic-search/internal/cbm"
	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/merkle"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/store"
)

type (
	graphToolArguments map[string]json.RawMessage
	graphIndexTask     struct {
		codebaseID    string
		canonicalPath string
		snapshotHash  string
		complete      func(context.Context)
	}
	graphLifecycleState struct {
		clearing bool
		indexing bool
		active   int
		idle     *sync.Cond
	}
)

const defaultGraphIndexTimeout = 5 * time.Minute

var allowedGraphTools = map[string]struct{}{
	"query_graph":      {},
	"trace_path":       {},
	"get_architecture": {},
	"manage_adr":       {},
}

// GraphTool calls a cbm graph engine tool for a tracked codebase.
func (manager *Manager) GraphTool(ctx context.Context, codebaseID string, toolName string, argsJSON string) (string, error) {
	if err := validateGraphToolName(toolName); err != nil {
		return "", err
	}

	engine, release, err := manager.graphEngine(ctx, codebaseID)
	if err != nil {
		var adapterErr *adapterr.AdapterError
		if errors.As(err, &adapterErr) {
			return "", err
		}
		slog.ErrorContext(ctx, "open graph engine failed", "codebase_id", codebaseID, "err", err)
		return "", fmt.Errorf("open graph engine for %s: %w", codebaseID, err)
	}
	defer release()

	enrichedArgs, err := MarshalGraphToolArguments(argsJSON, codebaseID)
	if err != nil {
		return "", err
	}
	resultJSON, err := engine.Tool(toolName, enrichedArgs)
	if err != nil {
		slog.ErrorContext(ctx, "graph tool call failed", "codebase_id", codebaseID, "tool_name", toolName, "err", err)
		return "", fmt.Errorf("call graph tool %s for %s: %w", toolName, codebaseID, err)
	}
	return resultJSON, nil
}

func validateGraphToolName(toolName string) error {
	if _, found := allowedGraphTools[toolName]; found {
		return nil
	}
	return adapterr.NewInvalidArgument(fmt.Sprintf("unsupported graph tool_name %q", toolName))
}

func newGraphIndexTask(codebaseID string, canonicalPath string, snapshotHash string, complete func(context.Context)) *graphIndexTask {
	return &graphIndexTask{
		codebaseID:    codebaseID,
		canonicalPath: canonicalPath,
		snapshotHash:  snapshotHash,
		complete:      complete,
	}
}

func (manager *Manager) runGraphIndexTask(ctx context.Context, task *graphIndexTask) {
	if task == nil {
		return
	}
	manager.recordGraphIndexNonFatal(ctx, task.codebaseID, task.canonicalPath, task.snapshotHash)
	if task.complete != nil {
		task.complete(ctx)
	}
}

func (manager *Manager) graphLifecycleStateLocked(codebaseID string) *graphLifecycleState {
	state, found := manager.graphLifecycle[codebaseID]
	if found {
		return state
	}
	state = &graphLifecycleState{
		clearing: false,
		indexing: false,
		active:   0,
		idle:     sync.NewCond(&manager.graphMutex),
	}
	manager.graphLifecycle[codebaseID] = state
	return state
}

func (manager *Manager) graphIndexing(codebaseID string) bool {
	if codebaseID == "" {
		return false
	}
	manager.graphMutex.Lock()
	defer manager.graphMutex.Unlock()

	state := manager.graphLifecycle[codebaseID]
	return state != nil && state.indexing
}

// beginGraphIndex claims the single in-flight graph-index slot for codebaseID.
// It returns ok=false when an index for this codebase is already running, so a
// redundant concurrent trigger (for example a sync sweep overlapping a manual
// build) skips instead of parsing the same tree twice into the same db. The
// release closure frees the slot. The daemon already dedups same-codebase index
// jobs, so this is a defensive guard against overlapping graph passes rather
// than the primary serialization.
func (manager *Manager) beginGraphIndex(codebaseID string) (func(), bool) {
	manager.graphMutex.Lock()
	defer manager.graphMutex.Unlock()

	state := manager.graphLifecycleStateLocked(codebaseID)
	if state.indexing {
		return nil, false
	}
	state.indexing = true

	return func() {
		manager.graphMutex.Lock()
		defer manager.graphMutex.Unlock()
		state.indexing = false
	}, true
}

func (manager *Manager) beginGraphOperation(codebaseID string) (func(), error) {
	manager.graphMutex.Lock()
	defer manager.graphMutex.Unlock()

	state := manager.graphLifecycleStateLocked(codebaseID)
	if state.clearing {
		return nil, adapterr.NewConflictingJob("graph cache for "+codebaseID+" is being cleared", nil)
	}
	state.active++

	return func() {
		manager.graphMutex.Lock()
		defer manager.graphMutex.Unlock()

		state.active--
		if state.active == 0 {
			state.idle.Broadcast()
		}
	}, nil
}

func (manager *Manager) graphEngine(ctx context.Context, codebaseID string) (*cbm.Engine, func(), error) {
	release, err := manager.beginGraphOperation(codebaseID)
	if err != nil {
		return nil, nil, err
	}

	manager.graphMutex.Lock()
	defer manager.graphMutex.Unlock()

	if engine, found := manager.graphEngines[codebaseID]; found {
		return engine, release, nil
	}

	engine, err := cbm.Open(codebaseID, manager.config.GraphDir)
	if err != nil {
		manager.graphMutex.Unlock()
		release()
		manager.graphMutex.Lock()
		slog.ErrorContext(ctx, "open cbm engine failed", "codebase_id", codebaseID, "err", err)
		return nil, nil, fmt.Errorf("open cbm engine: %w", err)
	}
	manager.graphEngines[codebaseID] = engine
	return engine, release, nil
}

// CloseGraphEngines flushes the manager's journal writer, closes every idle
// cached graph engine, and blocks new graph operations. An engine with an
// in-flight call is left open on purpose: the blocking C call cannot be
// interrupted, closing under it would free memory the call still reads, and
// waiting for it could stall shutdown behind a detached post-timeout call, so
// process exit reclaims that handle instead. It is safe to call more than once.
func (manager *Manager) CloseGraphEngines() {
	manager.closeJobJournal()

	manager.graphMutex.Lock()
	defer manager.graphMutex.Unlock()

	for codebaseID, engine := range manager.graphEngines {
		state := manager.graphLifecycleStateLocked(codebaseID)
		state.clearing = true
		if state.active > 0 {
			slog.Warn("graph engine left open at shutdown; in-flight call still running", "codebase_id", codebaseID, "active", state.active)
			continue
		}
		engine.Close()
		delete(manager.graphEngines, codebaseID)
	}
}

func defaultGraphIndex(ctx context.Context, engine *cbm.Engine, canonicalPath string, mode string) error {
	if err := engine.Index(ctx, canonicalPath, mode); err != nil {
		slog.WarnContext(ctx, "cbm graph index failed", "path", canonicalPath, "err", err)
		return fmt.Errorf("cbm index graph: %w", err)
	}
	return nil
}

func (manager *Manager) clearGraphCache(ctx context.Context, codebaseID string) error {
	manager.graphMutex.Lock()
	state := manager.graphLifecycleStateLocked(codebaseID)
	state.clearing = true
	for state.active > 0 {
		state.idle.Wait()
	}

	if engine, found := manager.graphEngines[codebaseID]; found {
		engine.Close()
		delete(manager.graphEngines, codebaseID)
	}
	var removeErr error
	for _, path := range manager.graphPaths(codebaseID) {
		if err := store.RemoveFile(path); err != nil {
			slog.ErrorContext(ctx, "remove graph file failed", "codebase_id", codebaseID, "path", path, "err", err)
			if removeErr == nil {
				removeErr = fmt.Errorf("remove graph file %s: %w", path, err)
			}
		}
	}
	delete(manager.graphLifecycle, codebaseID)
	manager.graphMutex.Unlock()
	return removeErr
}

// indexGraphNonFatal runs one bounded graph index pass. releaseIndex frees the
// caller's in-flight dupe-guard slot and runs when the worker goroutine exits,
// not when this function returns: a cancel or timeout detaches from the
// blocking C call, and the slot must stay held until that call actually
// finishes so a new trigger cannot start a second parse alongside it.
func (manager *Manager) indexGraphNonFatal(ctx context.Context, codebaseID string, canonicalPath string, releaseIndex func()) error {
	if releaseIndex == nil {
		releaseIndex = func() {}
	}
	if err := ctx.Err(); err != nil {
		releaseIndex()
		slog.WarnContext(ctx, "graph indexing skipped because job is cancelled", "codebase_id", codebaseID, "path", canonicalPath, "err", err)
		return fmt.Errorf("index graph cancelled before start: %w", err)
	}

	engine, release, err := manager.graphEngine(ctx, codebaseID)
	if err != nil {
		releaseIndex()
		slog.WarnContext(ctx, "open graph engine failed; continuing without graph index", "codebase_id", codebaseID, "err", err)
		return fmt.Errorf("open graph engine: %w", err)
	}

	result := make(chan error, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(ctx, "graph index worker panicked", "codebase_id", codebaseID, "err", recovered)
				result <- fmt.Errorf("graph index worker panicked: %v", recovered)
			}
		}()
		defer releaseIndex()
		defer release()
		result <- manager.graphIndex(ctx, engine, canonicalPath, "fast")
	}()

	timer := time.NewTimer(defaultGraphIndexTimeout)
	defer timer.Stop()

	select {
	case err = <-result:
	case <-ctx.Done():
		err = fmt.Errorf("index graph cancelled: %w", ctx.Err())
	case <-timer.C:
		err = fmt.Errorf("index graph timed out after %s", defaultGraphIndexTimeout)
	}
	if err != nil {
		slog.WarnContext(ctx, "graph indexing failed; continuing with semantic index", "codebase_id", codebaseID, "path", canonicalPath, "err", err)
		return fmt.Errorf("index graph: %w", err)
	}
	return nil
}

func (manager *Manager) recordGraphIndexNonFatal(ctx context.Context, codebaseID string, canonicalPath string, snapshotHash string) {
	releaseIndex, ok := manager.beginGraphIndex(codebaseID)
	if !ok {
		slog.InfoContext(ctx, "graph index already in flight; skipping duplicate", "codebase_id", codebaseID, "path", canonicalPath)
		return
	}

	if manager.graphIndexHook != nil {
		manager.graphIndexHook()
	}
	err := manager.indexGraphNonFatal(ctx, codebaseID, canonicalPath, releaseIndex)
	if err == nil {
		manager.updateGraphState(ctx, codebaseID, model.GraphStateReady, snapshotHash)
		return
	}
	manager.updateGraphState(ctx, codebaseID, model.GraphStateStale, "")
}

func (manager *Manager) updateGraphState(ctx context.Context, codebaseID string, graphState model.GraphState, snapshotHash string) {
	manager.policyMutationMutex.Lock()
	defer manager.policyMutationMutex.Unlock()

	manager.mu.Lock()
	defer manager.mu.Unlock()

	codebase, found := manager.codebases[codebaseID]
	if !found {
		return
	}
	now := clock.Now()
	codebase.GraphState = graphState
	if graphState != model.GraphStateReady {
		codebase.GraphSnapshotHash = ""
	} else if snapshotHash != "" {
		codebase.GraphSnapshotHash = snapshotHash
	}
	if graphState == model.GraphStateReady {
		codebase.GraphUpdatedAt = now
	}
	codebase.UpdatedAt = now
	manager.codebases[codebaseID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after graph state update failed", "codebase_id", codebaseID, "err", err)
	}
}

func (manager *Manager) shouldReconcileGraph(codebaseID string, currentSnapshotHash string, presence collectionPresence) bool {
	if presence != collectionPresencePresent {
		return false
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	codebase, found := manager.codebases[codebaseID]
	if !found {
		return false
	}
	if codebase.Kind == model.CodebaseKindDocument {
		return false
	}
	graphState := codebase.GraphState
	if graphState == "" {
		graphState = model.GraphStateAbsent
	}
	return graphState != model.GraphStateReady || codebase.GraphSnapshotHash != currentSnapshotHash
}

func snapshotHashForGraph(snapshot merkle.Snapshot, configDigest string) string {
	snapshot.ConfigDigest = configDigest
	return snapshot.Hash()
}

func (manager *Manager) graphDiagnostic(ctx context.Context, codebase model.Codebase) string {
	if codebase.Kind == model.CodebaseKindDocument {
		return ""
	}
	graphState := codebase.GraphState
	if graphState == "" {
		graphState = model.GraphStateAbsent
	}

	checkpoint := manager.loadLiveCheckpoint(ctx, codebase, codebase.EffectiveConfig.IgnoreDigest)
	if checkpoint.state == liveCheckpointNeverWritten {
		// A codebase whose last completed run indexed no file has no files for a
		// code graph to cover, so there is nothing here an operator could act on.
		// Reporting the checkpoint it never wrote would leave doctor listing a
		// healthy empty repository as a problem for as long as it stays tracked.
		return ""
	}
	if !checkpoint.usable() {
		return codebase.CanonicalPath + ": can't confirm the code graph is current"
	}
	currentHash := snapshotHashForGraph(checkpoint.snapshot, codebase.EffectiveConfig.IgnoreDigest)
	if graphState == model.GraphStateReady && codebase.GraphSnapshotHash == currentHash {
		return ""
	}
	if graphState == model.GraphStateStale {
		return codebase.CanonicalPath + ": code graph's last update didn't finish (retries automatically)"
	}
	return codebase.CanonicalPath + ": code graph is behind the current files (rebuilds automatically)"
}

func (manager *Manager) graphPaths(codebaseID string) []string {
	basePath := filepath.Join(manager.config.GraphDir, codebaseID+".db")
	return []string{
		basePath,
		basePath + "-wal",
		basePath + "-shm",
	}
}

// MarshalGraphToolArguments returns graph tool arguments with the daemon-owned project id.
func MarshalGraphToolArguments(argsJSON string, codebaseID string) (string, error) {
	var args graphToolArguments
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		slog.Warn("unmarshal graph tool arguments failed", "codebase_id", codebaseID, "err", err)
		return "", adapterr.NewInvalidArgument("args_json must be a JSON object or null")
	}
	if args == nil {
		args = graphToolArguments{}
	}

	projectJSON, err := json.Marshal(codebaseID)
	if err != nil {
		slog.Error("marshal graph project argument failed", "codebase_id", codebaseID, "err", err)
		return "", fmt.Errorf("marshal graph project argument: %w", err)
	}
	args["project"] = projectJSON

	enrichedArgs, err := json.Marshal(args)
	if err != nil {
		slog.Error("marshal graph tool arguments failed", "codebase_id", codebaseID, "err", err)
		return "", fmt.Errorf("marshal graph tool arguments: %w", err)
	}
	return string(enrichedArgs), nil
}
