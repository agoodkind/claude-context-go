package daemon

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/config"
	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/merkle"
	"goodkind.io/lm-semantic-search/internal/metrics"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/semantic"
	"goodkind.io/lm-semantic-search/internal/tshash"
)

// fakeSemantic is a semanticIndex double for converge tests. reindex and
// copyChunks are the only behaviors a converge exercises; the rest return inert
// values so the manager treats the backend as available and empty.
type fakeSemantic struct {
	unavailable bool
	probeErr    error
	// backendName and embeddingProviderName are what this double reports about
	// itself. They default to the local backend and the embedded model so a test
	// that does not care about identity still describes a coherent backend, and a
	// test that does care states the value it wants outright.
	//
	// reportProviderVerbatim turns off the default, so a test can describe a
	// backend that built no embedder at all rather than one that left the field
	// unset.
	backendName            model.VectorBackend
	embeddingProviderName  model.EmbeddingProvider
	reportProviderVerbatim bool
	// probe, when set, replaces the canned probe outcome so a test can observe
	// the context the probe runs under or hold the probe open. probeCount records
	// every ProbeHealth call, which is how a test measures probe volume rather
	// than assuming the debounce holds.
	probe                 func(ctx context.Context) error
	probeCount            atomic.Int64
	ensureMmap            func(context.Context)
	backfillCollections   func(context.Context)
	reindex               func(ctx context.Context, codebasePath string, chunks []model.StoredChunk, removed []string) error
	reindexWithReuse      func(ctx context.Context, codebasePath string, chunks []model.StoredChunk, removed []string, progress func(semantic.Progress), reuse map[string][]float32) error
	stageReindexWithReuse func(ctx context.Context, codebasePath string, chunks []model.StoredChunk, removed []string, progress func(semantic.Progress), reuse map[string][]float32) error
	copyChunks            func(ctx context.Context, codebasePath string, src string, dst string) (int, error)
	deleteConversation    func(ctx context.Context, collectionName string, conversationID string) error
	backfillConversations func(ctx context.Context, collectionName string, enrichment semantic.ConversationEnrichment, dryRun bool) (int, int, error)
	collectionName        func(codebasePath string) string
	conversationName      func(collectionID string) string
	inspectCollection     func(context.Context, string) (semantic.CollectionFacts, error)
	listCollections       func(context.Context) ([]string, error)
	hasCollectionForPath  func(context.Context, string) (bool, error)
	collectionState       func(context.Context, string) (bool, bool, error)
	observeCollection     func(context.Context, string) (semantic.CollectionObservation, error)
	hasStaging            func(context.Context, string) (bool, error)
	search                func(context.Context, string, string, int32, []string, string) ([]model.StoredChunk, error)
	conversationSearch    func(context.Context, string, string, int32) ([]model.StoredChunk, error)
	prepareCollection     func(context.Context, string) error
	acquireCollection     func(context.Context, string) (semantic.CollectionLease, error)
	pinStaging            func(context.Context, string) (semantic.CollectionPin, error)
	count                 func(context.Context, string) (int32, error)
	// loadReuse, when set, supplies the reuse map a merge-down build receives and
	// records which collections were asked for. dropped records every Drop call
	// so a test can prove an absorb never drops the absorbed child collection.
	loadReuse        func(ctx context.Context, collectionNames []string) (map[string][]float32, error)
	reuseCollections [][]string
	// loadReuseForPrefix, when set, supplies the per-conversation reuse map a
	// conversation ingest loads by conv/<id>/ prefix; reusePrefixCalls records
	// every such load and reindexReuse records the reuse map each conversation's
	// Reindex actually received.
	loadReuseForPrefix   func(ctx context.Context, collectionName string, relativePathPrefix string) (map[string][]float32, error)
	reusePrefixCalls     []reusePrefixCall
	loadReuseForPath     func(ctx context.Context, collectionName string, relativePath string) (map[string][]float32, error)
	reusePathCalls       []reusePathCall
	loadReuseForContents func(ctx context.Context, collectionName string, chunks []model.StoredChunk) (map[string][]float32, error)
	loadMessageState     func(ctx context.Context, collectionName string, conversationPrefix string) (map[int32]semantic.StoredMessageState, map[string][]float32, error)
	messageStateCalls    []messageStateCall
	// loadDerivedBatch, when set, supplies the batched stored-row read the
	// examination path issues once per run; derivedBatchCalls records the
	// conversation-id batches each call asked for. When it is nil but
	// loadMessageState is set, LoadConversationDerivedBatch synthesizes the batch
	// from per-conversation state so existing base-text integration tests keep
	// their fixtures.
	loadDerivedBatch  func(ctx context.Context, collectionName string, conversationIDs []string) (semantic.ConversationBatchState, error)
	derivedBatchCalls [][]string
	reindexReuse      map[string]map[string][]float32
	// conversationSearchScopes records the conversation-id scope each
	// conversation search received, so tests can prove native scoping.
	conversationSearchScopes [][]string
	dropped                  []string
	droppedStaging           []string
	reindexCalls             []reindexCall
	stageCalls               []reindexCall
	promoted                 []string
	promoteStaging           func(context.Context, string) error
	// reindexEmit, when set, is invoked with the live progress callback during
	// Reindex and StageReindex so a test can drive reuse-vs-embed progress
	// reporting, including a conversation ingest's batch progress.
	reindexEmit func(progress func(semantic.Progress))
	mu          sync.Mutex
}

type reindexCall struct {
	CodebasePath string
	Chunks       int
	Removed      []string
	Removal      semantic.Removal
	ColumnSet    semantic.StoreColumnSet
}

func (f *fakeSemantic) BackendName() model.VectorBackend {
	if f.backendName != "" {
		return f.backendName
	}
	return config.IndexBackendLocal
}

func (f *fakeSemantic) EmbeddingProviderName() model.EmbeddingProvider {
	if f.reportProviderVerbatim || f.embeddingProviderName != "" {
		return f.embeddingProviderName
	}
	return config.EmbeddingProviderONNX
}

func (f *fakeSemantic) Available() bool { return !f.unavailable }
func (f *fakeSemantic) ProbeHealth(ctx context.Context) error {
	f.probeCount.Add(1)
	if f.probe != nil {
		return f.probe(ctx)
	}
	if f.unavailable {
		return semantic.ErrUnavailable
	}
	return f.probeErr
}

func (f *fakeSemantic) CollectionName(codebasePath string) string {
	if f.collectionName != nil {
		return f.collectionName(codebasePath)
	}
	return "code_chunks_test"
}

func (f *fakeSemantic) ConversationCollectionName(collectionID string) string {
	if f.conversationName != nil {
		return f.conversationName(collectionID)
	}
	return "conv_chunks_" + tshash.PathPrefix(collectionID)
}

func (f *fakeSemantic) HasStaging(ctx context.Context, codebasePath string) (bool, error) {
	if f.hasStaging != nil {
		return f.hasStaging(ctx, codebasePath)
	}
	return false, nil
}

func (f *fakeSemantic) Search(ctx context.Context, codebasePath string, query string, limit int32, extensionFilter []string, relativePathPrefix string) ([]model.StoredChunk, error) {
	if f.search != nil {
		return f.search(ctx, codebasePath, query, limit, extensionFilter, relativePathPrefix)
	}
	return nil, nil
}

func (f *fakeSemantic) AcquireCollection(
	ctx context.Context,
	collectionName string,
) (semantic.CollectionLease, error) {
	if f.acquireCollection != nil {
		return f.acquireCollection(ctx, collectionName)
	}
	return fakeCollectionLease{}, nil
}

func (f *fakeSemantic) PrepareCollection(ctx context.Context, collectionName string) error {
	if f.prepareCollection != nil {
		return f.prepareCollection(ctx, collectionName)
	}
	return nil
}

func (f *fakeSemantic) PinStaging(
	ctx context.Context,
	codebasePath string,
) (semantic.CollectionPin, error) {
	if f.pinStaging != nil {
		return f.pinStaging(ctx, codebasePath)
	}
	return fakeCollectionLease{}, nil
}

type fakeCollectionLease struct {
	release func()
}

func (lease fakeCollectionLease) Release() {
	if lease.release != nil {
		lease.release()
	}
}

func (lease fakeCollectionLease) ReleaseContext(context.Context) {
	lease.Release()
}

func (f *fakeSemantic) SearchConversationCollectionCapped(ctx context.Context, collectionName string, query string, limit int32, _ int32, _ float64, filter semantic.ConversationFilter) ([]model.StoredChunk, error) {
	f.mu.Lock()
	f.conversationSearchScopes = append(f.conversationSearchScopes, append([]string(nil), filter.ConversationIDs...))
	f.mu.Unlock()
	if f.conversationSearch != nil {
		return f.conversationSearch(ctx, collectionName, query, limit)
	}
	return nil, nil
}

func (f *fakeSemantic) Count(ctx context.Context, codebasePath string) (int32, error) {
	if f.count != nil {
		return f.count(ctx, codebasePath)
	}
	return 0, nil
}

func (f *fakeSemantic) ListCollections(ctx context.Context) ([]string, error) {
	if f.listCollections != nil {
		return f.listCollections(ctx)
	}
	return []string{"code_chunks_test"}, nil
}

func (f *fakeSemantic) InspectCollection(ctx context.Context, collectionName string) (semantic.CollectionFacts, error) {
	if f.inspectCollection != nil {
		return f.inspectCollection(ctx, collectionName)
	}
	if f.hasCollectionForPath != nil {
		// This fallback preserves older repair-test fixtures, but this path
		// passes a collection name. Fixtures keyed by codebase path must set
		// inspectCollection explicitly so they do not compare unlike values.
		exists, err := f.hasCollectionForPath(ctx, collectionName)
		if err != nil {
			return semantic.CollectionFacts{}, err
		}
		if !exists {
			return semantic.CollectionFacts{Exists: false, Rows: 0, RowsKnown: false}, nil
		}
	}
	return semantic.CollectionFacts{Exists: true, Rows: 1, RowsKnown: true}, nil
}

func (f *fakeSemantic) HasCollectionForPath(ctx context.Context, codebasePath string) (bool, error) {
	if f.hasCollectionForPath != nil {
		return f.hasCollectionForPath(ctx, codebasePath)
	}
	return true, nil
}

func (f *fakeSemantic) CollectionState(ctx context.Context, codebasePath string) (bool, bool, error) {
	if f.collectionState != nil {
		return f.collectionState(ctx, codebasePath)
	}
	return true, true, nil
}

func (f *fakeSemantic) ObserveCollection(
	ctx context.Context,
	codebasePath string,
) (semantic.CollectionObservation, error) {
	if f.observeCollection != nil {
		return f.observeCollection(ctx, codebasePath)
	}
	exists, loaded, err := f.CollectionState(ctx, codebasePath)
	if err != nil {
		return semantic.CollectionObservation{}, err
	}
	state := semantic.CollectionStateReady
	if !exists {
		state = semantic.CollectionStateAbsent
	} else if !loaded {
		state = semantic.CollectionStateLoading
	}
	observation := semantic.CollectionObservation{State: state}
	if state == semantic.CollectionStateReady {
		rows, countErr := f.Count(ctx, codebasePath)
		if countErr == nil {
			observation.Rows = rows
			observation.RowsKnown = true
		}
	}
	return observation, nil
}

func (f *fakeSemantic) LoadReuseVectors(ctx context.Context, collectionNames []string) (map[string][]float32, error) {
	f.mu.Lock()
	f.reuseCollections = append(f.reuseCollections, collectionNames)
	f.mu.Unlock()
	if f.loadReuse != nil {
		return f.loadReuse(ctx, collectionNames)
	}
	return map[string][]float32{}, nil
}

// reusePrefixCall records one prefix-scoped reuse load: the collection asked
// for and the relativePath prefix that scoped the read.
type reusePrefixCall struct {
	Collection string
	Prefix     string
}

// reusePathCall records one exact-path reuse load.
type reusePathCall struct {
	Collection string
	Path       string
}

// messageStateCall records one per-message state load.
type messageStateCall struct {
	Collection string
	Prefix     string
}

func (f *fakeSemantic) LoadReuseVectorsForPrefix(ctx context.Context, collectionName string, relativePathPrefix string) (map[string][]float32, error) {
	f.mu.Lock()
	f.reusePrefixCalls = append(f.reusePrefixCalls, reusePrefixCall{Collection: collectionName, Prefix: relativePathPrefix})
	f.mu.Unlock()
	if f.loadReuseForPrefix != nil {
		return f.loadReuseForPrefix(ctx, collectionName, relativePathPrefix)
	}
	return map[string][]float32{}, nil
}

func (f *fakeSemantic) LoadReuseVectorsForPath(ctx context.Context, collectionName string, relativePath string) (map[string][]float32, error) {
	f.mu.Lock()
	f.reusePathCalls = append(f.reusePathCalls, reusePathCall{Collection: collectionName, Path: relativePath})
	f.mu.Unlock()
	if f.loadReuseForPath != nil {
		return f.loadReuseForPath(ctx, collectionName, relativePath)
	}
	return map[string][]float32{}, nil
}

func (f *fakeSemantic) LoadReuseVectorsForContents(
	ctx context.Context,
	collectionName string,
	chunks []model.StoredChunk,
) (map[string][]float32, error) {
	if f.loadReuseForContents != nil {
		return f.loadReuseForContents(ctx, collectionName, chunks)
	}
	return map[string][]float32{}, nil
}

func (f *fakeSemantic) LoadConversationMessageState(ctx context.Context, collectionName string, conversationPrefix string) (map[int32]semantic.StoredMessageState, map[string][]float32, error) {
	f.mu.Lock()
	f.messageStateCalls = append(f.messageStateCalls, messageStateCall{Collection: collectionName, Prefix: conversationPrefix})
	f.mu.Unlock()
	if f.loadMessageState != nil {
		return f.loadMessageState(ctx, collectionName, conversationPrefix)
	}
	return map[int32]semantic.StoredMessageState{}, map[string][]float32{}, nil
}

func (f *fakeSemantic) LoadConversationDerivedBatch(ctx context.Context, collectionName string, conversationIDs []string) (semantic.ConversationBatchState, error) {
	f.mu.Lock()
	f.derivedBatchCalls = append(f.derivedBatchCalls, append([]string(nil), conversationIDs...))
	f.mu.Unlock()
	if f.loadDerivedBatch != nil {
		return f.loadDerivedBatch(ctx, collectionName, conversationIDs)
	}
	if f.loadMessageState != nil {
		return f.conversationBatchFromMessageState(ctx, collectionName, conversationIDs)
	}
	return semantic.ConversationBatchState{Rows: map[string]semantic.ConversationStoredRows{}, Reuse: map[string][]float32{}}, nil
}

// conversationBatchFromMessageState synthesizes a batched read from the
// per-conversation loadMessageState hook, so a base-text integration test that
// only stubs message state keeps working. It carries no derived-path identities,
// so a test that stores derived rows must stub loadDerivedBatch directly.
func (f *fakeSemantic) conversationBatchFromMessageState(ctx context.Context, collectionName string, conversationIDs []string) (semantic.ConversationBatchState, error) {
	state := semantic.ConversationBatchState{Rows: map[string]semantic.ConversationStoredRows{}, Reuse: map[string][]float32{}}
	for _, conversationID := range conversationIDs {
		messages, reuse, err := f.LoadConversationMessageState(ctx, collectionName, "conv/"+conversationID+"/")
		if err != nil {
			return semantic.ConversationBatchState{}, err
		}
		state.Rows[conversationID] = semantic.ConversationStoredRows{Messages: messages, DerivedPaths: map[string]string{}}
		for key, vector := range reuse {
			state.Reuse[key] = vector
		}
	}
	return state, nil
}

func (f *fakeSemantic) derivedBatchCallsSnapshot() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]string(nil), f.derivedBatchCalls...)
}

// reusePrefixCallsSnapshot returns a copy of the recorded prefix reuse loads.
func (f *fakeSemantic) reusePrefixCallsSnapshot() []reusePrefixCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]reusePrefixCall(nil), f.reusePrefixCalls...)
}

func (f *fakeSemantic) reusePathCallsSnapshot() []reusePathCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]reusePathCall(nil), f.reusePathCalls...)
}

func (f *fakeSemantic) messageStateCallsSnapshot() []messageStateCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]messageStateCall(nil), f.messageStateCalls...)
}

// reindexReuseSnapshot returns, per conversation id, a copy of the reuse map
// the last Reindex for that conversation received.
func (f *fakeSemantic) reindexReuseSnapshot() map[string]map[string][]float32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]map[string][]float32, len(f.reindexReuse))
	for conversationID, reuse := range f.reindexReuse {
		copied := make(map[string][]float32, len(reuse))
		maps.Copy(copied, reuse)
		out[conversationID] = copied
	}
	return out
}

// recordReindexReuse stores the reuse map a Reindex call carried, keyed by the
// conversation id of its first chunk, so conversation tests can assert which
// reuse map reached which conversation's reindex.
func (f *fakeSemantic) recordReindexReuse(chunks []model.StoredChunk, reuse map[string][]float32) {
	if len(chunks) == 0 || chunks[0].ConversationID == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reindexReuse == nil {
		f.reindexReuse = make(map[string]map[string][]float32)
	}
	copied := make(map[string][]float32, len(reuse))
	maps.Copy(copied, reuse)
	f.reindexReuse[chunks[0].ConversationID] = copied
}

func (f *fakeSemantic) Reindex(ctx context.Context, codebasePath string, chunks []model.StoredChunk, removal semantic.Removal, progress func(semantic.Progress), reuse map[string][]float32, columnSet semantic.StoreColumnSet) error {
	recordedRemoval := copyRemoval(removal)
	f.mu.Lock()
	f.reindexCalls = append(f.reindexCalls, reindexCall{CodebasePath: codebasePath, Chunks: len(chunks), Removed: removalPaths(recordedRemoval), Removal: recordedRemoval, ColumnSet: columnSet})
	f.mu.Unlock()
	f.recordReindexReuse(chunks, reuse)
	if f.reindexWithReuse != nil {
		return f.reindexWithReuse(ctx, codebasePath, chunks, removalPaths(removal), progress, reuse)
	}
	if f.reindexEmit != nil && progress != nil {
		f.reindexEmit(progress)
	}
	if f.reindex != nil {
		return f.reindex(ctx, codebasePath, chunks, removalPaths(removal))
	}
	return nil
}

func (f *fakeSemantic) StageReindex(ctx context.Context, codebasePath string, chunks []model.StoredChunk, removal semantic.Removal, progress func(semantic.Progress), reuse map[string][]float32, columnSet semantic.StoreColumnSet) error {
	recordedRemoval := copyRemoval(removal)
	f.mu.Lock()
	f.stageCalls = append(f.stageCalls, reindexCall{CodebasePath: codebasePath, Chunks: len(chunks), Removed: removalPaths(recordedRemoval), Removal: recordedRemoval, ColumnSet: columnSet})
	f.mu.Unlock()
	f.recordReindexReuse(chunks, reuse)
	if f.stageReindexWithReuse != nil {
		return f.stageReindexWithReuse(ctx, codebasePath, chunks, removalPaths(removal), progress, reuse)
	}
	if f.reindexEmit != nil && progress != nil {
		f.reindexEmit(progress)
	}
	return nil
}

func (f *fakeSemantic) PromoteStaging(ctx context.Context, codebasePath string) error {
	f.mu.Lock()
	f.promoted = append(f.promoted, codebasePath)
	f.mu.Unlock()
	if f.promoteStaging != nil {
		return f.promoteStaging(ctx, codebasePath)
	}
	return nil
}

// removalPaths flattens a removal into the legacy path list the converge tests
// assert on: exact paths first, then prefixes.
func removalPaths(removal semantic.Removal) []string {
	combined := make([]string, 0, len(removal.Paths)+len(removal.Prefixes))
	combined = append(combined, removal.Paths...)
	combined = append(combined, removal.Prefixes...)
	return combined
}

func copyRemoval(removal semantic.Removal) semantic.Removal {
	return semantic.Removal{
		Paths:    append([]string(nil), removal.Paths...),
		Prefixes: append([]string(nil), removal.Prefixes...),
	}
}

func (f *fakeSemantic) DeleteConversation(ctx context.Context, collectionName string, conversationID string) error {
	if f.deleteConversation != nil {
		return f.deleteConversation(ctx, collectionName, conversationID)
	}
	return nil
}

func (f *fakeSemantic) BackfillConversationEnrichment(ctx context.Context, collectionName string, enrichment semantic.ConversationEnrichment, dryRun bool) (int, int, error) {
	if f.backfillConversations != nil {
		return f.backfillConversations(ctx, collectionName, enrichment, dryRun)
	}
	return 0, 0, nil
}

func (f *fakeSemantic) CopyChunks(ctx context.Context, codebasePath string, src string, dst string) (int, error) {
	if f.copyChunks != nil {
		return f.copyChunks(ctx, codebasePath, src, dst)
	}
	return 0, nil
}

func (f *fakeSemantic) PruneToCurrent(context.Context, string, []string) error { return nil }

func (f *fakeSemantic) EnsureMmapEnabledAllCollections(ctx context.Context) {
	if f.ensureMmap != nil {
		f.ensureMmap(ctx)
	}
}

func (f *fakeSemantic) BackfillConversationCollectionsOnce(ctx context.Context) {
	if f.backfillCollections != nil {
		f.backfillCollections(ctx)
	}
}

func (f *fakeSemantic) Drop(_ context.Context, codebasePath string) error {
	f.mu.Lock()
	f.dropped = append(f.dropped, codebasePath)
	f.mu.Unlock()
	return nil
}

func (f *fakeSemantic) DropStaging(_ context.Context, codebasePath string) error {
	f.mu.Lock()
	f.droppedStaging = append(f.droppedStaging, codebasePath)
	f.mu.Unlock()
	return nil
}

// TestConvergeViaWatcherRunsCodebasesConcurrentlyUpToCap proves that several
// codebases converge at once up to the index-slot cap while another waits, that
// the shared lock is held while any converge runs, and that another holder can
// acquire it once all converges finish.
func TestConvergeViaWatcherRunsCodebasesConcurrentlyUpToCap(t *testing.T) {
	const cap = 2
	const codebases = 3

	manager, cfg := newTestManagerWithCap(t, cap)
	entered := make(chan struct{}, codebases)
	release := make(chan struct{})
	inFlight := atomic.Int32{}
	maxInFlight := atomic.Int32{}
	manager.semantic = &fakeSemantic{
		reindex: func(_ context.Context, _ string, _ []model.StoredChunk, _ []string) error {
			current := inFlight.Add(1)
			for {
				observed := maxInFlight.Load()
				if current <= observed || maxInFlight.CompareAndSwap(observed, current) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			inFlight.Add(-1)
			return nil
		},
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.queue = NewEventQueue(time.Hour, func(string, []string) {})

	ids := make([]string, 0, codebases)
	for i := range codebases {
		canonical := newCapTestRepo(t)
		id := fmt.Sprintf("cb-converge-%d", i)
		manager.mu.Lock()
		manager.codebases[id] = model.Codebase{
			ID:              id,
			CanonicalPath:   canonical,
			Status:          model.CodebaseStatusIndexed,
			EffectiveConfig: defaultIndexConfig(),
		}
		manager.mu.Unlock()
		ids = append(ids, id)
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(codebaseID string) {
			defer wg.Done()
			syncer.convergeViaWatcher(context.Background(), codebaseID, []string{"main.go"})
		}(id)
	}

	// Exactly cap converges embed before any slot frees.
	for range cap {
		<-entered
	}
	waitForCondition(t, func() bool { return inFlight.Load() == int32(cap) })
	if got := maxInFlight.Load(); got > int32(cap) {
		t.Fatalf("max concurrent converges = %d, want <= %d", got, cap)
	}

	// The kernel lock file is never unlinked, so its presence proves nothing. An
	// independent descriptor conflicts with the converge's own hold, which is what
	// proves the lock is still held while the embeds run.
	lockPath := filepath.Join(cfg.ContextRoot, "mcp-sync.flock")
	if !probeLockHeld(t, lockPath) {
		t.Fatal("sync lock should be held while converges run")
	}
	close(release)
	for i := cap; i < codebases; i++ {
		<-entered
	}
	wg.Wait()

	if got := maxInFlight.Load(); got > int32(cap) {
		t.Fatalf("max concurrent converges over the run = %d, want <= %d", got, cap)
	}

	if probeLockHeld(t, lockPath) {
		t.Fatal("sync lock should be released after converges finish")
	}
}

// TestWatcherUsesSchedulerWhenExternalLockBusy proves a converge releases its
// scheduler slot while an external process holds the sync lock, then resumes
// the same queued job after the lock becomes available.
func TestWatcherUsesSchedulerWhenExternalLockBusy(t *testing.T) {
	manager, cfg := newTestManagerWithCap(t, 2)
	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(_ context.Context, _ string, _ []model.StoredChunk, _ []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.queue = NewEventQueue(time.Hour, func(string, []string) {})

	canonical := newCapTestRepo(t)
	codebaseID := "cb-external-lock"
	manager.mu.Lock()
	manager.codebases[codebaseID] = model.Codebase{
		ID:              codebaseID,
		CanonicalPath:   canonical,
		Status:          model.CodebaseStatusIndexed,
		EffectiveConfig: defaultIndexConfig(),
	}
	manager.mu.Unlock()

	lockPath := filepath.Join(cfg.ContextRoot, "mcp-sync.flock")
	if err := os.MkdirAll(cfg.ContextRoot, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	holder, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = holder.Close()
	})
	if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("Flock returned error: %v", err)
	}

	convergeDone := make(chan struct{})
	go func() {
		defer close(convergeDone)
		syncer.convergeViaWatcher(context.Background(), codebaseID, []string{"main.go"})
	}()

	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("converge embedded %d time(s) while the lock was held externally, want 0", got)
	}
	waitForCondition(t, func() bool {
		snapshot := manager.jobScheduler.Snapshot()
		slots, _ := manager.IndexSlots()
		return snapshot.Paused[model.JobPriorityNormal] == 1 && slots == 0
	})
	if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatalf("unlock external holder: %v", err)
	}
	select {
	case <-convergeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher converge did not resume after the external lock released")
	}
	if got := reindexCalls.Load(); got != 1 {
		t.Fatalf("converge embedded %d time(s), want 1 after lock release", got)
	}
}

// TestConvergeCopyChunksFiresOnRename proves a renamed file converges through
// the CopyChunks fast path rather than a re-embed, incrementing
// converge_copy_chunks_total.
func TestConvergeCopyChunksFiresOnRename(t *testing.T) {
	manager, _ := newTestManagerWithCap(t, 2)
	var copyCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		copyChunks: func(_ context.Context, _ string, _ string, _ string) (int, error) {
			copyCalls.Add(1)
			return 5, nil
		},
		reindex: func(_ context.Context, _ string, _ []model.StoredChunk, _ []string) error {
			return nil
		},
	}

	canonical := newCapTestRepo(t)
	if err := os.WriteFile(filepath.Join(canonical, "src.go"), []byte("package main\nfunc Moved() {}\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	cfg := defaultIndexConfig()
	cfg.IgnoreDigest = "sha256:rename-test"
	codebaseID := "cb-rename"
	manager.mu.Lock()
	manager.codebases[codebaseID] = model.Codebase{
		ID:              codebaseID,
		CanonicalPath:   canonical,
		Status:          model.CodebaseStatusIndexed,
		EffectiveConfig: cfg,
	}
	manager.mu.Unlock()

	// Seed a checkpoint recording src.go with its real content hash and inode,
	// so the renamed file is recognized as a move of src.go.
	captured, err := merkle.Capture(context.Background(), manager.indexability, codebaseID, canonical, cfg)
	if err != nil {
		t.Fatalf("Capture returned error: %v", err)
	}
	identity, err := statInode(filepath.Join(canonical, "src.go"))
	if err != nil {
		t.Fatalf("statInode returned error: %v", err)
	}
	checkpoint := merkle.Snapshot{
		ConfigDigest: cfg.IgnoreDigest,
		Files:        map[string]string{"src.go": captured.Files["src.go"]},
		Inodes:       map[string]merkle.InodeRef{"src.go": {Device: identity.device, Inode: identity.inode}},
	}
	if err := merkle.WriteSnapshot(manager.merklePath(codebaseID), checkpoint); err != nil {
		t.Fatalf("WriteSnapshot returned error: %v", err)
	}

	// Rename on the same filesystem preserves the inode, which is what the fast
	// path keys on.
	if err := os.Rename(filepath.Join(canonical, "src.go"), filepath.Join(canonical, "dst.go")); err != nil {
		t.Fatalf("Rename returned error: %v", err)
	}

	before := metrics.Read().ConvergeCopyChunksTotal
	if _, err := manager.ConvergePaths(context.Background(), codebaseID, []string{"src.go", "dst.go"}, nil); err != nil {
		t.Fatalf("ConvergePaths returned error: %v", err)
	}

	if got := copyCalls.Load(); got != 1 {
		t.Fatalf("CopyChunks called %d time(s), want 1 (the rename fast path)", got)
	}
	if delta := metrics.Read().ConvergeCopyChunksTotal - before; delta != 1 {
		t.Fatalf("converge_copy_chunks_total moved by %d, want 1", delta)
	}

	snapshot, err := merkle.ReadSnapshot(manager.merklePath(codebaseID))
	if err != nil {
		t.Fatalf("ReadSnapshot returned error: %v", err)
	}
	if _, present := snapshot.Files["dst.go"]; !present {
		t.Fatalf("snapshot missing renamed destination dst.go; have %v", snapshot.Files)
	}
}

// gatedRecordingSemantic builds a fakeSemantic that records every conversation
// id reaching an embed call and blocks the FIRST embed on release after
// signalling entered. A test uses it to hold the first conversation job active
// inside its embed, submit a coalescing second job, then release and prove the
// drained successor embedded the coalesced ids. The returned snapshot is a
// concurrency-safe copy of the embedded-id set.
func gatedRecordingSemantic() (*fakeSemantic, func() map[string]struct{}, chan struct{}, chan struct{}) {
	var mu sync.Mutex
	embedded := map[string]struct{}{}
	var calls atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	record := func(chunks []model.StoredChunk) {
		mu.Lock()
		for _, chunk := range chunks {
			if chunk.ConversationID != "" {
				embedded[chunk.ConversationID] = struct{}{}
			}
		}
		mu.Unlock()
	}
	gate := func() {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-release
		}
	}
	embed := func(_ context.Context, _ string, chunks []model.StoredChunk, _ []string, _ func(semantic.Progress), _ map[string][]float32) error {
		record(chunks)
		gate()
		return nil
	}
	fake := &fakeSemantic{stageReindexWithReuse: embed, reindexWithReuse: embed}
	snapshot := func() map[string]struct{} {
		mu.Lock()
		defer mu.Unlock()
		out := make(map[string]struct{}, len(embedded))
		for id := range embedded {
			out[id] = struct{}{}
		}
		return out
	}
	return fake, snapshot, entered, release
}

func waitForCompletedJobCount(t *testing.T, manager *Manager, want int) {
	t.Helper()
	waitForCondition(t, func() bool {
		jobs := manager.ListJobs("")
		if len(jobs) != want {
			return false
		}
		for _, job := range jobs {
			if job.State != model.JobStateCompleted {
				return false
			}
		}
		return true
	})
}

// TestConversationUpsertCoalescesWithoutContention proves the no-contention
// contract: a normal ingest and a backfill on the SAME collection do not
// collide. While the first ingest holds the embed, the second submission returns
// the ACTIVE job id and does not error with the removed conflict message.
func TestConversationUpsertCoalescesWithoutContention(t *testing.T) {
	manager, _, _ := newTestManager(t)
	fake, _, entered, release := gatedRecordingSemantic()
	manager.semantic = fake
	ctx := context.Background()
	collectionID := "coalesce-no-contention"

	firstDocs := []model.ConversationDocument{{ConversationID: "conv-a", MessageIndex: 0, Role: "user", Text: "a"}}
	firstJob, err := manager.upsertConversationDocuments(ctx, collectionID, firstDocs, testConversationManifest("conv-a"), testClientInfo(), absenceRetain, false, false)
	if err != nil {
		t.Fatalf("first upsert returned error: %v", err)
	}
	<-entered // the first job is now blocked inside the embed, ActiveJobID set

	backfillDocs := []model.ConversationDocument{{ConversationID: "conv-b", MessageIndex: 0, Role: "user", Text: "b"}}
	secondJob, err := manager.upsertConversationDocuments(ctx, collectionID, backfillDocs, testConversationManifest("conv-b"), testClientInfo(), absenceRetain, true, false)
	if err != nil {
		t.Fatalf("second same-collection upsert returned error, want coalesced success: %v", err)
	}
	if secondJob.ID != firstJob.ID {
		t.Fatalf("coalesced submission returned job %s, want the active job %s", secondJob.ID, firstJob.ID)
	}

	codebase, err := manager.RegisterConversationCollection(ctx, collectionID)
	if err != nil {
		t.Fatalf("RegisterConversationCollection returned error: %v", err)
	}
	manager.mu.Lock()
	pending, ok := manager.pendingConversationJobs[codebase.ID]
	manager.mu.Unlock()
	if !ok {
		t.Fatal("pending slot empty after coalesce, want the merged backfill payload")
	}
	if _, present := pending.Manifest["conv-b"]; !present {
		t.Fatalf("pending manifest = %v, want conv-b", pending.Manifest)
	}
	if !pending.Backfill {
		t.Fatal("pending payload lost the backfill intent")
	}

	close(release)
	waitForCompletedJobCount(t, manager, 2)
}

// TestConversationCoalesceDrainsPendingAfterTerminal proves the drain: after the
// first job reaches terminal and clears ActiveJobID, the coalesced pending
// payload runs as a fresh job and BOTH id sets end up embedded.
func TestConversationCoalesceDrainsPendingAfterTerminal(t *testing.T) {
	manager, _, _ := newTestManager(t)
	fake, embeddedSnapshot, entered, release := gatedRecordingSemantic()
	manager.semantic = fake
	ctx := context.Background()
	collectionID := "coalesce-drain"

	firstDocs := []model.ConversationDocument{{ConversationID: "conv-a", MessageIndex: 0, Role: "user", Text: "a"}}
	firstJob, err := manager.upsertConversationDocuments(ctx, collectionID, firstDocs, testConversationManifest("conv-a"), testClientInfo(), absenceRetain, false, false)
	if err != nil {
		t.Fatalf("first upsert returned error: %v", err)
	}
	<-entered

	secondDocs := []model.ConversationDocument{{ConversationID: "conv-b", MessageIndex: 0, Role: "user", Text: "b"}}
	if _, err := manager.upsertConversationDocuments(ctx, collectionID, secondDocs, testConversationManifest("conv-b"), testClientInfo(), absenceRetain, false, false); err != nil {
		t.Fatalf("second upsert returned error: %v", err)
	}

	close(release)
	waitForConversationJobState(t, manager, firstJob.ID, model.JobStateCompleted)
	// The drained successor runs as a separate second job.
	waitForCompletedJobCount(t, manager, 2)

	embedded := embeddedSnapshot()
	for _, want := range []string{"conv-a", "conv-b"} {
		if _, present := embedded[want]; !present {
			t.Fatalf("embedded ids = %v, want both conv-a and conv-b (drained work not lost)", embedded)
		}
	}
	manager.mu.Lock()
	_, stillPending := manager.pendingConversationJobs[firstJob.CodebaseID]
	manager.mu.Unlock()
	if stillPending {
		t.Fatal("pending slot not drained after terminal transition")
	}
}

// TestConversationCoalesceDepthOneMergesThirdSubmission proves depth 1: with one
// job active and one pending, a third submission merges into the single pending
// payload rather than growing an unbounded queue, and the one drained job covers
// every delivered id.
func TestConversationCoalesceDepthOneMergesThirdSubmission(t *testing.T) {
	manager, _, _ := newTestManager(t)
	fake, embeddedSnapshot, entered, release := gatedRecordingSemantic()
	manager.semantic = fake
	ctx := context.Background()
	collectionID := "coalesce-depth-one"

	firstDocs := []model.ConversationDocument{{ConversationID: "conv-a", MessageIndex: 0, Role: "user", Text: "a"}}
	firstJob, err := manager.upsertConversationDocuments(ctx, collectionID, firstDocs, testConversationManifest("conv-a"), testClientInfo(), absenceRetain, false, false)
	if err != nil {
		t.Fatalf("first upsert returned error: %v", err)
	}
	<-entered

	secondDocs := []model.ConversationDocument{{ConversationID: "conv-b", MessageIndex: 0, Role: "user", Text: "b"}}
	if _, err := manager.upsertConversationDocuments(ctx, collectionID, secondDocs, testConversationManifest("conv-b"), testClientInfo(), absenceRetain, false, false); err != nil {
		t.Fatalf("second upsert returned error: %v", err)
	}
	thirdDocs := []model.ConversationDocument{{ConversationID: "conv-c", MessageIndex: 0, Role: "user", Text: "c"}}
	if _, err := manager.upsertConversationDocuments(ctx, collectionID, thirdDocs, testConversationManifest("conv-c"), testClientInfo(), absenceRetain, false, false); err != nil {
		t.Fatalf("third upsert returned error: %v", err)
	}

	codebase, err := manager.RegisterConversationCollection(ctx, collectionID)
	if err != nil {
		t.Fatalf("RegisterConversationCollection returned error: %v", err)
	}
	manager.mu.Lock()
	pending, ok := manager.pendingConversationJobs[codebase.ID]
	manager.mu.Unlock()
	if !ok {
		t.Fatal("pending slot empty, want the merged conv-b + conv-c payload")
	}
	for _, want := range []string{"conv-b", "conv-c"} {
		if _, present := pending.Manifest[want]; !present {
			t.Fatalf("pending manifest = %v, want both conv-b and conv-c (depth-1 merge)", pending.Manifest)
		}
	}

	close(release)
	waitForConversationJobState(t, manager, firstJob.ID, model.JobStateCompleted)
	// Exactly one drained job runs (first + drained = 2), proving no unbounded queue.
	waitForCompletedJobCount(t, manager, 2)

	embedded := embeddedSnapshot()
	for _, want := range []string{"conv-a", "conv-b", "conv-c"} {
		if _, present := embedded[want]; !present {
			t.Fatalf("embedded ids = %v, want conv-a, conv-b, and conv-c (no dropped ids)", embedded)
		}
	}
}

// TestCodeIndexCoalescesNonMatchingConfigAndDrains proves the code admission
// path: a non-matching-config request while a code job is active coalesces
// (returns the active job id, no conflict error), a matching-config request
// still dedups, and the pending config drains into a fresh sync after terminal.
func TestCodeIndexCoalescesNonMatchingConfigAndDrains(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var indexCalls atomic.Int32
	manager.runner = fakeRunner{
		index:      nil,
		indexFiles: nil,
		indexOne: func(_ context.Context, _ string, relativePath string, _ model.IndexConfig) (indexer.OneFileResult, error) {
			if indexCalls.Add(1) == 1 {
				entered <- struct{}{}
				<-release
			}
			content := "package main\n"
			return indexer.OneFileResult{
				Chunks:   []model.StoredChunk{{Content: content, RelativePath: relativePath, StartLine: 1, EndLine: 1, Language: "go", FileExtension: ".go"}},
				FileHash: hashText(content),
				Skipped:  false,
				Removed:  false,
			}, nil
		},
	}

	firstJob, _, deduplicated, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget)
	if err != nil {
		t.Fatalf("first StartIndex returned error: %v", err)
	}
	if deduplicated {
		t.Fatal("first request should not be a dedup hit")
	}
	<-entered // first job blocked inside the indexer, ActiveJobID set

	// A matching-config request still dedups onto the active job.
	dedupJob, _, dedupHit, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget)
	if err != nil {
		t.Fatalf("matching-config StartIndex returned error: %v", err)
	}
	if !dedupHit || dedupJob.ID != firstJob.ID {
		t.Fatalf("matching-config request returned (dedup=%v, job=%s), want dedup onto %s", dedupHit, dedupJob.ID, firstJob.ID)
	}

	// A non-matching-config request coalesces instead of refusing.
	conflictConfig := defaultIndexConfig()
	conflictConfig.SplitterType = "langchain"
	coalescedJob, _, coalescedHit, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), conflictConfig, false, emptyAdmissionBudget)
	if err != nil {
		t.Fatalf("non-matching-config StartIndex returned error, want coalesced success: %v", err)
	}
	if !coalescedHit || coalescedJob.ID != firstJob.ID {
		t.Fatalf("non-matching-config request returned (coalesced=%v, job=%s), want the active job %s", coalescedHit, coalescedJob.ID, firstJob.ID)
	}

	codebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil || !found {
		t.Fatalf("GetIndex returned err=%v found=%v", err, found)
	}
	manager.mu.Lock()
	pendingCode, ok := manager.pendingCodeJobs[codebase.ID]
	manager.mu.Unlock()
	if !ok {
		t.Fatal("pending code slot empty after non-matching coalesce")
	}
	if pendingCode.indexConfig.SplitterType != "langchain" {
		t.Fatalf("pending code config SplitterType = %q, want langchain", pendingCode.indexConfig.SplitterType)
	}

	close(release)
	// The drained sync runs as a second job carrying the coalesced langchain config.
	waitForCompletedJobCount(t, manager, 2)
	drainedFound := false
	for _, job := range manager.ListJobs("") {
		if job.ID != firstJob.ID {
			if job.Config.SplitterType != "langchain" {
				t.Fatalf("drained job config SplitterType = %q, want langchain", job.Config.SplitterType)
			}
			drainedFound = true
		}
	}
	if !drainedFound {
		t.Fatal("no drained successor job found after terminal")
	}
}

// TestMergePendingConversationPayloadORsBackfillAndForce proves the depth-1
// coalescing merge keeps both orthogonal flags sticky true, independent of order:
// a coalesced backfill stays a backfill and a coalesced force stays a force, so a
// force that lands next to a plain or backfill submission is never downgraded.
func TestMergePendingConversationPayloadORsBackfillAndForce(t *testing.T) {
	t.Parallel()

	upsert := func(backfill bool, force bool) conversationJobPayload {
		return conversationJobPayload{
			Kind:           conversationJobKindUpsert,
			CollectionName: "conv_chunks_merge",
			Manifest:       map[string]string{"conv-a": "fp"},
			Documents:      []model.ConversationDocument{{ConversationID: "conv-a", MessageIndex: 0, Role: "user", Text: "a"}},
			ConversationID: "",
			Absence:        absenceRetain,
			Backfill:       backfill,
			Force:          force,
		}
	}

	cases := []struct {
		name         string
		first        conversationJobPayload
		second       conversationJobPayload
		wantBackfill bool
		wantForce    bool
	}{
		{"backfill then force keeps both", upsert(true, false), upsert(false, true), true, true},
		{"force then backfill keeps both", upsert(false, true), upsert(true, false), true, true},
		{"force then plain stays force", upsert(false, true), upsert(false, false), false, true},
		{"backfill then plain stays backfill", upsert(true, false), upsert(false, false), true, false},
		{"plain then plain stays plain", upsert(false, false), upsert(false, false), false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager, _, _ := newTestManager(t)
			const codebaseID = "cb-merge"

			manager.mu.Lock()
			manager.mergePendingConversationPayloadLocked(codebaseID, tc.first)
			manager.mergePendingConversationPayloadLocked(codebaseID, tc.second)
			merged := manager.pendingConversationJobs[codebaseID]
			manager.mu.Unlock()

			if merged.Backfill != tc.wantBackfill {
				t.Fatalf("merged Backfill = %v, want %v", merged.Backfill, tc.wantBackfill)
			}
			if merged.Force != tc.wantForce {
				t.Fatalf("merged Force = %v, want %v", merged.Force, tc.wantForce)
			}
		})
	}
}
