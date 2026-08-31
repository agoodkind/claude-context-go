package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/adapterr"
	"goodkind.io/lm-semantic-search/internal/cbm"
	"goodkind.io/lm-semantic-search/internal/merkle"
	"goodkind.io/lm-semantic-search/internal/model"
)

type graphToolEnvelope struct {
	StructuredContent graphToolResult `json:"structuredContent"`
	IsError           bool            `json:"isError"`
}

type graphToolResult struct {
	Rows []json.RawMessage `json:"rows"`
}

func TestValidateGraphToolNameAllowsReadOnlyToolsOnly(t *testing.T) {
	for _, toolName := range []string{"query_graph", "trace_path", "get_architecture", "manage_adr"} {
		if err := validateGraphToolName(toolName); err != nil {
			t.Fatalf("validateGraphToolName(%q) returned error: %v", toolName, err)
		}
	}

	err := validateGraphToolName("delete_project")
	if err == nil {
		t.Fatal("validateGraphToolName accepted delete_project, want invalid argument")
	}
	var adapterErr *adapterr.AdapterError
	if !errors.As(err, &adapterErr) {
		t.Fatalf("validateGraphToolName returned %T, want AdapterError", err)
	}
	if adapterErr.Class != adapterr.ClassInvalidArgument {
		t.Fatalf("adapter error class = %q, want %q", adapterErr.Class, adapterr.ClassInvalidArgument)
	}
}

func TestManagerGraphToolIndexesAndQueriesCodebase(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	t.Cleanup(func() {
		manager.CloseGraphEngines()
	})
	if err := os.WriteFile(filepath.Join(repoPath, "main.go"), []byte("package main\n\nfunc GraphTarget() string {\n\treturn \"ok\"\n}\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	codebase := newCodebaseRecord(repoPath)
	codebase.Status = model.CodebaseStatusIndexed
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mu.Unlock()

	engine, release, err := manager.graphEngine(context.Background(), codebase.ID)
	if err != nil {
		t.Fatalf("graphEngine returned error: %v", err)
	}
	defer release()
	if err = engine.Index(context.Background(), repoPath, "fast"); err != nil {
		t.Fatalf("Index returned error: %v", err)
	}

	resultJSON, err := manager.GraphTool(
		context.Background(),
		codebase.ID,
		"query_graph",
		`{"query":"MATCH (f:Function) RETURN f.name LIMIT 25","project":"`+codebase.ID+`","max_rows":200}`,
	)
	if err != nil {
		t.Fatalf("GraphTool returned error: %v", err)
	}

	var envelope graphToolEnvelope
	if err = json.Unmarshal([]byte(resultJSON), &envelope); err != nil {
		t.Fatalf("query_graph returned invalid JSON: %v\n%s", err, resultJSON)
	}
	if envelope.IsError {
		t.Fatalf("query_graph returned error: %s", resultJSON)
	}
	if len(envelope.StructuredContent.Rows) == 0 {
		t.Fatalf("query_graph returned no rows: %s", resultJSON)
	}

	graphPath := filepath.Join(cfg.GraphDir, codebase.ID+".db")
	if _, err = os.Stat(graphPath); err != nil {
		t.Fatalf("Stat(%q) returned error: %v", graphPath, err)
	}

	t.Logf("query_graph JSON: %s", resultJSON)
}

func TestRunJobAsyncRunsGraphIndexAfterSemanticLockAndSlotRelease(t *testing.T) {
	manager, cfg := newTestManagerWithCap(t, 1)
	manager.semantic = &fakeSemantic{
		count: func(context.Context, string) (int32, error) {
			return 1, nil
		},
	}

	// The hook runs on the job's goroutine, so it probes the kernel lock through
	// an independent descriptor and reports through the channel rather than
	// failing the test from off the test goroutine.
	lockPath := filepath.Join(cfg.ContextRoot, "mcp-sync.flock")
	observed := make(chan string, 1)
	manager.graphIndexHook = func() {
		held, err := lockHeldByAnother(lockPath)
		if err != nil {
			observed <- fmt.Sprintf("probe sync lock returned error: %v", err)
			return
		}
		slotCount, _ := manager.IndexSlots()
		if held || slotCount != 0 {
			observed <- fmt.Sprintf("sync lock held = %t, index slot count = %d", held, slotCount)
			return
		}
		observed <- ""
	}

	repoPath := newCapTestRepo(t)
	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget); err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}

	select {
	case message := <-observed:
		if message != "" {
			t.Fatal(message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("graph index hook was not called")
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)
}

func TestGraphStateRecordsReadyAndReconcilesStaleGraph(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	t.Cleanup(func() {
		manager.CloseGraphEngines()
	})

	codebase := newCodebaseRecord(repoPath)
	codebase.Kind = ""
	codebase.Status = model.CodebaseStatusIndexed
	codebase.EffectiveConfig = defaultIndexConfig()
	codebase.EffectiveConfig.IgnoreDigest = digestIndexConfig(codebase.EffectiveConfig)
	codebase.MerkleSnapshotPath = manager.merklePath(codebase.ID)

	snapshot, err := merkle.Capture(
		context.Background(),
		manager.indexability,
		codebase.ID,
		repoPath,
		codebase.EffectiveConfig,
	)
	if err != nil {
		t.Fatalf("Capture returned error: %v", err)
	}
	snapshot.ConfigDigest = codebase.EffectiveConfig.IgnoreDigest
	snapshotHash := snapshot.Hash()
	if err = merkle.WriteSnapshot(codebase.MerkleSnapshotPath, snapshot); err != nil {
		t.Fatalf("WriteSnapshot returned error: %v", err)
	}

	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mu.Unlock()

	manager.recordGraphIndexNonFatal(context.Background(), codebase.ID, repoPath, snapshotHash)

	manager.mu.Lock()
	recorded := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if recorded.GraphState != model.GraphStateReady {
		t.Fatalf("GraphState = %q, want %q", recorded.GraphState, model.GraphStateReady)
	}
	if recorded.GraphSnapshotHash == "" {
		t.Fatal("GraphSnapshotHash is empty, want hash after graph build")
	}
	if recorded.GraphSnapshotHash != snapshotHash {
		t.Fatalf("GraphSnapshotHash = %q, want %q", recorded.GraphSnapshotHash, snapshotHash)
	}

	manager.mu.Lock()
	recorded.GraphState = model.GraphStateStale
	manager.codebases[codebase.ID] = recorded
	manager.mu.Unlock()
	if err = manager.clearGraphCache(context.Background(), codebase.ID); err != nil {
		t.Fatalf("clearGraphCache returned error: %v", err)
	}
	if !manager.shouldReconcileGraph(codebase.ID, snapshotHash, collectionPresencePresent) {
		t.Fatal("shouldReconcileGraph returned false for stale graph, want true")
	}

	manager.recordGraphIndexNonFatal(context.Background(), codebase.ID, repoPath, snapshotHash)

	manager.mu.Lock()
	reconciled := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if reconciled.GraphState != model.GraphStateReady {
		t.Fatalf("GraphState after reconcile = %q, want %q", reconciled.GraphState, model.GraphStateReady)
	}
	graphPath := filepath.Join(cfg.GraphDir, codebase.ID+".db")
	if _, err = os.Stat(graphPath); err != nil {
		t.Fatalf("Stat(%q) returned error after reconcile: %v", graphPath, err)
	}
}

func TestClearGraphCacheRejectsNewOpenAndWaitsForActiveOperation(t *testing.T) {
	manager, cfg, _ := newTestManager(t)
	codebaseID := "graph-lifecycle"
	graphPath := filepath.Join(cfg.GraphDir, codebaseID+".db")
	if err := os.WriteFile(graphPath, []byte("graph"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	release, err := manager.beginGraphOperation(codebaseID)
	if err != nil {
		t.Fatalf("beginGraphOperation returned error: %v", err)
	}

	clearDone := make(chan error, 1)
	go func() {
		clearDone <- manager.clearGraphCache(context.Background(), codebaseID)
	}()
	waitForGraphClearing(t, manager, codebaseID)

	if _, _, err = manager.graphEngine(context.Background(), codebaseID); err == nil {
		t.Fatal("graphEngine returned nil error while graph cache was clearing")
	} else if !strings.Contains(err.Error(), "being cleared") {
		t.Fatalf("graphEngine error = %v, want clear-in-progress error", err)
	}
	if _, statErr := os.Stat(graphPath); statErr != nil {
		t.Fatalf("graph file was removed before active operation released: %v", statErr)
	}

	select {
	case err = <-clearDone:
		t.Fatalf("clearGraphCache completed before active operation released: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	release()
	select {
	case err = <-clearDone:
		if err != nil {
			t.Fatalf("clearGraphCache returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("clearGraphCache did not finish after active operation released")
	}
	if _, statErr := os.Stat(graphPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Stat(%q) error = %v, want os.ErrNotExist", graphPath, statErr)
	}
}

func TestIndexGraphNonFatalReturnsOnCancelAndClearWaitsForWorker(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	codebaseID := "graph-cancel"
	entered := make(chan struct{})
	releaseWorker := make(chan struct{})
	manager.graphIndex = func(ctx context.Context, engine *cbm.Engine, canonicalPath string, mode string) error {
		close(entered)
		<-releaseWorker
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- manager.indexGraphNonFatal(ctx, codebaseID, repoPath, nil)
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("fake graph index did not start")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("indexGraphNonFatal returned nil error after cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("indexGraphNonFatal did not return promptly after cancellation")
	}

	graphPath := filepath.Join(cfg.GraphDir, codebaseID+".db")
	if err := os.WriteFile(graphPath, []byte("graph"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	clearDone := make(chan error, 1)
	go func() {
		clearDone <- manager.clearGraphCache(context.Background(), codebaseID)
	}()

	select {
	case err := <-clearDone:
		t.Fatalf("clearGraphCache completed while fake graph index was blocked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseWorker)
	select {
	case err := <-clearDone:
		if err != nil {
			t.Fatalf("clearGraphCache returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("clearGraphCache did not finish after fake graph index released")
	}
	if _, statErr := os.Stat(graphPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Stat(%q) error = %v, want os.ErrNotExist", graphPath, statErr)
	}
}

func TestMarshalGraphToolArgumentsAcceptsNull(t *testing.T) {
	resultJSON, err := MarshalGraphToolArguments("null", "project-id")
	if err != nil {
		t.Fatalf("MarshalGraphToolArguments returned error: %v", err)
	}
	var args graphToolArguments
	if err = json.Unmarshal([]byte(resultJSON), &args); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	var project string
	if err = json.Unmarshal(args["project"], &project); err != nil {
		t.Fatalf("project unmarshal returned error: %v", err)
	}
	if project != "project-id" {
		t.Fatalf("project = %q, want project-id", project)
	}
}

func TestUpdateGraphStateClearsSnapshotHashWhenNotReady(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	codebase := newCodebaseRecord(repoPath)
	codebase.GraphState = model.GraphStateReady
	codebase.GraphSnapshotHash = "old-hash"
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mu.Unlock()

	manager.updateGraphState(context.Background(), codebase.ID, model.GraphStateStale, "")

	manager.mu.Lock()
	recorded := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if recorded.GraphSnapshotHash != "" {
		t.Fatalf("GraphSnapshotHash = %q, want empty", recorded.GraphSnapshotHash)
	}
}

func TestUpdateGraphStateRecordsSuccessfulBuildTime(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	codebase := newCodebaseRecord(repoPath)
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	untouched := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if !untouched.GraphUpdatedAt.IsZero() {
		t.Fatalf("GraphUpdatedAt = %v, want zero before graph state update", untouched.GraphUpdatedAt)
	}

	manager.updateGraphState(context.Background(), codebase.ID, model.GraphStateReady, "ready-hash")

	manager.mu.Lock()
	ready := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if ready.GraphUpdatedAt.IsZero() {
		t.Fatal("GraphUpdatedAt is zero after ready graph state update")
	}
	successfulBuildTime := ready.GraphUpdatedAt

	manager.updateGraphState(context.Background(), codebase.ID, model.GraphStateStale, "")

	manager.mu.Lock()
	stale := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if !stale.GraphUpdatedAt.Equal(successfulBuildTime) {
		t.Fatalf("GraphUpdatedAt = %v, want preserved successful build time %v", stale.GraphUpdatedAt, successfulBuildTime)
	}
	if stale.GraphSnapshotHash != "" {
		t.Fatalf("GraphSnapshotHash = %q, want empty", stale.GraphSnapshotHash)
	}
}

func TestResolveGetIndexViewPopulatesGraphFields(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	useRelativeTimeNowForTest(t, now)

	cases := []struct {
		name               string
		codebase           model.Codebase
		wantUpdatedAt      string
		wantBuilding       bool
		wantFailed         bool
		wantReadyNoTime    bool
		wantNotBuilt       bool
		wantAllFieldsEmpty bool
	}{
		{
			name: "ever built",
			codebase: model.Codebase{
				CanonicalPath:  repoPath,
				Kind:           model.CodebaseKindCode,
				Status:         model.CodebaseStatusIndexed,
				GraphState:     model.GraphStateReady,
				GraphUpdatedAt: now.Add(-6 * time.Minute),
				LastSuccessfulRun: &model.IndexRunSummary{
					CompletedAt: now,
				},
			},
			wantUpdatedAt: formatStampWithRelative(now.Add(-6 * time.Minute)),
		},
		{
			name: "legacy empty kind ever built",
			codebase: model.Codebase{
				CanonicalPath:  repoPath,
				Status:         model.CodebaseStatusIndexed,
				GraphState:     model.GraphStateReady,
				GraphUpdatedAt: now.Add(-6 * time.Minute),
				LastSuccessfulRun: &model.IndexRunSummary{
					CompletedAt: now,
				},
			},
			wantUpdatedAt: formatStampWithRelative(now.Add(-6 * time.Minute)),
		},
		{
			name: "building",
			codebase: model.Codebase{
				ID:            "cb-building",
				CanonicalPath: repoPath,
				Kind:          model.CodebaseKindCode,
				Status:        model.CodebaseStatusIndexed,
				GraphState:    model.GraphStateAbsent,
			},
			wantBuilding: true,
		},
		{
			name: "failed",
			codebase: model.Codebase{
				CanonicalPath: repoPath,
				Kind:          model.CodebaseKindCode,
				Status:        model.CodebaseStatusIndexed,
				GraphState:    model.GraphStateStale,
			},
			wantFailed: true,
		},
		{
			name: "ready no time",
			codebase: model.Codebase{
				CanonicalPath: repoPath,
				Kind:          model.CodebaseKindCode,
				Status:        model.CodebaseStatusIndexed,
				GraphState:    model.GraphStateReady,
			},
			wantReadyNoTime: true,
		},
		{
			name: "never built",
			codebase: model.Codebase{
				CanonicalPath: repoPath,
				Kind:          model.CodebaseKindCode,
				Status:        model.CodebaseStatusIndexed,
				GraphState:    model.GraphStateAbsent,
			},
			wantNotBuilt: true,
		},
		{
			name: "non code",
			codebase: model.Codebase{
				CanonicalPath: "chat:///thread-alpha",
				Kind:          model.CodebaseKindDocument,
				Status:        model.CodebaseStatusIndexed,
			},
			wantAllFieldsEmpty: true,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.wantBuilding {
				manager.graphMutex.Lock()
				state := manager.graphLifecycleStateLocked(testCase.codebase.ID)
				state.indexing = true
				manager.graphMutex.Unlock()
			}
			got := manager.resolveGetIndexView(
				context.Background(),
				testCase.codebase.CanonicalPath,
				true,
				&testCase.codebase,
				nil,
				dependencyHealth{},
				collectionNotApplicable,
				nil,
				nil,
				nil,
			)
			if testCase.wantBuilding {
				manager.graphMutex.Lock()
				state := manager.graphLifecycleStateLocked(testCase.codebase.ID)
				state.indexing = false
				manager.graphMutex.Unlock()
			}
			if got.Status.GraphUpdatedAt != testCase.wantUpdatedAt {
				t.Fatalf("GraphUpdatedAt = %q, want %q", got.Status.GraphUpdatedAt, testCase.wantUpdatedAt)
			}
			if got.Status.GraphBuilding != testCase.wantBuilding {
				t.Fatalf("GraphBuilding = %t, want %t", got.Status.GraphBuilding, testCase.wantBuilding)
			}
			if got.Status.GraphFailed != testCase.wantFailed {
				t.Fatalf("GraphFailed = %t, want %t", got.Status.GraphFailed, testCase.wantFailed)
			}
			if got.Status.GraphReadyNoTime != testCase.wantReadyNoTime {
				t.Fatalf("GraphReadyNoTime = %t, want %t", got.Status.GraphReadyNoTime, testCase.wantReadyNoTime)
			}
			if got.Status.GraphNotBuilt != testCase.wantNotBuilt {
				t.Fatalf("GraphNotBuilt = %t, want %t", got.Status.GraphNotBuilt, testCase.wantNotBuilt)
			}
			if testCase.wantAllFieldsEmpty && (got.Status.GraphUpdatedAt != "" || got.Status.GraphBuilding || got.Status.GraphFailed || got.Status.GraphReadyNoTime || got.Status.GraphNotBuilt) {
				t.Fatalf("non code graph fields = %+v, want all zero", got.Status)
			}
		})
	}
}

func TestGraphDiagnosticUsesPlainDoctorMessages(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	baseCodebase := newCodebaseRecord(repoPath)
	baseCodebase.Status = model.CodebaseStatusIndexed
	baseCodebase.EffectiveConfig = defaultIndexConfig()
	baseCodebase.EffectiveConfig.IgnoreDigest = digestIndexConfig(baseCodebase.EffectiveConfig)
	baseCodebase.MerkleSnapshotPath = manager.merklePath(baseCodebase.ID)
	// Every case but the zero-file one describes a codebase that indexed files,
	// which is what makes an absent checkpoint a loss worth reporting.
	baseCodebase.LastSuccessfulRun = &model.IndexRunSummary{IndexedFiles: 1, TotalChunks: 1, Status: "completed"}

	captured, err := merkle.Capture(
		context.Background(),
		manager.indexability,
		baseCodebase.ID,
		repoPath,
		baseCodebase.EffectiveConfig,
	)
	if err != nil {
		t.Fatalf("Capture returned error: %v", err)
	}
	// writeCompletedArtifacts stamps the run's config digest onto every snapshot
	// it writes, so the stored shape here matches what a real run leaves behind.
	snapshot := captured
	snapshot.ConfigDigest = baseCodebase.EffectiveConfig.IgnoreDigest
	snapshotHash := snapshotHashForGraph(snapshot, baseCodebase.EffectiveConfig.IgnoreDigest)

	cases := []struct {
		name          string
		graphState    model.GraphState
		snapshotState string
		graphHash     string
		codebaseKind  model.CodebaseKind
		emptyKind     bool
		canonicalPath string
		zeroFileRun   bool
		want          string
	}{
		{
			name:          "missing snapshot",
			graphState:    model.GraphStateReady,
			snapshotState: "missing",
			graphHash:     snapshotHash,
			want:          repoPath + ": can't confirm the code graph is current",
		},
		{
			// A run that indexed no file wrote no checkpoint and has no files for a
			// graph to cover, so doctor has nothing to report about it. Reporting
			// the absent checkpoint would leave a healthy empty repository listed as
			// a problem on every doctor run.
			name:          "zero file run missing snapshot",
			graphState:    model.GraphStateAbsent,
			snapshotState: "missing",
			zeroFileRun:   true,
			want:          "",
		},
		{
			name:          "unreadable snapshot",
			graphState:    model.GraphStateReady,
			snapshotState: "unreadable",
			graphHash:     snapshotHash,
			want:          repoPath + ": can't confirm the code graph is current",
		},
		{
			name:          "failed graph build",
			graphState:    model.GraphStateStale,
			snapshotState: "current",
			graphHash:     snapshotHash,
			want:          repoPath + ": code graph's last update didn't finish (retries automatically)",
		},
		{
			name:          "ready graph behind current files",
			graphState:    model.GraphStateReady,
			snapshotState: "current",
			graphHash:     "different-hash",
			want:          repoPath + ": code graph is behind the current files (rebuilds automatically)",
		},
		{
			name:          "absent graph behind current files",
			graphState:    model.GraphStateAbsent,
			snapshotState: "current",
			want:          repoPath + ": code graph is behind the current files (rebuilds automatically)",
		},
		{
			name:          "ready and current",
			graphState:    model.GraphStateReady,
			snapshotState: "current",
			graphHash:     snapshotHash,
			want:          "",
		},
		{
			name:          "legacy empty kind missing snapshot",
			graphState:    model.GraphStateReady,
			snapshotState: "missing",
			graphHash:     snapshotHash,
			emptyKind:     true,
			want:          repoPath + ": can't confirm the code graph is current",
		},
		{
			name:          "non code codebase",
			graphState:    model.GraphStateReady,
			snapshotState: "missing",
			graphHash:     snapshotHash,
			codebaseKind:  model.CodebaseKindDocument,
			canonicalPath: "chat:///thread-alpha",
			want:          "",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			codebase := baseCodebase
			codebase.GraphState = testCase.graphState
			codebase.GraphSnapshotHash = testCase.graphHash
			if testCase.zeroFileRun {
				codebase.LastSuccessfulRun = &model.IndexRunSummary{IndexedFiles: 0, TotalChunks: 0, Status: "completed"}
			}
			if testCase.emptyKind {
				codebase.Kind = ""
			} else if testCase.codebaseKind != "" {
				codebase.Kind = testCase.codebaseKind
			}
			if testCase.canonicalPath != "" {
				codebase.CanonicalPath = testCase.canonicalPath
			}
			if err = os.Remove(codebase.MerkleSnapshotPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Remove returned error: %v", err)
			}
			switch testCase.snapshotState {
			case "current":
				if err = merkle.WriteSnapshot(codebase.MerkleSnapshotPath, snapshot); err != nil {
					t.Fatalf("WriteSnapshot returned error: %v", err)
				}
			case "unreadable":
				if err = os.WriteFile(codebase.MerkleSnapshotPath, []byte("{"), 0o644); err != nil {
					t.Fatalf("WriteFile returned error: %v", err)
				}
			case "missing":
			default:
				t.Fatalf("unknown snapshot state %q", testCase.snapshotState)
			}

			diagnostic := manager.graphDiagnostic(context.Background(), codebase)
			if diagnostic != testCase.want {
				t.Fatalf("graphDiagnostic = %q, want %q", diagnostic, testCase.want)
			}
		})
	}
}

func TestGraphStatusMissingSnapshotDoesNotLogError(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	codebase := newCodebaseRecord(repoPath)
	codebase.MerkleSnapshotPath = filepath.Join(t.TempDir(), "missing.json")

	handler := &recordingSlogHandler{}
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	_ = manager.graphDiagnostic(context.Background(), codebase)

	if handler.errorCount() != 0 {
		t.Fatalf("logged %d ERROR entries for missing snapshot, want 0", handler.errorCount())
	}
}

func waitForGraphClearing(t *testing.T, manager *Manager, codebaseID string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		manager.graphMutex.Lock()
		state := manager.graphLifecycle[codebaseID]
		clearing := state != nil && state.clearing
		manager.graphMutex.Unlock()
		if clearing {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("graph cache did not enter clearing state")
}

type recordingSlogHandler struct {
	mutex  sync.Mutex
	errors int
}

func (handler *recordingSlogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (handler *recordingSlogHandler) Handle(ctx context.Context, record slog.Record) error {
	_ = ctx
	if record.Level >= slog.LevelError {
		handler.mutex.Lock()
		handler.errors++
		handler.mutex.Unlock()
	}
	return nil
}

func (handler *recordingSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	_ = attrs
	return handler
}

func (handler *recordingSlogHandler) WithGroup(name string) slog.Handler {
	_ = name
	return handler
}

func (handler *recordingSlogHandler) errorCount() int {
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	return handler.errors
}
