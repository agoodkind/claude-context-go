package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/store"
)

func TestUpdateJobCompletedDoesNotOverwriteCancelledJob(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	codebase := newCodebaseRecord(repoPath)
	job := model.Job{ID: "job-cancelled", CodebaseID: codebase.ID, State: model.JobStateCancelled}
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[job.ID] = job
	manager.mu.Unlock()

	manager.updateJobCompleted(context.Background(), job.ID, indexer.Result{IndexedFiles: 1, TotalChunks: 1})

	manager.mu.Lock()
	recorded := manager.jobs[job.ID]
	manager.mu.Unlock()
	if recorded.State != model.JobStateCancelled {
		t.Fatalf("job state = %q, want %q", recorded.State, model.JobStateCancelled)
	}
}

func TestUpdateJobCompletedTurnsCancellingIntoCancelled(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	codebase := newCodebaseRecord(repoPath)
	job := model.Job{ID: "job-cancelling", CodebaseID: codebase.ID, State: model.JobStateCancelling}
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[job.ID] = job
	manager.mu.Unlock()

	manager.updateJobCompleted(context.Background(), job.ID, indexer.Result{IndexedFiles: 1, TotalChunks: 1})

	manager.mu.Lock()
	recorded := manager.jobs[job.ID]
	manager.mu.Unlock()
	if recorded.State != model.JobStateCancelled {
		t.Fatalf("job state = %q, want %q", recorded.State, model.JobStateCancelled)
	}
	if recorded.CompletedAt == nil {
		t.Fatal("CompletedAt is nil, want cancelled terminal timestamp")
	}
}

func TestUpdateJobCompletedRunsCancellationFollowupForDrainedSuccessor(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	configuration := manager.enrichIndexConfig(defaultIndexConfig())
	configuration.IgnoreDigest = digestIndexConfig(configuration)

	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	manager.runner = fakeRunner{
		indexOne: func(ctx context.Context, _ string, relativePath string, _ model.IndexConfig) (indexer.OneFileResult, error) {
			startOnce.Do(func() { close(started) })
			select {
			case <-release:
			case <-ctx.Done():
				return indexer.OneFileResult{}, ctx.Err()
			}
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{{
					Content:       "package main\n",
					RelativePath:  relativePath,
					StartLine:     1,
					EndLine:       1,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHash: hashText("package main\n"),
			}, nil
		},
	}

	codebase := newCodebaseRecord(repoPath)
	codebase.Status = model.CodebaseStatusIndexing
	codebase.EffectiveConfig = configuration
	job := model.Job{
		ID:            "job-cancelling-with-pending-successor",
		CodebaseID:    codebase.ID,
		CanonicalPath: repoPath,
		State:         model.JobStateCancelling,
		Config:        configuration,
	}
	codebase.ActiveJobID = job.ID
	stopped := make(chan string, 1)
	manager.SetCodebaseLifecycleHook(callbackLifecycleHook{
		indexStopped: func(_ context.Context, codebaseID string) {
			stopped <- codebaseID
		},
	})
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[job.ID] = job
	manager.pendingCodeJobs[codebase.ID] = pendingCodeRequest{
		requestedPath: repoPath,
		canonicalPath: repoPath,
		client:        testClientInfo(),
		indexConfig:   configuration,
	}
	manager.mu.Unlock()

	manager.updateJobCompleted(context.Background(), job.ID, indexer.Result{})

	select {
	case codebaseID := <-stopped:
		if codebaseID != codebase.ID {
			t.Fatalf("IndexStopped codebase = %q, want %q", codebaseID, codebase.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("IndexStopped was not called after cancelling completion")
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("drained successor never started")
	}
	close(release)

	var successor model.Job
	waitForCondition(t, func() bool {
		for _, candidate := range manager.ListJobs(codebase.ID) {
			if candidate.ID != job.ID {
				successor = candidate
				return candidate.State == model.JobStateCompleted ||
					candidate.State == model.JobStateFailed ||
					candidate.State == model.JobStateCancelled
			}
		}
		return false
	})
	if successor.ID == "" {
		t.Fatal("drained successor is missing")
	}
}

func TestUpdateJobCompletedJournalsAfterRegistryFinalization(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	codebase := newCodebaseRecord(repoPath)
	codebase.Status = model.CodebaseStatusIndexing
	job := model.Job{ID: "job-completed-journal-order", CodebaseID: codebase.ID, CanonicalPath: repoPath, State: model.JobStateRunning}
	codebase.ActiveJobID = job.ID
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[job.ID] = job
	manager.mu.Unlock()

	manager.appendJobTransition = func(event model.JobEvent) error {
		if event.Event != "job_completed" {
			t.Fatalf("journal event = %q, want job_completed", event.Event)
		}
		registry, err := store.ReadRegistry(cfg.RegistryPath)
		if err != nil {
			t.Fatalf("ReadRegistry: %v", err)
		}
		if len(registry.Codebases) != 1 {
			t.Fatalf("registry codebases = %d, want 1", len(registry.Codebases))
		}
		persisted := registry.Codebases[0]
		if persisted.Status != model.CodebaseStatusIndexed || persisted.ActiveJobID != "" {
			t.Fatalf("persisted completion = %+v", persisted)
		}
		if persisted.LastSuccessfulRun == nil || persisted.LastSuccessfulRun.IndexedFiles != 2 || persisted.LastSuccessfulRun.TotalChunks != 3 {
			t.Fatalf("persisted completion summary = %+v", persisted.LastSuccessfulRun)
		}
		return nil
	}

	manager.updateJobCompleted(context.Background(), job.ID, indexer.Result{IndexedFiles: 2, TotalChunks: 3})
}

func TestUpdateJobFailedJournalsAfterMissingSourceClassification(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	missingPath := repoPath + "-missing"
	codebase := newCodebaseRecord(missingPath)
	codebase.Status = model.CodebaseStatusIndexing
	job := model.Job{ID: "job-failed-journal-order", CodebaseID: codebase.ID, CanonicalPath: missingPath, State: model.JobStateRunning}
	codebase.ActiveJobID = job.ID
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[job.ID] = job
	manager.mu.Unlock()

	manager.appendJobTransition = func(event model.JobEvent) error {
		if event.Event != "job_failed" {
			t.Fatalf("journal event = %q, want job_failed", event.Event)
		}
		registry, err := store.ReadRegistry(cfg.RegistryPath)
		if err != nil {
			t.Fatalf("ReadRegistry: %v", err)
		}
		if len(registry.Codebases) != 1 {
			t.Fatalf("registry codebases = %d, want 1", len(registry.Codebases))
		}
		persisted := registry.Codebases[0]
		if persisted.Status != model.CodebaseStatusMissing || persisted.ActiveJobID != "" || persisted.LastFailedRun != nil {
			t.Fatalf("persisted missing-source failure = %+v", persisted)
		}
		return nil
	}

	manager.updateJobFailed(context.Background(), job.ID, errors.New("source vanished"))
}
