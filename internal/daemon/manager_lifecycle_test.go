package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/model"
)

type callbackLifecycleHook struct {
	indexReady   func(context.Context, model.Codebase)
	indexStopped func(context.Context, string)
}

func (hook callbackLifecycleHook) AddCodebase(context.Context, model.Codebase) {}

func (hook callbackLifecycleHook) RemoveCodebase(context.Context, string) {}

func (hook callbackLifecycleHook) IndexReady(ctx context.Context, codebase model.Codebase) {
	if hook.indexReady == nil {
		return
	}
	hook.indexReady(ctx, codebase)
}

func (hook callbackLifecycleHook) IndexStopped(ctx context.Context, codebaseID string) {
	if hook.indexStopped == nil {
		return
	}
	hook.indexStopped(ctx, codebaseID)
}

func TestIndexLifecycleHooksCanCallBackIntoManager(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(context.Context, *Manager, string)
	}{
		{
			name: "completed",
			run: func(ctx context.Context, manager *Manager, jobID string) {
				manager.updateJobCompleted(ctx, jobID, indexer.Result{IndexedFiles: 1, TotalChunks: 1})
			},
		},
		{
			name: "failed",
			run: func(ctx context.Context, manager *Manager, jobID string) {
				manager.updateJobFailed(ctx, jobID, errors.New("boom"))
			},
		},
		{
			name: "cancelled update",
			run: func(ctx context.Context, manager *Manager, jobID string) {
				manager.updateJobCancelled(ctx, jobID)
			},
		},
		{
			name: "cancel job",
			run: func(ctx context.Context, manager *Manager, jobID string) {
				if _, err := manager.CancelJob(ctx, jobID); err != nil {
					t.Errorf("CancelJob returned error: %v", err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			manager, _, repoPath := newTestManager(t)
			codebase := newCodebaseRecord(repoPath)
			codebase.Status = model.CodebaseStatusIndexing
			job := model.Job{ID: "job-" + test.name, CodebaseID: codebase.ID, State: model.JobStateRunning}
			codebase.ActiveJobID = job.ID

			manager.mu.Lock()
			manager.codebases[codebase.ID] = codebase
			manager.jobs[job.ID] = job
			if err := manager.saveLocked(); err != nil {
				manager.mu.Unlock()
				t.Fatalf("saveLocked returned error: %v", err)
			}
			manager.mu.Unlock()

			manager.SetCodebaseLifecycleHook(callbackLifecycleHook{
				indexReady: func(_ context.Context, _ model.Codebase) {
					if _, found := manager.GetJob(job.ID); !found {
						t.Errorf("GetJob(%s) not found from IndexReady hook", job.ID)
					}
				},
				indexStopped: func(_ context.Context, _ string) {
					if test.name == "cancelled update" || test.name == "cancel job" {
						if !manager.policyMutationMutex.TryLock() {
							t.Error("IndexStopped hook ran while policy mutation lock was held")
							return
						}
						manager.policyMutationMutex.Unlock()
						return
					}
					if _, found := manager.GetJob(job.ID); !found {
						t.Errorf("GetJob(%s) not found from IndexStopped hook", job.ID)
					}
				},
			})

			done := make(chan struct{})
			go func() {
				test.run(context.Background(), manager, job.ID)
				close(done)
			}()

			// A real hook-under-lock deadlock hangs forever, so a generous
			// timeout still catches it while tolerating slow CI: the update
			// paths do disk writes (saveLocked, snapshot/chunk writes) before
			// invoking the hook, which a tight bound could flake on.
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("lifecycle hook could not call back into Manager before timeout")
			}
		})
	}
}
