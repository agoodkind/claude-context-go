package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/jobscheduler"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/platformactivity"
	"goodkind.io/lm-semantic-search/internal/store"
)

type readyActivitySource struct{}

func (readyActivitySource) Sample(context.Context) platformactivity.Snapshot {
	return platformactivity.Snapshot{
		InputAvailable:   true,
		InputIdleFor:     24 * time.Hour,
		ThermalAvailable: false,
	}
}

func (readyActivitySource) Close() {}

func TestUpdateCodebasePolicyRejectsEmptyPatch(t *testing.T) {
	manager, _, repoPath := newTestManager(t)

	_, err := manager.UpdateCodebasePolicy(
		context.Background(),
		repoPath,
		model.SchedulingPolicyPatch{},
	)
	if err == nil || err.Error() != "scheduling policy patch must set at least one field" {
		t.Fatalf("UpdateCodebasePolicy error = %v, want empty patch rejection", err)
	}
}

func TestDeduplicatedPolicyOverrideQueuesSuccessor(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	canonicalPath, err := manager.resolveCanonicalPath(repoPath)
	if err != nil {
		t.Fatalf("resolveCanonicalPath: %v", err)
	}
	codebase := newCodebaseRecord(canonicalPath)
	codebase.Status = model.CodebaseStatusIndexing
	job := newQueuedJob(
		codebase.ID,
		repoPath,
		canonicalPath,
		testClientInfo(),
		string(jobOperationIndex),
		false,
		defaultIndexConfig(),
		emptyAdmissionBudget,
		time.Now(),
	)
	job.State = model.JobStateRunning
	codebase.ActiveJobID = job.ID
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[job.ID] = job
	manager.mu.Unlock()

	priority := model.JobPriorityHigh
	patch := model.SchedulingPolicyPatch{Priority: &priority}
	manager.queueDeduplicatedPolicyOverride(
		job,
		repoPath,
		canonicalPath,
		testClientInfo(),
		defaultIndexConfig(),
		false,
		patch,
	)

	manager.mu.Lock()
	pending, found := manager.pendingCodeJobs[codebase.ID]
	manager.mu.Unlock()
	if !found {
		t.Fatal("deduplicated override did not queue a successor")
	}
	if pending.policyPatch.Priority == nil ||
		*pending.policyPatch.Priority != model.JobPriorityHigh {
		t.Fatalf("pending override = %+v, want high priority", pending.policyPatch)
	}
}

func TestFirstExplicitIndexPersistsPolicyAfterDiscovery(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	canonicalPath, err := manager.resolveCanonicalPath(repoPath)
	if err != nil {
		t.Fatalf("resolveCanonicalPath: %v", err)
	}
	codebase := newCodebaseRecord(canonicalPath)
	codebase.Status = model.CodebaseStatusDiscovered
	codebase.PolicyPendingInitialization = true
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mu.Unlock()

	priority := model.JobPriorityHigh
	quiet := true
	idleAfterSeconds := int32(600)
	patch := model.SchedulingPolicyPatch{
		Priority:         &priority,
		Quiet:            &quiet,
		IdleAfterSeconds: &idleAfterSeconds,
	}
	job, updated, _, _, err := manager.StartIndexWithPolicy(
		context.Background(),
		repoPath,
		testClientInfo(),
		defaultIndexConfig(),
		false,
		emptyAdmissionBudget,
		patch,
	)
	if err != nil {
		t.Fatalf("StartIndexWithPolicy: %v", err)
	}
	want := model.SchedulingPolicy{
		Priority:         model.JobPriorityHigh,
		Quiet:            true,
		IdleAfterSeconds: 600,
	}
	if updated.SchedulingPolicy != want || updated.PolicyPendingInitialization {
		t.Fatalf("codebase policy = %+v pending=%v, want %+v and false", updated.SchedulingPolicy, updated.PolicyPendingInitialization, want)
	}
	if job.EffectiveSchedulingPolicy != want || job.SchedulingOverride != patch {
		t.Fatalf("job policy = %+v override = %+v, want %+v and %+v", job.EffectiveSchedulingPolicy, job.SchedulingOverride, want, patch)
	}
	if job.QueueSequence == 0 {
		t.Fatal("job QueueSequence = 0, want assigned before journaling")
	}
	waitForTerminalJob(t, manager, job.ID)
}

func TestExistingCodebaseUsesIndexPolicyForOneRun(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	manager.jobScheduler.Close()
	manager.jobScheduler = jobscheduler.New(context.Background(), 1, readyActivitySource{})
	t.Cleanup(manager.jobScheduler.Close)
	canonicalPath, err := manager.resolveCanonicalPath(repoPath)
	if err != nil {
		t.Fatalf("resolveCanonicalPath: %v", err)
	}
	codebase := newCodebaseRecord(canonicalPath)
	stored := model.SchedulingPolicy{
		Priority:         model.JobPriorityNormal,
		Quiet:            false,
		IdleAfterSeconds: 300,
	}
	codebase.Status = model.CodebaseStatusIndexed
	codebase.SchedulingPolicy = stored
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mu.Unlock()

	priority := model.JobPriorityLow
	quiet := true
	patch := model.SchedulingPolicyPatch{Priority: &priority, Quiet: &quiet}
	job, updated, _, _, err := manager.StartIndexWithPolicy(
		context.Background(),
		repoPath,
		testClientInfo(),
		defaultIndexConfig(),
		false,
		emptyAdmissionBudget,
		patch,
	)
	if err != nil {
		t.Fatalf("StartIndexWithPolicy: %v", err)
	}
	wantEffective := model.SchedulingPolicy{
		Priority:         model.JobPriorityLow,
		Quiet:            true,
		IdleAfterSeconds: 300,
	}
	if updated.SchedulingPolicy != stored {
		t.Fatalf("stored policy = %+v, want %+v", updated.SchedulingPolicy, stored)
	}
	if job.EffectiveSchedulingPolicy != wantEffective || job.SchedulingOverride != patch {
		t.Fatalf("job policy = %+v override = %+v, want %+v and %+v", job.EffectiveSchedulingPolicy, job.SchedulingOverride, wantEffective, patch)
	}
	waitForTerminalJob(t, manager, job.ID)
}

func waitForTerminalJob(t *testing.T, manager *Manager, jobID string) {
	t.Helper()
	waitForCondition(t, func() bool {
		job, found := manager.GetJob(jobID)
		if !found {
			return false
		}
		return job.State == model.JobStateCompleted || job.State == model.JobStateFailed || job.State == model.JobStateCancelled
	})
}

func TestPendingCodeRequestMergesPolicyFields(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	canonicalPath, err := manager.resolveCanonicalPath(repoPath)
	if err != nil {
		t.Fatalf("resolveCanonicalPath: %v", err)
	}
	codebase := newCodebaseRecord(canonicalPath)
	codebase.Status = model.CodebaseStatusIndexed
	indexConfig := defaultIndexConfig()
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)
	firstPriority := model.JobPriorityHigh
	secondQuiet := true
	secondIdleAfterSeconds := int32(900)
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mergePendingCodeRequestLocked(codebase.ID, pendingCodeRequest{
		requestedPath: repoPath,
		canonicalPath: canonicalPath,
		client:        testClientInfo(),
		indexConfig:   indexConfig,
		policyPatch:   model.SchedulingPolicyPatch{Priority: &firstPriority},
	})
	manager.mergePendingCodeRequestLocked(codebase.ID, pendingCodeRequest{
		requestedPath: repoPath,
		canonicalPath: canonicalPath,
		client:        testClientInfo(),
		indexConfig:   indexConfig,
		policyPatch: model.SchedulingPolicyPatch{
			Quiet:            &secondQuiet,
			IdleAfterSeconds: &secondIdleAfterSeconds,
		},
	})
	merged := manager.pendingCodeJobs[codebase.ID]
	jobID, drained := manager.drainPendingJobLocked(context.Background(), codebase.ID)
	manager.mu.Unlock()
	if !drained || jobID == "" {
		t.Fatal("pending request did not drain into a successor job")
	}
	if merged.policyPatch.Priority == nil || *merged.policyPatch.Priority != model.JobPriorityHigh {
		t.Fatalf("merged priority = %v, want high", merged.policyPatch.Priority)
	}
	if merged.policyPatch.Quiet == nil || !*merged.policyPatch.Quiet {
		t.Fatalf("merged quiet = %v, want true", merged.policyPatch.Quiet)
	}
	if merged.policyPatch.IdleAfterSeconds == nil || *merged.policyPatch.IdleAfterSeconds != 900 {
		t.Fatalf("merged idle after = %v, want 900", merged.policyPatch.IdleAfterSeconds)
	}
	job, found := manager.GetJob(jobID)
	if !found {
		t.Fatalf("drained job %q is missing", jobID)
	}
	want := model.SchedulingPolicy{
		Priority:         model.JobPriorityHigh,
		Quiet:            true,
		IdleAfterSeconds: 900,
	}
	if job.EffectiveSchedulingPolicy != want {
		t.Fatalf("drained job policy = %+v, want %+v", job.EffectiveSchedulingPolicy, want)
	}
}

func TestResumePlanPreservesInterruptedEffectivePolicy(t *testing.T) {
	priority := model.JobPriorityHigh
	quiet := true
	idleAfterSeconds := int32(900)
	wantPolicy := model.SchedulingPolicy{
		Priority:         priority,
		Quiet:            quiet,
		IdleAfterSeconds: idleAfterSeconds,
	}
	wantOverride := model.SchedulingPolicyPatch{
		Priority:         &priority,
		Quiet:            &quiet,
		IdleAfterSeconds: &idleAfterSeconds,
	}
	plan := resumePlan{
		effectiveSchedulingPolicy: wantPolicy,
		schedulingOverride:        wantOverride,
		queueSequence:             7,
	}
	if plan.effectiveSchedulingPolicy != wantPolicy || plan.schedulingOverride != wantOverride || plan.queueSequence != 7 {
		t.Fatalf("resume plan = %+v, want preserved policy, override, and queue sequence", plan)
	}
}

func TestRecoveredJobsKeepQueueSequenceOrder(t *testing.T) {
	plans := []resumePlan{
		{codebaseID: "unknown", queueSequence: 0},
		{codebaseID: "third", queueSequence: 30},
		{codebaseID: "first", queueSequence: 10},
		{codebaseID: "second", queueSequence: 20},
	}
	sortResumePlans(plans)
	got := []string{plans[0].codebaseID, plans[1].codebaseID, plans[2].codebaseID, plans[3].codebaseID}
	want := []string{"first", "second", "third", "unknown"}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("recovered order = %v, want %v", got, want)
		}
	}
}

func TestUpdateCodebasePolicyReclassifiesActiveQueuedAndPausedJobs(t *testing.T) {
	states := []model.JobState{
		model.JobStateRunning,
		model.JobStateQueued,
		model.JobStatePaused,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			manager, cfg, repoPath := newTestManager(t)
			manager.jobScheduler.Close()
			manager.jobScheduler = jobscheduler.New(
				context.Background(),
				1,
				readyActivitySource{},
			)
			t.Cleanup(manager.jobScheduler.Close)

			originalCodebase, originalJob := seedPolicyUpdateJob(
				t,
				manager,
				repoPath,
				state,
			)
			cleanupScheduler := registerPolicyUpdateSchedulerJob(
				t,
				manager.jobScheduler,
				originalJob,
			)
			t.Cleanup(cleanupScheduler)

			priority := model.JobPriorityHigh
			updated, err := manager.UpdateCodebasePolicy(
				context.Background(),
				repoPath,
				model.SchedulingPolicyPatch{Priority: &priority},
			)
			if err != nil {
				t.Fatalf("UpdateCodebasePolicy: %v", err)
			}
			if updated.SchedulingPolicy.Priority != model.JobPriorityHigh ||
				updated.SchedulingPolicy.Quiet != originalCodebase.SchedulingPolicy.Quiet ||
				updated.SchedulingPolicy.IdleAfterSeconds != originalCodebase.SchedulingPolicy.IdleAfterSeconds ||
				updated.PolicyPendingInitialization {
				t.Fatalf("updated codebase policy = %+v pending=%v", updated.SchedulingPolicy, updated.PolicyPendingInitialization)
			}

			job, found := manager.GetJob(originalJob.ID)
			if !found {
				t.Fatalf("updated job %q is missing", originalJob.ID)
			}
			if job.State != state ||
				job.EffectiveSchedulingPolicy.Priority != model.JobPriorityHigh ||
				!job.EffectiveSchedulingPolicy.Quiet ||
				job.EffectiveSchedulingPolicy.IdleAfterSeconds != 900 {
				t.Fatalf("updated job = %+v", job)
			}
			if job.SchedulingOverride.Priority != nil ||
				job.SchedulingOverride.Quiet == nil ||
				!*job.SchedulingOverride.Quiet ||
				job.SchedulingOverride.IdleAfterSeconds == nil ||
				*job.SchedulingOverride.IdleAfterSeconds != 900 {
				t.Fatalf("updated override = %+v", job.SchedulingOverride)
			}

			snapshot := manager.jobScheduler.Snapshot()
			switch state {
			case model.JobStateRunning:
				if snapshot.Running[model.JobPriorityHigh] != 1 ||
					snapshot.Running[model.JobPriorityLow] != 0 {
					t.Fatalf("running snapshot = %+v", snapshot)
				}
			case model.JobStateQueued:
				if snapshot.Queued[model.JobPriorityHigh] != 1 ||
					snapshot.Queued[model.JobPriorityLow] != 0 {
					t.Fatalf("queued snapshot = %+v", snapshot)
				}
			case model.JobStatePaused:
				if snapshot.Paused[model.JobPriorityHigh] != 1 ||
					snapshot.Paused[model.JobPriorityLow] != 0 {
					t.Fatalf("paused snapshot = %+v", snapshot)
				}
			case model.JobStateCancelling,
				model.JobStateCompleted,
				model.JobStateFailed,
				model.JobStateCancelled:
				t.Fatalf("unexpected test state %q", state)
			}

			registry, err := store.ReadRegistry(cfg.RegistryPath)
			if err != nil {
				t.Fatalf("ReadRegistry: %v", err)
			}
			if len(registry.Codebases) != 1 ||
				registry.Codebases[0].SchedulingPolicy != updated.SchedulingPolicy {
				t.Fatalf("stored registry = %+v", registry.Codebases)
			}
			assertNoPolicyUpdateMarker(t, cfg.RegistryPath)
		})
	}
}

func TestUpdateCodebasePolicyReclassifiesWatcherQueuedRunningAndPausedWork(
	t *testing.T,
) {
	states := []model.JobState{
		model.JobStateQueued,
		model.JobStateRunning,
		model.JobStatePaused,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			manager, _, repoPath := newTestManager(t)
			manager.jobScheduler.Close()
			manager.jobScheduler = jobscheduler.New(
				context.Background(),
				1,
				readyActivitySource{},
			)
			t.Cleanup(manager.jobScheduler.Close)

			_, watcherJob := seedWatcherPolicyUpdateJob(
				t,
				manager,
				repoPath,
				state,
			)
			cleanupScheduler := registerPolicyUpdateSchedulerJob(
				t,
				manager.jobScheduler,
				watcherJob,
			)
			t.Cleanup(cleanupScheduler)

			priority := model.JobPriorityHigh
			if _, err := manager.UpdateCodebasePolicy(
				context.Background(),
				repoPath,
				model.SchedulingPolicyPatch{Priority: &priority},
			); err != nil {
				t.Fatalf("UpdateCodebasePolicy: %v", err)
			}

			updatedJob, found := manager.GetJob(watcherJob.ID)
			if !found {
				t.Fatalf("watcher job %q is missing", watcherJob.ID)
			}
			if updatedJob.EffectiveSchedulingPolicy.Priority != model.JobPriorityHigh ||
				updatedJob.SchedulingOverride.Priority != nil {
				t.Fatalf(
					"watcher policy = %+v override=%+v, want high without priority override",
					updatedJob.EffectiveSchedulingPolicy,
					updatedJob.SchedulingOverride,
				)
			}

			snapshot := manager.jobScheduler.Snapshot()
			switch state {
			case model.JobStateQueued:
				if snapshot.Queued[model.JobPriorityHigh] != 1 ||
					snapshot.Queued[model.JobPriorityLow] != 0 {
					t.Fatalf("queued watcher snapshot = %+v", snapshot)
				}
			case model.JobStateRunning:
				if snapshot.Running[model.JobPriorityHigh] != 1 ||
					snapshot.Running[model.JobPriorityLow] != 0 {
					t.Fatalf("running watcher snapshot = %+v", snapshot)
				}
			case model.JobStatePaused:
				if snapshot.Paused[model.JobPriorityHigh] != 1 ||
					snapshot.Paused[model.JobPriorityLow] != 0 {
					t.Fatalf("paused watcher snapshot = %+v", snapshot)
				}
			case model.JobStateCancelling,
				model.JobStateCompleted,
				model.JobStateFailed,
				model.JobStateCancelled:
				t.Fatalf("unexpected watcher test state %q", state)
			}
		})
	}
}

func TestUpdateCodebasePolicyChangesAcceptedPendingWork(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	manager.semantic = &fakeSemantic{}
	manager.jobScheduler.Close()
	manager.jobScheduler = jobscheduler.New(
		context.Background(),
		1,
		readyActivitySource{},
	)
	t.Cleanup(manager.jobScheduler.Close)

	entered := make(chan struct{}, 2)
	release := make(chan struct{}, 2)
	t.Cleanup(func() {
		for range 2 {
			select {
			case release <- struct{}{}:
			default:
			}
		}
	})
	manager.runner = fakeRunner{
		index:      nil,
		indexFiles: nil,
		indexOne: func(
			ctx context.Context,
			_ string,
			relativePath string,
			_ model.IndexConfig,
		) (indexer.OneFileResult, error) {
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return indexer.OneFileResult{
					Chunks:   nil,
					FileHash: "",
					Skipped:  false,
					Removed:  false,
				}, ctx.Err()
			}
			content := "package main\n"
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{{
					Content:       content,
					RelativePath:  relativePath,
					StartLine:     1,
					EndLine:       1,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHash: hashText(content),
				Skipped:  false,
				Removed:  false,
			}, nil
		},
	}

	firstJob, _, _, _, err := manager.StartIndex(
		context.Background(),
		repoPath,
		testClientInfo(),
		defaultIndexConfig(),
		false,
		emptyAdmissionBudget,
	)
	if err != nil {
		t.Fatalf("StartIndex first job: %v", err)
	}
	waitForPolicyRunnerEntry(t, entered)

	pendingConfig := defaultIndexConfig()
	pendingConfig.IgnorePatterns = []string{"pending-policy-change"}
	priority := model.JobPriorityLow
	quiet := true
	deduplicatedJob, _, deduplicated, _, err := manager.StartIndexWithPolicy(
		context.Background(),
		repoPath,
		testClientInfo(),
		pendingConfig,
		false,
		emptyAdmissionBudget,
		model.SchedulingPolicyPatch{Priority: &priority, Quiet: &quiet},
	)
	if err != nil {
		t.Fatalf("StartIndexWithPolicy pending job: %v", err)
	}
	if !deduplicated || deduplicatedJob.ID != firstJob.ID {
		t.Fatalf(
			"pending request returned job=%q deduplicated=%v, want active job %q",
			deduplicatedJob.ID,
			deduplicated,
			firstJob.ID,
		)
	}

	highPriority := model.JobPriorityHigh
	if _, err := manager.UpdateCodebasePolicy(
		context.Background(),
		repoPath,
		model.SchedulingPolicyPatch{Priority: &highPriority},
	); err != nil {
		t.Fatalf("UpdateCodebasePolicy: %v", err)
	}
	manager.mu.Lock()
	pending := manager.pendingCodeJobs[firstJob.CodebaseID]
	manager.mu.Unlock()
	if pending.policyPatch.Priority != nil ||
		pending.policyPatch.Quiet == nil ||
		!*pending.policyPatch.Quiet {
		t.Fatalf("pending policy patch after stored update = %+v", pending.policyPatch)
	}

	release <- struct{}{}
	waitForPolicyRunnerEntry(t, entered)
	var successor model.Job
	waitForCondition(t, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		for _, job := range manager.jobs {
			if job.ID == firstJob.ID || job.CodebaseID != firstJob.CodebaseID {
				continue
			}
			successor = job
			return true
		}
		return false
	})
	if successor.EffectiveSchedulingPolicy.Priority != model.JobPriorityHigh ||
		!successor.EffectiveSchedulingPolicy.Quiet ||
		successor.SchedulingOverride.Priority != nil ||
		successor.SchedulingOverride.Quiet == nil ||
		!*successor.SchedulingOverride.Quiet {
		t.Fatalf(
			"drained successor policy = %+v override=%+v",
			successor.EffectiveSchedulingPolicy,
			successor.SchedulingOverride,
		)
	}
	release <- struct{}{}
	waitForTerminalJob(t, manager, successor.ID)
}

func TestUpdateCodebasePolicyCoordinatesQueuedSchedulerRegistration(
	t *testing.T,
) {
	manager, _, repoPath := newTestManager(t)
	manager.jobScheduler.Close()
	manager.jobScheduler = jobscheduler.New(
		context.Background(),
		1,
		readyActivitySource{},
	)
	t.Cleanup(manager.jobScheduler.Close)
	_, queuedJob := seedPolicyUpdateJob(
		t,
		manager,
		repoPath,
		model.JobStateQueued,
	)

	blockerPolicy := model.DefaultSchedulingPolicy()
	blockerPolicy.Priority = model.JobPriorityHigh
	blocker, err := manager.jobScheduler.Acquire(
		context.Background(),
		jobscheduler.Entry{
			JobID:         "queued-registration-blocker",
			Policy:        blockerPolicy,
			QueueSequence: 1,
		},
	)
	if err != nil {
		t.Fatalf("Acquire blocker: %v", err)
	}

	highPriority := model.JobPriorityHigh
	if _, err := manager.UpdateCodebasePolicy(
		context.Background(),
		repoPath,
		model.SchedulingPolicyPatch{Priority: &highPriority},
	); err != nil {
		blocker.Release()
		t.Fatalf("UpdateCodebasePolicy: %v", err)
	}

	registrationContext, cancelRegistration := context.WithCancel(context.Background())
	registrationResult := make(chan *jobCapacity, 1)
	go func() {
		capacity, _, _ := manager.acquireJobCapacity(
			registrationContext,
			queuedJob,
			false,
		)
		registrationResult <- capacity
	}()
	t.Cleanup(func() {
		cancelRegistration()
		blocker.Release()
		select {
		case capacity := <-registrationResult:
			if capacity != nil {
				capacity.release(context.Background())
			}
		case <-time.After(5 * time.Second):
			t.Errorf("queued scheduler registration did not stop")
		}
	})

	waitForCondition(t, func() bool {
		snapshot := manager.jobScheduler.Snapshot()
		return snapshot.Queued[model.JobPriorityHigh]+
			snapshot.Queued[model.JobPriorityLow] == 1
	})
	snapshot := manager.jobScheduler.Snapshot()
	if snapshot.Queued[model.JobPriorityHigh] != 1 ||
		snapshot.Queued[model.JobPriorityLow] != 0 {
		t.Fatalf(
			"post-ack queued registration snapshot = %+v, want high policy",
			snapshot,
		)
	}
}

func TestUpdateCodebasePolicyRollsBackRegistryAfterJournalFailure(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	originalCodebase, originalJob := seedPolicyUpdateJob(
		t,
		manager,
		repoPath,
		model.JobStateRunning,
	)
	originalWatcherJob := addDetachedPolicyUpdateJob(
		t,
		manager,
		originalCodebase,
		model.JobStatePaused,
	)
	originalAppendTransition := manager.appendJobTransition
	journalErr := errors.New("injected policy journal failure")
	observedPatchedRegistry := false
	policyEventCount := 0
	manager.appendJobTransition = func(event model.JobEvent) error {
		if event.Event == "job_policy_updated" {
			policyEventCount++
			if policyEventCount == 1 {
				return originalAppendTransition(event)
			}
			registry, readErr := store.ReadRegistry(cfg.RegistryPath)
			if readErr != nil {
				return fmt.Errorf("read registry before injected journal failure: %w", readErr)
			}
			if len(registry.Codebases) != 1 ||
				registry.Codebases[0].SchedulingPolicy.Priority != model.JobPriorityHigh {
				return fmt.Errorf(
					"registry before injected journal failure = %+v, want high priority",
					registry.Codebases,
				)
			}
			observedPatchedRegistry = true
			return journalErr
		}
		return originalAppendTransition(event)
	}
	t.Cleanup(func() { manager.appendJobTransition = originalAppendTransition })

	priority := model.JobPriorityHigh
	_, err := manager.UpdateCodebasePolicy(
		context.Background(),
		repoPath,
		model.SchedulingPolicyPatch{Priority: &priority},
	)
	if !errors.Is(err, journalErr) {
		t.Fatalf("UpdateCodebasePolicy error = %v, want injected journal failure", err)
	}
	if !observedPatchedRegistry {
		t.Fatal("journal failure ran before the patched registry reached disk")
	}

	manager.mu.Lock()
	gotCodebase := manager.codebases[originalCodebase.ID]
	gotJob := manager.jobs[originalJob.ID]
	gotWatcherJob := manager.jobs[originalWatcherJob.ID]
	manager.mu.Unlock()
	if !reflect.DeepEqual(gotCodebase, originalCodebase) {
		t.Fatalf("in-memory codebase after rollback = %+v, want %+v", gotCodebase, originalCodebase)
	}
	if !reflect.DeepEqual(gotJob, originalJob) {
		t.Fatalf("in-memory job after rollback = %+v, want %+v", gotJob, originalJob)
	}
	if !reflect.DeepEqual(gotWatcherJob, originalWatcherJob) {
		t.Fatalf(
			"in-memory watcher job after rollback = %+v, want %+v",
			gotWatcherJob,
			originalWatcherJob,
		)
	}

	registry, readErr := store.ReadRegistry(cfg.RegistryPath)
	if readErr != nil {
		t.Fatalf("ReadRegistry: %v", readErr)
	}
	if len(registry.Codebases) != 1 ||
		registry.Codebases[0].ID != originalCodebase.ID ||
		registry.Codebases[0].SchedulingPolicy != originalCodebase.SchedulingPolicy ||
		registry.Codebases[0].PolicyPendingInitialization !=
			originalCodebase.PolicyPendingInitialization {
		t.Fatalf("registry after rollback = %+v, want %+v", registry.Codebases, originalCodebase)
	}
	jobs, readErr := store.ReadJobEvents(cfg.JobsPath)
	if readErr != nil {
		t.Fatalf("ReadJobEvents: %v", readErr)
	}
	journalJob := jobs[originalJob.ID]
	if journalJob.ID != originalJob.ID ||
		journalJob.State != originalJob.State ||
		journalJob.EffectiveSchedulingPolicy != originalJob.EffectiveSchedulingPolicy ||
		journalJob.SchedulingOverride.Priority == nil ||
		*journalJob.SchedulingOverride.Priority != model.JobPriorityLow {
		t.Fatalf("journal job after rollback = %+v, want old policy", journalJob)
	}
	journalWatcherJob := jobs[originalWatcherJob.ID]
	if journalWatcherJob.ID != originalWatcherJob.ID ||
		journalWatcherJob.State != originalWatcherJob.State ||
		journalWatcherJob.EffectiveSchedulingPolicy !=
			originalWatcherJob.EffectiveSchedulingPolicy ||
		journalWatcherJob.SchedulingOverride.Priority == nil ||
		*journalWatcherJob.SchedulingOverride.Priority != model.JobPriorityLow {
		t.Fatalf(
			"journal watcher job after rollback = %+v, want old policy",
			journalWatcherJob,
		)
	}
	assertNoPolicyUpdateMarker(t, cfg.RegistryPath)
}

func TestManagerStartupRollsBackPendingPolicyUpdateBeforeReplay(t *testing.T) {
	cfg, repoPath := newTestManagerConfig(t)
	codebase := newCodebaseRecord(repoPath)
	codebase.Status = model.CodebaseStatusIndexing
	codebase.PolicyPendingInitialization = true
	job := newQueuedJob(
		codebase.ID,
		repoPath,
		repoPath,
		testClientInfo(),
		string(jobOperationSync),
		false,
		defaultIndexConfig(),
		emptyAdmissionBudget,
		time.Now(),
	)
	job.State = model.JobStateRunning
	lowPriority := model.JobPriorityLow
	job.EffectiveSchedulingPolicy.Priority = lowPriority
	job.SchedulingOverride.Priority = &lowPriority
	codebase.ActiveJobID = job.ID
	watcherJob := newQueuedJob(
		codebase.ID,
		repoPath,
		repoPath,
		model.ClientInfo{Name: "daemon-watcher", PID: 0},
		"converge",
		false,
		defaultIndexConfig(),
		emptyAdmissionBudget,
		time.Now(),
	)
	watcherJob.State = model.JobStatePaused
	watcherJob.EffectiveSchedulingPolicy.Priority = lowPriority
	watcherJob.SchedulingOverride.Priority = &lowPriority

	patchedCodebase := codebase
	patchedCodebase.SchedulingPolicy.Priority = model.JobPriorityHigh
	patchedCodebase.PolicyPendingInitialization = false
	patchedJob := job
	patchedJob.EffectiveSchedulingPolicy.Priority = model.JobPriorityHigh
	patchedJob.SchedulingOverride.Priority = nil
	patchedWatcherJob := watcherJob
	patchedWatcherJob.EffectiveSchedulingPolicy.Priority = model.JobPriorityHigh
	patchedWatcherJob.SchedulingOverride.Priority = nil

	transaction := model.PolicyUpdateTransaction{
		CodebaseID:      codebase.ID,
		OldCodebase:     codebase,
		OldActiveJob:    &job,
		OldDetachedJobs: []model.Job{watcherJob},
	}
	markerPath := store.PolicyUpdatePath(cfg.RegistryPath)
	if err := store.WritePolicyUpdate(markerPath, transaction); err != nil {
		t.Fatalf("WritePolicyUpdate: %v", err)
	}
	if err := store.WriteRegistry(cfg.RegistryPath, model.RegistryFile{
		Codebases: []model.Codebase{patchedCodebase},
		UpdatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("WriteRegistry: %v", err)
	}
	if err := store.AppendJobEventSync(cfg.JobsPath, model.JobEvent{
		Event:      "job_policy_updated",
		OccurredAt: time.Now(),
		Job:        patchedJob,
	}); err != nil {
		t.Fatalf("AppendJobEventSync: %v", err)
	}
	if err := store.AppendJobEventSync(cfg.JobsPath, model.JobEvent{
		Event:      "job_policy_updated",
		OccurredAt: time.Now(),
		Job:        patchedWatcherJob,
	}); err != nil {
		t.Fatalf("AppendJobEventSync watcher: %v", err)
	}

	restarted, err := NewManager(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if closeErr := restarted.Close(ctx); closeErr != nil {
			t.Errorf("Close restarted manager: %v", closeErr)
		}
	})

	restarted.mu.Lock()
	gotCodebase := restarted.codebases[codebase.ID]
	gotJob := restarted.jobs[job.ID]
	gotWatcherJob := restarted.jobs[watcherJob.ID]
	restarted.mu.Unlock()
	if !reflect.DeepEqual(gotCodebase, codebase) {
		t.Fatalf("restarted codebase = %+v, want %+v", gotCodebase, codebase)
	}
	if gotJob.EffectiveSchedulingPolicy != job.EffectiveSchedulingPolicy ||
		gotJob.SchedulingOverride.Priority == nil ||
		*gotJob.SchedulingOverride.Priority != lowPriority {
		t.Fatalf("restarted job policy = %+v override=%+v", gotJob.EffectiveSchedulingPolicy, gotJob.SchedulingOverride)
	}
	if gotWatcherJob.EffectiveSchedulingPolicy != watcherJob.EffectiveSchedulingPolicy ||
		gotWatcherJob.SchedulingOverride.Priority == nil ||
		*gotWatcherJob.SchedulingOverride.Priority != lowPriority {
		t.Fatalf(
			"restarted watcher policy = %+v override=%+v",
			gotWatcherJob.EffectiveSchedulingPolicy,
			gotWatcherJob.SchedulingOverride,
		)
	}
	assertNoPolicyUpdateMarker(t, cfg.RegistryPath)
}

func seedPolicyUpdateJob(
	t *testing.T,
	manager *Manager,
	repoPath string,
	state model.JobState,
) (model.Codebase, model.Job) {
	t.Helper()
	canonicalPath, err := manager.resolveCanonicalPath(repoPath)
	if err != nil {
		t.Fatalf("resolveCanonicalPath: %v", err)
	}
	codebase := newCodebaseRecord(canonicalPath)
	codebase.Status = model.CodebaseStatusIndexing
	codebase.PolicyPendingInitialization = true
	priority := model.JobPriorityLow
	quiet := true
	idleAfterSeconds := int32(900)
	job := newQueuedJob(
		codebase.ID,
		repoPath,
		canonicalPath,
		testClientInfo(),
		string(jobOperationSync),
		false,
		defaultIndexConfig(),
		emptyAdmissionBudget,
		time.Now(),
	)
	job.State = state
	job.EffectiveSchedulingPolicy = model.SchedulingPolicy{
		Priority:         priority,
		Quiet:            quiet,
		IdleAfterSeconds: idleAfterSeconds,
	}
	job.SchedulingOverride = model.SchedulingPolicyPatch{
		Priority:         &priority,
		Quiet:            &quiet,
		IdleAfterSeconds: &idleAfterSeconds,
	}
	codebase.ActiveJobID = job.ID

	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[job.ID] = job
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		t.Fatalf("saveLocked: %v", err)
	}
	manager.mu.Unlock()
	if err := manager.writeJobTransition(model.JobEvent{
		Event:      "job_" + string(state),
		OccurredAt: time.Now(),
		Job:        job,
	}); err != nil {
		t.Fatalf("writeJobTransition: %v", err)
	}
	return codebase, job
}

func seedWatcherPolicyUpdateJob(
	t *testing.T,
	manager *Manager,
	repoPath string,
	state model.JobState,
) (model.Codebase, model.Job) {
	t.Helper()
	canonicalPath, err := manager.resolveCanonicalPath(repoPath)
	if err != nil {
		t.Fatalf("resolveCanonicalPath: %v", err)
	}
	codebase := newCodebaseRecord(canonicalPath)
	codebase.Status = model.CodebaseStatusIndexed
	priority := model.JobPriorityLow
	job := newQueuedJob(
		codebase.ID,
		repoPath,
		canonicalPath,
		model.ClientInfo{Name: "daemon-watcher", PID: 0},
		"converge",
		false,
		defaultIndexConfig(),
		emptyAdmissionBudget,
		time.Now(),
	)
	job.State = state
	job.EffectiveSchedulingPolicy.Priority = priority
	job.SchedulingOverride.Priority = &priority

	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[job.ID] = job
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		t.Fatalf("saveLocked: %v", err)
	}
	manager.mu.Unlock()
	if err := manager.writeJobTransition(model.JobEvent{
		Event:      "converge_" + string(state),
		OccurredAt: time.Now(),
		Job:        job,
	}); err != nil {
		t.Fatalf("writeJobTransition: %v", err)
	}
	return codebase, job
}

func addDetachedPolicyUpdateJob(
	t *testing.T,
	manager *Manager,
	codebase model.Codebase,
	state model.JobState,
) model.Job {
	t.Helper()
	priority := model.JobPriorityLow
	job := newQueuedJob(
		codebase.ID,
		codebase.CanonicalPath,
		codebase.CanonicalPath,
		model.ClientInfo{Name: "daemon-watcher", PID: 0},
		"converge",
		false,
		codebase.EffectiveConfig,
		emptyAdmissionBudget,
		time.Now(),
	)
	job.State = state
	job.EffectiveSchedulingPolicy.Priority = priority
	job.SchedulingOverride.Priority = &priority
	manager.mu.Lock()
	manager.jobs[job.ID] = job
	manager.mu.Unlock()
	if err := manager.writeJobTransition(model.JobEvent{
		Event:      "converge_" + string(state),
		OccurredAt: time.Now(),
		Job:        job,
	}); err != nil {
		t.Fatalf("write detached job transition: %v", err)
	}
	return job
}

func registerPolicyUpdateSchedulerJob(
	t *testing.T,
	scheduler *jobscheduler.Scheduler,
	job model.Job,
) func() {
	t.Helper()
	entry := jobscheduler.Entry{
		JobID:         job.ID,
		Policy:        job.EffectiveSchedulingPolicy,
		QueueSequence: job.QueueSequence,
	}
	switch job.State {
	case model.JobStateRunning:
		lease, err := scheduler.Acquire(context.Background(), entry)
		if err != nil {
			t.Fatalf("Acquire running job: %v", err)
		}
		return lease.Release
	case model.JobStatePaused:
		lease, err := scheduler.Acquire(context.Background(), entry)
		if err != nil {
			t.Fatalf("Acquire paused job: %v", err)
		}
		if !lease.Yield("test pause") {
			t.Fatal("Yield paused job returned false")
		}
		return lease.Release
	case model.JobStateQueued:
		blockerPolicy := model.DefaultSchedulingPolicy()
		blockerPolicy.Priority = model.JobPriorityHigh
		blocker, err := scheduler.Acquire(context.Background(), jobscheduler.Entry{
			JobID:         "policy-update-blocker",
			Policy:        blockerPolicy,
			QueueSequence: 1,
		})
		if err != nil {
			t.Fatalf("Acquire blocker: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			lease, acquireErr := scheduler.Acquire(ctx, entry)
			if lease != nil {
				lease.Release()
			}
			result <- acquireErr
		}()
		waitForCondition(t, func() bool {
			return scheduler.Snapshot().Queued[model.JobPriorityLow] == 1
		})
		return func() {
			cancel()
			blocker.Release()
			<-result
		}
	case model.JobStateCancelling,
		model.JobStateCompleted,
		model.JobStateFailed,
		model.JobStateCancelled:
		t.Fatalf("unsupported scheduler test state %q", job.State)
		return func() {}
	}
	return func() {}
}

func assertNoPolicyUpdateMarker(t *testing.T, registryPath string) {
	t.Helper()
	_, err := store.ReadPolicyUpdate(store.PolicyUpdatePath(registryPath))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadPolicyUpdate error = %v, want marker absent", err)
	}
}

func waitForPolicyRunnerEntry(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("policy test runner did not start")
	}
}
