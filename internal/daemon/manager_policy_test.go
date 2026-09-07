package daemon

import (
	"context"
	"testing"

	"goodkind.io/lm-semantic-search/internal/model"
)

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
