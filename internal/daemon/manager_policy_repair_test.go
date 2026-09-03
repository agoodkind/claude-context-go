package daemon

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/jobscheduler"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/store"
)

func TestUpdateCodebasePolicyCleansAmbiguousCommittedMarker(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	canonicalPath, err := manager.resolveCanonicalPath(repoPath)
	if err != nil {
		t.Fatalf("resolveCanonicalPath: %v", err)
	}
	codebase := newCodebaseRecord(canonicalPath)
	codebase.Status = model.CodebaseStatusIndexed
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		t.Fatalf("saveLocked: %v", err)
	}
	manager.mu.Unlock()

	originalWrite := writePolicyUpdateTransaction
	syncErr := errors.New("injected post-rename directory sync failure")
	writePolicyUpdateTransaction = func(
		path string,
		transaction model.PolicyUpdateTransaction,
	) error {
		if err := store.WritePolicyUpdate(path, transaction); err != nil {
			return err
		}
		return errors.Join(store.ErrPolicyUpdateMarkerMayExist, syncErr)
	}
	t.Cleanup(func() { writePolicyUpdateTransaction = originalWrite })

	priority := model.JobPriorityHigh
	_, err = manager.UpdateCodebasePolicy(
		context.Background(),
		repoPath,
		model.SchedulingPolicyPatch{Priority: &priority},
	)
	if !errors.Is(err, syncErr) {
		t.Fatalf("UpdateCodebasePolicy error = %v, want post-rename sync failure", err)
	}
	assertNoPolicyUpdateMarker(t, cfg.RegistryPath)
	manager.mu.Lock()
	gotCodebase := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if gotCodebase.SchedulingPolicy != codebase.SchedulingPolicy {
		t.Fatalf(
			"in-memory policy after ambiguous marker rollback = %+v, want %+v",
			gotCodebase.SchedulingPolicy,
			codebase.SchedulingPolicy,
		)
	}
	registry, readErr := store.ReadRegistry(cfg.RegistryPath)
	if readErr != nil {
		t.Fatalf("ReadRegistry: %v", readErr)
	}
	if len(registry.Codebases) != 1 ||
		registry.Codebases[0].SchedulingPolicy != codebase.SchedulingPolicy {
		t.Fatalf("registry after ambiguous marker rollback = %+v", registry.Codebases)
	}
}

func TestUpdateCodebasePolicyRollbackRemovesStagedRegistration(t *testing.T) {
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

	originalRemove := removePolicyUpdateTransaction
	removeErr := errors.New("injected marker removal failure")
	removeCount := 0
	removePolicyUpdateTransaction = func(path string) error {
		removeCount++
		if removeCount == 1 {
			return removeErr
		}
		return store.RemovePolicyUpdate(path)
	}
	t.Cleanup(func() { removePolicyUpdateTransaction = originalRemove })

	priority := model.JobPriorityHigh
	_, err := manager.UpdateCodebasePolicy(
		context.Background(),
		repoPath,
		model.SchedulingPolicyPatch{Priority: &priority},
	)
	if !errors.Is(err, removeErr) {
		t.Fatalf("UpdateCodebasePolicy error = %v, want marker removal failure", err)
	}
	if got := queuedRegistrationPriority(t, manager, queuedJob); got != model.JobPriorityLow {
		t.Fatalf("queued priority after rollback = %q, want low", got)
	}
}

func TestPolicyUpdateRollbackFailureBlocksFurtherRegistryMutations(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	canonicalPath, err := manager.resolveCanonicalPath(repoPath)
	if err != nil {
		t.Fatalf("resolveCanonicalPath: %v", err)
	}
	codebase := newCodebaseRecord(canonicalPath)
	codebase.Status = model.CodebaseStatusIndexed
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		t.Fatalf("saveLocked: %v", err)
	}
	manager.mu.Unlock()

	originalRemove := removePolicyUpdateTransaction
	removeErr := errors.New("injected persistent marker removal failure")
	removePolicyUpdateTransaction = func(string) error { return removeErr }
	t.Cleanup(func() {
		removePolicyUpdateTransaction = originalRemove
		if cleanupErr := store.RemovePolicyUpdate(
			store.PolicyUpdatePath(cfg.RegistryPath),
		); cleanupErr != nil {
			t.Errorf("remove policy marker during cleanup: %v", cleanupErr)
		}
	})

	priority := model.JobPriorityHigh
	_, err = manager.UpdateCodebasePolicy(
		context.Background(),
		repoPath,
		model.SchedulingPolicyPatch{Priority: &priority},
	)
	if !errors.Is(err, removeErr) || !manager.policyMutationBlocked {
		t.Fatalf(
			"UpdateCodebasePolicy error=%v blocked=%v, want rollback failure and blocked gate",
			err,
			manager.policyMutationBlocked,
		)
	}
	if !manager.policyMutationMutex.TryLock() {
		t.Fatal("policy mutation mutex remained locked after rollback failure")
	}
	manager.policyMutationMutex.Unlock()
	if _, blockedErr := manager.UpdateCodebasePolicy(
		context.Background(),
		repoPath,
		model.SchedulingPolicyPatch{Priority: &priority},
	); blockedErr == nil || blockedErr.Error() != "scheduling policy mutations are blocked after a rollback failure" {
		t.Fatalf("UpdateCodebasePolicy after rollback failure = %v, want fail-fast block", blockedErr)
	}
	if manager.policyMutationMutex.TryLock() {
		manager.policyMutationMutex.Unlock()
	} else {
		t.Fatal("policy mutation mutex remained locked after fail-fast block")
	}
	if _, readErr := store.ReadPolicyUpdate(
		store.PolicyUpdatePath(cfg.RegistryPath),
	); readErr != nil {
		t.Fatalf("ReadPolicyUpdate after rollback failure: %v", readErr)
	}
}

func TestCancelQueuedJobDiscardsStagedPolicyRegistration(t *testing.T) {
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
	priority := model.JobPriorityHigh
	if _, err := manager.UpdateCodebasePolicy(
		context.Background(),
		repoPath,
		model.SchedulingPolicyPatch{Priority: &priority},
	); err != nil {
		t.Fatalf("UpdateCodebasePolicy: %v", err)
	}
	if _, err := manager.CancelJob(context.Background(), queuedJob.ID); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if got := queuedRegistrationPriority(t, manager, queuedJob); got != model.JobPriorityLow {
		t.Fatalf("queued priority after cancellation = %q, want low", got)
	}
}

func TestUpdateCodebasePolicySerializesClearIndex(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	manager.semantic = &fakeSemantic{}
	codebase, _ := seedWatcherPolicyUpdateJob(
		t,
		manager,
		repoPath,
		model.JobStateQueued,
	)
	journalEntered, releaseJournal, restoreJournal := blockPolicyUpdateJournal(manager)
	t.Cleanup(restoreJournal)
	var releaseOnce sync.Once
	releaseUpdate := func() { releaseOnce.Do(func() { close(releaseJournal) }) }
	t.Cleanup(releaseUpdate)

	priority := model.JobPriorityHigh
	updateResult := make(chan error, 1)
	go func() {
		_, updateErr := manager.UpdateCodebasePolicy(
			context.Background(),
			repoPath,
			model.SchedulingPolicyPatch{Priority: &priority},
		)
		updateResult <- updateErr
	}()
	waitForPolicyRunnerEntry(t, journalEntered)

	clearResult := make(chan error, 1)
	go func() {
		_, clearErr := manager.ClearIndex(
			context.Background(),
			repoPath,
			testClientInfo(),
		)
		clearResult <- clearErr
	}()
	assertPolicyMutationBlocked(t, clearResult, "ClearIndex")
	releaseUpdate()
	if err := receivePolicyMutationResult(t, updateResult, "UpdateCodebasePolicy"); err != nil {
		t.Fatalf("UpdateCodebasePolicy: %v", err)
	}
	if err := receivePolicyMutationResult(t, clearResult, "ClearIndex"); err != nil {
		t.Fatalf("ClearIndex: %v", err)
	}
	manager.mu.Lock()
	_, found := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if found {
		t.Fatal("ClearIndex completed but UpdateCodebasePolicy republished the codebase")
	}
}

func TestUpdateCodebasePolicySerializesCancelJob(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	manager.jobScheduler.Close()
	manager.jobScheduler = jobscheduler.New(
		context.Background(),
		1,
		readyActivitySource{},
	)
	t.Cleanup(manager.jobScheduler.Close)
	codebase, job := seedPolicyUpdateJob(
		t,
		manager,
		repoPath,
		model.JobStateRunning,
	)
	cleanupScheduler := registerPolicyUpdateSchedulerJob(
		t,
		manager.jobScheduler,
		job,
	)
	t.Cleanup(cleanupScheduler)
	journalEntered, releaseJournal, restoreJournal := blockPolicyUpdateJournal(manager)
	t.Cleanup(restoreJournal)
	var releaseOnce sync.Once
	releaseUpdate := func() { releaseOnce.Do(func() { close(releaseJournal) }) }
	t.Cleanup(releaseUpdate)

	priority := model.JobPriorityHigh
	updateResult := make(chan error, 1)
	go func() {
		_, updateErr := manager.UpdateCodebasePolicy(
			context.Background(),
			repoPath,
			model.SchedulingPolicyPatch{Priority: &priority},
		)
		updateResult <- updateErr
	}()
	waitForPolicyRunnerEntry(t, journalEntered)

	cancelResult := make(chan error, 1)
	go func() {
		_, cancelErr := manager.CancelJob(context.Background(), job.ID)
		cancelResult <- cancelErr
	}()
	assertPolicyMutationBlocked(t, cancelResult, "CancelJob")
	releaseUpdate()
	if err := receivePolicyMutationResult(t, updateResult, "UpdateCodebasePolicy"); err != nil {
		t.Fatalf("UpdateCodebasePolicy: %v", err)
	}
	if err := receivePolicyMutationResult(t, cancelResult, "CancelJob"); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}

	manager.mu.Lock()
	gotCodebase := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if gotCodebase.ActiveJobID != "" {
		t.Fatalf("active job after serialized cancellation = %q, want empty", gotCodebase.ActiveJobID)
	}
	registry, err := store.ReadRegistry(cfg.RegistryPath)
	if err != nil {
		t.Fatalf("ReadRegistry: %v", err)
	}
	if len(registry.Codebases) != 1 || registry.Codebases[0].ActiveJobID != "" {
		t.Fatalf("registry after serialized cancellation = %+v", registry.Codebases)
	}
}

func TestUpdateCodebasePolicySerializesSyncAdmission(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	manager.semantic = &fakeSemantic{}
	seedWatcherPolicyUpdateJob(t, manager, repoPath, model.JobStateQueued)
	journalEntered, releaseJournal, restoreJournal := blockPolicyUpdateJournal(manager)
	t.Cleanup(restoreJournal)
	var releaseOnce sync.Once
	releaseUpdate := func() { releaseOnce.Do(func() { close(releaseJournal) }) }
	t.Cleanup(releaseUpdate)

	priority := model.JobPriorityHigh
	updateResult := make(chan error, 1)
	go func() {
		_, updateErr := manager.UpdateCodebasePolicy(
			context.Background(),
			repoPath,
			model.SchedulingPolicyPatch{Priority: &priority},
		)
		updateResult <- updateErr
	}()
	waitForPolicyRunnerEntry(t, journalEntered)

	syncResult := make(chan policySyncResult, 1)
	go func() {
		job, _, _, syncErr := manager.SyncIndex(
			context.Background(),
			repoPath,
			testClientInfo(),
		)
		syncResult <- policySyncResult{job: job, err: syncErr}
	}()
	select {
	case result := <-syncResult:
		t.Fatalf("SyncIndex completed during policy journal barrier: %v", result.err)
	case <-time.After(100 * time.Millisecond):
	}
	releaseUpdate()
	if err := receivePolicyMutationResult(t, updateResult, "UpdateCodebasePolicy"); err != nil {
		t.Fatalf("UpdateCodebasePolicy: %v", err)
	}
	var result policySyncResult
	select {
	case result = <-syncResult:
	case <-time.After(5 * time.Second):
		t.Fatal("SyncIndex did not complete after policy update")
	}
	if result.err != nil {
		t.Fatalf("SyncIndex: %v", result.err)
	}
	if result.job.EffectiveSchedulingPolicy.Priority != model.JobPriorityHigh {
		t.Fatalf(
			"admitted sync priority = %q, want high",
			result.job.EffectiveSchedulingPolicy.Priority,
		)
	}
}

func TestUpdateCodebasePolicySerializesConversationRegistration(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	manager.semantic = &fakeSemantic{}
	codebase, _ := seedWatcherPolicyUpdateJob(
		t,
		manager,
		repoPath,
		model.JobStateQueued,
	)
	journalEntered, releaseJournal, restoreJournal := blockPolicyUpdateJournal(manager)
	t.Cleanup(restoreJournal)
	var releaseOnce sync.Once
	releaseUpdate := func() { releaseOnce.Do(func() { close(releaseJournal) }) }
	t.Cleanup(releaseUpdate)

	priority := model.JobPriorityHigh
	updateResult := make(chan error, 1)
	go func() {
		_, updateErr := manager.UpdateCodebasePolicy(
			context.Background(),
			repoPath,
			model.SchedulingPolicyPatch{Priority: &priority},
		)
		updateResult <- updateErr
	}()
	waitForPolicyRunnerEntry(t, journalEntered)

	registrationResult := make(chan policyConversationRegistrationResult, 1)
	go func() {
		registered, registrationErr := manager.RegisterConversationCollection(
			context.Background(),
			"policy-race-conversations",
		)
		registrationResult <- policyConversationRegistrationResult{
			codebase: registered,
			err:      registrationErr,
		}
	}()
	select {
	case result := <-registrationResult:
		t.Fatalf(
			"RegisterConversationCollection completed during policy journal barrier: %v",
			result.err,
		)
	case <-time.After(100 * time.Millisecond):
	}
	releaseUpdate()
	if err := receivePolicyMutationResult(t, updateResult, "UpdateCodebasePolicy"); err != nil {
		t.Fatalf("UpdateCodebasePolicy: %v", err)
	}
	var registration policyConversationRegistrationResult
	select {
	case registration = <-registrationResult:
	case <-time.After(5 * time.Second):
		t.Fatal("RegisterConversationCollection did not complete after policy update")
	}
	if registration.err != nil {
		t.Fatalf("RegisterConversationCollection: %v", registration.err)
	}
	if registration.codebase.ID == "" {
		t.Fatal("RegisterConversationCollection returned an empty codebase")
	}

	registry, err := store.ReadRegistry(cfg.RegistryPath)
	if err != nil {
		t.Fatalf("ReadRegistry: %v", err)
	}
	foundPolicy := false
	foundConversation := false
	for _, storedCodebase := range registry.Codebases {
		if storedCodebase.ID == codebase.ID {
			foundPolicy = storedCodebase.SchedulingPolicy.Priority ==
				model.JobPriorityHigh
		}
		if storedCodebase.ID == registration.codebase.ID {
			foundConversation = true
		}
	}
	if !foundPolicy || !foundConversation {
		t.Fatalf(
			"registry lost serialized writes: policy=%v conversation=%v records=%+v",
			foundPolicy,
			foundConversation,
			registry.Codebases,
		)
	}
}

func TestForceIndexReleasesPolicyLockWhileCancellationFinishes(t *testing.T) {
	manager, _ := newTestManagerWithCap(t, 1)
	manager.semantic = &fakeSemantic{}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var inFlight atomic.Int32
	var maximumInFlight atomic.Int32
	manager.runner = blockingRunner(
		entered,
		release,
		&inFlight,
		&maximumInFlight,
	)
	t.Cleanup(func() { closeOnce(release) })
	repoPath := newCapTestRepo(t)

	if _, _, _, _, err := manager.StartIndex(
		context.Background(),
		repoPath,
		testClientInfo(),
		defaultIndexConfig(),
		false,
		emptyAdmissionBudget,
	); err != nil {
		t.Fatalf("StartIndex initial job: %v", err)
	}
	waitForPolicyRunnerEntry(t, entered)

	forceContext, cancelForce := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancelForce()
	forceConfig := defaultIndexConfig()
	forceConfig.IgnorePatterns = []string{"force-policy-lock-recheck"}
	forcedJob, _, _, _, err := manager.StartIndex(
		forceContext,
		repoPath,
		testClientInfo(),
		forceConfig,
		true,
		emptyAdmissionBudget,
	)
	if err != nil {
		t.Fatalf("force StartIndex: %v", err)
	}
	if forcedJob.ID == "" {
		t.Fatal("force StartIndex returned an empty job")
	}
	closeOnce(release)
	waitForTerminalJob(t, manager, forcedJob.ID)
}

type policySyncResult struct {
	job model.Job
	err error
}

type policyConversationRegistrationResult struct {
	codebase model.Codebase
	err      error
}

func blockPolicyUpdateJournal(
	manager *Manager,
) (<-chan struct{}, chan struct{}, func()) {
	originalAppendTransition := manager.appendJobTransition
	entered := make(chan struct{})
	release := make(chan struct{})
	manager.appendJobTransition = func(event model.JobEvent) error {
		if event.Event == "job_policy_updated" {
			close(entered)
			<-release
		}
		return originalAppendTransition(event)
	}
	return entered, release, func() {
		manager.appendJobTransition = originalAppendTransition
	}
}

func queuedRegistrationPriority(
	t *testing.T,
	manager *Manager,
	queuedJob model.Job,
) model.JobPriority {
	t.Helper()
	blockerPolicy := model.DefaultSchedulingPolicy()
	blockerPolicy.Priority = model.JobPriorityHigh
	blocker, err := manager.jobScheduler.Acquire(
		context.Background(),
		jobscheduler.Entry{
			JobID:         "repair-registration-blocker-" + queuedJob.ID,
			Policy:        blockerPolicy,
			QueueSequence: 1,
		},
	)
	if err != nil {
		t.Fatalf("Acquire blocker: %v", err)
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
	waitForCondition(t, func() bool {
		snapshot := manager.jobScheduler.Snapshot()
		return snapshot.Queued[model.JobPriorityHigh]+
			snapshot.Queued[model.JobPriorityNormal]+
			snapshot.Queued[model.JobPriorityLow] == 1
	})
	snapshot := manager.jobScheduler.Snapshot()
	priority := model.JobPriority("")
	for _, candidate := range []model.JobPriority{
		model.JobPriorityHigh,
		model.JobPriorityNormal,
		model.JobPriorityLow,
	} {
		if snapshot.Queued[candidate] == 1 {
			priority = candidate
		}
	}
	cancelRegistration()
	blocker.Release()
	select {
	case capacity := <-registrationResult:
		if capacity != nil {
			capacity.release(context.Background())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued registration did not stop")
	}
	return priority
}

func assertPolicyMutationBlocked(
	t *testing.T,
	result <-chan error,
	operation string,
) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("%s completed during policy journal barrier: %v", operation, err)
	case <-time.After(100 * time.Millisecond):
	}
}

func receivePolicyMutationResult(
	t *testing.T,
	result <-chan error,
	operation string,
) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not complete", operation)
		return nil
	}
}
