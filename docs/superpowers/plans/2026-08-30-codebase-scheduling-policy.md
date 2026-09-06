# Codebase Scheduling Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add persistent and per-run codebase priority, cooperative preemption, and optional host-idle scheduling on macOS and Linux.

**Architecture:** A daemon-owned scheduler replaces direct indexing-slot contention and owns both slot and sync-lock leases. Jobs carry a persisted effective policy, pause only at file boundaries, and use injected platform activity sources for quiet eligibility. Protocol, CLI, MCP index, status, and recovery surfaces share the same model.

**Tech Stack:** Go 1.26.5, gRPC and protobuf through `buf`, Cobra, mcp-go, Core Graphics and Foundation through cgo, systemd login1 through go-systemd, Linux thermal sysfs, JSON registry, JSONL job journal.

**Spec:** [Codebase scheduling policy design](../specs/2026-08-30-codebase-scheduling-policy-design.md)

## Global Constraints

- Stored defaults are `priority=normal`, `quiet=false`, and
  `idle_after_seconds=300`.
- Free slots go to waiting `high`, then `normal`, then `low` jobs.
- A higher-priority arrival pauses only enough lower-priority jobs to obtain capacity.
- Equal-priority jobs use first-in, first-out order. A paused job keeps its original queue sequence.
- An effective `high` priority bypasses input-idle gating but not thermal safety when quiet is enabled.
- Activity unavailability keeps quiet work queued or paused. Missing thermal data does not block work.
- GPU load, power draw, and ordinary temperature movement never define quiet eligibility.
- Scheduling fields stay outside `IndexConfig` and never affect Merkle, collection, schema, or chunk identity.
- Conversation jobs remain normal priority but participate in global admission and preemption.
- The scheduler lease is the only owner of indexing capacity and the shared sync-lock lease.
- Pause and resume journal events must reach a synchronous file flush before work releases or regains writable capacity.
- Objective-C code stays in its own `.m` file. Linux activity code adds no new cgo dependency.
- Do not add an MCP sync tool.
- Generated protobuf Go files change only through `make proto`.
- Use public daemon, protocol, command, and installed-service boundaries for behavior tests.
- Run `make check` before every commit.
- Create every commit with `git commit -S` and `Co-authored-by: Codex <noreply@openai.com>`.

## File Ownership Map

- `internal/model/scheduling.go`: closed scheduling types, defaults, validation, and field-level patches.
- `internal/daemon/job_scheduler.go`: priority queue, slot ordering, lease state, and snapshots.
- `internal/daemon/manager_policy.go`: stored policy resolution and live field-level mutation.
- `internal/platformactivity`: platform-neutral snapshots plus Darwin and Linux sources.
- `proto/lmsemanticsearch/v1/service.proto`: wire policy, request override, mutation, and status fields.
- `internal/view/scheduling.go` and `internal/render/scheduling.go`: one presentation model and formatter.
- Existing manager files keep their current responsibilities. Do not move unrelated code.

---

### Task 1: Add scheduling policy and legacy registry semantics

**Files:**
- Create: `internal/model/scheduling.go`
- Create: `internal/model/scheduling_test.go`
- Modify: `internal/model/types.go:70-310`
- Modify: `internal/daemon/manager.go:191-465`
- Modify: `internal/daemon/manager_load.go:12-38`
- Modify: `internal/daemon/manager_worktree.go:62-151`
- Modify: `internal/daemon/manager_adopt.go:26-110`
- Test: `internal/daemon/manager_discover_test.go`
- Test: `internal/daemon/manager_adopt_test.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Produces: `model.JobPriority`, `model.SchedulingPolicy`, `model.SchedulingPolicyPatch`, `model.DefaultSchedulingPolicy()`, `model.ValidateSchedulingPolicy(model.SchedulingPolicy) error`, and `model.ApplySchedulingPolicyPatch(model.SchedulingPolicy, model.SchedulingPolicyPatch) (model.SchedulingPolicy, error)`.
- Produces: `model.JobStatePaused`, stored codebase policy, `PolicyPendingInitialization`, job effective policy, `QueueSequence`, and `SchedulingReason`.
- Consumes: nothing from later tasks.

- [ ] **Step 1: Write failing model and registry tests**

Add table tests with these exact cases:

```go
func TestSchedulingPolicyDefaults(t *testing.T) {
    got := DefaultSchedulingPolicy()
    want := SchedulingPolicy{
        Priority:         JobPriorityNormal,
        Quiet:            false,
        IdleAfterSeconds: 300,
    }
    if got != want {
        t.Fatalf("default policy = %+v, want %+v", got, want)
    }
}

func TestApplySchedulingPolicyPatchPreservesOmittedFields(t *testing.T) {
    priority := JobPriorityLow
    stored := SchedulingPolicy{
        Priority:         JobPriorityNormal,
        Quiet:            true,
        IdleAfterSeconds: 900,
    }
    got, err := ApplySchedulingPolicyPatch(
        stored,
        SchedulingPolicyPatch{Priority: &priority},
    )
    if err != nil {
        t.Fatalf("ApplySchedulingPolicyPatch: %v", err)
    }
    want := SchedulingPolicy{
        Priority:         JobPriorityLow,
        Quiet:            true,
        IdleAfterSeconds: 900,
    }
    if got != want {
        t.Fatalf("patched policy = %+v, want %+v", got, want)
    }
}
```

Add registry fixtures that omit every new field and assert they load as initialized `normal` policy. Add discovery and adoption tests that assert only an unbuilt discovered worktree sets `PolicyPendingInitialization=true`.

- [ ] **Step 2: Run the focused tests and verify failure**

Run:

```bash
go test ./internal/model ./internal/store ./internal/daemon -run 'SchedulingPolicy|PolicyPendingInitialization'
```

Expected: FAIL because scheduling types and persisted fields do not exist.

- [ ] **Step 3: Add the closed model types**

Implement this complete public shape:

```go
package model

import "fmt"

const DefaultIdleAfterSeconds int32 = 300

type JobPriority string

const (
    JobPriorityHigh   JobPriority = "high"
    JobPriorityNormal JobPriority = "normal"
    JobPriorityLow    JobPriority = "low"
)

type SchedulingPolicy struct {
    Priority         JobPriority `json:"priority,omitempty"`
    Quiet            bool        `json:"quiet,omitempty"`
    IdleAfterSeconds int32       `json:"idle_after_seconds,omitempty"`
}

type SchedulingPolicyPatch struct {
    Priority         *JobPriority `json:"priority,omitempty"`
    Quiet            *bool        `json:"quiet,omitempty"`
    IdleAfterSeconds *int32       `json:"idle_after_seconds,omitempty"`
}

func DefaultSchedulingPolicy() SchedulingPolicy {
    return SchedulingPolicy{
        Priority:         JobPriorityNormal,
        Quiet:            false,
        IdleAfterSeconds: DefaultIdleAfterSeconds,
    }
}

func ValidateSchedulingPolicy(policy SchedulingPolicy) error {
    switch policy.Priority {
    case JobPriorityHigh, JobPriorityNormal, JobPriorityLow:
    default:
        return fmt.Errorf("priority must be high, normal, or low")
    }
    if policy.IdleAfterSeconds <= 0 {
        return fmt.Errorf("idle after seconds must be positive")
    }
    return nil
}
```

Implement `ApplySchedulingPolicyPatch` by copying the stored value, replacing only non-nil fields, filling legacy zero values from `DefaultSchedulingPolicy`, then calling `ValidateSchedulingPolicy`.

- [ ] **Step 4: Add persisted fields and normalization**

Add these fields:

```go
SchedulingPolicy            SchedulingPolicy `json:"scheduling_policy,omitzero"`
PolicyPendingInitialization bool             `json:"policy_pending_initialization,omitempty"`
```

to `Codebase`, and these fields to `Job`:

```go
EffectiveSchedulingPolicy SchedulingPolicy `json:"effective_scheduling_policy,omitzero"`
SchedulingOverride        SchedulingPolicyPatch `json:"scheduling_override,omitzero"`
QueueSequence             uint64           `json:"queue_sequence,omitempty"`
SchedulingReason          string           `json:"scheduling_reason,omitempty"`
```

Add `JobStatePaused = "paused"`. Normalize missing policies after registry and journal load. Keep legacy codebases initialized. Mark only newly discovered, unbuilt worktrees pending initialization. Keep adopted collections initialized.

- [ ] **Step 5: Run focused tests and all checks**

Run:

```bash
go test ./internal/model ./internal/store ./internal/daemon -run 'SchedulingPolicy|PolicyPendingInitialization'
make check
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/model/scheduling.go internal/model/scheduling_test.go internal/model/types.go internal/daemon/manager.go internal/daemon/manager_load.go internal/daemon/manager_worktree.go internal/daemon/manager_adopt.go internal/daemon/manager_discover_test.go internal/daemon/manager_adopt_test.go internal/store/store_test.go
git commit -S -m "Add scheduling policy model and registry fields" -m "Co-authored-by: Codex <noreply@openai.com>"
```

---

### Task 2: Resolve stored, per-run, pending, and recovered policy

**Files:**
- Create: `internal/daemon/manager_policy.go`
- Create: `internal/daemon/manager_policy_test.go`
- Modify: `internal/daemon/manager.go:520-743`
- Modify: `internal/daemon/manager_pending.go:1-289`
- Modify: `internal/daemon/manager_resume.go:22-117`
- Test: `internal/daemon/manager_test.go`
- Test: `internal/daemon/converge_concurrency_test.go`
- Test: `internal/daemon/manager_resume_test.go`

**Interfaces:**
- Consumes: model policy types from Task 1.
- Produces: backward-compatible `StartIndex` and `SyncIndex` wrappers, policy-aware `StartIndexWithPolicy` and `SyncIndexWithPolicy`, `resolveIndexPolicyLocked`, policy-aware `pendingCodeRequest`, and policy-aware `resumePlan`.
- Produces: a monotonically increasing queue sequence assigned before the first job journal event.

- [ ] **Step 1: Write failing manager behavior tests**

Cover these public outcomes:

```go
func TestFirstExplicitIndexPersistsPolicyAfterDiscovery(t *testing.T)
func TestExistingCodebaseUsesIndexPolicyForOneRun(t *testing.T)
func TestPendingCodeRequestMergesPolicyFields(t *testing.T)
func TestResumePlanPreservesInterruptedEffectivePolicy(t *testing.T)
func TestRecoveredJobsKeepQueueSequenceOrder(t *testing.T)
```

Use real temporary codebases and the existing manager test runner. Assert registry state, returned job policy, drained successor policy, and recovered job order.

- [ ] **Step 2: Run tests and verify failure**

```bash
go test ./internal/daemon -run 'FirstExplicitIndexPersistsPolicy|ExistingCodebaseUsesIndexPolicy|PendingCodeRequestMergesPolicy|ResumePlanPreserves|RecoveredJobsKeepQueue'
```

Expected: FAIL because manager requests do not carry policy.

- [ ] **Step 3: Add internal policy intent and resolution**

Use this private request shape:

```go
type indexPolicyIntent struct {
    Patch      model.SchedulingPolicyPatch
    Initialize bool
}

func (manager *Manager) resolveIndexPolicyLocked(
    codebase model.Codebase,
    intent indexPolicyIntent,
) (model.Codebase, model.SchedulingPolicy, error)
```

The public `StartIndex` path uses `Initialize=true`. Deferred discovery uses `Initialize=false`. Existing initialized codebases keep stored policy and apply request fields only to the returned effective policy. An uninitialized codebase persists the first explicit index patch and clears its marker.

Keep the existing `StartIndex` and `SyncIndex` signatures unchanged. Make them delegate with an empty patch to `StartIndexWithPolicy` and `SyncIndexWithPolicy`. The policy-aware sync never initializes stored policy. Deferred discovery calls the private start path with `Initialize=false`. Task 10 moves gRPC to the policy-aware methods, so every intermediate commit still compiles without changing the existing 127 callers.

- [ ] **Step 4: Carry policy through pending and recovery paths**

Add `policyPatch` to `pendingCodeRequest`. Merge each non-nil field independently so the latest supplied field wins and omitted fields remain pending. Store effective policy and override on every new job.

Extend `resumePlan` with the interrupted job policy, override, and queue sequence. Sort plans by queue sequence before launching successors. Recover watcher converges as full sync successors because their path batch is not durable.

- [ ] **Step 5: Run tests and all checks**

```bash
go test ./internal/daemon -run 'FirstExplicitIndexPersistsPolicy|ExistingCodebaseUsesIndexPolicy|PendingCodeRequestMergesPolicy|ResumePlanPreserves|RecoveredJobsKeepQueue'
make check
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/daemon/manager_policy.go internal/daemon/manager_policy_test.go internal/daemon/manager.go internal/daemon/manager_pending.go internal/daemon/manager_resume.go internal/daemon/manager_test.go internal/daemon/converge_concurrency_test.go internal/daemon/manager_resume_test.go
git commit -S -m "Add manager scheduling policy resolution" -m "Co-authored-by: Codex <noreply@openai.com>"
```

---

### Task 3: Add durable pause and resume journal barriers

**Files:**
- Modify: `internal/store/store.go:190-215`
- Test: `internal/store/store_test.go`
- Modify: `internal/daemon/job_journal_writer.go:1-266`
- Test: `internal/daemon/manager_journal_writer_test.go`

**Interfaces:**
- Produces: `store.AppendJobEventSync(string, model.JobEvent) error`.
- Produces: `(*jobJournalWriter).enqueueAndSync(model.JobEvent) error`.
- Preserves: asynchronous `enqueue` for progress and ordinary state updates.

- [ ] **Step 1: Write failing durability and ordering tests**

Add tests that replace the store sync seam and prove this order:

```text
encode event
sync journal file
return from enqueueAndSync
release caller
```

Add a writer test that queues an asynchronous event, then calls `enqueueAndSync`, and asserts both events reach disk in order before the barrier returns. Add sync-failure injection and assert the exact error reaches the caller.

- [ ] **Step 2: Run tests and verify failure**

```bash
go test ./internal/store ./internal/daemon -run 'JobEventSync|JournalBarrier'
```

Expected: FAIL because no synchronous journal append exists.

- [ ] **Step 3: Add the durable append path**

Refactor the store append into one helper:

```go
func appendJobEvent(path string, event model.JobEvent, sync bool) error

func AppendJobEvent(path string, event model.JobEvent) error {
    return appendJobEvent(path, event, false)
}

func AppendJobEventSync(path string, event model.JobEvent) error {
    return appendJobEvent(path, event, true)
}
```

When `sync` is true, call the existing `syncFile` seam after encoding and before closing. Return sync and close errors with the journal path.

- [ ] **Step 4: Add an ordered writer barrier**

Extend `jobJournalWriteRequest` with `durable bool`. `enqueueAndSync` always sends a result channel. The single writer goroutine calls the synchronous append function only for durable requests, preserving queue order and keeping all file I/O outside `manager.mu`.

- [ ] **Step 5: Run tests and all checks**

```bash
go test ./internal/store ./internal/daemon -run 'JobEventSync|JournalBarrier'
make check
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/store/store.go internal/store/store_test.go internal/daemon/job_journal_writer.go internal/daemon/manager_journal_writer_test.go
git commit -S -m "Add durable job journal transition barriers" -m "Co-authored-by: Codex <noreply@openai.com>"
```

---

### Task 4: Implement the priority scheduler

**Files:**
- Create: `internal/daemon/job_scheduler.go`
- Create: `internal/daemon/job_scheduler_test.go`

**Interfaces:**
- Consumes: job policy and queue sequence from Tasks 1 and 2.
- Produces: `jobScheduler`, `schedulerLease`, `schedulerEntry`, `schedulerSnapshot`, and reason constants.
- Produces: context-aware acquire, policy update, checkpoint, yield, reacquire, release, and snapshot operations.

- [ ] **Step 1: Write failing four-slot scheduling tests**

Create deterministic tests for the approved examples:

```go
func TestSchedulerFillsFourSlotsByWaitingPriority(t *testing.T)
func TestSchedulerGivesAllFourSlotsToHighPriority(t *testing.T)
func TestSchedulerGivesReleasedSlotToHighestWaitingPriority(t *testing.T)
func TestSchedulerPausesOnlyEnoughLowerJobs(t *testing.T)
func TestSchedulerPausedJobKeepsFIFOPosition(t *testing.T)
func TestSchedulerPausesLowBeforeNormal(t *testing.T)
func TestSchedulerClearsPauseRequestWhenSlotOpens(t *testing.T)
```

For the first test, enqueue two high, two normal, and two low entries. Assert the running set contains two high and two normal entries. For the second, enqueue four high, two normal, and two low entries. Assert only high entries run. Release one high entry and assert the oldest waiting normal entry receives the slot when no high entry waits.

- [ ] **Step 2: Run tests and verify failure**

```bash
go test ./internal/daemon -run '^TestScheduler'
```

Expected: FAIL because `jobScheduler` does not exist.

- [ ] **Step 3: Add scheduler state and notification loop**

Use these concrete shapes:

```go
type schedulerEntry struct {
    JobID          string
    Policy         model.SchedulingPolicy
    QueueSequence  uint64
    State          schedulerEntryState
    Reason         string
    PauseRequested bool
}

type schedulerSnapshot struct {
    Capacity int
    Running  map[model.JobPriority]int
    Queued   map[model.JobPriority]int
    Paused   map[model.JobPriority]int
}

type jobScheduler struct {
    mutex    sync.Mutex
    capacity int
    entries  map[string]*schedulerEntry
    changed  chan struct{}
}
```

Close and replace `changed` after every state mutation. Waiters select on that channel and `ctx.Done()`. Pick the waiting entry by priority, then queue sequence. Recompute pause requests after every mutation and clear requests that are no longer needed. Select victims from the lowest running priority first, then select the newest queue sequence within that tier. Never pause normal while a low victim remains, and never pause equal priority.

- [ ] **Step 4: Add lease idempotency and policy updates**

Make every lease operation verify one internal state transition under the scheduler mutex. Repeated yield, release, or cancellation must not decrement running capacity twice. `UpdatePolicy` changes only supplied fields, reruns victim selection, and wakes every waiter.

- [ ] **Step 5: Run scheduler tests, race tests, and checks**

```bash
go test ./internal/daemon -run '^TestScheduler'
make test GOFLAGS=-race
make check
```

Expected: PASS with no race report.

- [ ] **Step 6: Commit**

```bash
git add internal/daemon/job_scheduler.go internal/daemon/job_scheduler_test.go
git commit -S -m "Add priority job scheduler" -m "Co-authored-by: Codex <noreply@openai.com>"
```

---

### Task 5: Add durable cooperative pause primitives

**Files:**
- Create: `internal/daemon/manager_pause.go`
- Create: `internal/daemon/manager_pause_test.go`
- Modify: `internal/daemon/manager.go:68-124`
- Modify: `internal/daemon/manager_jobs_state.go:29-400`
- Modify: `internal/daemon/manager_conversations.go:480-550`
- Modify: `internal/daemon/quarantine.go:250-320`
- Modify: `internal/daemon/manager_delta.go:629-683`
- Modify: `internal/daemon/converge.go:41-125`
- Modify: `internal/daemon/manager_active_job.go:10-75`
- Test: `internal/daemon/converge_test.go`
- Test: `internal/daemon/manager_conversations_test.go`
- Test: `internal/daemon/quarantine_test.go`
- Test: `internal/daemon/orphan_progress_test.go`

**Interfaces:**
- Produces: serialized `pauseJob`, `resumeJob`, and terminal transitions plus lease `Checkpoint` callbacks.
- Consumes: Task 3 durable barriers and Task 4 lease state.
- Preserves: one job identifier and its staging data across an in-process pause.

- [ ] **Step 1: Write failing pause lifecycle and race tests**

Add these cases:

```go
func TestPriorityPauseFinishesCurrentFileBeforeRelease(t *testing.T)
func TestPausedJobReleasesLeaseAndResumesSameJob(t *testing.T)
func TestPauseJournalFailureTerminatesBeforeRelease(t *testing.T)
func TestResumeJournalFailureTerminatesAndReleasesLease(t *testing.T)
func TestCancellationBetweenPauseSnapshotAndJournalStaysTerminal(t *testing.T)
func TestConversationTerminalCannotBeFollowedByPause(t *testing.T)
func TestQuarantineTerminalCannotBeFollowedByPause(t *testing.T)
```

Use a runner that blocks one real per-file call. Inject journal barriers and scheduler leases. Assert pause occurs only after the file, stale pause cannot follow cancellation in the journal, and every failure releases capacity exactly once.

- [ ] **Step 2: Run tests and verify failure**

```bash
go test ./internal/daemon -run 'PriorityPause|PausedJob|PauseJournal|ResumeJournal|CancellationBetweenPause|ConversationTerminalCannot|QuarantineTerminalCannot'
```

Expected: FAIL because durable pause transitions do not exist.

- [ ] **Step 3: Serialize every job transition**

Add a manager transition mutex separate from `manager.mu`. Running, paused, resumed, completed, failed, cancelling, cancelled, conversation-terminal, detached-terminal, and quarantined transitions must acquire it. Route conversation and quarantine terminal writes through the same serialized helpers instead of assigning state and appending directly. Never hold `manager.mu` while waiting for a journal write. This serialization prevents any terminal event from being followed by a stale paused event.

Use these signatures:

```go
func (manager *Manager) pauseJob(
    ctx context.Context,
    jobID string,
    reason string,
) error

func (manager *Manager) resumeJob(
    ctx context.Context,
    jobID string,
) error
```

- [ ] **Step 4: Define failure outcomes and lease disposition**

Pause barrier failure ends through the existing failure cleanup with code `pause_journal_failed`, then releases the lease as terminal work. Resume barrier failure ends with code `resume_journal_failed`, releases the newly reserved slot and sync-lock lease, cleans staging, and never retries or writes another file.

- [ ] **Step 5: Add optional file-boundary checkpoints**

Call the lease checkpoint from context after each completed delta item and each completed converge path. The helper is a no-op when no scheduler lease is present, so this intermediate commit preserves current production behavior. Keep the in-memory converge path batch across a pause.

- [ ] **Step 6: Run tests, race tests, and checks**

```bash
go test ./internal/daemon -run 'PriorityPause|PausedJob|PauseJournal|ResumeJournal|CancellationBetweenPause|ConversationTerminalCannot|QuarantineTerminalCannot'
make test GOFLAGS=-race
make check
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/daemon/manager_pause.go internal/daemon/manager_pause_test.go internal/daemon/manager.go internal/daemon/manager_jobs_state.go internal/daemon/manager_conversations.go internal/daemon/quarantine.go internal/daemon/manager_delta.go internal/daemon/converge.go internal/daemon/manager_active_job.go internal/daemon/converge_test.go internal/daemon/manager_conversations_test.go internal/daemon/quarantine_test.go internal/daemon/orphan_progress_test.go
git commit -S -m "Add durable cooperative pause transitions" -m "Co-authored-by: Codex <noreply@openai.com>"
```

---

### Task 6: Route every indexing source and close the paused lifecycle

**Files:**
- Modify: `internal/daemon/manager.go:68-257`
- Modify: `internal/daemon/manager_runner.go:19-150`
- Modify: `internal/daemon/job_capacity.go:1-300`
- Modify: `internal/daemon/background_sync.go:448-638`
- Modify: `internal/daemon/manager_activity.go:1-71`
- Modify: `internal/daemon/manager_journal.go:61-110`
- Modify: `internal/daemon/job_journal_compaction.go:1-100`
- Modify: `internal/daemon/manager_resume.go:22-117`
- Modify: `internal/daemon/manager_close.go:12-57`
- Test: `internal/daemon/manager_cap_test.go`
- Test: `internal/daemon/converge_concurrency_test.go`
- Test: `internal/daemon/manager_close_test.go`
- Test: `internal/daemon/sync_lock_test.go`

**Interfaces:**
- Consumes: Task 4 scheduler and Task 5 durable transitions.
- Produces: one scheduler lease as sole owner of the slot and `syncLockLease` for every job source.
- Removes: direct production use of `Manager.indexSlots` and direct watcher capacity acquisition.

- [ ] **Step 1: Write failing integration and recovery tests**

Enter through `Manager.StartIndex`, `Manager.SyncIndex`, conversation ingest, watcher converge, cancellation, shutdown, and boot recovery. Assert queued registration, priority ordering, paused cancellation, recovered policy and queue order, and terminal replacement of paused journal state.

Extend stalled-read tests to prove watchdog yield calls the durable pause and resume transitions, joins scheduler order, and cannot double-release when priority revocation races it. Replace the old five-second terminal reacquire expectation with visible paused waiting.

- [ ] **Step 2: Run tests and verify failure**

```bash
go test ./internal/daemon -run 'SchedulerAdmission|WatcherUsesScheduler|ConversationUsesNormalPriority|StuckReuseLoad|WatchdogPriorityRace|CancelPaused|BootRecoveryPreservesPaused|TerminalEventSupersedesPaused'
```

Expected: FAIL because production paths still use `indexSlots`.

- [ ] **Step 3: Replace manager capacity with scheduler leases**

Construct `jobScheduler` with `MaxConcurrentIndexJobs`. Register queued work before admission. Acquire the shared lock only through the scheduler lease. If an external process owns the lock, return the slot to the queue, report the lock wait, and retry. Publish running state only after both resources are held. Force conversation jobs to normal priority.

- [ ] **Step 4: Route watcher and stalled-read work**

Register converge work as queued before admission. Keep path-scoped behavior unchanged. Delete independent slot and lock ownership from `jobCapacity`. Its 4.5-second watchdog calls the same durable lease pause and resume path as priority revocation. Scheduler waiting replaces the old terminal reacquire timeout.

- [ ] **Step 5: Close every paused lifecycle path**

Treat paused as active in cancellation, pending work, snapshots, compaction, shutdown, and orphan sanitization. Wake queued and paused waiters on cancellation. Sort recovered successors by queue sequence. Recover interrupted converge as a full sync successor because its path batch is not durable. Make `IndexSlots()` read the scheduler snapshot.

- [ ] **Step 6: Run tests, restart unit coverage, race tests, and checks**

```bash
go test ./internal/daemon -run 'SchedulerAdmission|WatcherUsesScheduler|ConversationUsesNormalPriority|StuckReuseLoad|WatchdogPriorityRace|CancelPaused|BootRecoveryPreservesPaused|TerminalEventSupersedesPaused'
make restart-acceptance-unit
make test GOFLAGS=-race
make check
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/daemon/manager.go internal/daemon/manager_runner.go internal/daemon/job_capacity.go internal/daemon/background_sync.go internal/daemon/manager_activity.go internal/daemon/manager_journal.go internal/daemon/job_journal_compaction.go internal/daemon/manager_resume.go internal/daemon/manager_close.go internal/daemon/manager_cap_test.go internal/daemon/converge_concurrency_test.go internal/daemon/manager_close_test.go internal/daemon/sync_lock_test.go
git commit -S -m "Route indexing through durable scheduler leases" -m "Co-authored-by: Codex <noreply@openai.com>"
```

---

### Task 7: Add platform activity contract and quiet admission

**Files:**
- Create: `internal/platformactivity/activity.go`
- Create: `internal/platformactivity/activity_other.go`
- Create: `internal/platformactivity/activity_test.go`
- Modify: `internal/daemon/job_scheduler.go`
- Modify: `internal/daemon/job_scheduler_test.go`
- Modify: `internal/daemon/manager.go:191-257`
- Modify: `internal/daemon/manager_close.go:12-28`

**Interfaces:**
- Produces: `platformactivity.Snapshot`, `platformactivity.Source`, and `platformactivity.NewUnavailable(string)`.
- Produces: injected `managerDependencies.activitySource` and a two-second scheduler sampler.
- Consumes: quiet policy fields and pause lifecycle from earlier tasks.

- [ ] **Step 1: Write failing quiet admission tests**

Test these snapshots through the scheduler:

```go
type Snapshot struct {
    InputAvailable    bool
    InputIdleFor      time.Duration
    InputReason       string
    ThermalAvailable  bool
    ThermalUnsafe     bool
    ThermalReason     string
}
```

Cases must prove five-minute admission, activity-triggered pause, unavailable waiting, automatic recovery, missing thermal nonblocking behavior, thermal pause, and high-priority idle bypass with thermal enforcement.

- [ ] **Step 2: Run tests and verify failure**

```bash
go test ./internal/platformactivity ./internal/daemon -run 'ActivitySnapshot|QuietAdmission|ActivityUnavailable|ThermalSafety|HighBypassesIdle'
```

Expected: FAIL because the activity contract does not exist.

- [ ] **Step 3: Add the platform-neutral contract**

```go
type Source interface {
    Sample(context.Context) Snapshot
    Close()
}
```

The unsupported-platform source returns `InputAvailable=false` with a stable reason and `ThermalAvailable=false`. Source failures are snapshot state, not Go errors.

- [ ] **Step 4: Inject and sample activity**

Add `managerDependencies` with semantic and activity dependencies. Keep `NewManager` unchanged and make the existing semantic test constructor delegate to the new constructor. Until Tasks 8 and 9 install both supported platform factories, production uses `NewUnavailable("platform activity source not installed")`. The scheduler owns the two-second ticker and cached snapshot. Status reads only the cache.

Quiet admission compares `InputIdleFor` with the job threshold. New activity or unsafe thermal state requests cooperative pause. Close the source after jobs stop and before the journal closes.

- [ ] **Step 5: Run tests, race tests, and checks**

```bash
go test ./internal/platformactivity ./internal/daemon -run 'ActivitySnapshot|QuietAdmission|ActivityUnavailable|ThermalSafety|HighBypassesIdle'
make test GOFLAGS=-race
make check
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/platformactivity/activity.go internal/platformactivity/activity_other.go internal/platformactivity/activity_test.go internal/daemon/job_scheduler.go internal/daemon/job_scheduler_test.go internal/daemon/manager.go internal/daemon/manager_close.go
git commit -S -m "Add quiet activity scheduling contract" -m "Co-authored-by: Codex <noreply@openai.com>"
```

---

### Task 8: Add the macOS activity source

**Files:**
- Create: `internal/platformactivity/source_darwin.go`
- Create: `internal/platformactivity/source_darwin_nocgo.go`
- Create: `internal/platformactivity/activity_bridge_darwin.h`
- Create: `internal/platformactivity/activity_bridge_darwin.m`
- Create: `internal/platformactivity/source_darwin_test.go`

**Interfaces:**
- Consumes: Task 7 `Source` and `Snapshot`.
- Produces: Darwin `New()` using Core Graphics and Foundation in process.

- [ ] **Step 1: Write failing Darwin bridge tests**

Inject the native reader behind a package variable. Test valid idle seconds, invalid or nonfinite idle values, nominal thermal state, and `serious` or `critical` unsafe states. An invalid input reading must produce `InputAvailable=false`, never idle.

- [ ] **Step 2: Run the Darwin tests and verify failure**

```bash
go test ./internal/platformactivity -run 'Darwin|NativeActivity'
```

Expected: FAIL because the Darwin source does not exist.

- [ ] **Step 3: Add the Objective-C bridge**

Define this C result exactly:

```c
typedef struct {
    double idle_seconds;
    int32_t input_available;
    int32_t thermal_available;
    int32_t thermal_unsafe;
} lms_activity_result;

lms_activity_result lms_activity_read(void);
```

Call `CGEventSourceSecondsSinceLastEventType` with combined-session state and any input event. Read `NSProcessInfo.thermalState`. Mark only serious and critical unsafe. Do not request Input Monitoring authorization from the daemon.

- [ ] **Step 4: Add the cgo adapter**

Use `//go:build darwin && cgo` and link `CoreGraphics` and `Foundation`. Convert the bridge result to the shared snapshot. `Close` is a no-op.

Add a `darwin && !cgo` source that returns stable activity-unavailable state so `CGO_ENABLED=0` package builds remain defined.

- [ ] **Step 5: Run tests and checks**

```bash
go test ./internal/platformactivity -run 'Darwin|NativeActivity'
make check
```

Expected: PASS on macOS.

- [ ] **Step 6: Commit**

```bash
git add internal/platformactivity/source_darwin.go internal/platformactivity/source_darwin_nocgo.go internal/platformactivity/activity_bridge_darwin.h internal/platformactivity/activity_bridge_darwin.m internal/platformactivity/source_darwin_test.go
git commit -S -m "Add macOS input and thermal activity source" -m "Co-authored-by: Codex <noreply@openai.com>"
```

---

### Task 9: Add the Linux activity source

**Files:**
- Create: `internal/platformactivity/source_linux.go`
- Create: `internal/platformactivity/login1_linux.go`
- Create: `internal/platformactivity/thermal_linux.go`
- Create: `internal/platformactivity/source_linux_test.go`
- Create: `internal/platformactivity/thermal_linux_test.go`
- Modify: `go.mod`
- Modify: `go.sum`
- Modify: `internal/daemon/manager.go:191-257`

**Interfaces:**
- Consumes: Task 7 activity contract.
- Produces: login1 session reader, monotonic idle calculation, and sysfs thermal reader.
- Promotes: `github.com/coreos/go-systemd/v22` to a direct dependency.

- [ ] **Step 1: Write failing Linux session and thermal tests**

Use injected session rows with this shape:

```go
type sessionActivity struct {
    UID                    uint32
    Remote                 bool
    Active                 bool
    Class                  string
    IdleHint               bool
    IdleSinceMonotonicUsec uint64
}
```

Test selection of local active `user` sessions for `os.Getuid()`. Require every selected session to be idle long enough. Test no session, bus failure, invalid monotonic time, and one active session.

Build temporary `thermal_zone*` trees. Test paired `hot` and `critical` trip points, safe readings, unsafe readings, malformed values, and no usable trip pair.

- [ ] **Step 2: Run Linux tests and verify failure**

Run this step on a Linux host or required Linux CI lane:

```bash
go test ./internal/platformactivity -list 'Linux|LoginSession|ThermalZone'
go test ./internal/platformactivity -run 'Linux|LoginSession|ThermalZone'
```

Expected: the list command names every planned Linux test, then the test command FAILS because Linux readers do not exist. A zero-test run does not satisfy this step.

- [ ] **Step 3: Add login1 activity**

Use `login1.Conn.ListSessionsContext` and `GetSessionPropertiesContext`. Compare `IdleSinceHintMonotonic` with `unix.ClockGettime(unix.CLOCK_MONOTONIC)`. Do not use wall-clock time, display variables, or compositor APIs. Close the login1 connection from `Source.Close`.

- [ ] **Step 4: Add thermal sysfs activity**

Production uses `/sys/class/thermal`. For each zone, pair `trip_point_N_type` with `trip_point_N_temp`. Mark unsafe only when current `temp` reaches a `hot` or `critical` trip. No valid pair means thermal unavailable.

After Darwin, Linux, and unsupported factories all exist, change the production manager dependency default from `NewUnavailable` to `platformactivity.New()`.

- [ ] **Step 5: Tidy dependencies and run tests**

```bash
go mod tidy
go test ./internal/platformactivity -list 'Linux|LoginSession|ThermalZone'
go test ./internal/platformactivity -run 'Linux|LoginSession|ThermalZone'
make check
```

Expected: the list command names every planned Linux test, the tests PASS on Linux, and Darwin CI has no build regression.

- [ ] **Step 6: Commit**

```bash
git add internal/platformactivity/source_linux.go internal/platformactivity/login1_linux.go internal/platformactivity/thermal_linux.go internal/platformactivity/source_linux_test.go internal/platformactivity/thermal_linux_test.go internal/daemon/manager.go go.mod go.sum
git commit -S -m "Add Linux session and thermal activity source" -m "Co-authored-by: Codex <noreply@openai.com>"
```

---

### Task 10: Expose policy through protobuf and gRPC

**Files:**
- Modify: `proto/lmsemanticsearch/v1/service.proto:9-420`
- Regenerate: `gen/go/lmsemanticsearch/v1/service.pb.go`
- Regenerate: `gen/go/lmsemanticsearch/v1/service_grpc.pb.go`
- Modify: `internal/pbconv/pbconv.go:13-320`
- Test: `internal/pbconv/pbconv_test.go`
- Modify: `internal/daemon/grpc_server.go:215-550`
- Create: `internal/daemon/grpc_policy_test.go`
- Modify: `internal/daemon/manager_policy.go`
- Modify: `internal/daemon/manager_load.go:12-38`
- Test: `internal/daemon/manager_policy_test.go`
- Modify: `internal/model/scheduling.go`
- Create: `internal/store/policy_update.go`
- Create: `internal/store/policy_update_test.go`

**Interfaces:**
- Produces: wire priority, full policy, presence-aware patch, update RPC, request overrides, and policy-bearing codebase and job views.
- Consumes: manager policy and scheduler update paths from Tasks 2 through 7.

- [ ] **Step 1: Write failing conversion and gRPC tests**

Test omitted override fields, explicit `quiet=false`, invalid unspecified or unknown priority, invalid idle duration, initial persistence, one-run existing override, field-level stored mutation, and immediate active-job reclassification. Inject failure after the new registry reaches disk but before the active-job journal event. Assert rollback restores the old stored and effective policy. Add a boot fixture with a pending policy transaction and assert startup rolls it back before serving.

- [ ] **Step 2: Add exact protobuf fields**

Add:

```proto
enum SchedulingPriority {
  SCHEDULING_PRIORITY_UNSPECIFIED = 0;
  SCHEDULING_PRIORITY_HIGH = 1;
  SCHEDULING_PRIORITY_NORMAL = 2;
  SCHEDULING_PRIORITY_LOW = 3;
}

message SchedulingPolicy {
  SchedulingPriority priority = 1;
  bool quiet = 2;
  int32 idle_after_seconds = 3;
}

message SchedulingPolicyPatch {
  optional SchedulingPriority priority = 1;
  optional bool quiet = 2;
  optional int32 idle_after_seconds = 3;
}
```

Use `Codebase.scheduling_policy=19`, `Codebase.policy_pending_initialization=20`, `Job.effective_scheduling_policy=21`, `Job.queue_sequence=22`, and `Job.scheduling_reason=23`. Use `StartIndexRequest.scheduling_policy=9` and `SyncIndexRequest.scheduling_policy=3`.

Add `UpdateCodebasePolicy` to the service. Its request uses `path=1`, `patch=2`, and `client=3`. Its response uses `codebase=1` and `display_text=2`.

- [ ] **Step 3: Regenerate and convert**

```bash
make proto
```

Add presence-aware conversion that returns an error before manager mutation. Map stored and effective policies both ways.

- [ ] **Step 4: Add transactional policy mutation**

Add a small `PolicyUpdateTransaction` model containing the codebase id, old codebase record, and optional old active-job record. Persist it atomically beside the registry before changing either durable source. Remove it with a directory sync only after the registry and active-job journal agree.

Implement `Manager.UpdateCodebasePolicy` under the job-transition serializer. Apply only supplied fields. Clear matching one-run override fields on current work while preserving unrelated overrides. Use this commit order:

1. Write and sync the transaction marker.
2. Under `manager.mu`, write the patched registry while keeping the old policy unpublished in memory.
3. Release `manager.mu` and append and sync the patched active-job event.
4. Publish the codebase, job, and scheduler policy.
5. Remove the marker durably, then acknowledge the command.

On any failure after step 1, restore the old codebase in the current registry, append and sync the old job snapshot when needed, and remove the marker. If rollback itself fails, return both errors and leave the marker. Call marker recovery at the start of `loadState`, before registry and journal replay. Test this through a real new manager over a state root containing a pending marker. Startup must finish rollback before loading jobs or accepting requests. This prevents a failed command or crash from exposing half a mutation.

Add the gRPC handler. Convert and validate before canonicalizing or mutating. Return the updated codebase and shared mutation acknowledgment.

- [ ] **Step 5: Run tests and checks**

```bash
go test ./internal/pbconv ./internal/daemon -run 'SchedulingPolicy|UpdateCodebasePolicy|RunPolicyOverride'
make check
```

Expected: PASS and generated files match the proto.

- [ ] **Step 6: Commit**

```bash
git add proto/lmsemanticsearch/v1/service.proto gen/go/lmsemanticsearch/v1/service.pb.go gen/go/lmsemanticsearch/v1/service_grpc.pb.go internal/pbconv/pbconv.go internal/pbconv/pbconv_test.go internal/daemon/grpc_server.go internal/daemon/grpc_policy_test.go internal/daemon/manager_policy.go internal/daemon/manager_load.go internal/daemon/manager_policy_test.go internal/model/scheduling.go internal/store/policy_update.go internal/store/policy_update_test.go
git commit -S -m "Add scheduling policy protocol and mutation" -m "Co-authored-by: Codex <noreply@openai.com>"
```

---

### Task 11: Add CLI and MCP index controls

**Files:**
- Create: `cmd/lm-semantic-search/codebase_policy.go`
- Create: `cmd/lm-semantic-search/codebase_policy_test.go`
- Modify: `cmd/lm-semantic-search/codebase.go:20-202`
- Test: `cmd/lm-semantic-search/help_smoke_test.go`
- Create: `internal/mcpserver/scheduling_tools.go`
- Create: `internal/mcpserver/scheduling_tools_test.go`
- Modify: `internal/mcpserver/server.go:59-180`
- Test: `internal/mcpserver/required_args_test.go`

**Interfaces:**
- Consumes: Task 10 wire messages and RPC.
- Produces: index and sync flags, stored `priority` and `quiet` commands, and MCP `index_codebase` overrides.
- Does not produce: an MCP sync tool.

- [ ] **Step 1: Write failing request-shape and command tests**

Test no flags, `--priority=high`, `--quiet`, explicit `--quiet=false`, and `--idle-after=5m`. Assert a new index persists values while an existing index or sync sends one-run fields only. Test `codebase priority PATH VALUE` and `codebase quiet PATH on|off --idle-after=5m`.

Test MCP argument omission by inspecting raw arguments. Assert `index_codebase` accepts `priority`, `quiet`, and `idle_after_seconds`. Assert the registered tool list still contains no `sync_codebase`.

- [ ] **Step 2: Run tests and verify failure**

```bash
go test ./cmd/lm-semantic-search ./internal/mcpserver -run 'SchedulingPolicy|CodebasePriority|CodebaseQuiet|IndexSchedulingArguments|NoSyncTool'
```

Expected: FAIL because commands and fields do not exist.

- [ ] **Step 3: Add shared CLI patch parsing**

Use Cobra `Flags().Changed` for every optional flag. Convert `time.Duration` to validated whole seconds. Build one `pb.SchedulingPolicyPatch` helper used by index and sync. The stored commands call `UpdateCodebasePolicy` with only their owned fields.

- [ ] **Step 4: Extend MCP index only**

Read raw tool arguments to distinguish omission from false or zero. Convert through the same protobuf validation path. Keep the existing index response and wait behavior unchanged.

- [ ] **Step 5: Run tests and checks**

```bash
go test ./cmd/lm-semantic-search ./internal/mcpserver -run 'SchedulingPolicy|CodebasePriority|CodebaseQuiet|IndexSchedulingArguments|NoSyncTool'
make check
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/lm-semantic-search/codebase_policy.go cmd/lm-semantic-search/codebase_policy_test.go cmd/lm-semantic-search/codebase.go cmd/lm-semantic-search/help_smoke_test.go internal/mcpserver/scheduling_tools.go internal/mcpserver/scheduling_tools_test.go internal/mcpserver/server.go internal/mcpserver/required_args_test.go
git commit -S -m "Add codebase scheduling policy commands" -m "Co-authored-by: Codex <noreply@openai.com>"
```

---

### Task 12: Show policy, pause reasons, and scheduler metrics

**Files:**
- Create: `internal/view/scheduling.go`
- Create: `internal/render/scheduling.go`
- Modify: `internal/view/view.go:416-650`
- Modify: `internal/render/render.go:108-410`
- Modify: `internal/status/status.go:182-312`
- Modify: `internal/daemon/status_present.go:180-850`
- Modify: `internal/daemon/manager_snapshot.go:15-93`
- Modify: `internal/daemon/status_metrics.go:145-400`
- Modify: `cmd/lm-semantic-search/codebase_list_tui.go:327-500`
- Test: `internal/daemon/render_test.go`
- Test: `internal/daemon/present_views_test.go`
- Test: `internal/daemon/status_metrics_test.go`
- Test: `internal/status/status_test.go`
- Test: `cmd/lm-semantic-search/codebase_list_tui_test.go`

**Interfaces:**
- Consumes: scheduler snapshot, activity cache, and wire policy.
- Produces: one shared scheduling view for human and JSON surfaces.

- [ ] **Step 1: Write failing presentation tests**

Assert codebase list and status show stored priority, quiet state, and idle threshold. Assert job list and get show effective priority, paused state, and one of these exact reasons:

```text
higher-priority work
user active
activity unavailable
thermal safety
```

Assert the TUI renders the same values. Assert JSON carries structured policy fields without parsing display text.

- [ ] **Step 2: Write failing metric tests**

Require queued, running, and paused counts for each priority. Require input availability, thermal availability, and thermal unsafe state. Keep aggregate `activity.running` and `activity.queued` metrics for compatibility. Never expose input events, session identifiers, or paths in activity-source metrics.

- [ ] **Step 3: Run tests and verify failure**

```bash
go test ./internal/status ./internal/daemon ./cmd/lm-semantic-search -run 'Scheduling|Paused|ActivityPriorityMetrics'
```

Expected: FAIL because presentation fields are missing.

- [ ] **Step 4: Add one presentation model and renderer**

Create a scheduling view with priority, quiet, idle label, state, and reason. Populate it in daemon presentation code. Render it from CLI, MCP display text, and TUI without re-deriving policy.

Add paused to every status vocabulary and terminality switch. Resolve a paused codebase as waiting with its scheduling reason.

- [ ] **Step 5: Add cached scheduler and activity metrics**

Build metrics only from `StatusSnapshot`. Do not sample the platform source during status reads. Add per-priority counts and availability fields with stable names. Update existing slot metrics from scheduler capacity.

- [ ] **Step 6: Run tests and checks**

```bash
go test ./internal/status ./internal/daemon ./cmd/lm-semantic-search -run 'Scheduling|Paused|ActivityPriorityMetrics'
make check
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/view/scheduling.go internal/render/scheduling.go internal/view/view.go internal/render/render.go internal/status/status.go internal/daemon/status_present.go internal/daemon/manager_snapshot.go internal/daemon/status_metrics.go cmd/lm-semantic-search/codebase_list_tui.go internal/daemon/render_test.go internal/daemon/present_views_test.go internal/daemon/status_metrics_test.go internal/status/status_test.go cmd/lm-semantic-search/codebase_list_tui_test.go
git commit -S -m "Show scheduling policy and pause status" -m "Co-authored-by: Codex <noreply@openai.com>"
```

---

### Task 13: Prove installed services and complete acceptance coverage

**Files:**
- Create: `test/serviceactivity/activity_service_test.go`
- Modify: `Makefile:130-170`
- Modify: `test/restartacceptance/scenarios_e_h_test.go`
- Modify: `docs/superpowers/specs/2026-08-30-codebase-scheduling-policy-design.md`

**Interfaces:**
- Consumes: complete scheduler, activity, protocol, and status surfaces.
- Produces: opt-in `service-activity-live` proof and final restart acceptance.

- [ ] **Step 1: Add installed-service acceptance**

Create a `serviceactivitylive` test that connects to the default installed daemon and calls `GetStatus`. Require scheduler metrics, activity availability, and stable unavailable reasons. The test must not start a direct daemon process.

Add this target:

```make
.PHONY: service-activity-live
service-activity-live: | $(GO_MK_PREREQS)
	go test -tags serviceactivitylive -count=1 ./test/serviceactivity/
```

- [ ] **Step 2: Extend restart acceptance**

Add an isolated scenario that pauses a low-priority job, restarts the installed daemon, and proves the successor preserves effective policy and completes. Assert the operator Milvus inventory remains unchanged outside the sandbox database.

- [ ] **Step 3: Prove the real macOS launch agent**

Run on a clean macOS host:

```bash
make install
make service-install
make daemon-wait
make service-activity-live
```

Record whether combined-session idle reading requires Input Monitoring. If authorization is unavailable, require `activity unavailable`; never request permission from the daemon.

- [ ] **Step 4: Prove the real Linux user service**

Run the same four commands on a Linux host with `systemd-logind`. Require a local active session, login1 idle metrics, and thermal availability or the explicit thermal-unavailable note. Do not treat a release container as user-service proof.

- [ ] **Step 5: Add the manual downgrade release note**

Update the design status to implemented only after every test passes. Retain the rule that a manual downgrade must finish or cancel paused jobs first. Do not add a downgrade mechanism or weaken journal parsing.

- [ ] **Step 6: Run the final local gates**

```bash
make test
make test GOFLAGS=-race
make offline-live
make restart-acceptance-unit
make check
```

Expected: every command passes.

- [ ] **Step 7: Commit**

```bash
git add test/serviceactivity/activity_service_test.go Makefile test/restartacceptance/scenarios_e_h_test.go docs/superpowers/specs/2026-08-30-codebase-scheduling-policy-design.md
git commit -S -m "Add scheduling policy service acceptance" -m "Co-authored-by: Codex <noreply@openai.com>"
```

## Final Review Gate

Before opening a pull request:

1. Fetch `origin` and inspect `origin/main..HEAD`.
2. Run `git verify-commit` for every branch-local commit.
3. Confirm every raw commit object contains `gpgsig`.
4. Run the strongest-model adversarial review because this change affects concurrency, lifetime, persistence, and user-visible behavior.
5. Reproduce every required test result from a clean worktree.
6. Keep merge authorization separate from daemon deployment authorization.
