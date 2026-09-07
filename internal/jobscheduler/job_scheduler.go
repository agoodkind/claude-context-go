// Package jobscheduler orders indexing work by effective scheduling policy.
package jobscheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"goodkind.io/lm-semantic-search/internal/model"
)

const (
	// ReasonWaitingForCapacity means every slot is occupied by equal-priority work.
	ReasonWaitingForCapacity = "waiting for capacity"
	// ReasonHigherPriorityWork means higher-priority work controls admission.
	ReasonHigherPriorityWork = "higher-priority work"
)

// EntryState records one scheduler entry's private lifecycle.
type EntryState uint8

const (
	// EntryWaiting means the entry is queued for capacity.
	EntryWaiting EntryState = iota
	// EntryRunning means the entry owns one capacity slot.
	EntryRunning
	// EntryPaused means the entry released capacity and retained its queue sequence.
	EntryPaused
)

// Entry describes one job competing for scheduler capacity.
type Entry struct {
	JobID            string
	Policy           model.SchedulingPolicy
	QueueSequence    uint64
	State            EntryState
	Reason           string
	PauseRequested   bool
	generation       uint64
	policyGeneration uint64
}

// Snapshot reports capacity use by scheduling priority.
type Snapshot struct {
	Capacity int
	Running  map[model.JobPriority]int
	Queued   map[model.JobPriority]int
	Paused   map[model.JobPriority]int
	Yields   uint64
}

// Scheduler admits jobs and requests cooperative priority pauses.
type Scheduler struct {
	mutex               sync.Mutex
	capacity            int
	entries             map[string]*Entry
	pauseGenerations    map[string]uint64
	nextEntryGeneration uint64
	pauseClaims         map[string]uint64
	retryWaiting        map[string]time.Time
	retryShared         map[string]bool
	retryTimer          *time.Timer
	retryTimerDeadline  time.Time
	retryGeneration     uint64
	yields              uint64
	changed             chan struct{}
	now                 func() time.Time
}

// Lease owns one scheduler entry across running, paused, and waiting states.
type Lease struct {
	scheduler  *Scheduler
	jobID      string
	generation uint64
}

// PauseClaim gives one observer exclusive permission to pause and yield a
// running lease once.
type PauseClaim struct {
	lease      *Lease
	generation uint64
	reason     string
}

// New creates a scheduler with the supplied capacity.
func New(capacity int) *Scheduler {
	return &Scheduler{
		mutex:               sync.Mutex{},
		capacity:            capacity,
		entries:             map[string]*Entry{},
		pauseGenerations:    map[string]uint64{},
		pauseClaims:         map[string]uint64{},
		nextEntryGeneration: 0,
		retryWaiting:        map[string]time.Time{},
		retryShared:         map[string]bool{},
		retryTimer:          nil,
		retryTimerDeadline:  time.Time{},
		retryGeneration:     0,
		yields:              0,
		changed:             make(chan struct{}),
		now:                 time.Now,
	}
}

// Acquire registers an entry and waits until it owns capacity or ctx ends.
func (scheduler *Scheduler) Acquire(
	ctx context.Context,
	entry Entry,
) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("acquire scheduler lease: %w", err)
	}
	if entry.JobID == "" {
		return nil, fmt.Errorf("scheduler job id is required")
	}
	if err := model.ValidateSchedulingPolicy(entry.Policy); err != nil {
		slog.Warn("validate scheduler policy failed", "job_id", entry.JobID, "err", err)
		return nil, fmt.Errorf("validate scheduler policy: %w", err)
	}

	scheduler.mutex.Lock()
	if _, found := scheduler.entries[entry.JobID]; found {
		scheduler.mutex.Unlock()
		return nil, fmt.Errorf("scheduler job %s already exists", entry.JobID)
	}
	scheduler.nextEntryGeneration++
	entry.generation = scheduler.nextEntryGeneration
	entry.State = EntryWaiting
	entry.Reason = ReasonWaitingForCapacity
	entry.PauseRequested = false
	scheduler.entries[entry.JobID] = &entry
	scheduler.rebalanceLocked()
	scheduler.notifyLocked()
	scheduler.mutex.Unlock()

	if err := scheduler.waitForRunning(ctx, entry.JobID, entry.generation, true); err != nil {
		return nil, err
	}
	return &Lease{scheduler: scheduler, jobID: entry.JobID, generation: entry.generation}, nil
}

// UpdatePolicy applies supplied fields and immediately recomputes admission.
func (scheduler *Scheduler) UpdatePolicy(
	jobID string,
	patch model.SchedulingPolicyPatch,
) error {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()

	entry, found := scheduler.entries[jobID]
	if !found {
		return fmt.Errorf("scheduler job %s is missing", jobID)
	}
	return scheduler.updatePolicyLocked(entry, patch)
}

func (scheduler *Scheduler) updatePolicyLocked(
	entry *Entry,
	patch model.SchedulingPolicyPatch,
) error {
	policy, err := model.ApplySchedulingPolicyPatch(entry.Policy, patch)
	if err != nil {
		slog.Warn("update scheduler policy failed", "job_id", entry.JobID, "err", err)
		return fmt.Errorf("update scheduler policy: %w", err)
	}
	entry.Policy = policy
	entry.policyGeneration++
	scheduler.rebalanceLocked()
	scheduler.notifyLocked()
	return nil
}

// DiscardStagedPolicyUpdate is a no-op until policy updates can precede
// scheduler registration. It keeps cancellation safe for that future path.
func (scheduler *Scheduler) DiscardStagedPolicyUpdate(string) {}

// Snapshot returns a consistent copy of scheduler counts.
func (scheduler *Scheduler) Snapshot() Snapshot {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()

	snapshot := Snapshot{
		Capacity: scheduler.capacity,
		Running:  newSchedulerPriorityCounts(),
		Queued:   newSchedulerPriorityCounts(),
		Paused:   newSchedulerPriorityCounts(),
		Yields:   scheduler.yields,
	}
	for _, entry := range scheduler.entries {
		switch entry.State {
		case EntryWaiting:
			snapshot.Queued[entry.Policy.Priority]++
		case EntryRunning:
			snapshot.Running[entry.Policy.Priority]++
		case EntryPaused:
			snapshot.Paused[entry.Policy.Priority]++
		}
	}
	return snapshot
}

// Checkpoint reports whether running work must cooperatively pause.
func (lease *Lease) Checkpoint() (bool, string) {
	lease.scheduler.mutex.Lock()
	defer lease.scheduler.mutex.Unlock()

	entry, found := lease.scheduler.entries[lease.jobID]
	if !found || entry.generation != lease.generation || entry.State != EntryRunning {
		return false, ""
	}
	if !entry.PauseRequested || lease.scheduler.pauseClaims[lease.jobID] != 0 {
		return false, ""
	}
	return true, entry.Reason
}

// ClaimPauseRequest claims the current cooperative pause request once.
func (lease *Lease) ClaimPauseRequest() (*PauseClaim, bool) {
	return lease.claimPause("", true)
}

// ClaimYield claims a running lease for one non-priority yield.
func (lease *Lease) ClaimYield(reason string) (*PauseClaim, bool) {
	return lease.claimPause(reason, false)
}

func (lease *Lease) claimPause(
	reason string,
	requireRequest bool,
) (*PauseClaim, bool) {
	lease.scheduler.mutex.Lock()
	defer lease.scheduler.mutex.Unlock()

	entry, found := lease.scheduler.entries[lease.jobID]
	if !found || entry.generation != lease.generation || entry.State != EntryRunning {
		return nil, false
	}
	if lease.scheduler.pauseClaims[lease.jobID] != 0 {
		return nil, false
	}
	if requireRequest && !entry.PauseRequested {
		return nil, false
	}
	if reason == "" {
		reason = entry.Reason
	}
	generation := lease.scheduler.pauseGenerations[lease.jobID] + 1
	lease.scheduler.pauseGenerations[lease.jobID] = generation
	lease.scheduler.pauseClaims[lease.jobID] = generation
	return &PauseClaim{
		lease:      lease,
		generation: generation,
		reason:     reason,
	}, true
}

// Reason returns the scheduling reason captured by this claim.
func (claim *PauseClaim) Reason() string {
	return claim.reason
}

// Cancel releases an unused claim without changing capacity.
func (claim *PauseClaim) Cancel() {
	claim.lease.scheduler.mutex.Lock()
	defer claim.lease.scheduler.mutex.Unlock()
	entry, found := claim.lease.scheduler.entries[claim.lease.jobID]
	if !found || entry.generation != claim.lease.generation {
		return
	}
	if claim.lease.scheduler.pauseClaims[claim.lease.jobID] != claim.generation {
		return
	}
	delete(claim.lease.scheduler.pauseClaims, claim.lease.jobID)
	claim.lease.scheduler.notifyLocked()
}

// Yield consumes this claim and releases capacity once.
func (claim *PauseClaim) Yield() bool {
	claim.lease.scheduler.mutex.Lock()
	defer claim.lease.scheduler.mutex.Unlock()

	scheduler := claim.lease.scheduler
	entry, found := scheduler.entries[claim.lease.jobID]
	if !found || entry.generation != claim.lease.generation || entry.State != EntryRunning ||
		scheduler.pauseClaims[claim.lease.jobID] != claim.generation {
		return false
	}
	delete(scheduler.pauseClaims, claim.lease.jobID)
	delete(scheduler.retryWaiting, claim.lease.jobID)
	delete(scheduler.retryShared, claim.lease.jobID)
	entry.State = EntryPaused
	entry.Reason = claim.reason
	entry.PauseRequested = false
	scheduler.yields++
	scheduler.rebalanceLocked()
	scheduler.notifyLocked()
	return true
}

// Yield releases capacity once and retains the entry for reacquisition.
func (lease *Lease) Yield(reason string) bool {
	claim, claimed := lease.ClaimYield(reason)
	if !claimed {
		return false
	}
	return claim.Yield()
}

// RetryAfter yields capacity and joins the scheduler's next shared retry round.
func (lease *Lease) RetryAfter(
	ctx context.Context,
	delay time.Duration,
	reason string,
) error {
	return lease.retryAfter(ctx, delay, reason, false)
}

// RetrySharedAfter yields capacity until the next probe of a shared external
// resource. Entries with the same reason join that round so admission still
// selects the highest-priority waiter when the resource may be available.
func (lease *Lease) RetrySharedAfter(
	ctx context.Context,
	delay time.Duration,
	reason string,
) error {
	return lease.retryAfter(ctx, delay, reason, true)
}

func (lease *Lease) retryAfter(
	ctx context.Context,
	delay time.Duration,
	reason string,
	shared bool,
) error {
	if err := ctx.Err(); err != nil {
		wrappedErr := fmt.Errorf("retry scheduler lease: %w", err)
		slog.Warn("retry scheduler lease canceled", "job_id", lease.jobID, "err", wrappedErr)
		return wrappedErr
	}
	if delay <= 0 {
		return fmt.Errorf("retry delay must be positive")
	}

	lease.scheduler.mutex.Lock()
	entry, found := lease.scheduler.entries[lease.jobID]
	if !found || entry.generation != lease.generation {
		lease.scheduler.mutex.Unlock()
		return fmt.Errorf("scheduler job %s is missing", lease.jobID)
	}
	if entry.State != EntryRunning {
		lease.scheduler.mutex.Unlock()
		return fmt.Errorf("scheduler job %s is not running", lease.jobID)
	}
	delete(lease.scheduler.pauseClaims, lease.jobID)
	entry.State = EntryPaused
	entry.Reason = reason
	entry.PauseRequested = false
	now := lease.scheduler.now()
	lease.scheduler.retryWaiting[lease.jobID] = now.Add(delay)
	lease.scheduler.retryShared[lease.jobID] = shared
	lease.scheduler.yields++
	lease.scheduler.scheduleNextRetryLocked(now)
	lease.scheduler.rebalanceLocked()
	lease.scheduler.notifyLocked()
	lease.scheduler.mutex.Unlock()

	return lease.scheduler.waitForRunning(ctx, lease.jobID, lease.generation, false)
}

// Reacquire waits for the paused entry to regain capacity.
func (lease *Lease) Reacquire(ctx context.Context) error {
	lease.scheduler.mutex.Lock()
	entry, found := lease.scheduler.entries[lease.jobID]
	if !found || entry.generation != lease.generation {
		lease.scheduler.mutex.Unlock()
		return fmt.Errorf("scheduler job %s is missing", lease.jobID)
	}
	if entry.State == EntryRunning {
		lease.scheduler.mutex.Unlock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		lease.scheduler.mutex.Unlock()
		wrappedErr := fmt.Errorf("reacquire scheduler lease: %w", err)
		slog.Warn("reacquire scheduler lease canceled", "job_id", lease.jobID, "err", wrappedErr)
		return wrappedErr
	}
	if entry.State == EntryPaused {
		delete(lease.scheduler.retryWaiting, lease.jobID)
		delete(lease.scheduler.retryShared, lease.jobID)
		entry.State = EntryWaiting
		entry.PauseRequested = false
		lease.scheduler.rebalanceLocked()
		lease.scheduler.notifyLocked()
	}
	lease.scheduler.mutex.Unlock()

	return lease.scheduler.waitForRunning(ctx, lease.jobID, lease.generation, false)
}

// Release removes the entry once from running, waiting, or paused state.
func (lease *Lease) Release() {
	lease.scheduler.mutex.Lock()
	defer lease.scheduler.mutex.Unlock()

	entry, found := lease.scheduler.entries[lease.jobID]
	if !found || entry.generation != lease.generation {
		return
	}
	delete(lease.scheduler.entries, lease.jobID)
	delete(lease.scheduler.pauseClaims, lease.jobID)
	delete(lease.scheduler.pauseGenerations, lease.jobID)
	delete(lease.scheduler.retryWaiting, lease.jobID)
	delete(lease.scheduler.retryShared, lease.jobID)
	lease.scheduler.rebalanceLocked()
	lease.scheduler.notifyLocked()
}

// UpdatePolicy applies supplied fields to this lease's entry.
func (lease *Lease) UpdatePolicy(patch model.SchedulingPolicyPatch) error {
	lease.scheduler.mutex.Lock()
	defer lease.scheduler.mutex.Unlock()
	entry, found := lease.scheduler.entries[lease.jobID]
	if !found || entry.generation != lease.generation {
		return fmt.Errorf("scheduler job %s is no longer owned by this lease", lease.jobID)
	}
	return lease.scheduler.updatePolicyLocked(entry, patch)
}

func (scheduler *Scheduler) waitForRunning(
	ctx context.Context,
	jobID string,
	generation uint64,
	removeOnCancel bool,
) error {
	for {
		scheduler.mutex.Lock()
		entry, found := scheduler.entries[jobID]
		if !found || entry.generation != generation {
			scheduler.mutex.Unlock()
			return fmt.Errorf("scheduler job %s is missing", jobID)
		}
		if err := ctx.Err(); err != nil {
			scheduler.cancelWaitLocked(entry, removeOnCancel)
			scheduler.mutex.Unlock()
			wrappedErr := fmt.Errorf("wait for scheduler job %s: %w", jobID, err)
			slog.Warn("scheduler wait canceled", "job_id", jobID, "err", wrappedErr)
			return wrappedErr
		}
		if entry.State == EntryRunning {
			scheduler.mutex.Unlock()
			return nil
		}
		changed := scheduler.changed
		scheduler.mutex.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
		}
	}
}

func (scheduler *Scheduler) cancelWaitLocked(
	entry *Entry,
	remove bool,
) {
	if remove {
		delete(scheduler.entries, entry.JobID)
		delete(scheduler.pauseClaims, entry.JobID)
		delete(scheduler.pauseGenerations, entry.JobID)
		delete(scheduler.retryWaiting, entry.JobID)
		delete(scheduler.retryShared, entry.JobID)
	} else {
		entry.State = EntryPaused
		entry.PauseRequested = false
		delete(scheduler.pauseClaims, entry.JobID)
		delete(scheduler.retryWaiting, entry.JobID)
		if entry.Reason == "" {
			entry.Reason = ReasonWaitingForCapacity
		}
	}
	scheduler.rebalanceLocked()
	scheduler.notifyLocked()
}

func (scheduler *Scheduler) startRetryRoundLocked(delay time.Duration) {
	scheduler.retryGeneration++
	generation := scheduler.retryGeneration
	scheduler.retryTimerDeadline = scheduler.now().Add(delay)
	scheduler.retryTimer = time.AfterFunc(delay, func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("scheduler retry round panic", "err", fmt.Errorf("panic: %v", recovered))
			}
		}()
		scheduler.openRetryRound(generation)
	})
}

func (scheduler *Scheduler) openRetryRound(generation uint64) {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()

	if generation != scheduler.retryGeneration {
		return
	}
	scheduler.retryTimer = nil
	scheduler.retryTimerDeadline = time.Time{}
	now := scheduler.now()
	sharedReasons := map[string]struct{}{}
	for jobID, deadline := range scheduler.retryWaiting {
		if deadline.After(now) || !scheduler.retryShared[jobID] {
			continue
		}
		entry, found := scheduler.entries[jobID]
		if found && entry.State == EntryPaused {
			sharedReasons[entry.Reason] = struct{}{}
		}
	}
	for jobID, deadline := range scheduler.retryWaiting {
		entry, found := scheduler.entries[jobID]
		if !found {
			delete(scheduler.retryWaiting, jobID)
			delete(scheduler.retryShared, jobID)
			continue
		}
		_, sharedRound := sharedReasons[entry.Reason]
		if deadline.After(now) && !sharedRound {
			continue
		}
		if found && entry.State == EntryPaused {
			entry.State = EntryWaiting
			entry.PauseRequested = false
		}
		delete(scheduler.retryWaiting, jobID)
		delete(scheduler.retryShared, jobID)
	}
	scheduler.scheduleNextRetryLocked(now)
	scheduler.rebalanceLocked()
	scheduler.notifyLocked()
}

func (scheduler *Scheduler) scheduleNextRetryLocked(now time.Time) {
	if len(scheduler.retryWaiting) == 0 {
		return
	}
	var next time.Time
	for _, deadline := range scheduler.retryWaiting {
		if next.IsZero() || deadline.Before(next) {
			next = deadline
		}
	}
	delay := max(next.Sub(now), 0)
	if scheduler.retryTimer != nil {
		if !next.Before(scheduler.retryTimerDeadline) {
			return
		}
		scheduler.retryTimer.Stop()
		scheduler.retryTimer = nil
		scheduler.retryTimerDeadline = time.Time{}
	}
	scheduler.startRetryRoundLocked(delay)
}

func (scheduler *Scheduler) rebalanceLocked() {
	waiting := scheduler.waitingEntriesLocked()
	runningCount := scheduler.runningCountLocked()
	for runningCount < scheduler.capacity && len(waiting) > 0 {
		entry := waiting[0]
		waiting = waiting[1:]
		entry.State = EntryRunning
		entry.Reason = ""
		entry.PauseRequested = false
		runningCount++
	}

	waiting = scheduler.waitingEntriesLocked()
	for _, entry := range scheduler.entries {
		if entry.State == EntryRunning {
			entry.PauseRequested = false
			entry.Reason = ""
		}
		if entry.State == EntryWaiting {
			entry.Reason = scheduler.waitingReasonLocked(entry)
		}
	}

	selectedVictims := map[string]bool{}
	freeSlots := scheduler.capacity - scheduler.runningCountLocked()
	for _, waitingEntry := range waiting {
		if freeSlots > 0 {
			freeSlots--
			continue
		}
		victim := scheduler.selectVictimLocked(
			waitingEntry.Policy.Priority,
			selectedVictims,
		)
		if victim == nil {
			continue
		}
		selectedVictims[victim.JobID] = true
		victim.PauseRequested = true
		victim.Reason = ReasonHigherPriorityWork
	}
}

func (scheduler *Scheduler) waitingEntriesLocked() []*Entry {
	waiting := make([]*Entry, 0, len(scheduler.entries))
	for _, entry := range scheduler.entries {
		if entry.State == EntryWaiting {
			waiting = append(waiting, entry)
		}
	}
	sort.Slice(waiting, func(i int, j int) bool {
		leftRank := schedulerPriorityRank(waiting[i].Policy.Priority)
		rightRank := schedulerPriorityRank(waiting[j].Policy.Priority)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		if waiting[i].QueueSequence != waiting[j].QueueSequence {
			return waiting[i].QueueSequence < waiting[j].QueueSequence
		}
		return waiting[i].JobID < waiting[j].JobID
	})
	return waiting
}

func (scheduler *Scheduler) selectVictimLocked(
	waitingPriority model.JobPriority,
	selected map[string]bool,
) *Entry {
	var victim *Entry
	for _, candidate := range scheduler.entries {
		if candidate.State != EntryRunning || selected[candidate.JobID] {
			continue
		}
		if schedulerPriorityRank(candidate.Policy.Priority) <= schedulerPriorityRank(waitingPriority) {
			continue
		}
		if victim == nil || schedulerVictimBefore(candidate, victim) {
			victim = candidate
		}
	}
	return victim
}

func schedulerVictimBefore(left *Entry, right *Entry) bool {
	leftRank := schedulerPriorityRank(left.Policy.Priority)
	rightRank := schedulerPriorityRank(right.Policy.Priority)
	if leftRank != rightRank {
		return leftRank > rightRank
	}
	if left.QueueSequence != right.QueueSequence {
		return left.QueueSequence > right.QueueSequence
	}
	return left.JobID > right.JobID
}

func (scheduler *Scheduler) waitingReasonLocked(entry *Entry) string {
	entryRank := schedulerPriorityRank(entry.Policy.Priority)
	for _, candidate := range scheduler.entries {
		if candidate.JobID == entry.JobID || candidate.State == EntryPaused {
			continue
		}
		if schedulerPriorityRank(candidate.Policy.Priority) < entryRank {
			return ReasonHigherPriorityWork
		}
	}
	return ReasonWaitingForCapacity
}

func (scheduler *Scheduler) runningCountLocked() int {
	running := 0
	for _, entry := range scheduler.entries {
		if entry.State == EntryRunning {
			running++
		}
	}
	return running
}

func (scheduler *Scheduler) notifyLocked() {
	close(scheduler.changed)
	scheduler.changed = make(chan struct{})
}

func schedulerPriorityRank(priority model.JobPriority) int {
	switch priority {
	case model.JobPriorityHigh:
		return 0
	case model.JobPriorityNormal:
		return 1
	case model.JobPriorityLow:
		return 2
	default:
		return 3
	}
}

func newSchedulerPriorityCounts() map[model.JobPriority]int {
	return map[model.JobPriority]int{
		model.JobPriorityHigh:   0,
		model.JobPriorityNormal: 0,
		model.JobPriorityLow:    0,
	}
}
