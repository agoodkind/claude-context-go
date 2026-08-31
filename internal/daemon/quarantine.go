package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"

	"goodkind.io/gklog/correlation"
	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/merkle"
	"goodkind.io/lm-semantic-search/internal/metrics"
	"goodkind.io/lm-semantic-search/internal/model"
)

const (
	suspiciousRemovalAbsoluteThreshold  int32 = 100
	suspiciousRemovalRatioNumerator     int32 = 1
	suspiciousRemovalRatioDenominator   int32 = 4
	quarantineConfirmationObservations  int32 = 2
	quarantineTriggerFullScan                 = "full_scan"
	quarantineReasonFullScanLargeDelete       = "A full scan still sees a suspiciously large delete wave, so destructive sync remains paused until the signal repeats."
)

type quarantineSignal struct {
	reason       string
	trigger      string
	missingCount int32
	totalCount   int32
}

func quarantineConfirmed(codebase model.Codebase) bool {
	return codebase.Quarantine != nil &&
		codebase.Quarantine.ObservationCount >= quarantineConfirmationObservations &&
		codebase.Quarantine.LastTrigger == quarantineTriggerFullScan
}

func emptyQuarantineSignal() quarantineSignal {
	return quarantineSignal{
		reason:       "",
		trigger:      "",
		missingCount: 0,
		totalCount:   0,
	}
}

func trackedFileTotalForSuspicion(codebase model.Codebase, snapshot merkle.Snapshot) int32 {
	if len(snapshot.Files) > 0 {
		return safeInt32(len(snapshot.Files))
	}
	if codebase.LastSuccessfulRun != nil && codebase.LastSuccessfulRun.IndexedFiles > 0 {
		return codebase.LastSuccessfulRun.IndexedFiles
	}
	if codebase.LiveFileTotal > 0 {
		return codebase.LiveFileTotal
	}
	return 0
}

func shouldQuarantineLargeRemoval(codebase model.Codebase, missingCount int32, totalCount int32) bool {
	if codebase.Kind != model.CodebaseKindCode {
		return false
	}
	if codebase.LastSuccessfulRun == nil {
		return false
	}
	if totalCount <= 0 || missingCount < suspiciousRemovalAbsoluteThreshold {
		return false
	}
	return missingCount*suspiciousRemovalRatioDenominator >= totalCount*suspiciousRemovalRatioNumerator
}

// vcsOperationInProgress reports whether a git operation that transiently
// removes or rewrites tracked files is mid-flight (checkout, rebase, merge,
// cherry-pick, revert, bisect). Files vanish and reappear during these, so a
// disappearance observed while one is active must pause, never drive deletes.
// It assumes a regular .git directory; worktrees and submodules that use a
// .git file pointing elsewhere are not resolved here.
func vcsOperationInProgress(root string) bool {
	gitDir := filepath.Join(root, ".git")
	for _, marker := range []string{
		"index.lock",
		"MERGE_HEAD",
		"CHERRY_PICK_HEAD",
		"REVERT_HEAD",
		"BISECT_LOG",
		"rebase-apply",
		"rebase-merge",
	} {
		if fileExists(filepath.Join(gitDir, marker)) {
			return true
		}
	}
	return false
}

func assessDeltaDeleteWave(codebase model.Codebase, diff merkle.Diff, snapshot merkle.Snapshot, root string) (quarantineSignal, bool) {
	if quarantineConfirmed(codebase) {
		return emptyQuarantineSignal(), false
	}
	missingPaths := physicallyMissingPaths(root, diff.Removed)
	missingCount := safeInt32(len(missingPaths))
	totalCount := trackedFileTotalForSuspicion(codebase, snapshot)
	if !shouldQuarantineLargeRemoval(codebase, missingCount, totalCount) {
		return emptyQuarantineSignal(), false
	}
	return quarantineSignal{
		reason:       quarantineReasonFullScanLargeDelete,
		trigger:      quarantineTriggerFullScan,
		missingCount: missingCount,
		totalCount:   totalCount,
	}, true
}

func physicallyMissingPaths(root string, relativePaths []string) []string {
	missing := make([]string, 0, len(relativePaths))
	for _, relativePath := range relativePaths {
		if fileExists(filepath.Join(root, relativePath)) {
			continue
		}
		missing = append(missing, relativePath)
	}
	return missing
}

func (manager *Manager) markCodebaseMissing(ctx context.Context, codebaseID string) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	codebase, found := manager.codebases[codebaseID]
	if !found {
		return
	}
	if codebase.Status == model.CodebaseStatusMissing && codebase.Quarantine == nil {
		return
	}
	codebase.Status = model.CodebaseStatusMissing
	codebase.ActiveJobID = ""
	codebase.Quarantine = nil
	codebase.LastFailedRun = nil
	codebase.UpdatedAt = clock.Now()
	manager.codebases[codebaseID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after marking missing failed", "codebase_id", codebaseID, "err", err)
	}
}

func applyQuarantineState(codebase model.Codebase, signal quarantineSignal) model.Codebase {
	now := clock.Now()
	quarantine := codebase.Quarantine
	if quarantine == nil {
		quarantine = &model.QuarantineState{
			Reason:           signal.reason,
			FirstObservedAt:  now,
			LastObservedAt:   now,
			ObservationCount: 1,
			LastTrigger:      signal.trigger,
			LastMissingCount: signal.missingCount,
			LastTotalCount:   signal.totalCount,
		}
	} else {
		quarantine.Reason = signal.reason
		quarantine.LastObservedAt = now
		quarantine.ObservationCount++
		quarantine.LastTrigger = signal.trigger
		quarantine.LastMissingCount = signal.missingCount
		quarantine.LastTotalCount = signal.totalCount
	}
	codebase.Status = model.CodebaseStatusQuarantined
	codebase.Quarantine = quarantine
	codebase.UpdatedAt = now
	return codebase
}

func (manager *Manager) quarantineCodebase(ctx context.Context, codebaseID string, signal quarantineSignal) int32 {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	codebase, found := manager.codebases[codebaseID]
	if !found {
		return 0
	}
	codebase = applyQuarantineState(codebase, signal)
	manager.codebases[codebaseID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after quarantine failed", "codebase_id", codebaseID, "err", err)
	}
	return codebase.Quarantine.ObservationCount
}

func quarantineJobMessage(signal quarantineSignal) string {
	return fmt.Sprintf(
		"Suspicious large disappearance quarantined after %d of %d tracked files were marked missing; destructive sync is paused until a later full scan corroborates it.",
		signal.missingCount,
		signal.totalCount,
	)
}

func (manager *Manager) updateJobQuarantined(ctx context.Context, jobID string, signal quarantineSignal) {
	traceID := string(correlation.FromContext(ctx).TraceID)
	job, transitioned, journalErr := manager.serializeJobTransition(
		jobID,
		"job_failed",
		func(job *model.Job) bool {
			if isTerminalJobState(job.State) {
				return false
			}
			now := clock.Now()
			job.State = model.JobStateFailed
			job.UpdatedAt = now
			job.CompletedAt = &now
			job.Progress.Phase = "quarantined"
			job.Progress.LastEventAt = now
			job.Progress.HeartbeatAt = now
			job.Error = &model.JobError{
				Message:   quarantineJobMessage(signal),
				Code:      "",
				Retryable: false,
				TraceID:   traceID,
				JobID:     jobID,
			}
			return true
		},
	)
	if !transitioned {
		return
	}
	metrics.JobFailed()
	if journalErr != nil {
		slog.ErrorContext(ctx, "append quarantined job event failed", "job_id", jobID, "err", journalErr)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	delete(manager.conversationJobs, jobID)
	codebase, found := manager.codebases[job.CodebaseID]
	if !found {
		return
	}
	codebase.ActiveJobID = ""
	codebase.LastFailedRun = nil
	codebase = applyQuarantineState(codebase, signal)
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after quarantining job failed", "job_id", jobID, "err", err)
	}
}

func (manager *Manager) clearCodebaseQuarantine(ctx context.Context, codebaseID string, status model.CodebaseStatus) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	codebase, found := manager.codebases[codebaseID]
	if !found {
		return
	}
	if codebase.Quarantine == nil && codebase.Status == status {
		return
	}
	codebase.Quarantine = nil
	codebase.Status = status
	if status == model.CodebaseStatusIndexed || status == model.CodebaseStatusMissing {
		codebase.LastFailedRun = nil
	}
	codebase.UpdatedAt = clock.Now()
	manager.codebases[codebaseID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after clearing quarantine failed", "codebase_id", codebaseID, "err", err)
	}
}

// quarantinedCodebases returns one human-readable line per quarantined codebase
// for the doctor surface.
func (manager *Manager) quarantinedCodebases() []string {
	manager.mu.Lock()
	codebases := make([]model.Codebase, 0, len(manager.codebases))
	for _, codebase := range manager.codebases {
		codebases = append(codebases, codebase)
	}
	manager.mu.Unlock()

	lines := make([]string, 0)
	for _, codebase := range codebases {
		if codebase.Status != model.CodebaseStatusQuarantined || codebase.Quarantine == nil {
			continue
		}
		line := fmt.Sprintf(
			"%s: %d of %d tracked files in %d %s, last trigger %s",
			codebase.CanonicalPath,
			codebase.Quarantine.LastMissingCount,
			codebase.Quarantine.LastTotalCount,
			codebase.Quarantine.ObservationCount,
			plural("observation", int(codebase.Quarantine.ObservationCount)),
			defaultQuarantineTrigger(codebase.Quarantine.LastTrigger),
		)
		if strings.TrimSpace(codebase.Quarantine.Reason) != "" {
			line += " (" + codebase.Quarantine.Reason + ")"
		}
		lines = append(lines, line)
	}
	sort.Strings(lines)
	return lines
}

func defaultQuarantineTrigger(trigger string) string {
	if strings.TrimSpace(trigger) == "" {
		return "unknown"
	}
	return trigger
}
