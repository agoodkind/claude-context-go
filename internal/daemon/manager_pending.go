package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"maps"

	"goodkind.io/gklog/correlation"
	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/gitworktree"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/spans"
)

// pendingCodeRequest is the depth-1 coalesced code sync a codebase queues when a
// non-matching-config index or sync request arrives while a code job is active.
// It carries exactly what enqueueing a fresh sync needs, so the drain re-runs the
// request against the finished checkpoint after the active job clears.
type pendingCodeRequest struct {
	requestedPath string
	canonicalPath string
	client        model.ClientInfo
	indexConfig   model.IndexConfig
	// force ORs across coalesced requests (sticky true). In the current call graph
	// a force StartIndex cancels the active job before admission, so a coalesced
	// request reaches this slot only with force false; the field keeps the merge
	// rule general.
	force       bool
	policyPatch model.SchedulingPolicyPatch
}

// activeJobResolution classifies how an incoming code request relates to a
// codebase's in-flight job, so the admission sites can dedup a matching-config
// request, coalesce a non-matching one, or queue when nothing is active.
type activeJobResolution int

const (
	// activeJobNone means no in-flight job blocks the request.
	activeJobNone activeJobResolution = iota
	// activeJobDedup means an in-flight job matches the caller's config, so the
	// request collapses onto it instead of embedding twice.
	activeJobDedup
	// activeJobConflict means an in-flight job carries a different config, so the
	// request coalesces onto the depth-1 pending slot instead of refusing.
	activeJobConflict
)

// activeJobLocked classifies a codebase's in-flight code job against an incoming
// request's config. Caller holds manager.mu.
func (manager *Manager) activeJobLocked(codebase model.Codebase, indexConfig model.IndexConfig) (model.Job, activeJobResolution, error) {
	var emptyJob model.Job
	if codebase.ActiveJobID == "" {
		return emptyJob, activeJobNone, nil
	}

	activeJob, found := manager.jobs[codebase.ActiveJobID]
	if !found {
		return emptyJob, activeJobNone, nil
	}

	switch activeJob.State {
	case model.JobStateCompleted, model.JobStateFailed, model.JobStateCancelled:
		return emptyJob, activeJobNone, nil
	case model.JobStateQueued, model.JobStateRunning, model.JobStateCancelling:
	default:
		return emptyJob, activeJobNone, fmt.Errorf("unknown job state %s for active job %s", activeJob.State, activeJob.ID)
	}

	if activeJob.Config.IgnoreDigest == indexConfig.IgnoreDigest && activeJob.Config.SplitterType == indexConfig.SplitterType {
		return activeJob, activeJobDedup, nil
	}

	return activeJob, activeJobConflict, nil
}

// mergePendingConversationPayloadLocked folds an incoming upsert payload into the
// codebase's single depth-1 pending slot. Documents and Manifest union by
// conversation id with the newer submission winning per id; Backfill and Force
// each OR (sticky true) so a coalesced backfill keeps its backfill intent and a
// coalesced force stays a force; Absence takes the most conservative (retain) of
// the two so a coalesced retain upsert never inherits a delete-on-absence policy
// it did not ask for. It writes only the pending slot and never touches the
// executing payload (manager.conversationJobs[activeJobID]). Caller holds
// manager.mu.
func (manager *Manager) mergePendingConversationPayloadLocked(codebaseID string, incoming conversationJobPayload) {
	existing, found := manager.pendingConversationJobs[codebaseID]
	if !found {
		manager.pendingConversationJobs[codebaseID] = clonePendingConversationPayload(incoming)
		return
	}
	merged := existing
	if merged.Manifest == nil && incoming.Manifest != nil {
		merged.Manifest = map[string]string{}
	}
	// maps.Copy applies latest-writer-wins per conversation id: the incoming
	// fingerprint replaces the pending one, and pending-only ids are kept.
	maps.Copy(merged.Manifest, incoming.Manifest)
	merged.Documents = unionConversationDocuments(merged.Documents, incoming.Documents)
	merged.Backfill = merged.Backfill || incoming.Backfill
	merged.Force = merged.Force || incoming.Force
	merged.Absence = mostConservativeAbsence(merged.Absence, incoming.Absence)
	manager.pendingConversationJobs[codebaseID] = merged
}

// clonePendingConversationPayload copies a payload plus its reference fields so
// the slot owns private Manifest and Documents that a later in-place merge can
// mutate without touching the caller's originals.
func clonePendingConversationPayload(payload conversationJobPayload) conversationJobPayload {
	cloned := payload
	if payload.Manifest != nil {
		cloned.Manifest = maps.Clone(payload.Manifest)
	}
	if payload.Documents != nil {
		cloned.Documents = append([]model.ConversationDocument(nil), payload.Documents...)
	}
	return cloned
}

// unionConversationDocuments merges two delivered document sets by conversation
// id: the incoming set replaces the existing set for any conversation id it
// carries, and documents for conversations only the existing set carried are
// kept. Order is deterministic: kept existing documents first, then the incoming
// documents.
func unionConversationDocuments(existing []model.ConversationDocument, incoming []model.ConversationDocument) []model.ConversationDocument {
	incomingIDs := make(map[string]struct{}, len(incoming))
	for _, document := range incoming {
		incomingIDs[document.ConversationID] = struct{}{}
	}
	merged := make([]model.ConversationDocument, 0, len(existing)+len(incoming))
	for _, document := range existing {
		if _, replaced := incomingIDs[document.ConversationID]; replaced {
			continue
		}
		merged = append(merged, document)
	}
	merged = append(merged, incoming...)
	return merged
}

// mostConservativeAbsence returns the safer of two absence policies. absenceRetain
// keeps conversations the manifest omits, so it is the conservative choice; only
// when both submissions opted into delete-on-absence does the merged run delete.
func mostConservativeAbsence(first absencePolicy, second absencePolicy) absencePolicy {
	if first == absenceRetain || second == absenceRetain {
		return absenceRetain
	}
	return absenceDeleteGuarded
}

// mergePendingCodeRequestLocked folds an incoming non-matching-config code sync
// into the codebase's single depth-1 pending slot. The newer request's config,
// path, and client win (latest-writer-wins) while force ORs sticky. Caller holds
// manager.mu.
func (manager *Manager) mergePendingCodeRequestLocked(codebaseID string, incoming pendingCodeRequest) {
	if existing, found := manager.pendingCodeJobs[codebaseID]; found {
		incoming.force = incoming.force || existing.force
		incoming.policyPatch = mergeSchedulingPolicyPatches(existing.policyPatch, incoming.policyPatch)
	}
	manager.pendingCodeJobs[codebaseID] = incoming
}

// enqueueConversationJobLocked writes the registry mutations for a fresh
// conversation job and returns it queued. It is the shared body of the first-time
// admission in queueConversationJob and the coalesced drain, so both queue a
// conversation ingest identically. On a persistence error it rolls back both the
// payload insert and the codebase mutation. Caller holds manager.mu and runs
// runJobAsync after unlocking.
func (manager *Manager) enqueueConversationJobLocked(current model.Codebase, client model.ClientInfo, payload conversationJobPayload) (model.Job, error) {
	original := current
	now := clock.Now()
	_, effectivePolicy, err := manager.resolveIndexPolicyLocked(current, indexPolicyIntent{
		Patch:      model.SchedulingPolicyPatch{Priority: nil, Quiet: nil, IdleAfterSeconds: nil},
		Initialize: false,
	})
	if err != nil {
		return model.Job{}, err
	}
	job := newQueuedJob(
		current.ID,
		current.CanonicalPath,
		current.CanonicalPath,
		client,
		string(jobOperationConversationIngest),
		false,
		current.EffectiveConfig,
		emptyAdmissionBudget,
		now,
	)
	applyJobSchedulingPolicy(&job, effectivePolicy, model.SchedulingPolicyPatch{Priority: nil, Quiet: nil, IdleAfterSeconds: nil}, manager.nextQueueSequenceLocked())
	current.Status = model.CodebaseStatusIndexing
	current.ActiveJobID = job.ID
	current.UpdatedAt = now
	manager.codebases[current.ID] = current
	manager.conversationJobs[job.ID] = payload
	if err := manager.saveLocked(); err != nil {
		delete(manager.conversationJobs, job.ID)
		manager.codebases[original.ID] = original
		return model.Job{}, err
	}
	// Pair the record write with one observer signal so no saveLocked path skips
	// invalidation; for a document collection it is a no-op delete.
	manager.observer.Invalidate(current.ID)
	if err := manager.appendJobLocked("conversation_ingest", job); err != nil {
		delete(manager.conversationJobs, job.ID)
		manager.codebases[original.ID] = original
		return model.Job{}, err
	}
	return job, nil
}

// enqueueCodeSyncJobLocked writes the registry mutations for a fresh code sync
// job from a coalesced pending request and returns it queued. It mirrors
// SyncIndex's post-admission body so a drained code job runs the same path a
// first-time sync does. On a persistence error it rolls back the codebase
// mutation. Caller holds manager.mu and runs runJobAsync after unlocking.
func (manager *Manager) enqueueCodeSyncJobLocked(current model.Codebase, request pendingCodeRequest) (model.Job, error) {
	original := current
	now := clock.Now()
	resolvedCodebase, effectivePolicy, err := manager.resolveIndexPolicyLocked(current, indexPolicyIntent{
		Patch:      request.policyPatch,
		Initialize: false,
	})
	if err != nil {
		return model.Job{}, err
	}
	current = resolvedCodebase
	current.Status = model.CodebaseStatusIndexing
	current.EffectiveConfig = request.indexConfig
	if manager.semantic != nil && manager.semantic.Available() {
		current.CollectionName = manager.semantic.CollectionName(request.canonicalPath)
	}
	if info, ok := gitworktree.Resolve(request.canonicalPath); ok && info.Linked {
		current.WorktreeCommonDir = info.CommonDir
	}
	current.UpdatedAt = now
	job := newQueuedJob(current.ID, request.requestedPath, request.canonicalPath, request.client, string(jobOperationSync), request.force, request.indexConfig, emptyAdmissionBudget, now)
	applyJobSchedulingPolicy(&job, effectivePolicy, request.policyPatch, manager.nextQueueSequenceLocked())
	current.ActiveJobID = job.ID
	manager.codebases[current.ID] = current
	if err := manager.saveLocked(); err != nil {
		manager.codebases[original.ID] = original
		return model.Job{}, err
	}
	if err := manager.appendJobLocked("start_sync", job); err != nil {
		manager.codebases[original.ID] = original
		return model.Job{}, err
	}
	// The re-enriched EffectiveConfig may carry new custom ignore patterns, so
	// signal the observer to invalidate; the next decision rebuilds from the
	// registry source of truth.
	manager.observer.Invalidate(current.ID)
	return job, nil
}

// drainPendingJobLocked queues the coalesced pending work for a codebase whose
// active job just cleared ActiveJobID on a terminal transition. Depth-1: it
// removes the single pending entry and enqueues it as a fresh job that runs after
// this transition, so it reads the finished checkpoint. It returns the queued job
// id and true when it drained work; the caller runs runDrainedJob after releasing
// manager.mu. Caller holds manager.mu and has already set ActiveJobID = "".
func (manager *Manager) drainPendingJobLocked(ctx context.Context, codebaseID string) (string, bool) {
	current, found := manager.codebases[codebaseID]
	if !found {
		// The codebase is gone, so drop any stale pending work rather than resurrect
		// a collection that was cleared.
		delete(manager.pendingConversationJobs, codebaseID)
		delete(manager.pendingCodeJobs, codebaseID)
		return "", false
	}
	if current.ActiveJobID != "" {
		// A successor is already active for this codebase, so the depth-1 slot waits
		// for the next terminal transition rather than double-queue.
		return "", false
	}

	if payload, ok := manager.pendingConversationJobs[codebaseID]; ok {
		delete(manager.pendingConversationJobs, codebaseID)
		job, err := manager.enqueueConversationJobLocked(current, model.ClientInfo{Name: "", PID: 0}, payload)
		if err != nil {
			slog.ErrorContext(ctx, "drain pending conversation job failed", "codebase_id", codebaseID, "err", err)
			return "", false
		}
		return job.ID, true
	}
	if request, ok := manager.pendingCodeJobs[codebaseID]; ok {
		delete(manager.pendingCodeJobs, codebaseID)
		job, err := manager.enqueueCodeSyncJobLocked(current, request)
		if err != nil {
			slog.ErrorContext(ctx, "drain pending code job failed", "codebase_id", codebaseID, "err", err)
			return "", false
		}
		return job.ID, true
	}
	return "", false
}

// runDrainedJob starts a drained successor job on the shared async path, attaching
// its identity to the span context the same way the admission sites do. Caller has
// released manager.mu.
func (manager *Manager) runDrainedJob(ctx context.Context, codebaseID string, jobID string) {
	ctx = spans.Attach(
		ctx,
		correlation.IdentityAttribute{Key: "job_id", Value: jobID},
		correlation.IdentityAttribute{Key: "codebase_id", Value: codebaseID},
	)
	manager.runJobAsync(ctx, jobID)
}
