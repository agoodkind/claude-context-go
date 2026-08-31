// Package pbconv converts daemon model types to generated protobuf types.
package pbconv

import (
	"time"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/view"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// FromStartIndexConfig maps a gRPC start-index request into daemon config.
func FromStartIndexConfig(request *pb.StartIndexRequest) model.IndexConfig {
	config := model.IndexConfig{
		SplitterType:       "ast",
		SplitterChunkSize:  2500,
		SplitterOverlap:    300,
		IgnorePatterns:     append([]string{}, request.GetIgnorePatterns()...),
		IncludeSubmodules:  append([]string{}, request.GetIncludeSubmodules()...),
		IgnoreDigest:       "",
		EmbeddingProvider:  "",
		EmbeddingModel:     "",
		EmbeddingDimension: 0,
		VectorBackend:      "milvus",
		Hybrid:             true,
	}
	if request.GetSplitter() != nil {
		if request.GetSplitter().GetType() != "" {
			config.SplitterType = request.GetSplitter().GetType()
		}
		if request.GetSplitter().GetChunkSize() > 0 {
			config.SplitterChunkSize = request.GetSplitter().GetChunkSize()
		}
		if request.GetSplitter().GetOverlap() > 0 {
			config.SplitterOverlap = request.GetSplitter().GetOverlap()
		}
	}
	return config
}

// FromStartIndexBudget maps request-local admission overrides outside
// IndexConfig, so budget changes do not invalidate merkle reuse. The proto
// fields are signed, so a negative value is clamped to 0 (unset) at the
// boundary, keeping the stored Job.Budget and the echoed-back record consistent
// with admission, which treats a non-positive budget as "no override".
func FromStartIndexBudget(request *pb.StartIndexRequest) model.AdmissionBudget {
	return model.AdmissionBudget{
		MaxJobChunks: max(request.GetMaxJobChunks(), 0),
		MaxJobBytes:  max(request.GetMaxJobBytes(), 0),
	}
}

// ToCodebase converts one daemon codebase record into its protobuf form.
func ToCodebase(codebase model.Codebase) *pb.Codebase {
	result := &pb.Codebase{
		Id:                    codebase.ID,
		CanonicalPath:         codebase.CanonicalPath,
		Status:                string(codebase.Status),
		ActiveJobId:           codebase.ActiveJobID,
		EffectiveConfig:       toIndexConfig(codebase.EffectiveConfig),
		CollectionName:        codebase.CollectionName,
		LegacyCollectionNames: append([]string{}, codebase.LegacyCollectionNames...),
		MerkleSnapshotPath:    codebase.MerkleSnapshotPath,
		InodeTrackingDisabled: codebase.InodeTrackingDisabled,
		UpdatedAt:             ts(codebase.UpdatedAt),
	}
	if codebase.LastSuccessfulRun != nil {
		result.LastSuccessfulRun = &pb.IndexRunSummary{
			IndexedFiles: codebase.LastSuccessfulRun.IndexedFiles,
			TotalChunks:  codebase.LastSuccessfulRun.TotalChunks,
			TotalBytes:   codebase.LastSuccessfulRun.TotalBytes,
			Status:       codebase.LastSuccessfulRun.Status,
			CompletedAt:  ts(codebase.LastSuccessfulRun.CompletedAt),
			SkippedFiles: append([]string{}, codebase.LastSuccessfulRun.SkippedFiles...),
		}
	}
	if codebase.LastFailedRun != nil {
		result.LastFailedRun = &pb.IndexRunFailure{
			Message:                 codebase.LastFailedRun.Message,
			Code:                    codebase.LastFailedRun.Code,
			LastAttemptedPercentage: codebase.LastFailedRun.LastAttemptedPercentage,
			FailedAt:                ts(codebase.LastFailedRun.FailedAt),
		}
	}
	return result
}

// ToJob converts one daemon job record into its protobuf form.
func ToJob(job model.Job) *pb.Job {
	result := &pb.Job{
		Id:            job.ID,
		CodebaseId:    job.CodebaseID,
		RequestedPath: job.RequestedPath,
		CanonicalPath: job.CanonicalPath,
		Client: &pb.ClientInfo{
			Name: job.Client.Name,
			Pid:  job.Client.PID,
		},
		Operation:   job.Operation,
		State:       string(job.State),
		Forced:      job.Forced,
		Trigger:     jobTrigger(job),
		Progress:    ToProgress(job.Progress),
		Config:      toIndexConfig(job.Config),
		StartedAt:   ts(job.StartedAt),
		UpdatedAt:   ts(job.UpdatedAt),
		CompletedAt: tsp(job.CompletedAt),
		Outcome:     jobOutcome(job.State),
	}
	if job.Error != nil {
		result.Error = &pb.JobError{
			Message:   job.Error.Message,
			Code:      job.Error.Code,
			Retryable: job.Error.Retryable,
		}
	}
	return result
}

// ToProgress converts a job's model.Progress into the proto Progress, including
// the resolved breakdown, so every response that carries progress carries the
// same structured tree the human surfaces render.
func ToProgress(p model.Progress) *pb.Progress {
	chunksEmbedded := p.ChunksEmbedded
	if chunksEmbedded == 0 {
		chunksEmbedded = p.ChunksGenerated
	}
	chunksProcessed := p.ChunksProcessed
	if chunksProcessed == 0 {
		chunksProcessed = chunksEmbedded + p.ChunksReused
	}
	// Both percents read the same counter snapshot, so they can never describe
	// two different moments of the same job.
	counts := ProgressCounts(p)
	return &pb.Progress{
		Phase:                     p.Phase,
		PhasePercent:              view.ResolvePhasePercent(counts),
		OverallPercent:            view.ResolveOverallPercent(counts, p.OverallPercent),
		Unit:                      p.Unit,
		FilesTotal:                p.FilesTotal,
		FilesProcessed:            p.FilesProcessed,
		ChunksProcessed:           chunksProcessed,
		ChunksReused:              p.ChunksReused,
		ChunksEmbedded:            chunksEmbedded,
		ChunksGenerated:           p.ChunksGenerated,
		ChunksDropped:             p.ChunksDropped,
		ReuseVectorsLoaded:        p.ReuseVectorsLoaded,
		EmbeddingBatchesTotal:     p.EmbeddingBatchesTotal,
		EmbeddingBatchesCompleted: p.EmbeddingBatchesCompleted,
		CollectionRowsWritten:     p.CollectionRowsWritten,
		LastEventAt:               ts(p.LastEventAt),
		HeartbeatAt:               ts(p.HeartbeatAt),
		Breakdown:                 BreakdownProto(p),
	}
}

// ProgressCounts maps a job's model.Progress into the resolver input. It is the
// one place the raw counters become view.ProgressCounts, shared by the daemon's
// human adapter and the wire breakdown, so the breakdown input never diverges.
func ProgressCounts(p model.Progress) view.ProgressCounts {
	return view.ProgressCounts{
		RunMode:                p.RunMode,
		Unit:                   p.Unit,
		FilesTotal:             p.FilesTotal,
		FilesProcessed:         p.FilesProcessed,
		FilesAdded:             p.FilesAdded,
		FilesModified:          p.FilesModified,
		FilesRemoved:           p.FilesRemoved,
		FilesEmbedded:          p.FilesEmbedded,
		FilesSkippedOversize:   p.FilesSkippedOversize,
		FilesSkippedUnreadable: p.FilesSkippedUnreadable,
		FilesPending:           p.FilesPending,
		ChunksTotal:            p.ChunksTotal,
		ChunksProcessed:        p.ChunksProcessed,
		ChunksReused:           p.ChunksReused,
		ChunksEmbedded:         p.ChunksEmbedded,
		ChunksGenerated:        p.ChunksGenerated,
		ReuseVectorsLoaded:     p.ReuseVectorsLoaded,

		EmbeddingBatchesTotal:     p.EmbeddingBatchesTotal,
		EmbeddingBatchesCompleted: p.EmbeddingBatchesCompleted,
	}
}

// BreakdownProto resolves a job's progress into the proto outcome breakdown, the
// wire mirror of view.ResolveBreakdown, so JSON consumers read the same tree the
// human surfaces render.
func BreakdownProto(p model.Progress) *pb.OutcomeBreakdown {
	breakdown := view.ResolveBreakdown(ProgressCounts(p))
	return &pb.OutcomeBreakdown{
		ScopeLabel:  breakdown.ScopeLabel,
		Processed:   breakdown.Processed,
		ScopeTotal:  breakdown.ScopeTotal,
		FileRows:    outcomeRowsToProto(breakdown.FileRows),
		ChunksTotal: breakdown.ChunksTotal,
		ChunkRows:   outcomeRowsToProto(breakdown.ChunkRows),
	}
}

// BreakdownFromProto rebuilds the view breakdown from its wire form so a client
// (the TUI) renders it through the same render.BreakdownLines as the daemon.
func BreakdownFromProto(breakdown *pb.OutcomeBreakdown) view.OutcomeBreakdown {
	if breakdown == nil {
		return view.ZeroBreakdown()
	}
	return view.OutcomeBreakdown{
		ScopeLabel:  breakdown.GetScopeLabel(),
		Processed:   breakdown.GetProcessed(),
		ScopeTotal:  breakdown.GetScopeTotal(),
		FileRows:    outcomeRowsFromProto(breakdown.GetFileRows()),
		ChunksTotal: breakdown.GetChunksTotal(),
		ChunkRows:   outcomeRowsFromProto(breakdown.GetChunkRows()),
	}
}

func outcomeRowsToProto(rows []view.OutcomeRow) []*pb.OutcomeRow {
	out := make([]*pb.OutcomeRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, &pb.OutcomeRow{Kind: outcomeKindToProto(row.Kind()), Count: row.Count()})
	}
	return out
}

func outcomeRowsFromProto(rows []*pb.OutcomeRow) []view.OutcomeRow {
	if len(rows) == 0 {
		return nil
	}
	out := make([]view.OutcomeRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, view.NewOutcomeRow(outcomeKindFromProto(row.GetKind()), row.GetCount()))
	}
	return out
}

func outcomeKindToProto(kind view.OutcomeKind) pb.OutcomeKind {
	switch kind {
	case view.KindEmbedded:
		return pb.OutcomeKind_OUTCOME_KIND_EMBEDDED
	case view.KindUnchanged:
		return pb.OutcomeKind_OUTCOME_KIND_UNCHANGED
	case view.KindRemoved:
		return pb.OutcomeKind_OUTCOME_KIND_REMOVED
	case view.KindPending:
		return pb.OutcomeKind_OUTCOME_KIND_PENDING
	case view.KindOversize:
		return pb.OutcomeKind_OUTCOME_KIND_OVERSIZE
	case view.KindUnreadable:
		return pb.OutcomeKind_OUTCOME_KIND_UNREADABLE
	case view.KindAdded:
		return pb.OutcomeKind_OUTCOME_KIND_ADDED
	case view.KindReused:
		return pb.OutcomeKind_OUTCOME_KIND_REUSED
	default:
		return pb.OutcomeKind_OUTCOME_KIND_UNSPECIFIED
	}
}

func outcomeKindFromProto(kind pb.OutcomeKind) view.OutcomeKind {
	switch kind {
	case pb.OutcomeKind_OUTCOME_KIND_EMBEDDED:
		return view.KindEmbedded
	case pb.OutcomeKind_OUTCOME_KIND_UNCHANGED:
		return view.KindUnchanged
	case pb.OutcomeKind_OUTCOME_KIND_REMOVED:
		return view.KindRemoved
	case pb.OutcomeKind_OUTCOME_KIND_PENDING:
		return view.KindPending
	case pb.OutcomeKind_OUTCOME_KIND_OVERSIZE:
		return view.KindOversize
	case pb.OutcomeKind_OUTCOME_KIND_UNREADABLE:
		return view.KindUnreadable
	case pb.OutcomeKind_OUTCOME_KIND_ADDED:
		return view.KindAdded
	case pb.OutcomeKind_OUTCOME_KIND_REUSED:
		return view.KindReused
	case pb.OutcomeKind_OUTCOME_KIND_UNSPECIFIED:
		return ""
	default:
		return ""
	}
}

// jobOutcome resolves the terminal result token for the wire: "succeeded",
// "failed", or "canceled", and "" while the job is live. It exists so machine
// consumers (the CLI's --wait follower in particular) never derive
// terminality from the raw state field.
func jobOutcome(state model.JobState) string {
	switch state {
	case model.JobStateCompleted:
		return "succeeded"
	case model.JobStateFailed:
		return "failed"
	case model.JobStateCancelled:
		return "canceled"
	case model.JobStateQueued, model.JobStateRunning, model.JobStatePaused, model.JobStateCancelling:
		return ""
	default:
		return ""
	}
}

// Operation values a daemon job can carry, mirrored here so the wire trigger
// token can be derived without importing the daemon package.
const (
	jobOperationIndex    = "index"
	triggerInitialBuild  = "initial_build"
	triggerForcedReindex = "forced_reindex"
	triggerChangedFiles  = "changed_files"
)

// jobTrigger derives the wire trigger token from the job's operation and force
// flag: a full build is an initial build unless forced, and any other operation
// is a changed-files run.
func jobTrigger(job model.Job) string {
	if job.Operation == jobOperationIndex {
		if job.Forced {
			return triggerForcedReindex
		}
		return triggerInitialBuild
	}
	return triggerChangedFiles
}

func toIndexConfig(config model.IndexConfig) *pb.IndexConfig {
	return &pb.IndexConfig{
		SplitterType:       config.SplitterType,
		SplitterChunkSize:  config.SplitterChunkSize,
		SplitterOverlap:    config.SplitterOverlap,
		IgnorePatterns:     append([]string{}, config.IgnorePatterns...),
		IncludeSubmodules:  append([]string{}, config.IncludeSubmodules...),
		IgnoreDigest:       config.IgnoreDigest,
		EmbeddingProvider:  config.EmbeddingProvider.String(),
		EmbeddingModel:     config.EmbeddingModel,
		EmbeddingDimension: config.EmbeddingDimension,
		VectorBackend:      config.VectorBackend.String(),
		Hybrid:             config.Hybrid,
	}
}

// ToPathClassification converts a daemon classification verdict into its
// protobuf form. A nil verdict returns nil so the response message stays
// canonical without an unspecified placeholder.
func ToPathClassification(classification *model.PathClassification) *pb.PathClassification {
	if classification == nil {
		return nil
	}
	return &pb.PathClassification{
		Kind:                pathClassificationKindToProto(classification.Kind),
		ExcludedByPattern:   classification.ExcludedByPattern,
		ExcludedByGitignore: classification.ExcludedByGitignore,
		CoveringCodebaseId:  classification.CoveringCodebaseID,
	}
}

func pathClassificationKindToProto(kind model.PathClassificationKind) pb.PathClassification_Kind {
	switch kind {
	case model.PathClassificationInScopeIndexed:
		return pb.PathClassification_KIND_IN_SCOPE_INDEXED
	case model.PathClassificationInScopeExcluded:
		return pb.PathClassification_KIND_IN_SCOPE_EXCLUDED
	case model.PathClassificationInScopeUnindexed:
		return pb.PathClassification_KIND_IN_SCOPE_UNINDEXED
	case model.PathClassificationOutOfScope:
		return pb.PathClassification_KIND_OUT_OF_SCOPE
	case model.PathClassificationUnspecified:
		return pb.PathClassification_KIND_UNSPECIFIED
	default:
		return pb.PathClassification_KIND_UNSPECIFIED
	}
}

func ts(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value)
}

func tsp(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return ts(*value)
}
