package pbconv

import (
	"strings"
	"testing"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/model"
)

func TestFromSchedulingPolicyPatchPreservesOmissionAndExplicitFalse(t *testing.T) {
	t.Parallel()

	omitted, err := FromSchedulingPolicyPatch(nil)
	if err != nil {
		t.Fatalf("FromSchedulingPolicyPatch(nil): %v", err)
	}
	if omitted.Priority != nil || omitted.Quiet != nil || omitted.IdleAfterSeconds != nil {
		t.Fatalf("omitted patch = %+v, want every field nil", omitted)
	}

	quiet := false
	converted, err := FromSchedulingPolicyPatch(&pb.SchedulingPolicyPatch{Quiet: &quiet})
	if err != nil {
		t.Fatalf("FromSchedulingPolicyPatch(false): %v", err)
	}
	if converted.Quiet == nil || *converted.Quiet {
		t.Fatalf("quiet = %v, want explicit false", converted.Quiet)
	}
	if converted.Priority != nil || converted.IdleAfterSeconds != nil {
		t.Fatalf("unprovided fields changed: %+v", converted)
	}
}

func TestFromSchedulingPolicyPatchRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		patch   *pb.SchedulingPolicyPatch
		message string
	}{
		{
			name: "unspecified priority",
			patch: &pb.SchedulingPolicyPatch{
				Priority: pb.SchedulingPriority_SCHEDULING_PRIORITY_UNSPECIFIED.Enum(),
			},
			message: "priority",
		},
		{
			name: "unknown priority",
			patch: &pb.SchedulingPolicyPatch{
				Priority: pb.SchedulingPriority(99).Enum(),
			},
			message: "priority",
		},
		{
			name: "zero idle duration",
			patch: &pb.SchedulingPolicyPatch{
				IdleAfterSeconds: new(int32),
			},
			message: "idle after seconds",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := FromSchedulingPolicyPatch(test.patch)
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("error = %v, want text %q", err, test.message)
			}
		})
	}
}

func TestSchedulingPolicyViewsCarryStoredAndEffectiveValues(t *testing.T) {
	t.Parallel()

	stored := model.SchedulingPolicy{
		Priority:         model.JobPriorityHigh,
		Quiet:            true,
		IdleAfterSeconds: 900,
	}
	codebase := ToCodebase(model.Codebase{
		SchedulingPolicy:            stored,
		PolicyPendingInitialization: true,
	})
	if codebase.GetSchedulingPolicy().GetPriority() != pb.SchedulingPriority_SCHEDULING_PRIORITY_HIGH ||
		!codebase.GetSchedulingPolicy().GetQuiet() ||
		codebase.GetSchedulingPolicy().GetIdleAfterSeconds() != 900 ||
		!codebase.GetPolicyPendingInitialization() {
		t.Fatalf("codebase policy view = %+v, pending=%v", codebase.GetSchedulingPolicy(), codebase.GetPolicyPendingInitialization())
	}

	job := ToJob(model.Job{
		State: model.JobStateQueued,
		EffectiveSchedulingPolicy: model.SchedulingPolicy{
			Priority:         model.JobPriorityLow,
			Quiet:            false,
			IdleAfterSeconds: 120,
		},
		QueueSequence:    42,
		SchedulingReason: model.SchedulingReasonUserActive,
	})
	if job.GetEffectiveSchedulingPolicy().GetPriority() != pb.SchedulingPriority_SCHEDULING_PRIORITY_LOW ||
		job.GetEffectiveSchedulingPolicy().GetQuiet() ||
		job.GetEffectiveSchedulingPolicy().GetIdleAfterSeconds() != 120 ||
		job.GetQueueSequence() != 42 ||
		job.GetSchedulingReason() != pb.SchedulingReason_SCHEDULING_REASON_USER_ACTIVE {
		t.Fatalf("job policy view = %+v sequence=%d reason=%v", job.GetEffectiveSchedulingPolicy(), job.GetQueueSequence(), job.GetSchedulingReason())
	}
}

func TestSchedulingReasonProtoConversionIsClosed(t *testing.T) {
	t.Parallel()

	testCases := map[model.SchedulingReason]pb.SchedulingReason{
		model.SchedulingReasonUnspecified:         pb.SchedulingReason_SCHEDULING_REASON_UNSPECIFIED,
		model.SchedulingReasonHigherPriorityWork:  pb.SchedulingReason_SCHEDULING_REASON_HIGHER_PRIORITY_WORK,
		model.SchedulingReasonUserActive:          pb.SchedulingReason_SCHEDULING_REASON_USER_ACTIVE,
		model.SchedulingReasonActivityUnavailable: pb.SchedulingReason_SCHEDULING_REASON_ACTIVITY_UNAVAILABLE,
		model.SchedulingReasonThermalSafety:       pb.SchedulingReason_SCHEDULING_REASON_THERMAL_SAFETY,
	}
	for modelReason, protoReason := range testCases {
		if got := schedulingReasonToProto(modelReason); got != protoReason {
			t.Errorf("reason %q converts to %v, want %v", modelReason, got, protoReason)
		}
		if got := schedulingReasonFromProto(protoReason); got != modelReason {
			t.Errorf("reason %v converts to %q, want %q", protoReason, got, modelReason)
		}
	}
	if got := schedulingReasonToProto(model.SchedulingReason("free-form")); got != pb.SchedulingReason_SCHEDULING_REASON_UNSPECIFIED {
		t.Fatalf("free-form model reason converts to %v, want unspecified", got)
	}
	if got := schedulingReasonFromProto(pb.SchedulingReason(99)); got != model.SchedulingReasonUnspecified {
		t.Fatalf("unknown proto reason converts to %q, want unspecified", got)
	}
}

func TestToProgressBackfillsLegacyChunkCounters(t *testing.T) {
	t.Parallel()

	progress := ToProgress(model.Progress{
		ChunksGenerated: 7,
		ChunksReused:    3,
	})

	if got := progress.GetChunksEmbedded(); got != 7 {
		t.Fatalf("ChunksEmbedded = %d, want legacy ChunksGenerated value 7", got)
	}
	if got := progress.GetChunksProcessed(); got != 10 {
		t.Fatalf("ChunksProcessed = %d, want legacy generated + reused total 10", got)
	}
	if got := progress.GetChunksGenerated(); got != 7 {
		t.Fatalf("ChunksGenerated = %d, want legacy value 7", got)
	}
	if got := progress.GetChunksReused(); got != 3 {
		t.Fatalf("ChunksReused = %d, want 3", got)
	}
}

func TestToProgressPreservesExplicitChunkCounters(t *testing.T) {
	t.Parallel()

	progress := ToProgress(model.Progress{
		ChunksProcessed: 5,
		ChunksEmbedded:  2,
		ChunksGenerated: 7,
		ChunksReused:    3,
		ChunksDropped:   4,
	})

	if got := progress.GetChunksEmbedded(); got != 2 {
		t.Fatalf("ChunksEmbedded = %d, want explicit value 2", got)
	}
	if got := progress.GetChunksProcessed(); got != 5 {
		t.Fatalf("ChunksProcessed = %d, want explicit value 5", got)
	}
	if got := progress.GetChunksGenerated(); got != 7 {
		t.Fatalf("ChunksGenerated = %d, want legacy value 7", got)
	}
	if got := progress.GetChunksReused(); got != 3 {
		t.Fatalf("ChunksReused = %d, want 3", got)
	}
	if got := progress.GetChunksDropped(); got != 4 {
		t.Fatalf("ChunksDropped = %d, want 4", got)
	}
}
