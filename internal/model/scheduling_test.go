package model

import "testing"

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

func TestApplySchedulingPolicyPatchRejectsInvalidExplicitValues(t *testing.T) {
	testCases := []struct {
		name  string
		patch SchedulingPolicyPatch
		want  string
	}{
		{
			name:  "empty priority",
			patch: SchedulingPolicyPatch{Priority: jobPriorityPointer("")},
			want:  "priority must be high, normal, or low",
		},
		{
			name:  "zero idle after",
			patch: SchedulingPolicyPatch{IdleAfterSeconds: int32Pointer(0)},
			want:  "idle after seconds must be positive",
		},
		{
			name:  "negative idle after",
			patch: SchedulingPolicyPatch{IdleAfterSeconds: int32Pointer(-1)},
			want:  "idle after seconds must be positive",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := ApplySchedulingPolicyPatch(DefaultSchedulingPolicy(), testCase.patch)
			if err == nil || err.Error() != testCase.want {
				t.Fatalf("ApplySchedulingPolicyPatch error = %v, want %q", err, testCase.want)
			}
		})
	}
}

func jobPriorityPointer(priority JobPriority) *JobPriority {
	return &priority
}

func int32Pointer(value int32) *int32 {
	return &value
}
