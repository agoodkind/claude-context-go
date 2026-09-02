//go:build darwin && cgo

package platformactivity

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestDarwinActivityReportsValidIdleAndNominalThermalState(t *testing.T) {
	stubNativeActivityReader(t, nativeActivityResult{
		idleSeconds:      125.25,
		inputAvailable:   true,
		thermalAvailable: true,
		thermalUnsafe:    false,
	})

	source := New()
	snapshot := source.Sample(context.Background())

	if !snapshot.InputAvailable {
		t.Fatal("InputAvailable = false, want true")
	}
	if snapshot.InputIdleFor != 125250*time.Millisecond {
		t.Fatalf("InputIdleFor = %s, want 2m5.25s", snapshot.InputIdleFor)
	}
	if !snapshot.ThermalAvailable {
		t.Fatal("ThermalAvailable = false, want true")
	}
	if snapshot.ThermalUnsafe {
		t.Fatal("ThermalUnsafe = true, want false")
	}
	source.Close()
}

func TestNativeActivityRejectsInvalidOrUnavailableIdle(t *testing.T) {
	testCases := map[string]nativeActivityResult{
		"unavailable": {
			idleSeconds:    30,
			inputAvailable: false,
		},
		"negative": {
			idleSeconds:    -1,
			inputAvailable: true,
		},
		"not a number": {
			idleSeconds:    math.NaN(),
			inputAvailable: true,
		},
		"positive infinity": {
			idleSeconds:    math.Inf(1),
			inputAvailable: true,
		},
		"duration overflow": {
			idleSeconds:    float64(math.MaxInt64)/float64(time.Second) + 1,
			inputAvailable: true,
		},
		"duration rounding boundary": {
			idleSeconds:    float64(math.MaxInt64) / float64(time.Second),
			inputAvailable: true,
		},
	}

	for name, result := range testCases {
		t.Run(name, func(t *testing.T) {
			stubNativeActivityReader(t, result)

			snapshot := New().Sample(context.Background())

			if snapshot.InputAvailable {
				t.Fatal("InputAvailable = true, want false")
			}
			if snapshot.InputIdleFor != 0 {
				t.Fatalf("InputIdleFor = %s, want 0", snapshot.InputIdleFor)
			}
			if snapshot.InputReason != "input activity unavailable" {
				t.Fatalf(
					"InputReason = %q, want input activity unavailable",
					snapshot.InputReason,
				)
			}
		})
	}
}

func TestDarwinActivityReportsSeriousAndCriticalThermalStatesUnsafe(t *testing.T) {
	for _, state := range []string{"serious", "critical"} {
		t.Run(state, func(t *testing.T) {
			stubNativeActivityReader(t, nativeActivityResult{
				idleSeconds:      300,
				inputAvailable:   true,
				thermalAvailable: true,
				thermalUnsafe:    true,
			})

			snapshot := New().Sample(context.Background())

			if !snapshot.ThermalAvailable {
				t.Fatal("ThermalAvailable = false, want true")
			}
			if !snapshot.ThermalUnsafe {
				t.Fatal("ThermalUnsafe = false, want true")
			}
			if snapshot.ThermalReason != "thermal state unsafe" {
				t.Fatalf(
					"ThermalReason = %q, want thermal state unsafe",
					snapshot.ThermalReason,
				)
			}
		})
	}
}

func stubNativeActivityReader(t *testing.T, result nativeActivityResult) {
	t.Helper()
	originalReader := readNativeActivity
	readNativeActivity = func() nativeActivityResult {
		return result
	}
	t.Cleanup(func() {
		readNativeActivity = originalReader
	})
}
