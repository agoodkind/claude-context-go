package platformactivity

import (
	"context"
	"testing"
)

func TestActivitySnapshotUnavailableSource(t *testing.T) {
	source := NewUnavailable("platform activity source not installed")
	snapshot := source.Sample(context.Background())

	if snapshot.InputAvailable {
		t.Fatal("InputAvailable = true, want false")
	}
	if snapshot.InputReason != "platform activity source not installed" {
		t.Fatalf(
			"InputReason = %q, want platform activity source not installed",
			snapshot.InputReason,
		)
	}
	if snapshot.ThermalAvailable {
		t.Fatal("ThermalAvailable = true, want false")
	}
	if snapshot.ThermalReason != "platform activity source not installed" {
		t.Fatalf(
			"ThermalReason = %q, want platform activity source not installed",
			snapshot.ThermalReason,
		)
	}
	source.Close()
}

func TestNewUsesUnavailableFallback(t *testing.T) {
	source := New()
	t.Cleanup(source.Close)
	snapshot := source.Sample(context.Background())

	if snapshot.InputReason != unavailableSourceReason ||
		snapshot.ThermalReason != unavailableSourceReason {
		t.Fatalf("fallback reasons = input %q thermal %q", snapshot.InputReason, snapshot.ThermalReason)
	}
}
