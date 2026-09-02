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

func TestNewReturnsSource(t *testing.T) {
	source := New()
	if source == nil {
		t.Fatal("New returned nil source")
	}
	t.Cleanup(source.Close)
}
