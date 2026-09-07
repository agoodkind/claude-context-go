package platformactivity

import (
	"context"
	"testing"

	"goodkind.io/lm-semantic-search/internal/model"
)

func TestActivitySnapshotUnavailableSource(t *testing.T) {
	reason := model.SchedulingReasonActivityUnavailable
	source := NewUnavailable(reason)
	snapshot := source.Sample(context.Background())

	if snapshot.InputAvailable {
		t.Fatal("InputAvailable = true, want false")
	}
	if snapshot.InputReason != reason {
		t.Fatalf(
			"InputReason = %q, want activity unavailable",
			snapshot.InputReason,
		)
	}
	if snapshot.ThermalAvailable {
		t.Fatal("ThermalAvailable = true, want false")
	}
	if snapshot.ThermalReason != reason {
		t.Fatalf(
			"ThermalReason = %q, want activity unavailable",
			snapshot.ThermalReason,
		)
	}
	source.Close()
}

func TestNewReturnsSource(t *testing.T) {
	source := New(context.Background())
	if source == nil {
		t.Fatal("New returned nil source")
	}
	t.Cleanup(source.Close)
}
