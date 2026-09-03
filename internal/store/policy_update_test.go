package store

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"goodkind.io/lm-semantic-search/internal/model"
)

func TestPolicyUpdateTransactionRoundTripsAndRemovesDurably(t *testing.T) {
	registryPath := filepath.Join(t.TempDir(), "registry.json")
	markerPath := PolicyUpdatePath(registryPath)
	oldJob := model.Job{ID: "job-1", CodebaseID: "codebase-1"}
	oldWatcherJob := model.Job{ID: "job-watcher", CodebaseID: "codebase-1"}
	want := model.PolicyUpdateTransaction{
		CodebaseID: "codebase-1",
		OldCodebase: model.Codebase{
			ID:               "codebase-1",
			SchedulingPolicy: model.DefaultSchedulingPolicy(),
		},
		OldActiveJob:    &oldJob,
		OldDetachedJobs: []model.Job{oldWatcherJob},
	}

	if err := WritePolicyUpdate(markerPath, want); err != nil {
		t.Fatalf("WritePolicyUpdate: %v", err)
	}
	got, err := ReadPolicyUpdate(markerPath)
	if err != nil {
		t.Fatalf("ReadPolicyUpdate: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadPolicyUpdate = %+v, want %+v", got, want)
	}

	originalSync := syncFile
	syncPaths := make([]string, 0, 1)
	syncFile = func(file *os.File) error {
		syncPaths = append(syncPaths, file.Name())
		return originalSync(file)
	}
	t.Cleanup(func() { syncFile = originalSync })
	if err := RemovePolicyUpdate(markerPath); err != nil {
		t.Fatalf("RemovePolicyUpdate: %v", err)
	}
	if len(syncPaths) != 1 || syncPaths[0] != filepath.Dir(markerPath) {
		t.Fatalf("remove sync paths = %v, want [%s]", syncPaths, filepath.Dir(markerPath))
	}
	_, err = ReadPolicyUpdate(markerPath)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadPolicyUpdate after remove = %v, want file not found", err)
	}
}

func TestWritePolicyUpdateReportsMarkerAfterDirectorySyncFailure(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "policy-update.json")
	transaction := model.PolicyUpdateTransaction{
		CodebaseID: "codebase-sync-failure",
		OldCodebase: model.Codebase{
			ID:               "codebase-sync-failure",
			SchedulingPolicy: model.DefaultSchedulingPolicy(),
		},
		OldActiveJob:    nil,
		OldDetachedJobs: nil,
	}
	originalSync := syncFile
	syncErr := errors.New("injected directory sync failure")
	syncCount := 0
	syncFile = func(file *os.File) error {
		syncCount++
		if syncCount == 2 {
			return syncErr
		}
		return originalSync(file)
	}
	t.Cleanup(func() { syncFile = originalSync })

	err := WritePolicyUpdate(markerPath, transaction)
	if !errors.Is(err, syncErr) ||
		!errors.Is(err, ErrPolicyUpdateMarkerMayExist) {
		t.Fatalf(
			"WritePolicyUpdate error = %v, want sync failure and committed marker",
			err,
		)
	}
	got, readErr := ReadPolicyUpdate(markerPath)
	if readErr != nil {
		t.Fatalf("ReadPolicyUpdate after sync failure: %v", readErr)
	}
	if !reflect.DeepEqual(got, transaction) {
		t.Fatalf("marker after sync failure = %+v, want %+v", got, transaction)
	}
}
