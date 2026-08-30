package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/model"
)

func TestSchedulingPolicyRegistryAndJobJournalRoundTrip(t *testing.T) {
	stateRoot := t.TempDir()
	registryPath := filepath.Join(stateRoot, "registry.json")
	jobsPath := filepath.Join(stateRoot, "jobs.jsonl")
	priority := model.JobPriorityHigh
	quiet := true
	idleAfterSeconds := int32(600)
	policy := model.SchedulingPolicy{
		Priority:         priority,
		Quiet:            quiet,
		IdleAfterSeconds: idleAfterSeconds,
	}
	codebase := model.Codebase{
		ID:                          "codebase-1",
		CanonicalPath:               "/tmp/codebase",
		SchedulingPolicy:            policy,
		PolicyPendingInitialization: true,
	}
	if err := WriteRegistry(registryPath, model.RegistryFile{Codebases: []model.Codebase{codebase}}); err != nil {
		t.Fatalf("WriteRegistry returned error: %v", err)
	}
	registry, err := ReadRegistry(registryPath)
	if err != nil {
		t.Fatalf("ReadRegistry returned error: %v", err)
	}
	if !reflect.DeepEqual(registry.Codebases, []model.Codebase{codebase}) {
		t.Fatalf("registry codebases = %#v, want %#v", registry.Codebases, []model.Codebase{codebase})
	}
	job := model.Job{
		ID:                        "job-1",
		EffectiveSchedulingPolicy: policy,
		SchedulingOverride: model.SchedulingPolicyPatch{
			Priority:         &priority,
			Quiet:            &quiet,
			IdleAfterSeconds: &idleAfterSeconds,
		},
		QueueSequence:    7,
		SchedulingReason: "operator_request",
	}
	if err := AppendJobEvent(jobsPath, model.JobEvent{Event: "job_queued", Job: job}); err != nil {
		t.Fatalf("AppendJobEvent returned error: %v", err)
	}
	jobs, err := ReadJobEvents(jobsPath)
	if err != nil {
		t.Fatalf("ReadJobEvents returned error: %v", err)
	}
	if !reflect.DeepEqual(jobs["job-1"], job) {
		t.Fatalf("job = %#v, want %#v", jobs["job-1"], job)
	}
}

func TestAppendJobEventSyncSyncsEncodedEventBeforeReturning(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	event := model.JobEvent{Event: "job_paused", Job: model.Job{ID: "job-sync"}}

	originalSync := syncFile
	syncPaths := make([]string, 0, 2)
	syncFile = func(file *os.File) error {
		syncPaths = append(syncPaths, file.Name())
		if file.Name() == filepath.Dir(journalPath) {
			return originalSync(file)
		}
		data, err := os.ReadFile(file.Name())
		if err != nil {
			t.Fatalf("ReadFile during sync returned error: %v", err)
		}
		var got model.JobEvent
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("Unmarshal encoded event during sync returned error: %v", err)
		}
		if !reflect.DeepEqual(got, event) {
			t.Fatalf("synced event = %#v, want %#v", got, event)
		}
		return nil
	}
	t.Cleanup(func() { syncFile = originalSync })

	if err := AppendJobEventSync(journalPath, event); err != nil {
		t.Fatalf("AppendJobEventSync returned error: %v", err)
	}
	if len(syncPaths) != 2 {
		t.Fatalf("sync calls = %d, want 2", len(syncPaths))
	}
	if syncPaths[0] != journalPath {
		t.Fatalf("journal sync path = %q, want %q", syncPaths[0], journalPath)
	}
	if syncPaths[1] != filepath.Dir(journalPath) {
		t.Fatalf("directory sync path = %q, want %q", syncPaths[1], filepath.Dir(journalPath))
	}
}

func TestAppendJobEventSyncReturnsSyncFailure(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	syncErr := errors.New("injected sync failure")
	originalSync := syncFile
	syncFile = func(*os.File) error { return syncErr }
	t.Cleanup(func() { syncFile = originalSync })

	err := AppendJobEventSync(
		journalPath,
		model.JobEvent{Event: "job_paused", Job: model.Job{ID: "job-sync-failure"}},
	)
	if !errors.Is(err, syncErr) {
		t.Fatalf("AppendJobEventSync error = %v, want injected sync failure", err)
	}
	if !strings.Contains(err.Error(), journalPath) {
		t.Fatalf("AppendJobEventSync error = %q, want journal path %q", err, journalPath)
	}
}

func TestReadJobEventsLatestReturnsLastEventPerJobID(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	occurredAt := []time.Time{
		time.Date(2026, time.July, 30, 8, 0, 0, 0, time.UTC),
		time.Date(2026, time.July, 30, 8, 1, 0, 0, time.UTC),
		time.Date(2026, time.July, 30, 8, 2, 0, 0, time.UTC),
	}
	events := []model.JobEvent{
		{Event: "job_queued", OccurredAt: occurredAt[0], Job: model.Job{ID: "job-1"}},
		{Event: "job_running", OccurredAt: occurredAt[1], Job: model.Job{ID: "job-1"}},
		{Event: "job_completed", OccurredAt: occurredAt[2], Job: model.Job{ID: "job-1"}},
	}
	for _, event := range events {
		if err := AppendJobEvent(journalPath, event); err != nil {
			t.Fatalf("AppendJobEvent returned error: %v", err)
		}
	}

	latest, err := ReadJobEventsLatest(journalPath)
	if err != nil {
		t.Fatalf("ReadJobEventsLatest returned error: %v", err)
	}
	got := latest["job-1"]
	if got.Event != "job_completed" {
		t.Fatalf("latest event = %q, want %q", got.Event, "job_completed")
	}
	if !got.OccurredAt.Equal(occurredAt[2]) {
		t.Fatalf("latest OccurredAt = %v, want %v", got.OccurredAt, occurredAt[2])
	}
}

func TestWriteJobEventsWritesOneLinePerEventAndRoundTrips(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	events := []model.JobEvent{
		{
			Event:      "job_running",
			OccurredAt: time.Date(2026, time.July, 30, 8, 0, 0, 0, time.UTC),
			Job:        model.Job{ID: "job-1", State: model.JobStateRunning},
		},
		{
			Event:      "job_completed",
			OccurredAt: time.Date(2026, time.July, 30, 8, 1, 0, 0, time.UTC),
			Job:        model.Job{ID: "job-2", State: model.JobStateCompleted},
		},
	}
	if err := WriteJobEvents(journalPath, events); err != nil {
		t.Fatalf("WriteJobEvents returned error: %v", err)
	}

	data, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != len(events) {
		t.Fatalf("journal line count = %d, want %d", len(lines), len(events))
	}
	for i, line := range lines {
		var got model.JobEvent
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("Unmarshal journal line %d returned error: %v", i, err)
		}
		if !reflect.DeepEqual(got, events[i]) {
			t.Fatalf("journal event %d = %#v, want %#v", i, got, events[i])
		}
	}

	latest, err := ReadJobEventsLatest(journalPath)
	if err != nil {
		t.Fatalf("ReadJobEventsLatest returned error: %v", err)
	}
	wantLatest := map[string]model.JobEvent{
		"job-1": events[0],
		"job-2": events[1],
	}
	if !reflect.DeepEqual(latest, wantLatest) {
		t.Fatalf("ReadJobEventsLatest = %#v, want %#v", latest, wantLatest)
	}
}

func TestWriteJobEventsSyncsFileBeforeRenameAndDirectoryAfter(t *testing.T) {
	journalDir := t.TempDir()
	journalPath := filepath.Join(journalDir, "jobs.jsonl")
	original := []byte("original journal\n")
	if err := os.WriteFile(journalPath, original, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	originalSync := syncFile
	syncPaths := make([]string, 0, 2)
	syncFile = func(file *os.File) error {
		syncPaths = append(syncPaths, file.Name())
		if len(syncPaths) == 1 {
			current, err := os.ReadFile(journalPath)
			if err != nil {
				t.Fatalf("ReadFile during temporary file sync returned error: %v", err)
			}
			if !bytes.Equal(current, original) {
				t.Fatalf("journal was replaced before temporary file sync: %q", current)
			}
		}
		return originalSync(file)
	}
	t.Cleanup(func() { syncFile = originalSync })

	events := []model.JobEvent{{
		Event: "job_completed",
		Job:   model.Job{ID: "job-1", State: model.JobStateCompleted},
	}}
	if err := WriteJobEvents(journalPath, events); err != nil {
		t.Fatalf("WriteJobEvents returned error: %v", err)
	}

	if len(syncPaths) != 2 {
		t.Fatalf("sync calls = %d, want 2", len(syncPaths))
	}
	if filepath.Dir(syncPaths[0]) != journalDir {
		t.Fatalf("temporary file sync path = %q, want directory %q", syncPaths[0], journalDir)
	}
	if syncPaths[1] != journalDir {
		t.Fatalf("directory sync path = %q, want %q", syncPaths[1], journalDir)
	}
}

func TestWriteJobEventsPreservesOriginalWhenTempSyncFails(t *testing.T) {
	journalDir := t.TempDir()
	journalPath := filepath.Join(journalDir, "jobs.jsonl")
	original := []byte("original journal\n")
	if err := os.WriteFile(journalPath, original, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	originalSync := syncFile
	syncErr := errors.New("injected sync failure")
	syncFile = func(*os.File) error { return syncErr }
	t.Cleanup(func() { syncFile = originalSync })

	writeErr := WriteJobEvents(
		journalPath,
		[]model.JobEvent{{Event: "job_completed", Job: model.Job{ID: "job-1"}}},
	)
	if !errors.Is(writeErr, syncErr) {
		t.Fatalf("WriteJobEvents error = %v, want injected sync failure", writeErr)
	}
	after, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatalf("ReadFile after sync failure returned error: %v", err)
	}
	if !bytes.Equal(after, original) {
		t.Fatalf("journal after sync failure = %q, want %q", after, original)
	}
}

func TestWriteJobEventsPreservesOriginalFileWhenWriteFails(t *testing.T) {
	journalDir := t.TempDir()
	journalPath := filepath.Join(journalDir, "jobs.jsonl")
	original := []byte("original journal\n")
	if err := os.WriteFile(journalPath, original, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	before, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatalf("ReadFile before failure returned error: %v", err)
	}

	if err := os.Chmod(journalDir, 0o555); err != nil {
		t.Fatalf("Chmod read-only returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(journalDir, 0o755); err != nil {
			t.Errorf("restore directory permissions: %v", err)
		}
	})
	writeErr := WriteJobEvents(
		journalPath,
		[]model.JobEvent{{Event: "job_completed", Job: model.Job{ID: "job-1"}}},
	)
	if err := os.Chmod(journalDir, 0o755); err != nil {
		t.Fatalf("restore directory permissions: %v", err)
	}
	if writeErr == nil {
		t.Fatal("WriteJobEvents returned nil, want an error")
	}

	after, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatalf("ReadFile after failure returned error: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("journal after failed write = %q, want %q", after, before)
	}
}

func TestWriteJobEventsCleansTempFileWhenEncodingFails(t *testing.T) {
	journalDir := t.TempDir()
	journalPath := filepath.Join(journalDir, "jobs.jsonl")
	original := []byte("original journal\n")
	if err := os.WriteFile(journalPath, original, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	invalidTime := time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
	writeErr := WriteJobEvents(journalPath, []model.JobEvent{{
		Event:      "job_running",
		OccurredAt: invalidTime,
		Job: model.Job{
			ID:        "job-1",
			State:     model.JobStateRunning,
			UpdatedAt: invalidTime,
		},
	}})
	if writeErr == nil {
		t.Fatal("WriteJobEvents returned nil, want an encoding error")
	}

	after, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatalf("ReadFile after failure returned error: %v", err)
	}
	if !bytes.Equal(after, original) {
		t.Fatalf("journal after failed write = %q, want %q", after, original)
	}
	entries, err := os.ReadDir(journalDir)
	if err != nil {
		t.Fatalf("ReadDir returned error: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".tmp-") {
			t.Fatalf("temporary journal %q was not removed", entry.Name())
		}
	}
}

func TestReadJobEventsProjectsLatestJobs(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	events := []model.JobEvent{
		{Event: "job_queued", Job: model.Job{ID: "job-1", State: model.JobStateQueued}},
		{Event: "job_running", Job: model.Job{ID: "job-1", State: model.JobStateRunning}},
		{Event: "job_completed", Job: model.Job{ID: "job-2", State: model.JobStateCompleted}},
	}
	for _, event := range events {
		if err := AppendJobEvent(journalPath, event); err != nil {
			t.Fatalf("AppendJobEvent returned error: %v", err)
		}
	}

	jobs, err := ReadJobEvents(journalPath)
	if err != nil {
		t.Fatalf("ReadJobEvents returned error: %v", err)
	}
	want := map[string]model.Job{
		"job-1": events[1].Job,
		"job-2": events[2].Job,
	}
	if !reflect.DeepEqual(jobs, want) {
		t.Fatalf("ReadJobEvents = %#v, want %#v", jobs, want)
	}
}
