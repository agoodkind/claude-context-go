package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/store"
)

func TestCompactJobJournalKeepsOneLatestEventPerRetainedJob(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	baseTime := time.Date(2026, time.July, 30, 8, 0, 0, 0, time.UTC)
	events := []model.JobEvent{
		{
			Event:      "job_queued",
			OccurredAt: baseTime,
			Job: model.Job{
				ID:         "job-1",
				CodebaseID: "codebase-1",
				State:      model.JobStateQueued,
				UpdatedAt:  baseTime,
			},
		},
		{
			Event:      "job_completed",
			OccurredAt: baseTime.Add(time.Minute),
			Job: terminalJournalJob(
				"job-1",
				"codebase-1",
				model.JobStateCompleted,
				baseTime.Add(time.Minute),
			),
		},
		{
			Event:      "job_failed",
			OccurredAt: baseTime.Add(2 * time.Minute),
			Job: terminalJournalJob(
				"job-2",
				"codebase-1",
				model.JobStateFailed,
				baseTime.Add(2*time.Minute),
			),
		},
	}
	appendJournalEvents(t, journalPath, events)
	beforeJobs := readJournalJobs(t, journalPath)
	beforeInfo := journalFileInfo(t, journalPath)

	kept, dropped, err := compactJobJournal(journalPath)
	if err != nil {
		t.Fatalf("compactJobJournal returned error: %v", err)
	}
	if kept != 2 {
		t.Fatalf("kept = %d, want 2", kept)
	}
	if dropped != 0 {
		t.Fatalf("dropped = %d, want 0", dropped)
	}

	afterJobs := readJournalJobs(t, journalPath)
	if !reflect.DeepEqual(afterJobs, beforeJobs) {
		t.Fatalf("jobs after compaction = %#v, want %#v", afterJobs, beforeJobs)
	}
	if got := journalLineCount(t, journalPath); got != len(afterJobs) {
		t.Fatalf("journal line count = %d, want %d", got, len(afterJobs))
	}
	afterInfo := journalFileInfo(t, journalPath)
	if afterInfo.Size() >= beforeInfo.Size() {
		t.Fatalf(
			"journal size after compaction = %d, want less than %d",
			afterInfo.Size(),
			beforeInfo.Size(),
		)
	}
}

func TestCompactJobJournalKeepsOldNonTerminalJob(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	baseTime := time.Date(2026, time.July, 1, 8, 0, 0, 0, time.UTC)
	events := []model.JobEvent{
		{
			Event:      "job_running",
			OccurredAt: baseTime,
			Job: model.Job{
				ID:         "old-running",
				CodebaseID: "codebase-1",
				State:      model.JobStateRunning,
				UpdatedAt:  baseTime,
			},
		},
		{
			Event:      "job_completed",
			OccurredAt: baseTime,
			Job: terminalJournalJob(
				"old-completed",
				"codebase-1",
				model.JobStateCompleted,
				baseTime,
			),
		},
	}
	for i := range 5 {
		completedAt := baseTime.Add(time.Duration(i+1) * time.Hour)
		events = append(events, model.JobEvent{
			Event:      "job_completed",
			OccurredAt: completedAt,
			Job: terminalJournalJob(
				fmt.Sprintf("new-completed-%d", i),
				"codebase-1",
				model.JobStateCompleted,
				completedAt,
			),
		})
	}
	appendJournalEvents(t, journalPath, events)

	if _, _, err := compactJobJournal(journalPath); err != nil {
		t.Fatalf("compactJobJournal returned error: %v", err)
	}
	jobs := readJournalJobs(t, journalPath)
	if _, found := jobs["old-running"]; !found {
		t.Fatal("old non-terminal job was dropped")
	}
	if _, found := jobs["old-completed"]; found {
		t.Fatal("old terminal job was retained")
	}
}

func TestCompactJobJournalKeepsUnrecognisedJobState(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	updatedAt := time.Date(2026, time.July, 1, 8, 0, 0, 0, time.UTC)
	appendJournalEvents(t, journalPath, []model.JobEvent{{
		Event:      "job_future",
		OccurredAt: updatedAt,
		Job: model.Job{
			ID:         "future-job",
			CodebaseID: "codebase-1",
			State:      model.JobState("future"),
			UpdatedAt:  updatedAt,
		},
	}})

	if _, _, err := compactJobJournal(journalPath); err != nil {
		t.Fatalf("compactJobJournal returned error: %v", err)
	}
	jobs := readJournalJobs(t, journalPath)
	if _, found := jobs["future-job"]; !found {
		t.Fatal("unrecognised job state was dropped")
	}
}

func TestJobJournalTerminalCapNeverDropsNonTerminalJob(t *testing.T) {
	baseTime := time.Date(2026, time.July, 1, 8, 0, 0, 0, time.UTC)
	latest := make(
		map[string]model.JobEvent,
		jobJournalRetainedTerminalTotal+6,
	)
	for i := range jobJournalRetainedTerminalTotal + 5 {
		completedAt := baseTime.Add(time.Duration(i) * time.Second)
		jobID := fmt.Sprintf("terminal-%04d", i)
		latest[jobID] = model.JobEvent{
			Event:      "job_completed",
			OccurredAt: completedAt,
			Job: terminalJournalJob(
				jobID,
				fmt.Sprintf(
					"codebase-%04d",
					i/jobJournalRetainedTerminalPerCodebase,
				),
				model.JobStateCompleted,
				completedAt,
			),
		}
	}
	latest["running-job"] = model.JobEvent{
		Event:      "job_running",
		OccurredAt: baseTime.Add(-time.Hour),
		Job: model.Job{
			ID:         "running-job",
			CodebaseID: "codebase-running",
			State:      model.JobStateRunning,
			UpdatedAt:  baseTime.Add(-time.Hour),
		},
	}

	retained := selectRetainedJobEvents(latest)
	terminalCount := 0
	runningFound := false
	for _, event := range retained {
		switch event.Job.State {
		case model.JobStateCompleted, model.JobStateFailed, model.JobStateCancelled:
			terminalCount++
		default:
			if event.Job.ID == "running-job" {
				runningFound = true
			}
		}
	}
	if !runningFound {
		t.Fatal("terminal cap dropped non-terminal job")
	}
	if terminalCount != jobJournalRetainedTerminalTotal {
		t.Fatalf(
			"retained terminal count = %d, want %d",
			terminalCount,
			jobJournalRetainedTerminalTotal,
		)
	}
}

func TestCompactJobJournalRetainsTerminalJobsPerCodebase(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	baseTime := time.Date(2026, time.July, 1, 8, 0, 0, 0, time.UTC)
	events := []model.JobEvent{
		{
			Event:      "job_completed",
			OccurredAt: baseTime,
			Job: terminalJournalJob(
				"quiet-1",
				"quiet",
				model.JobStateCompleted,
				baseTime,
			),
		},
		{
			Event:      "job_completed",
			OccurredAt: baseTime.Add(time.Minute),
			Job: terminalJournalJob(
				"quiet-2",
				"quiet",
				model.JobStateCompleted,
				baseTime.Add(time.Minute),
			),
		},
	}
	for i := range 50 {
		completedAt := baseTime.Add(time.Duration(i) * time.Hour)
		events = append(events, model.JobEvent{
			Event:      "job_completed",
			OccurredAt: completedAt,
			Job: terminalJournalJob(
				fmt.Sprintf("busy-%02d", i),
				"busy",
				model.JobStateCompleted,
				completedAt,
			),
		})
	}
	appendJournalEvents(t, journalPath, events)

	if _, _, err := compactJobJournal(journalPath); err != nil {
		t.Fatalf("compactJobJournal returned error: %v", err)
	}
	jobs := readJournalJobs(t, journalPath)
	for _, id := range []string{"quiet-1", "quiet-2"} {
		if _, found := jobs[id]; !found {
			t.Fatalf("quiet codebase job %q was dropped", id)
		}
	}
	wantBusyIDs := []string{"busy-45", "busy-46", "busy-47", "busy-48", "busy-49"}
	busyCount := 0
	for id := range jobs {
		if strings.HasPrefix(id, "busy-") {
			busyCount++
		}
	}
	if busyCount != len(wantBusyIDs) {
		t.Fatalf("busy job count = %d, want %d", busyCount, len(wantBusyIDs))
	}
	for _, id := range wantBusyIDs {
		if _, found := jobs[id]; !found {
			t.Fatalf("busy codebase job %q was dropped", id)
		}
	}
}

func TestJobJournalWriterWaitsForCompactionThreshold(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	baseTime := time.Date(2026, time.July, 30, 8, 0, 0, 0, time.UTC)
	initialEvent := model.JobEvent{
		Event:      "job_queued",
		OccurredAt: baseTime,
		Job: model.Job{
			ID:         "job-threshold",
			CodebaseID: "codebase-1",
			State:      model.JobStateQueued,
			UpdatedAt:  baseTime,
		},
	}
	appendJournalEvents(t, journalPath, []model.JobEvent{initialEvent})
	appendedEvent := model.JobEvent{
		Event:      "job_running",
		OccurredAt: baseTime.Add(time.Minute),
		Job: model.Job{
			ID:         "job-threshold",
			CodebaseID: "codebase-1",
			State:      model.JobStateRunning,
			UpdatedAt:  baseTime.Add(time.Minute),
		},
	}
	encoded, err := json.Marshal(appendedEvent)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	initialSize := journalFileInfo(t, journalPath).Size()
	threshold := initialSize + int64(len(encoded)) + 2

	writer := newJobJournalWriter(
		journalPath,
		store.AppendJobEvent,
		store.AppendJobEventSync,
		8,
		threshold,
	)
	if err := writer.enqueue(appendedEvent); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	writer.close()

	if got := journalLineCount(t, journalPath); got != 2 {
		t.Fatalf("journal line count = %d, want 2", got)
	}
}

func TestJobJournalWriterCompactsFirstAppendWhenSeededAboveThreshold(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	baseTime := time.Date(2026, time.July, 30, 8, 0, 0, 0, time.UTC)
	initialEvents := make([]model.JobEvent, 0, 6)
	for i := range 6 {
		updatedAt := baseTime.Add(time.Duration(i) * time.Minute)
		initialEvents = append(initialEvents, model.JobEvent{
			Event:      fmt.Sprintf("job_progress_%d", i),
			OccurredAt: updatedAt,
			Job: model.Job{
				ID:         "job-seeded",
				CodebaseID: "codebase-1",
				State:      model.JobStateRunning,
				UpdatedAt:  updatedAt,
			},
		})
	}
	appendJournalEvents(t, journalPath, initialEvents)
	beforeInfo := journalFileInfo(t, journalPath)
	threshold := beforeInfo.Size() - 1
	appendedAt := baseTime.Add(10 * time.Minute)
	appendedEvent := model.JobEvent{
		Event:      "job_completed",
		OccurredAt: appendedAt,
		Job: terminalJournalJob(
			"job-seeded",
			"codebase-1",
			model.JobStateCompleted,
			appendedAt,
		),
	}
	encoded, err := json.Marshal(appendedEvent)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	if int64(len(encoded)+1) >= threshold {
		t.Fatalf(
			"appended event size = %d, want less than threshold %d",
			len(encoded)+1,
			threshold,
		)
	}

	writer := newJobJournalWriter(
		journalPath,
		store.AppendJobEvent,
		store.AppendJobEventSync,
		8,
		threshold,
	)
	if err := writer.enqueue(appendedEvent); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	writer.close()

	if got := journalLineCount(t, journalPath); got != 1 {
		t.Fatalf("journal line count = %d, want 1", got)
	}
	afterInfo := journalFileInfo(t, journalPath)
	if afterInfo.Size() >= beforeInfo.Size() {
		t.Fatalf(
			"journal size after compaction = %d, want less than %d",
			afterInfo.Size(),
			beforeInfo.Size(),
		)
	}
	jobs := readJournalJobs(t, journalPath)
	if jobs["job-seeded"].State != model.JobStateCompleted {
		t.Fatalf(
			"job state = %q, want %q",
			jobs["job-seeded"].State,
			model.JobStateCompleted,
		)
	}
}

func TestJobJournalWriterBacksOffAfterCompactionFailure(t *testing.T) {
	const threshold = int64(1024)

	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	invalidJournal := []byte(strings.Repeat("{", int(threshold)+1))
	if err := os.WriteFile(journalPath, invalidJournal, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	var logBuffer bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	writer := newJobJournalWriter(
		journalPath,
		store.AppendJobEvent,
		store.AppendJobEventSync,
		8,
		threshold,
	)
	baseTime := time.Date(2026, time.July, 31, 8, 0, 0, 0, time.UTC)
	for i := range 2 {
		event := model.JobEvent{
			Event:      fmt.Sprintf("job_progress_%d", i),
			OccurredAt: baseTime.Add(time.Duration(i) * time.Minute),
			Job: model.Job{
				ID:         "job-retry",
				CodebaseID: "codebase-1",
				State:      model.JobStateRunning,
				UpdatedAt:  baseTime.Add(time.Duration(i) * time.Minute),
			},
		}
		if err := writer.enqueue(event); err != nil {
			t.Fatalf("enqueue event %d returned error: %v", i, err)
		}
	}
	writer.close()

	compactionAttempts := strings.Count(logBuffer.String(), "compact job journal failed")
	if compactionAttempts != 1 {
		t.Fatalf(
			"compaction attempts = %d, want 1 before journal grows by retry threshold",
			compactionAttempts,
		)
	}
}

// terminalJournalJob gives retention tests a completed timestamp independent
// from the production recency helper.
func terminalJournalJob(
	id string,
	codebaseID string,
	state model.JobState,
	completedAt time.Time,
) model.Job {
	return model.Job{
		ID:          id,
		CodebaseID:  codebaseID,
		State:       state,
		UpdatedAt:   completedAt.Add(-time.Minute),
		CompletedAt: &completedAt,
	}
}

// appendJournalEvents uses the public journal boundary so tests exercise the
// same JSONL format that compaction reads in production.
func appendJournalEvents(t *testing.T, path string, events []model.JobEvent) {
	t.Helper()
	for _, event := range events {
		if err := store.AppendJobEvent(path, event); err != nil {
			t.Fatalf("AppendJobEvent returned error: %v", err)
		}
	}
}

// readJournalJobs keeps assertions at the public replay boundary.
func readJournalJobs(t *testing.T, path string) map[string]model.Job {
	t.Helper()
	jobs, err := store.ReadJobEvents(path)
	if err != nil {
		t.Fatalf("ReadJobEvents returned error: %v", err)
	}
	return jobs
}

// journalFileInfo centralizes required file metadata checks without hiding
// their failure from the test.
func journalFileInfo(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat returned error: %v", err)
	}
	return info
}

// journalLineCount asserts that the journal remains newline-delimited JSON.
func journalLineCount(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	trimmed := strings.TrimSuffix(string(data), "\n")
	if trimmed == "" {
		return 0
	}
	return len(strings.Split(trimmed, "\n"))
}
