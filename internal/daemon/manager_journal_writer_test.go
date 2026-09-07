package daemon

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/store"
)

func TestStatusSnapshotDoesNotWaitForJournalWrite(t *testing.T) {
	manager, _, _ := newTestManager(t)

	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	manager.appendJobEvent = func(string, model.JobEvent) error {
		close(writeStarted)
		<-releaseWrite
		return nil
	}

	job := model.Job{ID: "job-status-read", State: model.JobStateRunning}
	appendDone := make(chan error, 1)
	go func() {
		manager.mu.Lock()
		err := manager.appendJobLocked("job_running", job)
		manager.mu.Unlock()
		appendDone <- err
	}()
	<-writeStarted

	statusDone := make(chan StatusSnapshot, 1)
	go func() {
		statusDone <- manager.StatusSnapshot()
	}()

	select {
	case <-statusDone:
		close(releaseWrite)
	case <-time.After(250 * time.Millisecond):
		close(releaseWrite)
		if err := <-appendDone; err != nil {
			t.Fatalf("appendJobLocked returned error: %v", err)
		}
		t.Fatal("StatusSnapshot blocked while journal append was in flight")
	}

	if err := <-appendDone; err != nil {
		t.Fatalf("appendJobLocked returned error: %v", err)
	}
}

func TestJobJournalWriterPreservesEventOrder(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	firstWriteStarted := make(chan struct{})
	releaseFirstWrite := make(chan struct{})
	t.Cleanup(func() {
		closeOnce(releaseFirstWrite)
	})

	appendJobEvent := func(path string, event model.JobEvent) error {
		if event.Event == "job_queued" {
			close(firstWriteStarted)
			<-releaseFirstWrite
		}
		return store.AppendJobEvent(path, event)
	}
	writer := newJobJournalWriter(journalPath, appendJobEvent, store.AppendJobEventSync, 1)
	t.Cleanup(writer.close)

	job := model.Job{ID: "job-ordered", State: model.JobStateQueued}
	if err := writer.enqueue(model.JobEvent{Event: "job_queued", Job: job}); err != nil {
		t.Fatalf("enqueue queued event returned error: %v", err)
	}
	<-firstWriteStarted

	job.State = model.JobStateRunning
	if err := writer.enqueue(model.JobEvent{Event: "job_running", Job: job}); err != nil {
		t.Fatalf("enqueue running event returned error: %v", err)
	}

	job.State = model.JobStateCompleted
	finalEnqueueDone := make(chan error, 1)
	go func() {
		finalEnqueueDone <- writer.enqueue(model.JobEvent{Event: "job_completed", Job: job})
	}()
	select {
	case err := <-finalEnqueueDone:
		t.Fatalf("full queue fallback returned before the prior write completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFirstWrite)
	if err := <-finalEnqueueDone; err != nil {
		t.Fatalf("enqueue completed event returned error: %v", err)
	}
	writer.close()

	data, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Fatalf("journal line count = %d, want 3", len(lines))
	}
	wantEvents := []string{"job_queued", "job_running", "job_completed"}
	for i, line := range lines {
		var event model.JobEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("Unmarshal journal line %d returned error: %v", i, err)
		}
		if event.Event != wantEvents[i] {
			t.Fatalf("journal event %d = %q, want %q", i, event.Event, wantEvents[i])
		}
	}

	jobs, err := store.ReadJobEvents(journalPath)
	if err != nil {
		t.Fatalf("ReadJobEvents returned error: %v", err)
	}
	if jobs[job.ID].State != model.JobStateCompleted {
		t.Fatalf("replayed state = %q, want %q", jobs[job.ID].State, model.JobStateCompleted)
	}
}

func TestJobJournalWriterJournalBarrierWaitsForPriorAsyncEvent(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	writer := newJobJournalWriter(journalPath, store.AppendJobEvent, store.AppendJobEventSync, 2)
	t.Cleanup(writer.close)

	job := model.Job{ID: "job-barrier", State: model.JobStateQueued}
	if err := writer.enqueue(model.JobEvent{Event: "job_queued", Job: job}); err != nil {
		t.Fatalf("enqueue queued event returned error: %v", err)
	}
	job.State = model.JobStateRunning
	if err := writer.enqueueAndSync(model.JobEvent{Event: "job_paused", Job: job}); err != nil {
		t.Fatalf("enqueueAndSync paused event returned error: %v", err)
	}

	data, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("journal line count = %d, want 2", len(lines))
	}
	wantEvents := []string{"job_queued", "job_paused"}
	for i, line := range lines {
		var event model.JobEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("Unmarshal journal line %d returned error: %v", i, err)
		}
		if event.Event != wantEvents[i] {
			t.Fatalf("journal event %d = %q, want %q", i, event.Event, wantEvents[i])
		}
	}
}

func TestJobJournalWriterJournalBarrierReturnsSyncFailure(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "jobs.jsonl")
	syncErr := errors.New("injected barrier sync failure")
	writer := newJobJournalWriter(
		journalPath,
		store.AppendJobEvent,
		func(string, model.JobEvent) error { return syncErr },
		1,
	)
	t.Cleanup(writer.close)

	err := writer.enqueueAndSync(
		model.JobEvent{Event: "job_paused", Job: model.Job{ID: "job-barrier-failure"}},
	)
	if !errors.Is(err, syncErr) {
		t.Fatalf("enqueueAndSync error = %v, want injected sync failure", err)
	}
	if !strings.Contains(err.Error(), journalPath) {
		t.Fatalf("enqueueAndSync error = %q, want journal path %q", err, journalPath)
	}
}

func TestManagerJobTransitionBarrierIsInitializedAndOrdered(t *testing.T) {
	manager, cfg, _ := newTestManager(t)
	if manager.appendJobTransition == nil {
		t.Fatal("appendJobTransition is nil")
	}

	job := model.Job{ID: "job-manager-barrier", State: model.JobStateQueued}
	if err := manager.jobJournal.enqueue(model.JobEvent{Event: "job_queued", Job: job}); err != nil {
		t.Fatalf("enqueue queued event returned error: %v", err)
	}
	job.State = model.JobStateRunning
	if err := manager.appendJobTransition(model.JobEvent{Event: "job_paused", Job: job}); err != nil {
		t.Fatalf("appendJobTransition returned error: %v", err)
	}

	data, err := os.ReadFile(cfg.JobsPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("journal line count = %d, want 2", len(lines))
	}
	wantEvents := []string{"job_queued", "job_paused"}
	for i, line := range lines {
		var event model.JobEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("Unmarshal journal line %d returned error: %v", i, err)
		}
		if event.Event != wantEvents[i] {
			t.Fatalf("journal event %d = %q, want %q", i, event.Event, wantEvents[i])
		}
	}
}
