package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/config"
	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/platformactivity"
)

type closeTrackingSemantic struct {
	semanticIndex
	closed          bool
	jobStopped      <-chan struct{}
	closedBeforeJob bool
}

type closeTrackingActivitySource struct {
	jobStopped         <-chan struct{}
	journalOpen        func() bool
	closedSignal       chan struct{}
	closed             bool
	closedBeforeJob    bool
	journalOpenAtClose bool
}

func (source *closeTrackingActivitySource) Sample(
	context.Context,
) platformactivity.Snapshot {
	return platformactivity.Snapshot{
		InputAvailable:   true,
		ThermalAvailable: false,
	}
}

func (source *closeTrackingActivitySource) Close() {
	source.closed = true
	if source.jobStopped != nil {
		select {
		case <-source.jobStopped:
		default:
			source.closedBeforeJob = true
		}
	}
	if source.journalOpen != nil {
		source.journalOpenAtClose = source.journalOpen()
	}
	if source.closedSignal != nil {
		close(source.closedSignal)
	}
}

func (semantic *closeTrackingSemantic) Close(context.Context) error {
	semantic.closed = true
	if semantic.jobStopped != nil {
		select {
		case <-semantic.jobStopped:
		default:
			semantic.closedBeforeJob = true
		}
	}
	return nil
}

func TestManagerCloseClosesSemanticBackend(t *testing.T) {
	manager, _, _ := newTestManager(t)
	semantic := &closeTrackingSemantic{semanticIndex: manager.semantic}
	manager.semantic = semantic

	if err := manager.Close(context.Background()); err != nil {
		t.Fatalf("Manager.Close returned error: %v", err)
	}
	if !semantic.closed {
		t.Fatal("Manager.Close did not close the semantic backend")
	}
}

func TestManagerCloseCancelsAndWaitsForDetachedJob(t *testing.T) {
	manager, _ := newTestManagerWithCap(t, 1)
	repoPath := newCapTestRepo(t)
	jobEntered := make(chan struct{}, 1)
	jobStopped := make(chan struct{})
	releaseJob := make(chan struct{})
	manager.runner = fakeRunner{indexOne: func(
		ctx context.Context,
		_ string,
		_ string,
		_ model.IndexConfig,
	) (indexer.OneFileResult, error) {
		jobEntered <- struct{}{}
		select {
		case <-ctx.Done():
			close(jobStopped)
			return indexer.OneFileResult{}, ctx.Err()
		case <-releaseJob:
			return indexer.OneFileResult{}, nil
		}
	}}
	semantic := &closeTrackingSemantic{
		semanticIndex: manager.semantic,
		jobStopped:    jobStopped,
	}
	manager.semantic = semantic
	_, job := seedBootstrapCodebase(t, manager, repoPath, defaultIndexConfig())
	requestContext, cancelRequest := context.WithCancel(context.Background())
	manager.runJobAsync(requestContext, job.ID)
	select {
	case <-jobEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("detached job did not enter the runner")
	}
	cancelRequest()

	manager.mu.Lock()
	jobDone := manager.done[job.ID]
	manager.mu.Unlock()
	t.Cleanup(func() {
		close(releaseJob)
		if jobDone != nil {
			<-jobDone
		}
	})

	closeContext, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelClose()
	if err := manager.Close(closeContext); err != nil {
		t.Fatalf("Manager.Close returned error: %v", err)
	}
	select {
	case <-jobStopped:
	default:
		t.Fatal("detached job did not observe manager close cancellation")
	}
	if semantic.closedBeforeJob {
		t.Fatal("semantic backend closed before the detached job stopped")
	}
	if !semantic.closed {
		t.Fatal("semantic backend did not close after the detached job stopped")
	}
}

func TestManagerCloseClosesActivityAfterJobsAndBeforeJournal(t *testing.T) {
	cfg, repoPath := newTestManagerConfig(t)
	jobEntered := make(chan struct{})
	jobStopped := make(chan struct{})
	activity := &closeTrackingActivitySource{jobStopped: jobStopped}
	manager, err := newManagerWithDependencies(
		context.Background(),
		cfg,
		managerDependencies{
			semanticFactory: func(context.Context, config.Config) (semanticIndex, error) {
				return &fakeSemantic{}, nil
			},
			activitySource: activity,
		},
	)
	if err != nil {
		t.Fatalf("newManagerWithDependencies returned error: %v", err)
	}
	activity.journalOpen = func() bool {
		return manager.jobJournal != nil
	}
	manager.runner = fakeRunner{indexOne: func(
		ctx context.Context,
		_ string,
		_ string,
		_ model.IndexConfig,
	) (indexer.OneFileResult, error) {
		close(jobEntered)
		<-ctx.Done()
		close(jobStopped)
		return indexer.OneFileResult{}, ctx.Err()
	}}
	_, job := seedBootstrapCodebase(t, manager, repoPath, defaultIndexConfig())
	manager.runJobAsync(context.Background(), job.ID)
	select {
	case <-jobEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("detached job did not enter the runner")
	}

	if err := manager.Close(context.Background()); err != nil {
		t.Fatalf("Manager.Close returned error: %v", err)
	}
	if !activity.closed {
		t.Fatal("activity source remained open")
	}
	if activity.closedBeforeJob {
		t.Fatal("activity source closed before the job stopped")
	}
	if !activity.journalOpenAtClose {
		t.Fatal("activity source closed after the journal")
	}
	if manager.jobJournal != nil {
		t.Fatal("job journal remained open")
	}
}

func TestManagerCloseDoesNotHoldMutexWhileClosingJournal(t *testing.T) {
	cfg, _ := newTestManagerConfig(t)
	activity := &closeTrackingActivitySource{closedSignal: make(chan struct{})}
	manager, err := newManagerWithDependencies(
		context.Background(),
		cfg,
		managerDependencies{
			semanticFactory: func(context.Context, config.Config) (semanticIndex, error) {
				return &fakeSemantic{}, nil
			},
			activitySource: activity,
		},
	)
	if err != nil {
		t.Fatalf("newManagerWithDependencies returned error: %v", err)
	}
	manager.closeJobJournal()

	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	writer := newJobJournalWriter(
		cfg.JobsPath,
		func(string, model.JobEvent) error {
			close(writeStarted)
			<-releaseWrite
			return nil
		},
		func(string, model.JobEvent) error { return nil },
		1,
	)
	manager.mu.Lock()
	manager.jobJournal = writer
	manager.mu.Unlock()
	if err := writer.enqueue(model.JobEvent{
		Event: "block_manager_close",
		Job:   model.Job{ID: "job-block-manager-close"},
	}); err != nil {
		t.Fatalf("enqueue blocking journal event: %v", err)
	}
	<-writeStarted

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- manager.Close(context.Background())
	}()
	select {
	case <-activity.closedSignal:
	case <-time.After(5 * time.Second):
		close(releaseWrite)
		t.Fatal("activity source did not close")
	}
	time.Sleep(50 * time.Millisecond)
	mutexAcquired := make(chan struct{})
	go func() {
		manager.mu.Lock()
		manager.mu.Unlock()
		close(mutexAcquired)
	}()
	mutexAvailable := false
	select {
	case <-mutexAcquired:
		mutexAvailable = true
	case <-time.After(250 * time.Millisecond):
	}
	close(releaseWrite)
	if err := <-closeResult; err != nil {
		t.Fatalf("Manager.Close returned error: %v", err)
	}
	if !mutexAvailable {
		<-mutexAcquired
		t.Fatal("Manager.Close held manager.mu while journal close waited for I/O")
	}
}

func TestNewManagerClosesSemanticBackendWhenLoadFails(t *testing.T) {
	cfg, _ := newTestManagerConfig(t)
	cfg.JobsPath = t.TempDir()
	semantic := &closeTrackingSemantic{semanticIndex: &fakeSemantic{}}

	manager, err := newManagerWithSemanticFactory(
		context.Background(),
		cfg,
		func(context.Context, config.Config) (semanticIndex, error) {
			return semantic, nil
		},
	)
	if err == nil {
		if manager != nil {
			_ = manager.Close(context.Background())
		}
		t.Fatal("newManagerWithSemanticFactory returned no load error")
	}
	if !semantic.closed {
		t.Fatal("semantic backend remained open after manager load failed")
	}
}

func TestNewManagerClosesActivityWhenSemanticFactoryFails(t *testing.T) {
	cfg, _ := newTestManagerConfig(t)
	activity := &closeTrackingActivitySource{}

	manager, err := newManagerWithDependencies(
		context.Background(),
		cfg,
		managerDependencies{
			semanticFactory: func(context.Context, config.Config) (semanticIndex, error) {
				return nil, errors.New("semantic factory failed")
			},
			activitySource: activity,
		},
	)
	if err == nil || manager != nil {
		if manager != nil {
			_ = manager.Close(context.Background())
		}
		t.Fatal("newManagerWithDependencies returned no semantic factory error")
	}
	if !activity.closed {
		t.Fatal("activity source remained open after semantic factory failure")
	}
}
