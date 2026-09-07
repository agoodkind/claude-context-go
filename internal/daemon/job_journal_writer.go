package daemon

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"goodkind.io/lm-semantic-search/internal/model"
)

const jobJournalQueueCapacity = 256

// jobJournalCompactionThresholdBytes is the journal size at which the writer
// rewrites the file. Eight megabytes bounds the file at roughly this plus the
// retained set, so a boot reads about 12 MB rather than the 235 MB this change
// was written against.
const jobJournalCompactionThresholdBytes = 8 * 1024 * 1024

type jobJournalWriteRequest struct {
	event   model.JobEvent
	durable bool
	result  chan error
}

type jobJournalWriter struct {
	path               string
	appendJobEvent     appendJobEventFunc
	appendJobEventSync appendJobEventFunc
	queue              chan jobJournalWriteRequest
	done               chan bool
	closeOnce          sync.Once
	// currentSizeBytes includes the existing journal so the first new event can
	// compact an oversized file immediately after deployment.
	currentSizeBytes int64
	// compactionThresholdBytes stays configurable for tests that exercise the
	// boundary without writing an eight megabyte fixture.
	compactionThresholdBytes int64
	// compactionRetryAtBytes prevents a persistent compaction failure from
	// blocking every append while preserving retries after bounded growth.
	compactionRetryAtBytes int64
}

func newJobJournalWriter(
	path string,
	appendJobEvent appendJobEventFunc,
	appendJobEventSync appendJobEventFunc,
	queueCapacity int,
	testCompactionThresholdBytes ...int64,
) *jobJournalWriter {
	compactionThresholdBytes := int64(jobJournalCompactionThresholdBytes)
	if len(testCompactionThresholdBytes) > 0 {
		compactionThresholdBytes = testCompactionThresholdBytes[0]
	}
	writer := &jobJournalWriter{
		path:                     path,
		appendJobEvent:           appendJobEvent,
		appendJobEventSync:       appendJobEventSync,
		queue:                    make(chan jobJournalWriteRequest, queueCapacity),
		done:                     make(chan bool, 1),
		closeOnce:                sync.Once{},
		currentSizeBytes:         initialJobJournalSize(path),
		compactionThresholdBytes: compactionThresholdBytes,
		compactionRetryAtBytes:   0,
	}
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error(
					"job journal writer panic",
					"path",
					writer.path,
					"err",
					fmt.Errorf("panic: %v", recovered),
				)
			}
			writer.done <- true
		}()
		writer.run()
	}()
	return writer
}

func (writer *jobJournalWriter) enqueue(event model.JobEvent) error {
	request := jobJournalWriteRequest{event: event, durable: false, result: nil}
	select {
	case writer.queue <- request:
		return nil
	default:
	}

	request.result = make(chan error, 1)
	slog.Warn(
		"job journal queue full; falling back to synchronous ordered write",
		"path",
		writer.path,
		"job_id",
		event.Job.ID,
		"event",
		event.Event,
		"queue_capacity",
		cap(writer.queue),
	)
	writer.queue <- request
	return <-request.result
}

func (writer *jobJournalWriter) enqueueAndSync(event model.JobEvent) error {
	request := jobJournalWriteRequest{
		event:   event,
		durable: true,
		result:  make(chan error, 1),
	}
	writer.queue <- request
	return <-request.result
}

func (writer *jobJournalWriter) run() {
	for request := range writer.queue {
		err := writer.write(request.event, request.durable)
		if err == nil {
			writer.recordSuccessfulAppend(request.event)
		}
		if request.result != nil {
			request.result <- err
		}
	}
}

// recordSuccessfulAppend measures growth only after durability and spaces
// failed compaction retries by one full compaction threshold.
func (writer *jobJournalWriter) recordSuccessfulAppend(event model.JobEvent) {
	eventSize, err := marshalJobEventsSize([]model.JobEvent{event})
	if err != nil {
		slog.Error(
			"measure appended job journal event failed",
			"component",
			"daemon",
			"subcomponent",
			"journal",
			"path",
			writer.path,
			"job_id",
			event.Job.ID,
			"err",
			err,
		)
		return
	}
	writer.currentSizeBytes += eventSize
	if writer.currentSizeBytes < writer.compactionThresholdBytes {
		return
	}
	if writer.compactionRetryAtBytes > 0 &&
		writer.currentSizeBytes < writer.compactionRetryAtBytes {
		return
	}

	if _, _, err := compactJobJournal(writer.path); err != nil {
		writer.compactionRetryAtBytes = writer.currentSizeBytes +
			writer.compactionThresholdBytes
		slog.Error(
			"compact job journal failed",
			"component",
			"daemon",
			"subcomponent",
			"journal",
			"path",
			writer.path,
			"err",
			err,
		)
		return
	}
	info, err := os.Stat(writer.path)
	if err != nil {
		writer.compactionRetryAtBytes = writer.currentSizeBytes +
			writer.compactionThresholdBytes
		slog.Error(
			"stat compacted job journal failed",
			"component",
			"daemon",
			"subcomponent",
			"journal",
			"path",
			writer.path,
			"err",
			err,
		)
		return
	}
	writer.currentSizeBytes = info.Size()
	writer.compactionRetryAtBytes = 0
}

// initialJobJournalSize treats a missing journal as empty while surfacing other
// stat failures before the writer starts serving its queue.
func initialJobJournalSize(path string) int64 {
	info, err := os.Stat(path)
	if err == nil {
		return info.Size()
	}
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	slog.Error(
		"stat jobs journal failed",
		"component",
		"daemon",
		"subcomponent",
		"journal",
		"path",
		path,
		"err",
		err,
	)
	return 0
}

func (writer *jobJournalWriter) write(event model.JobEvent, durable bool) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("append jobs journal %s: panic: %v", writer.path, recovered)
			slog.Error(
				"append jobs journal failed",
				"path",
				writer.path,
				"job_id",
				event.Job.ID,
				"event",
				event.Event,
				"err",
				err,
			)
		}
	}()
	appendJobEvent := writer.appendJobEvent
	if durable {
		appendJobEvent = writer.appendJobEventSync
	}
	return appendJobJournalEvent(writer.path, appendJobEvent, event)
}

func (writer *jobJournalWriter) close() {
	writer.closeOnce.Do(func() {
		close(writer.queue)
		<-writer.done
	})
}

func appendJobJournalEvent(
	path string,
	appendJobEvent appendJobEventFunc,
	event model.JobEvent,
) error {
	if err := appendJobEvent(path, event); err != nil {
		wrappedErr := fmt.Errorf("append jobs journal %s: %w", path, err)
		slog.Error(
			"append jobs journal failed",
			"path",
			path,
			"job_id",
			event.Job.ID,
			"event",
			event.Event,
			"err",
			wrappedErr,
		)
		return wrappedErr
	}
	return nil
}

func (manager *Manager) closeJobJournal() {
	manager.mu.Lock()
	journal := manager.jobJournal
	manager.jobJournal = nil
	manager.mu.Unlock()

	if journal != nil {
		journal.close()
	}
}
