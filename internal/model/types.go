// Package model defines the daemon's persisted and in-memory domain types.
package model

import "time"

// CodebaseStatus captures the lifecycle state of one tracked codebase.
type CodebaseStatus string

const (
	// CodebaseStatusNotIndexed means the codebase has no active index.
	CodebaseStatusNotIndexed CodebaseStatus = "not_indexed"
	// CodebaseStatusIndexing means the codebase currently has an active job.
	CodebaseStatusIndexing CodebaseStatus = "indexing"
	// CodebaseStatusIndexed means the codebase has a completed index.
	CodebaseStatusIndexed CodebaseStatus = "indexed"
	// CodebaseStatusFailed means the last index attempt failed.
	CodebaseStatusFailed CodebaseStatus = "failed"
	// CodebaseStatusStale means the index metadata is known to be stale.
	CodebaseStatusStale CodebaseStatus = "stale"
)

// JobState captures the lifecycle state of one daemon job.
type JobState string

const (
	// JobStateQueued means the job was accepted but not started.
	JobStateQueued JobState = "queued"
	// JobStateRunning means the job is actively running.
	JobStateRunning JobState = "running"
	// JobStateCancelling means the job is winding down after cancellation.
	JobStateCancelling JobState = "cancelling"
	// JobStateCompleted means the job finished successfully.
	JobStateCompleted JobState = "completed"
	// JobStateFailed means the job ended in failure.
	JobStateFailed JobState = "failed"
	// JobStateCancelled means the job was cancelled.
	JobStateCancelled JobState = "cancelled"
)

// ClientInfo identifies the caller that initiated a daemon request.
type ClientInfo struct {
	Name string `json:"name"`
	PID  int32  `json:"pid,omitempty"`
}

// IndexConfig records the effective configuration of one indexing request.
type IndexConfig struct {
	SplitterType       string   `json:"splitter_type"`
	SplitterChunkSize  int32    `json:"splitter_chunk_size"`
	SplitterOverlap    int32    `json:"splitter_overlap"`
	Extensions         []string `json:"extensions,omitempty"`
	IgnorePatterns     []string `json:"ignore_patterns,omitempty"`
	IgnoreDigest       string   `json:"ignore_digest"`
	EmbeddingProvider  string   `json:"embedding_provider,omitempty"`
	EmbeddingModel     string   `json:"embedding_model,omitempty"`
	EmbeddingDimension int32    `json:"embedding_dimension,omitempty"`
	VectorBackend      string   `json:"vector_backend,omitempty"`
	Hybrid             bool     `json:"hybrid"`
}

// Progress records daemon-visible structured progress for a job.
type Progress struct {
	Phase                     string    `json:"phase"`
	PhasePercent              float64   `json:"phase_percent"`
	OverallPercent            float64   `json:"overall_percent"`
	FilesTotal                int32     `json:"files_total"`
	FilesProcessed            int32     `json:"files_processed"`
	ChunksGenerated           int32     `json:"chunks_generated"`
	EmbeddingBatchesTotal     int32     `json:"embedding_batches_total"`
	EmbeddingBatchesCompleted int32     `json:"embedding_batches_completed"`
	CollectionRowsWritten     int32     `json:"collection_rows_written"`
	LastEventAt               time.Time `json:"last_event_at"`
	HeartbeatAt               time.Time `json:"heartbeat_at"`
}

// JobError records job-level failure details.
type JobError struct {
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// IndexRunSummary records the last successful indexing run for a codebase.
type IndexRunSummary struct {
	IndexedFiles int32     `json:"indexed_files"`
	TotalChunks  int32     `json:"total_chunks"`
	Status       string    `json:"status"`
	CompletedAt  time.Time `json:"completed_at"`
}

// IndexRunFailure records the last failed indexing run for a codebase.
type IndexRunFailure struct {
	Message                 string    `json:"message"`
	LastAttemptedPercentage int32     `json:"last_attempted_percentage"`
	FailedAt                time.Time `json:"failed_at"`
}

// Codebase records one canonical indexed codebase and its aliases.
type Codebase struct {
	ID                    string           `json:"id"`
	CanonicalPath         string           `json:"canonical_path"`
	Aliases               []string         `json:"aliases,omitempty"`
	Status                CodebaseStatus   `json:"status"`
	ActiveJobID           string           `json:"active_job_id,omitempty"`
	LastSuccessfulRun     *IndexRunSummary `json:"last_successful_run,omitempty"`
	LastFailedRun         *IndexRunFailure `json:"last_failed_run,omitempty"`
	EffectiveConfig       IndexConfig      `json:"effective_config"`
	CollectionName        string           `json:"collection_name,omitempty"`
	LegacyCollectionNames []string         `json:"legacy_collection_names,omitempty"`
	MerkleSnapshotPath    string           `json:"merkle_snapshot_path,omitempty"`
	UpdatedAt             time.Time        `json:"updated_at"`
}

// Job records one daemon job and its latest known state.
type Job struct {
	ID            string      `json:"id"`
	CodebaseID    string      `json:"codebase_id"`
	RequestedPath string      `json:"requested_path"`
	CanonicalPath string      `json:"canonical_path"`
	Client        ClientInfo  `json:"client"`
	Operation     string      `json:"operation"`
	State         JobState    `json:"state"`
	Progress      Progress    `json:"progress"`
	Config        IndexConfig `json:"config"`
	StartedAt     time.Time   `json:"started_at"`
	UpdatedAt     time.Time   `json:"updated_at"`
	CompletedAt   *time.Time  `json:"completed_at,omitempty"`
	Error         *JobError   `json:"error,omitempty"`
}

// RegistryFile is the durable JSON representation of tracked codebases.
type RegistryFile struct {
	Codebases []Codebase `json:"codebases"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// JobEvent is one append-only journal entry for a job mutation.
type JobEvent struct {
	Event      string    `json:"event"`
	OccurredAt time.Time `json:"occurred_at"`
	Job        Job       `json:"job"`
}
