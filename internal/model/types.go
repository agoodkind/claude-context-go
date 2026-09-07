// Package model defines the daemon's persisted and in-memory domain types.
package model

import (
	"time"
)

// CodebaseStatus captures the lifecycle state of one tracked codebase.
type CodebaseStatus string

const (
	// CodebaseStatusNotIndexed means the codebase has no active index.
	CodebaseStatusNotIndexed CodebaseStatus = "not_indexed"
	// CodebaseStatusPending means an index was requested and a job is queued, but
	// the build has not started running yet. It is distinct from indexing (live
	// work) so a just-requested codebase reads as pending, not as a store outage.
	// updateJobRunning flips it to indexing when the job acquires a slot.
	CodebaseStatusPending CodebaseStatus = "pending"
	// CodebaseStatusIndexing means the codebase currently has an active job.
	CodebaseStatusIndexing CodebaseStatus = "indexing"
	// CodebaseStatusIndexed means the codebase has a completed index.
	CodebaseStatusIndexed CodebaseStatus = "indexed"
	// CodebaseStatusFailed means the last index attempt failed.
	CodebaseStatusFailed CodebaseStatus = "failed"
	// CodebaseStatusStale means the index metadata is known to be stale.
	CodebaseStatusStale CodebaseStatus = "stale"
	// CodebaseStatusMissing means the codebase's source directory is absent. It
	// is distinct from failed (a build error) and never auto-retries; the index
	// is kept in case the directory returns, and is removed only by an explicit
	// clear or the removed-worktree auto-clean.
	CodebaseStatusMissing CodebaseStatus = "missing"
	// CodebaseStatusDiscovered means the codebase is registered and watched but
	// its first index has not been built yet. A read (status or search) of an
	// untracked git worktree of an indexed sibling registers it in this state and
	// defers the reuse-seeded build to a background trigger, so the read never
	// launches an embed job. The deferred build flips it to indexing, then indexed.
	CodebaseStatusDiscovered CodebaseStatus = "discovered"
	// CodebaseStatusQuarantined means destructive sync is paused because the
	// daemon observed a suspicious large disappearance and is waiting for later
	// corroboration before deleting live semantic rows.
	CodebaseStatusQuarantined CodebaseStatus = "quarantined"
)

// CodebaseKind distinguishes filesystem code indexes from virtual document
// collections. The empty value is accepted as code for older registry entries
// written before this field existed.
type CodebaseKind string

const (
	// CodebaseKindCode is a filesystem-backed source code collection.
	CodebaseKindCode CodebaseKind = "code"
	// CodebaseKindDocument is a virtual document collection with no source
	// directory on disk.
	CodebaseKindDocument CodebaseKind = "document"
)

// GraphState captures the best-known state of the per-codebase graph store.
// The empty value is accepted as absent for older registry records.
type GraphState string

const (
	// GraphStateAbsent means no successful graph build has been recorded.
	GraphStateAbsent GraphState = "absent"
	// GraphStateReady means the graph was built from GraphSnapshotHash.
	GraphStateReady GraphState = "ready"
	// GraphStateStale means a graph build failed or no longer matches the semantic snapshot.
	GraphStateStale GraphState = "stale"
)

// JobState captures the lifecycle state of one daemon job.
type JobState string

const (
	// JobStateQueued means the job was accepted but not started.
	JobStateQueued JobState = "queued"
	// JobStateRunning means the job is actively running.
	JobStateRunning JobState = "running"
	// JobStatePaused means the job retained its work state while yielding capacity.
	JobStatePaused JobState = "paused"
	// JobStateCancelling means the job is winding down after cancellation.
	JobStateCancelling JobState = "cancelling"
	// JobStateCompleted means the job finished successfully.
	JobStateCompleted JobState = "completed"
	// JobStateFailed means the job ended in failure.
	JobStateFailed JobState = "failed"
	// JobStateCancelled means the job was cancelled.
	JobStateCancelled JobState = "cancelled"
)

// RunMode values for Progress.RunMode.
const (
	RunModeFirstBuild    = "first_build"
	RunModeChanged       = "changed"
	RunModeForcedReindex = "forced_reindex"
	RunModeResuming      = "resuming"
)

// ClientInfo identifies the caller that initiated a daemon request.
type ClientInfo struct {
	Name string `json:"name"`
	PID  int32  `json:"pid,omitempty"`
}

// IndexConfig records the effective configuration of one indexing request.
type IndexConfig struct {
	SplitterType       string            `json:"splitter_type"`
	SplitterChunkSize  int32             `json:"splitter_chunk_size"`
	SplitterOverlap    int32             `json:"splitter_overlap"`
	IgnorePatterns     []string          `json:"ignore_patterns,omitempty"`
	IncludeSubmodules  []string          `json:"include_submodules,omitempty"`
	IgnoreDigest       string            `json:"ignore_digest"`
	EmbeddingProvider  EmbeddingProvider `json:"embedding_provider,omitempty"`
	EmbeddingModel     string            `json:"embedding_model,omitempty"`
	EmbeddingDimension int32             `json:"embedding_dimension,omitempty"`
	VectorBackend      VectorBackend     `json:"vector_backend,omitempty"`
	Hybrid             bool              `json:"hybrid"`
}

// AdmissionBudget carries per-request fixed caps that must not enter
// IndexConfig, because IndexConfig is digested for merkle reuse.
type AdmissionBudget struct {
	MaxJobChunks int32 `json:"max_job_chunks,omitempty"`
	MaxJobBytes  int64 `json:"max_job_bytes,omitempty"`
}

// Progress records daemon-visible structured progress for a job.
type Progress struct {
	Phase string `json:"phase"`
	// OverallPercent is the run's own cursor through its work. The wire figure
	// is resolved from it and the chunk counters, so a reader wanting the
	// headline percent should take the resolved one rather than this raw value.
	// The wire's companion phase percent is derived from the counters alone and
	// is therefore not stored here.
	OverallPercent float64 `json:"overall_percent"`
	// Unit is the human progress noun for the counted items: "file" for a code
	// index and "document" for a conversation index. An empty value reads as
	// "file" so older persisted jobs render unchanged.
	Unit string `json:"unit,omitempty"`
	// RunMode names the kind of pass: "first_build", "changed",
	// "forced_reindex", or "resuming". Set when the run plan is decided so
	// surfaces can label the denominator and name a resume.
	RunMode string `json:"runMode,omitempty"`
	// BootstrapReason names why a run routed to the full rebuild path. It is
	// empty for delta runs; values come from internal/daemon's vocabulary.
	BootstrapReason string `json:"bootstrapReason,omitempty"`
	// ScopeUnit is the unit of the added/modified/removed classification when
	// it differs from Unit. Empty means same as Unit.
	ScopeUnit      string `json:"scopeUnit,omitempty"`
	FilesTotal     int32  `json:"files_total"`
	FilesProcessed int32  `json:"files_processed"`
	// FilesAdded, FilesModified, and FilesRemoved record a delta sync's change
	// breakdown so status output can show the magnitude of a reconcile (for
	// example after a large merge). They stay zero for a full reindex.
	FilesAdded    int32 `json:"files_added"`
	FilesModified int32 `json:"files_modified"`
	FilesRemoved  int32 `json:"files_removed"`
	// FilesInCodebase is the total file count in the current snapshot, so an
	// incremental run can report unchanged as this total less the changed set.
	FilesInCodebase int32 `json:"files_in_codebase"`
	// FilesEmbedded counts files this run actually re-embedded, so an
	// incremental run can report unchanged (FilesProcessed - FilesEmbedded)
	// separately from re-embedded. It stays zero until embedding begins.
	FilesEmbedded int32 `json:"files_embedded"`
	// FilesSkippedOversize and FilesSkippedUnreadable count changed files the
	// indexer declined to embed: past the size cap, or not valid UTF-8.
	FilesSkippedOversize   int32 `json:"files_skipped_oversize"`
	FilesSkippedUnreadable int32 `json:"files_skipped_unreadable"`
	// FilesPending counts changed items whose content was not delivered this pass
	// (the conversation-ingest undelivered case). Transient, not an error; the
	// daemon re-requests them on the next sync.
	FilesPending int32 `json:"files_pending"`
	// ChunksTotal is the live whole-collection chunk count, populated at render
	// time for an in-flight incremental run so status can show the running total
	// rather than only the per-run additions. Zero means not populated.
	ChunksTotal int32 `json:"chunks_total"`
	// ChunksProcessed counts chunks produced by this pass. ChunksReused counts
	// chunks this run served from an already-embedded vector instead of calling
	// the embedder. ChunksEmbedded is the chunks sent to the embedder, and
	// ChunksGenerated is the older wire-compatible alias for ChunksEmbedded.
	ChunksProcessed           int32     `json:"chunks_processed"`
	ChunksReused              int32     `json:"chunks_reused"`
	ChunksEmbedded            int32     `json:"chunks_embedded"`
	ChunksGenerated           int32     `json:"chunks_generated"`
	ChunksDropped             int32     `json:"chunks_dropped"`
	ReuseVectorsLoaded        int32     `json:"reuse_vectors_loaded"`
	EmbeddingBatchesTotal     int32     `json:"embedding_batches_total"`
	EmbeddingBatchesCompleted int32     `json:"embedding_batches_completed"`
	CollectionRowsWritten     int32     `json:"collection_rows_written"`
	LastEventAt               time.Time `json:"last_event_at"`
	HeartbeatAt               time.Time `json:"heartbeat_at"`
}

// JobError records job-level failure details. TraceID and JobID tie the
// failure to the daemon's structured logs so an operator can grep for the
// full context behind a reported error.
type JobError struct {
	Message   string `json:"message"`
	Code      string `json:"code,omitempty"`
	Retryable bool   `json:"retryable"`
	TraceID   string `json:"trace_id,omitempty"`
	JobID     string `json:"job_id,omitempty"`
}

// IndexRunSummary records the last successful indexing run for a codebase.
//
// SkippedFiles names files the indexer refused to embed because their bytes
// are not valid UTF-8 and would fail the Milvus gRPC marshal. The list is
// persisted in the registry so the operator can audit it without re-walking
// the daemon logs.
type IndexRunSummary struct {
	IndexedFiles int32     `json:"indexed_files"`
	TotalChunks  int32     `json:"total_chunks"`
	TotalBytes   int64     `json:"total_bytes"`
	Status       string    `json:"status"`
	CompletedAt  time.Time `json:"completed_at"`
	SkippedFiles []string  `json:"skipped_files,omitempty"`
}

// IndexRunFailure records the last failed indexing run for a codebase.
// TraceID and JobID tie the failure to the daemon's structured logs so the
// reported error resolves to the full context by a log lookup.
type IndexRunFailure struct {
	Message                 string    `json:"message"`
	Code                    string    `json:"code,omitempty"`
	LastAttemptedPercentage int32     `json:"last_attempted_percentage"`
	FailedAt                time.Time `json:"failed_at"`
	TraceID                 string    `json:"trace_id,omitempty"`
	JobID                   string    `json:"job_id,omitempty"`
}

// Codebase records one canonical indexed codebase.
//
// InodeTrackingDisabled records that the root filesystem reported unstable
// inodes at registration time. When true, convergence falls back to
// path-only file tracking instead of (device, inode, contentHash).
type Codebase struct {
	ID                string           `json:"id"`
	Kind              CodebaseKind     `json:"kind,omitempty"`
	CanonicalPath     string           `json:"canonical_path"`
	Status            CodebaseStatus   `json:"status"`
	ActiveJobID       string           `json:"active_job_id,omitempty"`
	LastSuccessfulRun *IndexRunSummary `json:"last_successful_run,omitempty"`
	LastFailedRun     *IndexRunFailure `json:"last_failed_run,omitempty"`
	// LiveFileTotal and LiveChunkTotal track the latest known corpus size,
	// updated during runs rather than only at completion.
	LiveFileTotal               int32            `json:"liveFileTotal,omitempty"`
	LiveChunkTotal              int32            `json:"liveChunkTotal,omitempty"`
	EffectiveConfig             IndexConfig      `json:"effective_config"`
	SchedulingPolicy            SchedulingPolicy `json:"scheduling_policy,omitzero"`
	PolicyPendingInitialization bool             `json:"policy_pending_initialization,omitempty"`
	CollectionName              string           `json:"collection_name,omitempty"`
	LegacyCollectionNames       []string         `json:"legacy_collection_names,omitempty"`
	MerkleSnapshotPath          string           `json:"merkle_snapshot_path,omitempty"`
	GraphState                  GraphState       `json:"graph_state,omitempty"`
	GraphSnapshotHash           string           `json:"graph_snapshot_hash,omitempty"`
	GraphUpdatedAt              time.Time        `json:"graph_updated_at,omitzero"`
	Quarantine                  *QuarantineState `json:"quarantine,omitempty"`
	// WorktreeCommonDir is the shared git common dir when this codebase's root
	// is a linked git worktree, else empty. It lets the daemon recognize a
	// removed worktree (git deleted its admin entry) after the directory is gone
	// and auto-clean the disposable index.
	WorktreeCommonDir     string    `json:"worktree_common_dir,omitempty"`
	InodeTrackingDisabled bool      `json:"inode_tracking_disabled,omitempty"`
	UpdatedAt             time.Time `json:"updated_at"`
}

// QuarantineState records why destructive sync is paused for a codebase and
// what corroborating observations the daemon has seen so far.
type QuarantineState struct {
	Reason           string    `json:"reason,omitempty"`
	FirstObservedAt  time.Time `json:"first_observed_at"`
	LastObservedAt   time.Time `json:"last_observed_at"`
	ObservationCount int32     `json:"observation_count,omitempty"`
	LastTrigger      string    `json:"last_trigger,omitempty"`
	LastMissingCount int32     `json:"last_missing_count,omitempty"`
	LastTotalCount   int32     `json:"last_total_count,omitempty"`
}

// Job records one daemon job and its latest known state.
type Job struct {
	ID            string     `json:"id"`
	CodebaseID    string     `json:"codebase_id"`
	RequestedPath string     `json:"requested_path"`
	CanonicalPath string     `json:"canonical_path"`
	Client        ClientInfo `json:"client"`
	Operation     string     `json:"operation"`
	State         JobState   `json:"state"`
	// Forced records that the caller passed force=true on the index request, so a
	// trigger-aware heading can tell a forced reindex apart from a first build or
	// a changed-files sync, which otherwise share the same operation.
	Forced                    bool                  `json:"forced"`
	Progress                  Progress              `json:"progress"`
	Config                    IndexConfig           `json:"config"`
	Budget                    AdmissionBudget       `json:"budget,omitzero"`
	EffectiveSchedulingPolicy SchedulingPolicy      `json:"effective_scheduling_policy,omitzero"`
	SchedulingOverride        SchedulingPolicyPatch `json:"scheduling_override,omitzero"`
	QueueSequence             uint64                `json:"queue_sequence,omitempty"`
	SchedulingReason          string                `json:"scheduling_reason,omitempty"`
	StartedAt                 time.Time             `json:"started_at"`
	UpdatedAt                 time.Time             `json:"updated_at"`
	CompletedAt               *time.Time            `json:"completed_at,omitempty"`
	Error                     *JobError             `json:"error,omitempty"`
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

// StoredChunk is one persisted search chunk for a codebase.
type StoredChunk struct {
	Content        string `json:"content"`
	RelativePath   string `json:"relative_path"`
	StartLine      int32  `json:"start_line"`
	EndLine        int32  `json:"end_line"`
	Language       string `json:"language"`
	FileExtension  string `json:"file_extension"`
	ConversationID string `json:"conversation_id"`
	// ParentConversationID names the conversation this chunk's conversation
	// forked from, so a fork can be grouped with its parent. Empty for code
	// chunks and for conversations with no parent.
	ParentConversationID string `json:"parent_conversation_id"`
	MessageIndex         int32  `json:"message_index"`
	Role                 string `json:"role"`
	TimestampUnix        int64  `json:"timestamp_unix"`
	// WorkspaceRoot is the workspace a conversation chunk belongs to, stored as a
	// native scalar column so a search can filter by it. Empty for code chunks
	// and for conversation chunks whose caller did not supply it.
	WorkspaceRoot string `json:"workspace_root,omitempty"`
	Archived      bool   `json:"archived,omitempty"`
	// SplitPart identifies one piece of an oversized chunk the token-budget
	// splitter divided so each piece fits the embedding model's input limit. Zero
	// marks an unsplit chunk, whose primary key is unchanged. A positive value
	// encodes the piece's byte offset within the original content plus one, so
	// identical pieces of repeated content never collide on the primary key while
	// every piece keeps the parent relativePath so a message delete-by-prefix
	// still removes them all.
	SplitPart int32 `json:"split_part,omitempty"`
	// SplitPartRecorded distinguishes a stored nullable splitPart value from a
	// legacy row written before the field existed.
	SplitPartRecorded bool `json:"-"`
	// LoadRules is the caller's opaque loading-rules tag for a conversation
	// chunk: it names the rules that produced MessageIndex so a reader can
	// rebuild the same message sequence. Empty for code chunks and for rows
	// written before the tag existed.
	LoadRules string `json:"load_rules,omitempty"`
	// Score is the vector similarity for a semantic search. Zero on chunks that
	// did not come from a search.
	Score float64 `json:"score,omitempty"`
}

// ConversationDocument is one caller-provided conversation message chunk.
type ConversationDocument struct {
	ConversationID string `json:"conversation_id"`
	// ParentConversationID names the conversation this one forked from, carried
	// into chunk metadata so forks group with their parent. Empty when absent.
	ParentConversationID string                 `json:"parent_conversation_id"`
	MessageIndex         int32                  `json:"message_index"`
	Role                 string                 `json:"role"`
	TimestampUnix        int64                  `json:"timestamp_unix"`
	Text                 string                 `json:"text"`
	Tools                []ConversationToolCall `json:"tools,omitempty"`
	Thinking             string                 `json:"thinking,omitempty"`
	// WorkspaceRoot is the workspace the conversation belongs to. clyde supplies
	// it so the engine can store it as a filterable scalar column.
	WorkspaceRoot string `json:"workspace_root,omitempty"`
	Archived      bool   `json:"archived,omitempty"`
	// LoadRules is the caller's opaque loading-rules tag: it names the rules
	// that produced MessageIndex so a reader can rebuild the same message
	// sequence. The engine stores it per row and returns it on search hits.
	LoadRules string `json:"load_rules,omitempty"`
}

// ConversationToolCall is one structured tool call attached to a conversation document.
type ConversationToolCall struct {
	Name     string `json:"name,omitempty"`
	Display  string `json:"display,omitempty"`
	LangHint string `json:"lang_hint,omitempty"`
	Output   string `json:"output,omitempty"`
	IsError  bool   `json:"is_error,omitempty"`
}

// PathClassificationKind reports the daemon's verdict about one queried path.
type PathClassificationKind string

const (
	// PathClassificationUnspecified is the zero value; callers should treat
	// it as out-of-scope or unknown.
	PathClassificationUnspecified PathClassificationKind = ""
	// PathClassificationInScopeIndexed means the path is covered by a
	// codebase, survives ignore rules, and has a chunk row in the codebase
	// collection.
	PathClassificationInScopeIndexed PathClassificationKind = "in_scope_indexed"
	// PathClassificationInScopeExcluded means the path is covered but
	// excluded by the codebase's resolved ignore rules.
	PathClassificationInScopeExcluded PathClassificationKind = "in_scope_excluded"
	// PathClassificationInScopeUnindexed means the path is covered and not
	// excluded but has no chunk row (not yet indexed or removed).
	PathClassificationInScopeUnindexed PathClassificationKind = "in_scope_unindexed"
	// PathClassificationOutOfScope means no tracked codebase covers the
	// path's canonical form.
	PathClassificationOutOfScope PathClassificationKind = "out_of_scope"
)

// PathClassification carries the daemon's verdict about one queried path.
// CoveringCodebaseID names the longest-prefix covering codebase, when any.
// ExcludedByPattern and ExcludedByGitignore name the rule that excluded
// the path when Kind is PathClassificationInScopeExcluded.
type PathClassification struct {
	Kind                PathClassificationKind
	ExcludedByPattern   string
	ExcludedByGitignore string
	CoveringCodebaseID  string
}
