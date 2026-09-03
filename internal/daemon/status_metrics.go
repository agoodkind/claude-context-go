package daemon

import (
	"math"
	"runtime"
	"sort"
	"time"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/jobscheduler"
	"goodkind.io/lm-semantic-search/internal/metrics"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/pbconv"
	"goodkind.io/lm-semantic-search/internal/platformactivity"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Metric groups. They order the human surfaces and nothing else; a consumer
// selects by name, which is unique across every group.
const (
	statusGroupDaemon     = "daemon"
	statusGroupDependency = "dependency_health"
	statusGroupJobs       = "jobs"
	statusGroupScheduler  = "scheduler"
	statusGroupEmbed      = "embed"
	statusGroupConverge   = "converge"
	statusGroupMilvus     = "milvus"
	statusGroupRuntime    = "runtime"
	statusGroupCodebases  = "codebases"
	statusGroupActivity   = "activity"
)

// Units, each named for what its counter's increment call site actually counts.
// A converge upsert is one path and a skipped sync is one request, so neither
// borrows the other's noun.
const (
	unitSeconds     = "s"
	unitSlots       = "slots"
	unitJobs        = "jobs"
	unitRequests    = "requests"
	unitBatches     = "batches"
	unitVectors     = "vectors"
	unitChunks      = "chunks"
	unitPaths       = "paths"
	unitRuns        = "runs"
	unitFiles       = "files"
	unitRows        = "rows"
	unitGoroutines  = "goroutines"
	unitCycles      = "cycles"
	unitBytes       = "bytes"
	unitMillis      = "ms"
	unitPercent     = "%"
	unitCodebases   = "codebases"
	unitInputs      = "inputs"
	unitLoads       = "loads"
	unitUnloads     = "unloads"
	unitTimeouts    = "timeouts"
	unitLeases      = "leases"
	unitCollections = "collections"
	unitMigrations  = "migrations"
	// unitUnits counts units of work of mixed kinds: a job, a converge, and a
	// coalesced request are each one, and no narrower noun covers all three.
	unitUnits = "units"
)

// activitySourceJob and activitySourceWatcher name what started a unit of work.
// A watcher row has no job id, which is the difference worth reporting.
const (
	activitySourceJob     = "job"
	activitySourceWatcher = "watcher"
	activitySourcePending = "pending"
)

// intMetric builds one integer observation. Every builder goes through one of
// these constructors so a value always sets the oneof, which is what keeps zero
// and absent apart once protojson drops unset plain scalars.
func intMetric(group string, name string, value int64, unit string) *pb.Metric {
	return &pb.Metric{
		Group: group,
		Name:  name,
		Unit:  unit,
		Value: &pb.Metric_IntValue{IntValue: value},
	}
}

func doubleMetric(group string, name string, value float64, unit string) *pb.Metric {
	return &pb.Metric{
		Group: group,
		Name:  name,
		Unit:  unit,
		Value: &pb.Metric_DoubleValue{DoubleValue: value},
	}
}

func boolMetric(group string, name string, value bool) *pb.Metric {
	return &pb.Metric{
		Group: group,
		Name:  name,
		Unit:  "",
		Value: &pb.Metric_BoolValue{BoolValue: value},
	}
}

func stringMetric(group string, name string, value string) *pb.Metric {
	return &pb.Metric{
		Group: group,
		Name:  name,
		Unit:  "",
		Value: &pb.Metric_StringValue{StringValue: value},
	}
}

// absentMetric builds an observation with no value, which every surface renders
// as null. A file-change row uses it for job_id, because that work has no job
// and an empty id would read as one the job commands accept.
func absentMetric(group string, name string) *pb.Metric {
	return &pb.Metric{Group: group, Name: name, Unit: "", Value: nil}
}

// timeMetric renders a timestamp as UTC RFC3339, matching what protojson does
// with a Timestamp elsewhere in this service, or as absent when zero.
func timeMetric(group string, name string, value time.Time) *pb.Metric {
	if value.IsZero() {
		return absentMetric(group, name)
	}
	return stringMetric(group, name, value.UTC().Format(time.RFC3339Nano))
}

// protoTimeMetric renders an already-converted wire timestamp the same way, so
// a row built from a converted job and one built from a raw record agree.
func protoTimeMetric(group string, name string, value *timestamppb.Timestamp) *pb.Metric {
	if value == nil {
		return absentMetric(group, name)
	}
	return timeMetric(group, name, value.AsTime())
}

// safeInt64FromUint64 clamps a runtime byte gauge into the signed range the
// wire carries. A heap past nine exabytes is not reachable, so the clamp is a
// guard against a silent wrap rather than an expected path.
func safeInt64FromUint64(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(value)
}

// buildStatusMetrics projects the process counters and one daemon snapshot into
// the ordered metric list. daemon may be nil, which lets a unit test exercise
// the counter projection alone; the daemon-derived groups are then omitted
// rather than defaulted to a value nobody measured.
func buildStatusMetrics(daemon *StatusSnapshot, snapshot metrics.Snapshot, now time.Time) []*pb.Metric {
	list := make([]*pb.Metric, 0, 64)

	if daemon != nil {
		list = append(list, intMetric(statusGroupDaemon, "uptime_s",
			int64(now.Sub(daemon.StartedAt)/time.Second), unitSeconds))

		list = append(list,
			boolMetric(statusGroupDependency, "dependency_health.degraded", daemon.Health.Degraded()),
			stringMetric(statusGroupDependency, "dependency_health.mode", string(daemon.Health.Mode)),
			timeMetric(statusGroupDependency, "dependency_health.since", daemon.Health.Since),
			// The time reported beside the mode belongs to the dependency the mode
			// names, so this metric and the human banner cannot disagree about which
			// dependency they describe.
			timeMetric(statusGroupDependency, "dependency_health.last_healthy_at", daemon.Health.lastReachableAt()),
		)

		list = append(list,
			intMetric(statusGroupJobs, "index_slots_in_use", int64(schedulerRunningCount(daemon.Scheduler.Running)), unitSlots),
			intMetric(statusGroupJobs, "index_slots_total", int64(daemon.Scheduler.Capacity), unitSlots),
		)
	}

	list = append(list,
		intMetric(statusGroupJobs, "jobs_active", snapshot.JobsActive, unitJobs),
		intMetric(statusGroupJobs, "jobs_completed_total", snapshot.JobsCompletedTotal, unitJobs),
		intMetric(statusGroupJobs, "jobs_failed_total", snapshot.JobsFailedTotal, unitJobs),
		intMetric(statusGroupJobs, "jobs_cancelled_total", snapshot.JobsCancelledTotal, unitJobs),
		intMetric(statusGroupJobs, "boot_resumes_total", snapshot.BootResumesTotal, unitJobs),
		intMetric(statusGroupJobs, "sync_skipped_inflight_total", snapshot.SyncSkippedInflightTotal, unitRequests),

		intMetric(statusGroupEmbed, "embed_inflight", snapshot.EmbedInflight, unitBatches),
		intMetric(statusGroupEmbed, "embed_batches_total", snapshot.EmbedBatchesTotal, unitBatches),
		intMetric(statusGroupEmbed, "embed_batches_failed", snapshot.EmbedBatchesFailed, unitBatches),
		intMetric(statusGroupEmbed, "embed_vectors_total", snapshot.EmbedVectorsTotal, unitVectors),
		intMetric(statusGroupEmbed, "embed_latency_ms_sum", snapshot.EmbedLatencyMSSum, unitMillis),
		intMetric(statusGroupEmbed, "embed_chunks_reused_total", snapshot.EmbedChunksReusedTotal, unitChunks),
		intMetric(statusGroupEmbed, "embed_inputs_refused_empty", snapshot.EmbedInputsRefusedEmpty, unitInputs),

		intMetric(statusGroupConverge, "converge_upsert_total", snapshot.ConvergeUpsertTotal, unitPaths),
		intMetric(statusGroupConverge, "converge_remove_total", snapshot.ConvergeRemoveTotal, unitPaths),
		intMetric(statusGroupConverge, "converge_copy_chunks_total", snapshot.ConvergeCopyChunksTotal, unitPaths),
		intMetric(statusGroupConverge, "sweep_runs_total", snapshot.SweepRunsTotal, unitRuns),
		intMetric(statusGroupConverge, "sweep_changed_total", snapshot.SweepChangedTotal, unitRuns),

		intMetric(statusGroupMilvus, "milvus_collection_loads_total", snapshot.MilvusCollectionLoadsTotal, unitLoads),
		intMetric(statusGroupMilvus, "milvus_collection_load_failures_total", snapshot.MilvusCollectionLoadFailuresTotal, unitLoads),
		intMetric(statusGroupMilvus, "milvus_collection_load_wait_timeouts_total", snapshot.MilvusCollectionLoadWaitTimeoutsTotal, unitTimeouts),
		intMetric(statusGroupMilvus, "milvus_collection_load_inflight", snapshot.MilvusCollectionLoadInflight, unitLoads),
		intMetric(statusGroupMilvus, "milvus_collection_load_latency_ms_sum", snapshot.MilvusCollectionLoadLatencyMSSum, unitMillis),
		intMetric(statusGroupMilvus, "milvus_collection_unloads_total", snapshot.MilvusCollectionUnloadsTotal, unitUnloads),
		intMetric(statusGroupMilvus, "milvus_collection_unload_failures_total", snapshot.MilvusCollectionUnloadFailuresTotal, unitUnloads),
		intMetric(statusGroupMilvus, "milvus_collection_unload_skipped_in_use_total", snapshot.MilvusCollectionUnloadSkippedInUseTotal, unitUnloads),
		intMetric(statusGroupMilvus, "milvus_collection_unload_latency_ms_sum", snapshot.MilvusCollectionUnloadLatencyMSSum, unitMillis),
		intMetric(statusGroupMilvus, "milvus_collection_leases_active", snapshot.MilvusCollectionLeasesActive, unitLeases),
		intMetric(statusGroupMilvus, "milvus_collections_idle", snapshot.MilvusCollectionsIdle, unitCollections),
		intMetric(statusGroupMilvus, "milvus_collections_loading", snapshot.MilvusCollectionsLoading, unitCollections),
		intMetric(statusGroupMilvus, "milvus_collections_ready", snapshot.MilvusCollectionsReady, unitCollections),
		intMetric(statusGroupMilvus, "milvus_mmap_migrations_total", snapshot.MilvusMmapMigrationsTotal, unitMigrations),
		intMetric(statusGroupMilvus, "milvus_mmap_migration_failures_total", snapshot.MilvusMmapMigrationFailuresTotal, unitMigrations),
	)

	list = append(list, runtimeMetrics()...)

	if daemon != nil {
		list = append(list, schedulerPriorityMetrics(daemon.Scheduler)...)
		list = append(list, codebaseMetrics(daemon.Codebases)...)
		list = append(list, activitySourceMetrics(daemon.Scheduler.Activity)...)
		list = append(list, activityCountMetrics(daemon)...)
	}
	return list
}

var schedulerPriorities = []model.JobPriority{
	model.JobPriorityHigh,
	model.JobPriorityNormal,
	model.JobPriorityLow,
}

func schedulerPriorityMetrics(snapshot jobscheduler.Snapshot) []*pb.Metric {
	list := make([]*pb.Metric, 0, len(schedulerPriorities)*3)
	for _, priority := range schedulerPriorities {
		suffix := ".priority=" + string(priority)
		list = append(list,
			intMetric(statusGroupScheduler, "scheduler.running"+suffix, int64(snapshot.Running[priority]), unitJobs),
			intMetric(statusGroupScheduler, "scheduler.queued"+suffix, int64(snapshot.Queued[priority]), unitJobs),
			intMetric(statusGroupScheduler, "scheduler.paused"+suffix, int64(snapshot.Paused[priority]), unitJobs),
		)
	}
	return list
}

func schedulerRunningCount(counts map[model.JobPriority]int) int {
	total := 0
	for _, priority := range schedulerPriorities {
		total += counts[priority]
	}
	return total
}

func activitySourceMetrics(snapshot platformactivity.Snapshot) []*pb.Metric {
	return []*pb.Metric{
		boolMetric(statusGroupActivity, "activity.input_available", snapshot.InputAvailable),
		boolMetric(statusGroupActivity, "activity.thermal_available", snapshot.ThermalAvailable),
		boolMetric(statusGroupActivity, "activity.thermal_unsafe", snapshot.ThermalUnsafe),
	}
}

// activityCountMetrics summarises the rows so a reader sees how much work is
// outstanding without counting rows, and so a piped consumer can watch the
// queue drain with one grep. The counts come from the same snapshot the rows
// come from, so a summary and its rows cannot disagree.
//
// Registered jobs carry a lifecycle state; watcher and pending entries carry
// the watcher vocabulary. Anything else counts as running, because work the
// daemon reported but this function does not recognise is still work.
func activityCountMetrics(daemon *StatusSnapshot) []*pb.Metric {
	running := 0
	queued := 0
	// Preserve the legacy aggregate: it counted every non-queued job as running
	// before paused had its own per-priority metric.
	for _, job := range daemon.ActiveJobs {
		if job.State == model.JobStateQueued {
			queued++
		} else {
			running++
		}
	}
	for _, entry := range daemon.Watcher {
		if entry.State == WatcherStateQueued {
			queued++
		} else {
			running++
		}
	}
	// A pending slot is by definition waiting on its codebase's active job.
	queued += len(daemon.Pending)

	return []*pb.Metric{
		intMetric(statusGroupActivity, "activity.running", int64(running), unitUnits),
		intMetric(statusGroupActivity, "activity.queued", int64(queued), unitUnits),
	}
}

// runtimeMetrics samples the Go runtime gauges the periodic
// daemon.perf_counters line already carries, from one ReadMemStats so the heap
// numbers are internally consistent.
func runtimeMetrics() []*pb.Metric {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return []*pb.Metric{
		intMetric(statusGroupRuntime, "num_goroutine", int64(runtime.NumGoroutine()), unitGoroutines),
		intMetric(statusGroupRuntime, "heap_alloc_bytes", safeInt64FromUint64(mem.HeapAlloc), unitBytes),
		intMetric(statusGroupRuntime, "heap_inuse_bytes", safeInt64FromUint64(mem.HeapInuse), unitBytes),
		intMetric(statusGroupRuntime, "num_gc", int64(mem.NumGC), unitCycles),
	}
}

// everyDisplayStatus is the full display vocabulary, so a count line exists for
// each status whether or not any codebase holds it right now. A line that only
// appeared when its count was positive would vanish at zero rather than report
// zero, which reads as the category never having existed, and the changing set
// of names would shift the column widths of every other line between two reads.
var everyDisplayStatus = []displayStatus{
	displayPreparing,
	displayIndexing,
	displayIndexed,
	displayQuarantined,
	displayWaiting,
	displayStale,
	displayFailed,
	displayMissing,
	displayDiscovered,
	displayPending,
	displayLoading,
}

// codebaseMetrics counts tracked codebases by the display status the manager
// resolved, so this file reports a status and never decides one. A status the
// vocabulary does not know still reports, appended after the known ones, rather
// than being counted into the total and then dropped.
func codebaseMetrics(views []CodebaseView) []*pb.Metric {
	counts := make(map[string]int, len(views))
	for _, codebaseView := range views {
		counts[string(codebaseView.Display)]++
	}

	known := make(map[string]bool, len(everyDisplayStatus))
	list := make([]*pb.Metric, 0, len(everyDisplayStatus)+len(counts)+1)
	list = append(list, intMetric(statusGroupCodebases, "codebases_total", int64(len(views)), unitCodebases))
	for _, display := range everyDisplayStatus {
		name := string(display)
		known[name] = true
		list = append(list, intMetric(statusGroupCodebases, "codebases.status="+name, int64(counts[name]), unitCodebases))
	}

	unknown := make([]string, 0)
	for name := range counts {
		if !known[name] {
			unknown = append(unknown, name)
		}
	}
	sort.Strings(unknown)
	for _, name := range unknown {
		list = append(list, intMetric(statusGroupCodebases, "codebases.status="+name, int64(counts[name]), unitCodebases))
	}
	return list
}

// buildStatusActivity reports every unit of work the daemon is doing or has
// queued, from all three sources in one snapshot. Registered jobs come from the
// job store; file-change admission and queued paths come from the syncer until a
// converge job exists; coalesced requests come from the manager's depth-1 slots,
// which hold no job record until they drain.
//
// Every source is read at one instant, so a slot that drains into a job cannot
// fall between two reads and vanish, and cannot appear twice either.
func buildStatusActivity(daemon *StatusSnapshot) []*pb.ActivityRow {
	if daemon == nil {
		return nil
	}
	rows := make([]*pb.ActivityRow, 0, len(daemon.ActiveJobs)+len(daemon.Watcher)+len(daemon.Pending))
	for _, job := range daemon.ActiveJobs {
		rows = append(rows, jobActivityRow(job))
	}

	paths := daemon.CanonicalPaths()
	for _, entry := range daemon.Watcher {
		rows = append(rows, watcherActivityRow(entry, paths[entry.CodebaseID]))
	}
	for _, entry := range daemon.Pending {
		rows = append(rows, pendingActivityRow(entry))
	}
	return rows
}

// jobActivityRow projects one live job. It converts through pbconv first so the
// derived wire fields, trigger among them, come from the one converter every
// other surface uses rather than from a second derivation here.
func jobActivityRow(job model.Job) *pb.ActivityRow {
	wire := pbconv.ToJob(job)
	progress := wire.GetProgress()
	scheduling := pbconv.SchedulingFromProto(
		wire.GetEffectiveSchedulingPolicy(),
		wire.GetState(),
		wire.GetSchedulingReason(),
	)
	return &pb.ActivityRow{Metrics: []*pb.Metric{
		stringMetric(statusGroupActivity, "job_id", wire.GetId()),
		stringMetric(statusGroupActivity, "source", activitySourceJob),
		stringMetric(statusGroupActivity, "codebase_id", wire.GetCodebaseId()),
		stringMetric(statusGroupActivity, "canonical_path", wire.GetCanonicalPath()),
		stringMetric(statusGroupActivity, "operation", wire.GetOperation()),
		stringMetric(statusGroupActivity, "state", wire.GetState()),
		stringMetric(statusGroupActivity, "priority", scheduling.Priority),
		boolMetric(statusGroupActivity, "quiet", scheduling.Quiet),
		intMetric(
			statusGroupActivity,
			"idle_after_seconds",
			int64(wire.GetEffectiveSchedulingPolicy().GetIdleAfterSeconds()),
			unitSeconds,
		),
		stringMetric(statusGroupActivity, "trigger", wire.GetTrigger()),
		boolMetric(statusGroupActivity, "forced", wire.GetForced()),
		stringMetric(statusGroupActivity, "phase", progress.GetPhase()),
		doubleMetric(statusGroupActivity, "overall_percent", progress.GetOverallPercent(), unitPercent),
		doubleMetric(statusGroupActivity, "phase_percent", progress.GetPhasePercent(), unitPercent),
		intMetric(statusGroupActivity, "files_processed", int64(progress.GetFilesProcessed()), unitFiles),
		intMetric(statusGroupActivity, "files_total", int64(progress.GetFilesTotal()), unitFiles),
		intMetric(statusGroupActivity, "chunks_generated", int64(progress.GetChunksGenerated()), unitChunks),
		intMetric(statusGroupActivity, "chunks_embedded", int64(progress.GetChunksEmbedded()), unitChunks),
		intMetric(statusGroupActivity, "chunks_reused", int64(progress.GetChunksReused()), unitChunks),
		intMetric(statusGroupActivity, "chunks_dropped", int64(progress.GetChunksDropped()), unitChunks),
		intMetric(statusGroupActivity, "reuse_vectors_loaded", int64(progress.GetReuseVectorsLoaded()), unitVectors),
		intMetric(statusGroupActivity, "collection_rows_written", int64(progress.GetCollectionRowsWritten()), unitRows),
		intMetric(statusGroupActivity, "embedding_batches_completed", int64(progress.GetEmbeddingBatchesCompleted()), unitBatches),
		intMetric(statusGroupActivity, "embedding_batches_total", int64(progress.GetEmbeddingBatchesTotal()), unitBatches),
		protoTimeMetric(statusGroupActivity, "started_at", wire.GetStartedAt()),
		protoTimeMetric(statusGroupActivity, "last_event_at", progress.GetLastEventAt()),
		protoTimeMetric(statusGroupActivity, "heartbeat_at", progress.GetHeartbeatAt()),
	}}
}

// pendingActivityRow projects one coalesced request waiting on its codebase's
// active job. It carries an absent job_id for the same reason a watcher row
// does: the depth-1 slot only becomes a job when it drains, so the job commands
// cannot address it yet.
func pendingActivityRow(entry PendingWork) *pb.ActivityRow {
	return &pb.ActivityRow{Metrics: []*pb.Metric{
		absentMetric(statusGroupActivity, "job_id"),
		stringMetric(statusGroupActivity, "source", activitySourcePending),
		stringMetric(statusGroupActivity, "codebase_id", entry.CodebaseID),
		stringMetric(statusGroupActivity, "canonical_path", entry.CanonicalPath),
		stringMetric(statusGroupActivity, "operation", entry.Operation),
		stringMetric(statusGroupActivity, "state", WatcherStateQueued),
	}}
}

// watcherActivityRow projects file-change work that has no registered converge
// job yet. It carries an absent job_id rather than an empty string, because an
// empty id would read as one the job commands could accept.
func watcherActivityRow(entry WatcherActivity, canonicalPath string) *pb.ActivityRow {
	return &pb.ActivityRow{Metrics: []*pb.Metric{
		absentMetric(statusGroupActivity, "job_id"),
		stringMetric(statusGroupActivity, "source", activitySourceWatcher),
		stringMetric(statusGroupActivity, "codebase_id", entry.CodebaseID),
		stringMetric(statusGroupActivity, "canonical_path", canonicalPath),
		stringMetric(statusGroupActivity, "operation", "converge"),
		stringMetric(statusGroupActivity, "state", entry.State),
		intMetric(statusGroupActivity, "pending_paths", int64(entry.PendingPaths), unitPaths),
		timeMetric(statusGroupActivity, "started_at", entry.StartedAt),
	}}
}
