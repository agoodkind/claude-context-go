package daemon

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/jobscheduler"
	"goodkind.io/lm-semantic-search/internal/metrics"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/platformactivity"
)

// metricByName finds one assembled metric so a test asserts on the name an
// operator reads rather than on a slice position.
func metricByName(list []*pb.Metric, name string) *pb.Metric {
	for _, metric := range list {
		if metric.GetName() == name {
			return metric
		}
	}
	return nil
}

// TestStatusMetricsCarryCounterNamesAndUnits proves the assembled list uses the
// counter identifiers verbatim, so a name on screen greps the daemon log, and
// that each carries the unit of what its increment call site counts.
func TestStatusMetricsCarryCounterNamesAndUnits(t *testing.T) {
	t.Parallel()

	snapshot := metrics.Snapshot{
		EmbedVectorsTotal:        3946,
		EmbedChunksReusedTotal:   4025,
		ConvergeUpsertTotal:      843,
		SyncSkippedInflightTotal: 1524,
	}
	list := buildStatusMetrics(nil, snapshot, time.Unix(1785156767, 0))

	cases := []struct {
		name  string
		value int64
		unit  string
	}{
		{"embed_vectors_total", 3946, unitVectors},
		{"embed_chunks_reused_total", 4025, unitChunks},
		{"converge_upsert_total", 843, unitPaths},
		{"sync_skipped_inflight_total", 1524, unitRequests},
	}
	for _, testCase := range cases {
		metric := metricByName(list, testCase.name)
		if metric == nil {
			t.Fatalf("%s absent from %d metrics", testCase.name, len(list))
		}
		if metric.GetIntValue() != testCase.value {
			t.Fatalf("%s = %d, want %d", testCase.name, metric.GetIntValue(), testCase.value)
		}
		if metric.GetUnit() != testCase.unit {
			t.Fatalf("%s unit = %q, want %q", testCase.name, metric.GetUnit(), testCase.unit)
		}
	}
}

func TestStatusMetricsExposeMilvusCollectionMetrics(t *testing.T) {
	t.Parallel()

	snapshot := metrics.Snapshot{
		MilvusCollectionLoadsTotal:              1,
		MilvusCollectionLoadFailuresTotal:       2,
		MilvusCollectionLoadWaitTimeoutsTotal:   3,
		MilvusCollectionLoadInflight:            4,
		MilvusCollectionLoadLatencyMSSum:        5,
		MilvusCollectionUnloadsTotal:            6,
		MilvusCollectionUnloadFailuresTotal:     7,
		MilvusCollectionUnloadSkippedInUseTotal: 8,
		MilvusCollectionUnloadLatencyMSSum:      9,
		MilvusCollectionLeasesActive:            10,
		MilvusCollectionsIdle:                   11,
		MilvusCollectionsLoading:                12,
		MilvusCollectionsReady:                  13,
		MilvusMmapMigrationsTotal:               14,
		MilvusMmapMigrationFailuresTotal:        15,
	}
	list := buildStatusMetrics(nil, snapshot, time.Unix(1785156767, 0))
	cases := []struct {
		name string
		unit string
	}{
		{"milvus_collection_loads_total", unitLoads},
		{"milvus_collection_load_failures_total", unitLoads},
		{"milvus_collection_load_wait_timeouts_total", unitTimeouts},
		{"milvus_collection_load_inflight", unitLoads},
		{"milvus_collection_load_latency_ms_sum", unitMillis},
		{"milvus_collection_unloads_total", unitUnloads},
		{"milvus_collection_unload_failures_total", unitUnloads},
		{"milvus_collection_unload_skipped_in_use_total", unitUnloads},
		{"milvus_collection_unload_latency_ms_sum", unitMillis},
		{"milvus_collection_leases_active", unitLeases},
		{"milvus_collections_idle", unitCollections},
		{"milvus_collections_loading", unitCollections},
		{"milvus_collections_ready", unitCollections},
		{"milvus_mmap_migrations_total", unitMigrations},
		{"milvus_mmap_migration_failures_total", unitMigrations},
	}
	for index, testCase := range cases {
		metric := metricByName(list, testCase.name)
		if metric == nil {
			t.Fatalf("%s absent", testCase.name)
		}
		if metric.GetGroup() != statusGroupMilvus {
			t.Errorf("%s group = %q, want %q", testCase.name, metric.GetGroup(), statusGroupMilvus)
		}
		if metric.GetUnit() != testCase.unit {
			t.Errorf("%s unit = %q, want %q", testCase.name, metric.GetUnit(), testCase.unit)
		}
		if metric.GetIntValue() != int64(index+1) {
			t.Errorf("%s = %d, want %d", testCase.name, metric.GetIntValue(), index+1)
		}
	}
}

// TestStatusMetricsSetZeroValuedOneofs proves a zero counter still carries a
// value. protojson omits a plain proto3 scalar at its zero value, so a metric
// whose oneof went unset would be indistinguishable from an absent measurement
// once it reached JSON.
func TestStatusMetricsSetZeroValuedOneofs(t *testing.T) {
	t.Parallel()

	list := buildStatusMetrics(nil, metrics.Snapshot{}, time.Unix(1785156767, 0))
	for _, name := range []string{"embed_batches_failed", "jobs_active", "converge_remove_total"} {
		metric := metricByName(list, name)
		if metric == nil {
			t.Fatalf("%s absent", name)
		}
		if metric.GetValue() == nil {
			t.Fatalf("%s carries no value; zero and absent must stay distinguishable", name)
		}
		if metric.GetIntValue() != 0 {
			t.Fatalf("%s = %d, want 0", name, metric.GetIntValue())
		}
	}
}

// TestStatusMetricsOmitManagerGroupsWithoutAManager proves the counter
// projection stands alone, so a caller with no manager reports no uptime and no
// slot count rather than defaulting to numbers nobody measured.
func TestStatusMetricsOmitManagerGroupsWithoutAManager(t *testing.T) {
	t.Parallel()

	list := buildStatusMetrics(nil, metrics.Snapshot{}, time.Unix(1785156767, 0))
	for _, name := range []string{"uptime_s", "index_slots_total", "codebases_total"} {
		if metricByName(list, name) != nil {
			t.Fatalf("%s present with no manager, want omitted", name)
		}
	}
}

func TestActivityPriorityMetricsExposeCachedSchedulerState(t *testing.T) {
	t.Parallel()

	scheduler := jobscheduler.Snapshot{
		Capacity: 4,
		Running: map[model.JobPriority]int{
			model.JobPriorityHigh:   1,
			model.JobPriorityNormal: 1,
			model.JobPriorityLow:    1,
		},
		Queued: map[model.JobPriority]int{
			model.JobPriorityHigh:   2,
			model.JobPriorityNormal: 3,
			model.JobPriorityLow:    4,
		},
		Paused: map[model.JobPriority]int{
			model.JobPriorityHigh:   5,
			model.JobPriorityNormal: 6,
			model.JobPriorityLow:    7,
		},
		Activity: platformactivity.Snapshot{
			InputAvailable:   true,
			ThermalAvailable: true,
			ThermalUnsafe:    true,
		},
	}
	daemon := &StatusSnapshot{Scheduler: scheduler}
	list := buildStatusMetrics(daemon, metrics.Snapshot{}, time.Unix(1785156767, 0))

	integerCases := map[string]int64{
		"index_slots_in_use":                3,
		"index_slots_total":                 4,
		"scheduler.running.priority=high":   1,
		"scheduler.running.priority=normal": 1,
		"scheduler.running.priority=low":    1,
		"scheduler.queued.priority=high":    2,
		"scheduler.queued.priority=normal":  3,
		"scheduler.queued.priority=low":     4,
		"scheduler.paused.priority=high":    5,
		"scheduler.paused.priority=normal":  6,
		"scheduler.paused.priority=low":     7,
	}
	for name, want := range integerCases {
		metric := metricByName(list, name)
		if metric == nil {
			t.Fatalf("metric %q is absent", name)
		}
		if metric.GetIntValue() != want {
			t.Fatalf("%s = %d, want %d", name, metric.GetIntValue(), want)
		}
	}

	booleanCases := map[string]bool{
		"activity.input_available":   true,
		"activity.thermal_available": true,
		"activity.thermal_unsafe":    true,
	}
	for name, want := range booleanCases {
		metric := metricByName(list, name)
		if metric == nil {
			t.Fatalf("metric %q is absent", name)
		}
		if metric.GetBoolValue() != want {
			t.Fatalf("%s = %v, want %v", name, metric.GetBoolValue(), want)
		}
	}
	for _, metric := range list {
		if metric.GetGroup() != statusGroupActivity {
			continue
		}
		name := metric.GetName()
		for _, forbidden := range []string{"event", "session", "path"} {
			if strings.Contains(name, forbidden) {
				t.Fatalf("activity-source metric %q exposes %q", name, forbidden)
			}
		}
	}
}

type statusActivitySource struct {
	samples  atomic.Int32
	snapshot platformactivity.Snapshot
}

func (source *statusActivitySource) Sample(
	context.Context,
) platformactivity.Snapshot {
	source.samples.Add(1)
	return source.snapshot
}

func (*statusActivitySource) Close() {}

func TestActivityPriorityMetricsUseCachedActivitySnapshot(t *testing.T) {
	source := &statusActivitySource{snapshot: platformactivity.Snapshot{
		InputAvailable:   true,
		ThermalAvailable: true,
	}}
	scheduler := jobscheduler.New(context.Background(), 1, source)
	manager := &Manager{
		codebases: map[string]model.Codebase{},
		jobs: map[string]model.Job{
			"job-queued": {ID: "job-queued", State: model.JobStateQueued},
		},
		pendingCodeJobs:         map[string]pendingCodeRequest{},
		pendingConversationJobs: map[string]conversationJobPayload{},
		jobScheduler:            scheduler,
	}

	snapshot := manager.StatusSnapshot()
	_ = buildStatusMetrics(&snapshot, metrics.Snapshot{}, time.Now())
	_, _ = manager.GetJob("job-queued")
	_ = manager.ListJobs("")
	scheduler.Close()
	if samples := source.samples.Load(); samples != 1 {
		t.Fatalf("platform samples after status read = %d, want 1", samples)
	}
}

func TestActivityPriorityMetricsKeepCompatibilityCounts(t *testing.T) {
	t.Parallel()

	daemon := &StatusSnapshot{ActiveJobs: []model.Job{
		{State: model.JobStateRunning},
		{State: model.JobStateQueued},
		{State: model.JobStatePaused},
	}}
	list := activityCountMetrics(daemon)
	if got := metricByName(list, "activity.running").GetIntValue(); got != 2 {
		t.Fatalf("activity.running = %d, want legacy count 2", got)
	}
	if got := metricByName(list, "activity.queued").GetIntValue(); got != 1 {
		t.Fatalf("activity.queued = %d, want legacy count 1", got)
	}
}

func TestSchedulingActivityRowUsesStructuredPolicyWithoutReasonText(t *testing.T) {
	t.Parallel()

	row := jobActivityRow(model.Job{
		ID:    "job-paused",
		State: model.JobStatePaused,
		EffectiveSchedulingPolicy: model.SchedulingPolicy{
			Priority:         model.JobPriorityLow,
			Quiet:            true,
			IdleAfterSeconds: 600,
		},
		SchedulingReason: model.SchedulingReasonActivityUnavailable,
	})
	metrics := row.GetMetrics()
	if got := metricByName(metrics, "priority").GetStringValue(); got != "low" {
		t.Fatalf("priority = %q, want low", got)
	}
	if got := metricByName(metrics, "quiet").GetBoolValue(); !got {
		t.Fatal("quiet = false, want true")
	}
	if got := metricByName(metrics, "idle_after_seconds").GetIntValue(); got != 600 {
		t.Fatalf("idle_after_seconds = %d, want 600", got)
	}
	if got := metricByName(metrics, "scheduling_reason"); got != nil {
		t.Fatalf("scheduling_reason metric = %+v, want enum-only job field", got)
	}
}

// TestWatcherActivityRowCarriesAbsentJobID proves a file-change row states it
// has no job, rather than carrying an empty id a reader could mistake for one
// the job commands accept.
func TestWatcherActivityRowCarriesAbsentJobID(t *testing.T) {
	t.Parallel()

	row := watcherActivityRow(WatcherActivity{
		CodebaseID:   "cb-1",
		State:        WatcherStateQueued,
		PendingPaths: 12,
	}, "/Users/agoodkind/Sites/configs")

	jobID := metricByName(row.GetMetrics(), "job_id")
	if jobID == nil {
		t.Fatal("job_id absent from the row entirely; it must be present with no value")
	}
	if jobID.GetValue() != nil {
		t.Fatalf("job_id carries a value %+v, want none", jobID.GetValue())
	}

	source := metricByName(row.GetMetrics(), "source")
	if source.GetStringValue() != activitySourceWatcher {
		t.Fatalf("source = %q, want %q", source.GetStringValue(), activitySourceWatcher)
	}

	pending := metricByName(row.GetMetrics(), "pending_paths")
	if pending.GetIntValue() != 12 || pending.GetUnit() != unitPaths {
		t.Fatalf("pending_paths = %+v, want 12 paths", pending)
	}

	startedAt := metricByName(row.GetMetrics(), "started_at")
	if startedAt.GetValue() != nil {
		t.Fatalf("started_at carries a value for queued work that has not begun: %+v", startedAt.GetValue())
	}
}

// TestCodebaseMetricsEmitEveryStatusEvenAtZero proves a status with no
// codebases still reports a line. A line that appeared only when its count was
// positive would vanish at zero rather than report zero, which reads as the
// category never having existed, and the changing name set would shift the
// value column of every other line between two reads.
func TestCodebaseMetricsEmitEveryStatusEvenAtZero(t *testing.T) {
	t.Parallel()

	list := buildStatusMetrics(nil, metrics.Snapshot{}, time.Unix(1785156767, 0))
	if metricByName(list, "codebases.status=indexing") != nil {
		t.Fatal("codebase metrics present with no snapshot, want omitted")
	}

	// With no codebases tracked at all, every status still reports zero.
	withManager := codebaseMetrics(nil)
	for _, display := range everyDisplayStatus {
		name := "codebases.status=" + string(display)
		metric := metricByName(withManager, name)
		if metric == nil {
			t.Fatalf("%s absent; a status must report zero rather than vanish", name)
		}
		if metric.GetValue() == nil {
			t.Fatalf("%s carries no value, so zero and absent collapse", name)
		}
		if metric.GetIntValue() != 0 {
			t.Fatalf("%s = %d, want 0", name, metric.GetIntValue())
		}
	}
}

// TestPendingActivityRowNamesTheAbsentJob proves a coalesced request waiting on
// its codebase's active job reports as queued work with no job id, so it is
// visible rather than silently outstanding until it drains.
func TestPendingActivityRowNamesTheAbsentJob(t *testing.T) {
	t.Parallel()

	row := pendingActivityRow(PendingWork{
		CodebaseID:    "cb-1",
		CanonicalPath: "/Users/agoodkind/Sites/configs",
		Operation:     pendingOperationSync,
	})

	jobID := metricByName(row.GetMetrics(), "job_id")
	if jobID == nil || jobID.GetValue() != nil {
		t.Fatalf("job_id = %+v, want present with no value", jobID)
	}
	source := metricByName(row.GetMetrics(), "source")
	if source.GetStringValue() != activitySourcePending {
		t.Fatalf("source = %q, want %q", source.GetStringValue(), activitySourcePending)
	}
	state := metricByName(row.GetMetrics(), "state")
	if state.GetStringValue() != WatcherStateQueued {
		t.Fatalf("state = %q, want %q", state.GetStringValue(), WatcherStateQueued)
	}
}

// TestPendingWorkReportsBothSlotKinds proves both depth-1 slots reach a status
// read, since a code sync and a conversation upsert are stored in separate maps
// and reporting only one would hide the other.
func TestPendingWorkReportsBothSlotKinds(t *testing.T) {
	t.Parallel()

	manager := &Manager{
		codebases: map[string]model.Codebase{
			"cb-code": {ID: "cb-code", CanonicalPath: "/code"},
			"cb-conv": {ID: "cb-conv", CanonicalPath: "/conv"},
		},
		pendingCodeJobs:         map[string]pendingCodeRequest{"cb-code": {}},
		pendingConversationJobs: map[string]conversationJobPayload{"cb-conv": {}},
	}

	pending := manager.PendingWork()
	if len(pending) != 2 {
		t.Fatalf("pending work = %d rows, want 2: %+v", len(pending), pending)
	}
	byID := map[string]PendingWork{}
	for _, entry := range pending {
		byID[entry.CodebaseID] = entry
	}
	if byID["cb-code"].Operation != pendingOperationSync {
		t.Fatalf("code slot operation = %q, want %q", byID["cb-code"].Operation, pendingOperationSync)
	}
	if byID["cb-conv"].Operation != pendingOperationConversationIngest {
		t.Fatalf("conversation slot operation = %q, want %q",
			byID["cb-conv"].Operation, pendingOperationConversationIngest)
	}
	if byID["cb-code"].CanonicalPath != "/code" {
		t.Fatalf("code slot path = %q, want /code", byID["cb-code"].CanonicalPath)
	}
}
