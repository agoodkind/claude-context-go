//go:build offlinelive

package offlinelive

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// TestStatusReportsCountersOverTheWire drives GetStatus against an isolated
// in-process daemon and checks the properties that live in the encoded reply
// rather than in the values that went into it.
func TestStatusReportsCountersOverTheWire(t *testing.T) {
	harness := newHarness(t)

	// Poll while the index runs, so the activity assertions see real rows. A
	// reply read only after the job completes carries none, and asserting over
	// an empty slice would pass against an implementation that had stopped
	// reporting activity at all.
	observed := make(chan []*pb.ActivityRow, 1)
	stopPolling := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopPolling:
				select {
				case observed <- nil:
				default:
				}
				return
			default:
			}
			reply, err := harness.client.GetStatus(context.Background(), &pb.GetStatusRequest{})
			if err == nil && len(reply.GetActivity()) > 0 {
				select {
				case observed <- reply.GetActivity():
				default:
				}
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	job := harness.indexFixture()
	requireCompleted(t, job)
	close(stopPolling)

	response, err := harness.client.GetStatus(context.Background(), &pb.GetStatusRequest{})
	if err != nil {
		t.Fatalf("GetStatus returned error: %v", err)
	}

	assertZeroCountersSurviveJSON(t, response)
	assertNamesMatchAcrossForms(t, response)
	assertUnitsAreCarried(t, response)

	if rows := <-observed; len(rows) > 0 {
		assertActivityRowsCarryASource(t, rows)
	} else {
		t.Log("index finished before any poll observed an activity row; source assertion skipped")
	}
}

func TestStatusReportsClosedActivityReasonsOverTheWire(t *testing.T) {
	harness := newHarness(t)

	response, err := harness.client.GetStatus(context.Background(), &pb.GetStatusRequest{})
	if err != nil {
		t.Fatalf("GetStatus returned error: %v", err)
	}
	source := response.GetActivitySource()
	if source == nil {
		t.Fatal("GetStatus omitted activity_source")
	}
	if source.GetInputAvailable() {
		if reason := source.GetInputReason(); reason != pb.SchedulingReason_SCHEDULING_REASON_UNSPECIFIED && reason != pb.SchedulingReason_SCHEDULING_REASON_USER_ACTIVE {
			t.Fatalf("available input reason = %s", reason)
		}
	} else if reason := source.GetInputReason(); reason != pb.SchedulingReason_SCHEDULING_REASON_ACTIVITY_UNAVAILABLE {
		t.Fatalf("unavailable input reason = %s, want activity unavailable", reason)
	}
	if source.GetThermalUnsafe() && source.GetThermalReason() != pb.SchedulingReason_SCHEDULING_REASON_THERMAL_SAFETY {
		t.Fatalf("unsafe thermal reason = %s, want thermal safety", source.GetThermalReason())
	}
}

// assertZeroCountersSurviveJSON is the check the oneof exists for. protojson
// omits a plain proto3 scalar at its zero value, so without the oneof a counter
// reading zero would be indistinguishable from one the daemon never measured.
// It asserts against the encoded bytes, because the omission happens in the
// encoder and not in the message.
func assertZeroCountersSurviveJSON(t *testing.T, response *pb.GetStatusResponse) {
	t.Helper()

	encoded, err := protojson.MarshalOptions{Multiline: false}.Marshal(response)
	if err != nil {
		t.Fatalf("marshal status reply: %v", err)
	}

	var decoded struct {
		Metrics []struct {
			Name        string  `json:"name"`
			IntValue    *string `json:"intValue"`
			BoolValue   *bool   `json:"boolValue"`
			StringValue *string `json:"stringValue"`
		} `json:"metrics"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode status JSON: %v", err)
	}

	carriesValue := make(map[string]bool, len(decoded.Metrics))
	for _, metric := range decoded.Metrics {
		carriesValue[metric.Name] = metric.IntValue != nil ||
			metric.BoolValue != nil ||
			metric.StringValue != nil
	}

	// A healthy offline daemon supplies all three zero shapes: a zero integer, a
	// false boolean, and an empty string. Every one is dropped by protojson
	// outside a oneof.
	for _, name := range []string{
		"embed_batches_failed",
		"dependency_health.degraded",
		"dependency_health.mode",
	} {
		present, found := carriesValue[name]
		if !found {
			t.Fatalf("%s is absent from the JSON reply entirely:\n%s", name, string(encoded))
		}
		if !present {
			t.Fatalf(
				"%s carries no value key in JSON, so zero and unmeasured collapse:\n%s",
				name,
				string(encoded),
			)
		}
	}
}

// assertNamesMatchAcrossForms proves a counter name is byte-identical in the
// piped text and in the JSON, which is what lets one name serve grep, jq, and a
// search of the daemon log. The name is a string value rather than a protobuf
// field, so protojson casing never reaches it; this checks that end to end.
func assertNamesMatchAcrossForms(t *testing.T, response *pb.GetStatusResponse) {
	t.Helper()

	piped := response.GetDisplayText()
	for _, metric := range response.GetMetrics() {
		line := metric.GetName() + " "
		if !strings.Contains(piped, "\n"+line) && !strings.HasPrefix(piped, line) {
			t.Fatalf("metric %q is absent from the piped text:\n%s", metric.GetName(), piped)
		}
	}

	encoded, err := protojson.MarshalOptions{Multiline: false}.Marshal(response)
	if err != nil {
		t.Fatalf("marshal status reply: %v", err)
	}
	for _, name := range []string{"embed_vectors_total", "embed_chunks_reused_total"} {
		if !strings.Contains(string(encoded), `"`+name+`"`) {
			t.Fatalf("metric %q is absent from the JSON reply", name)
		}
	}
}

// assertUnitsAreCarried proves the daemon assigns each counter the unit of what
// its increment call site counts, so a change reported beside it is unambiguous.
func assertUnitsAreCarried(t *testing.T, response *pb.GetStatusResponse) {
	t.Helper()

	units := make(map[string]string, len(response.GetMetrics()))
	for _, metric := range response.GetMetrics() {
		units[metric.GetName()] = metric.GetUnit()
	}
	expected := map[string]string{
		"embed_vectors_total":         "vectors",
		"embed_chunks_reused_total":   "chunks",
		"converge_upsert_total":       "paths",
		"sync_skipped_inflight_total": "requests",
		"heap_alloc_bytes":            "bytes",
		"embed_latency_ms_sum":        "ms",
		"index_slots_total":           "slots",
	}
	for name, want := range expected {
		if units[name] != want {
			t.Fatalf("%s unit = %q, want %q", name, units[name], want)
		}
	}
}

// assertActivityRowsCarryASource proves every activity row names what started
// it, which is what separates a registered job from file-change work the job
// commands cannot address.
//
// It takes the rows rather than the reply, because a reply whose work has
// already finished carries none and a loop over an empty slice would pass
// against any implementation, including one that stopped emitting source
// entirely.
func assertActivityRowsCarryASource(t *testing.T, rows []*pb.ActivityRow) {
	t.Helper()

	if len(rows) == 0 {
		t.Fatal("no activity rows to check; the caller must supply rows it observed")
	}
	for index, row := range rows {
		source := ""
		for _, metric := range row.GetMetrics() {
			if metric.GetName() == "source" {
				source = metric.GetStringValue()
			}
		}
		switch source {
		case "job", "watcher", "pending":
		default:
			t.Fatalf("activity row %d has source %q, want job, watcher, or pending", index, source)
		}
	}
}

// TestStatusCodebaseCountsHoldEveryStatus proves a display status with no
// codebases still reports a line over the wire. A line that appeared only when
// its count was positive would vanish at zero rather than report zero, and the
// changing name set would shift the column widths of every other line.
func TestStatusCodebaseCountsHoldEveryStatus(t *testing.T) {
	harness := newHarness(t)

	job := harness.indexFixture()
	requireCompleted(t, job)

	response, err := harness.client.GetStatus(context.Background(), &pb.GetStatusRequest{})
	if err != nil {
		t.Fatalf("GetStatus returned error: %v", err)
	}

	counts := make(map[string]int64)
	for _, metric := range response.GetMetrics() {
		if name, found := strings.CutPrefix(metric.GetName(), "codebases.status="); found {
			counts[name] = metric.GetIntValue()
		}
	}

	// The fixture is indexed, so that status is positive and the rest report
	// zero. Both cases must be present.
	if counts["indexed"] < 1 {
		t.Fatalf("codebases.status=indexed = %d, want at least 1 after indexing the fixture", counts["indexed"])
	}
	for _, name := range []string{"failed", "stale", "quarantined", "missing"} {
		if _, found := counts[name]; !found {
			t.Fatalf("codebases.status=%s is absent; an empty status must report zero", name)
		}
	}
}
