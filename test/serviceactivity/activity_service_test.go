//go:build serviceactivitylive

package serviceactivity

import (
	"context"
	"testing"
	"time"

	daemonclient "goodkind.io/lm-semantic-search/client"
	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
)

const serviceActivityLiveTimeout = 15 * time.Second

func TestInstalledDaemonReportsSchedulingActivity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), serviceActivityLiveTimeout)
	defer cancel()

	socketPath := daemonclient.ResolveSocketPath()
	if socketPath == "" {
		t.Fatal("resolve default daemon socket path")
	}
	connection, client, err := daemonclient.DialDaemon(ctx, socketPath)
	if err != nil {
		t.Fatalf("dial installed daemon: %v", err)
	}
	defer func() { _ = connection.Close() }()

	response, err := client.GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		t.Fatalf("read installed daemon status: %v", err)
	}
	assertSchedulingMetrics(t, response.GetMetrics())
	assertActivitySource(t, response.GetActivitySource())
}

func assertSchedulingMetrics(t *testing.T, metrics []*pb.Metric) {
	t.Helper()

	byName := make(map[string]*pb.Metric, len(metrics))
	for _, metric := range metrics {
		byName[metric.GetName()] = metric
	}
	for _, priority := range []string{"high", "normal", "low"} {
		for _, state := range []string{"running", "queued", "paused"} {
			name := "scheduler." + state + ".priority=" + priority
			metric := byName[name]
			if metric == nil {
				t.Fatalf("installed status omits %q", name)
			}
			if _, ok := metric.GetValue().(*pb.Metric_IntValue); !ok || metric.GetIntValue() < 0 {
				t.Fatalf("installed status metric %q = %+v", name, metric)
			}
		}
	}
}

func assertActivitySource(t *testing.T, source *pb.ActivitySourceStatus) {
	t.Helper()
	if source == nil {
		t.Fatal("installed status omits activity source")
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
