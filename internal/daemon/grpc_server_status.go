package daemon

import (
	"context"
	"fmt"
	"math"
	"os"

	"goodkind.io/gklog/version"
	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/adapterr"
	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/metrics"
	"goodkind.io/lm-semantic-search/internal/pbconv"
	render "goodkind.io/lm-semantic-search/internal/render"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GetStatus reports what the daemon is doing and what its counters read, as one
// flat list of named observations plus the work in flight.
//
// It reports only non-terminal work, so the reply stays a few kilobytes rather
// than growing with job history the way ListJobs does. File-change waits appear
// as watcher activity until their converge job is registered.
func (server *GRPCServer) GetStatus(ctx context.Context, request *pb.GetStatusRequest) (resp *pb.GetStatusResponse, err error) {
	ctx, done := beginRPC(ctx, "GetStatus")
	defer done(&err)
	_ = request

	pid, pidErr := currentProcessID()
	if pidErr != nil {
		return nil, status.Error(adapterr.Respond(ctx, adapterr.NewInternal("resolve process id", pidErr)))
	}

	now := clock.Now()

	// One snapshot feeds the counters, the activity rows, and the banner, so no
	// two of them can describe different instants. The alternative, reading each
	// through its own accessor, lets a job terminate or a pending slot drain
	// between two reads and go unreported by both.
	daemon := server.manager.StatusSnapshot()
	statusMetrics := buildStatusMetrics(&daemon, metrics.Read(), now)
	activity := buildStatusActivity(&daemon)

	response := &pb.GetStatusResponse{
		ReadAt: timestamppb.New(now),
		Daemon: &pb.DaemonIdentity{
			Version:    version.String(),
			Commit:     version.Commit,
			Pid:        pid,
			SocketPath: server.manager.config.SocketPath,
			StartedAt:  timestamppb.New(daemon.StartedAt),
		},
		Metrics:  statusMetrics,
		Activity: activity,
		ActivitySource: &pb.ActivitySourceStatus{
			InputAvailable:   daemon.Scheduler.Activity.InputAvailable,
			ThermalAvailable: daemon.Scheduler.Activity.ThermalAvailable,
			ThermalUnsafe:    daemon.Scheduler.Activity.ThermalUnsafe,
			InputReason:      pbconv.SchedulingReasonToProto(daemon.Scheduler.Activity.InputReason),
			ThermalReason:    pbconv.SchedulingReasonToProto(daemon.Scheduler.Activity.ThermalReason),
		},
		DisplayText: "",
	}
	response.DisplayText = server.envelopeText(ctx, daemon.Health, render.StatusMetrics(response))
	return response, nil
}

// currentProcessID narrows the process id to the width the wire carries,
// refusing a value that does not fit rather than truncating it silently.
func currentProcessID() (int32, error) {
	pid := os.Getpid()
	if pid < 0 || pid > math.MaxInt32 {
		return 0, fmt.Errorf("process id %d does not fit in int32", pid)
	}
	return int32(pid), nil
}
