package daemon

import (
	"context"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/pbconv"
	render "goodkind.io/lm-semantic-search/internal/render"
	"goodkind.io/lm-semantic-search/internal/view"
)

// ListIndexes returns all tracked codebases.
func (server *GRPCServer) ListIndexes(ctx context.Context, request *pb.ListIndexesRequest) (resp *pb.ListIndexesResponse, err error) {
	ctx, done := beginRPC(ctx, "ListIndexes")
	defer done(&err)
	_ = request
	// This surface answers whether each tracked codebase can serve: the global
	// dependency mode folds into every row's display status inside
	// ListIndexesView. Probe before reading it so the rows and the banner both
	// describe the store as it is now, not as the last unrelated caller left it.
	server.manager.refreshDependencyHealth(ctx)
	views := server.manager.ListIndexesView()
	response := &pb.ListIndexesResponse{
		Indexes: make([]*pb.Codebase, 0, len(views)),
	}
	rows := make([]view.CodebaseRowView, 0, len(views))
	for _, codebaseView := range views {
		wire := server.codebaseWireView(ctx, codebaseView.Codebase)
		response.Indexes = append(response.Indexes, wire.codebase)
		rows = append(rows, view.CodebaseRowView{
			ID:            codebaseView.Codebase.ID,
			CanonicalPath: codebaseView.Codebase.CanonicalPath,
			Display:       view.Display(wire.display),
			Scheduling: pbconv.SchedulingFromProto(
				wire.codebase.GetSchedulingPolicy(),
				"",
				pb.SchedulingReason_SCHEDULING_REASON_UNSPECIFIED,
			),
			ReuseSiblingCount: wire.reuseSiblingCount,
			Active:            wire.active,
			Breakdown:         wire.breakdown,
		})
	}
	health := server.manager.DependencyHealth()
	response.DependencyHealth = toDependencyHealth(health)
	response.DisplayText = server.envelopeText(ctx, health, render.ListIndexes(rows))
	return response, nil
}

type resolvedCodebaseWireView struct {
	codebase          *pb.Codebase
	display           displayStatus
	reuseSiblingCount int32
	active            bool
	breakdown         view.OutcomeBreakdown
}

func (server *GRPCServer) codebaseWireView(
	ctx context.Context,
	codebase model.Codebase,
) resolvedCodebaseWireView {
	readiness, _ := server.manager.pathCollectionObservation(
		ctx,
		codebase.CanonicalPath,
		ownsLiveCollection(codebase),
	)
	display := server.manager.displayForCollectionReadiness(codebase, readiness)
	pbCodebase := pbconv.ToCodebase(codebase)
	applyDisplayTokens(pbCodebase, display)
	reuseSiblingCount := int32(0)
	if display == displayDiscovered {
		reuseSiblingCount = server.manager.worktreeReuseForecast(codebase)
	}
	applyReuseForecast(pbCodebase, reuseSiblingCount)

	active := false
	breakdown := view.ZeroBreakdown()
	if codebase.ActiveJobID != "" {
		if activeJob, found := server.manager.GetJob(codebase.ActiveJobID); found {
			pbCodebase.ActiveProgress = pbconv.ToProgress(activeJob.Progress)
			breakdown = resolveOutcomeBreakdown(activeJob.Progress)
			active = len(breakdown.FileRows) > 0 || len(breakdown.ChunkRows) > 0
		}
	}
	return resolvedCodebaseWireView{
		codebase:          pbCodebase,
		display:           display,
		reuseSiblingCount: reuseSiblingCount,
		active:            active,
		breakdown:         breakdown,
	}
}
