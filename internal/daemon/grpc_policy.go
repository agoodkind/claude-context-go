package daemon

import (
	"context"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/adapterr"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/pbconv"
	render "goodkind.io/lm-semantic-search/internal/render"
	"goodkind.io/lm-semantic-search/internal/view"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UpdateCodebasePolicy changes supplied stored-policy fields and current work.
func (server *GRPCServer) UpdateCodebasePolicy(
	ctx context.Context,
	request *pb.UpdateCodebasePolicyRequest,
) (resp *pb.UpdateCodebasePolicyResponse, err error) {
	ctx, done := beginRPC(ctx, "UpdateCodebasePolicy")
	defer done(&err)
	patch, patchErr := pbconv.FromSchedulingPolicyPatch(request.GetPatch())
	if patchErr != nil {
		return nil, status.Error(codes.InvalidArgument, patchErr.Error())
	}
	if argErr := requireNonEmpty(ctx, request.GetPath(), "absolutePath", true); argErr != nil {
		return nil, argErr
	}
	requestedPath, pathErr := resolveRequestPath(
		request.GetPath(),
		request.GetClient().GetCallerCwd(),
	)
	if pathErr != nil {
		return nil, status.Error(
			adapterr.Respond(ctx, adapterr.NewInvalidPath(pathErr.Error(), pathErr)),
		)
	}
	codebase, callErr := server.manager.UpdateCodebasePolicy(
		ctx,
		requestedPath,
		patch,
	)
	if callErr != nil {
		return nil, status.Error(
			adapterr.Respond(ctx, classifyManagerError(requestedPath, callErr)),
		)
	}
	server.manager.refreshDependencyHealth(ctx)
	pbCodebase := server.codebaseWireView(ctx, codebase).codebase
	health := server.manager.DependencyHealth()
	ack := view.MutationAckView{
		Kind:            view.AckUpdatePolicy,
		Path:            codebase.CanonicalPath,
		JobID:           "",
		StateLabel:      "",
		AlreadyTerminal: false,
		Deduplicated:    false,
		CollectionID:    "",
		CollectionName:  "",
		CodebaseID:      codebase.ID,
		ConversationID:  "",
		DocumentCount:   0,
		NeededCount:     0,
		TotalCount:      0,
	}
	return &pb.UpdateCodebasePolicyResponse{
		Codebase: pbCodebase,
		DisplayText: server.envelopeText(
			ctx,
			health,
			render.MutationAck(ack),
			"codebase_id",
			codebase.ID,
		),
	}, nil
}

func pbClient(client *pb.ClientInfo) model.ClientInfo {
	if client == nil {
		return model.ClientInfo{Name: "", PID: 0}
	}
	return model.ClientInfo{
		Name: client.GetName(),
		PID:  client.GetPid(),
	}
}
