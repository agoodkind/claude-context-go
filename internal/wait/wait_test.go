package wait

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/model"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestForIndexStatusWithClientReturnsImmediatelyWhenSearchable(t *testing.T) {
	client := &mockDaemonClient{
		responses: []*pb.GetIndexResponse{
			{Searchable: proto.Bool(true)},
		},
	}

	result, err := forIndexStatusWithClient(
		context.Background(),
		client,
		"/repo",
		&pb.ClientInfo{Name: "test"},
	)
	if err != nil {
		t.Fatalf("forIndexStatusWithClient returned error: %v", err)
	}
	if !result.GetSearchable() {
		t.Fatal("forIndexStatusWithClient returned non-searchable result")
	}
	if client.getIndexCalls != 1 {
		t.Fatalf("GetIndex calls = %d, want 1", client.getIndexCalls)
	}
}

func TestForIndexStatusWithClientPollsUntilSearchable(t *testing.T) {
	originalInterval := watchPollInterval
	watchPollInterval = time.Millisecond
	t.Cleanup(func() { watchPollInterval = originalInterval })

	client := &mockDaemonClient{
		responses: []*pb.GetIndexResponse{
			{CollectionReadiness: "building"},
			{CollectionReadiness: "loading"},
			{Searchable: proto.Bool(true)},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := forIndexStatusWithClient(ctx, client, "/repo", &pb.ClientInfo{Name: "test"})
	if err != nil {
		t.Fatalf("forIndexStatusWithClient returned error: %v", err)
	}
	if !result.GetSearchable() {
		t.Fatal("forIndexStatusWithClient did not return searchable result")
	}
	if client.getIndexCalls != 3 {
		t.Fatalf("GetIndex calls = %d, want 3", client.getIndexCalls)
	}
}

func TestForIndexStatusWithClientPollsDiscoveredUntilSearchable(t *testing.T) {
	originalInterval := watchPollInterval
	watchPollInterval = time.Millisecond
	t.Cleanup(func() { watchPollInterval = originalInterval })

	client := &mockDaemonClient{
		responses: []*pb.GetIndexResponse{
			{
				Codebase:   &pb.Codebase{Status: string(model.CodebaseStatusDiscovered)},
				Searchable: proto.Bool(false),
			},
			{Searchable: proto.Bool(true)},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := forIndexStatusWithClient(ctx, client, "/repo", &pb.ClientInfo{Name: "test"})
	if err != nil {
		t.Fatalf("forIndexStatusWithClient returned error: %v", err)
	}
	if !result.GetSearchable() {
		t.Fatal("forIndexStatusWithClient did not return searchable result")
	}
	if client.getIndexCalls != 2 {
		t.Fatalf("GetIndex calls = %d, want 2", client.getIndexCalls)
	}
}

func TestForIndexStatusWithClientReturnsTerminalNonreadyState(t *testing.T) {
	client := &mockDaemonClient{
		responses: []*pb.GetIndexResponse{
			{CollectionReadiness: "unknown", Searchable: proto.Bool(false)},
		},
	}

	result, err := forIndexStatusWithClient(
		context.Background(),
		client,
		"/repo",
		&pb.ClientInfo{Name: "test"},
	)
	if err != nil {
		t.Fatalf("forIndexStatusWithClient returned error: %v", err)
	}
	if result.GetSearchable() {
		t.Fatal("forIndexStatusWithClient returned searchable result")
	}
	if result.GetCollectionReadiness() != "unknown" {
		t.Fatalf("collection readiness = %q, want unknown", result.GetCollectionReadiness())
	}
}

func TestForIndexStatusWithClientReturnsTimeout(t *testing.T) {
	client := &mockDaemonClient{
		responses: []*pb.GetIndexResponse{
			{CollectionReadiness: "building"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	result, err := forIndexStatusWithClient(ctx, client, "/repo", &pb.ClientInfo{Name: "test"})
	if err == nil {
		t.Fatal("forIndexStatusWithClient returned nil error")
	}
	if err.Error() != "wait timeout expired" {
		t.Fatalf("error = %q, want wait timeout expired", err.Error())
	}
	if result == nil {
		t.Fatal("forIndexStatusWithClient returned nil result on timeout")
	}
}

func TestForIndexStatusWithClientReturnsLastResponseWhenRPCTimesOut(t *testing.T) {
	originalInterval := watchPollInterval
	watchPollInterval = time.Millisecond
	t.Cleanup(func() { watchPollInterval = originalInterval })

	client := &mockDaemonClient{
		responses: []*pb.GetIndexResponse{
			{CollectionReadiness: "building", Searchable: proto.Bool(false)},
		},
		blockAfterFirst: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	result, err := forIndexStatusWithClient(ctx, client, "/repo", &pb.ClientInfo{Name: "test"})
	if err == nil {
		t.Fatal("forIndexStatusWithClient returned nil error")
	}
	if err.Error() != "wait timeout expired" {
		t.Fatalf("error = %q, want wait timeout expired", err.Error())
	}
	if result == nil || result.GetCollectionReadiness() != "building" {
		t.Fatal("forIndexStatusWithClient did not return the last successful response")
	}
}

func TestForIndexStatusWithClientReturnsCancellation(t *testing.T) {
	client := &mockDaemonClient{
		responses: []*pb.GetIndexResponse{
			{CollectionReadiness: "building"},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := forIndexStatusWithClient(ctx, client, "/repo", &pb.ClientInfo{Name: "test"})
	if err == nil {
		t.Fatal("forIndexStatusWithClient returned nil error")
	}
	if err.Error() != "wait cancelled" {
		t.Fatalf("error = %q, want wait cancelled", err.Error())
	}
	if result == nil {
		t.Fatal("forIndexStatusWithClient returned nil result on cancellation")
	}
}

func TestForIndexStatusWithClientReturnsRPCError(t *testing.T) {
	client := &mockDaemonClient{
		getIndexErr: errors.New("rpc failed"),
	}

	_, err := forIndexStatusWithClient(
		context.Background(),
		client,
		"/repo",
		&pb.ClientInfo{Name: "test"},
	)
	if err == nil {
		t.Fatal("forIndexStatusWithClient returned nil error")
	}
	if err.Error() != "GetIndex: rpc failed" {
		t.Fatalf("error = %q, want GetIndex: rpc failed", err.Error())
	}
}

func TestForIndexStatusWithClientKeepsWaitingWhenActiveJobExists(t *testing.T) {
	originalInterval := watchPollInterval
	watchPollInterval = time.Millisecond
	t.Cleanup(func() { watchPollInterval = originalInterval })

	client := &mockDaemonClient{
		responses: []*pb.GetIndexResponse{
			{
				CollectionReadiness: "unknown",
				ActiveJob:           &pb.Job{Id: "job-1"},
			},
			{Searchable: proto.Bool(true)},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := forIndexStatusWithClient(ctx, client, "/repo", &pb.ClientInfo{Name: "test"})
	if err != nil {
		t.Fatalf("forIndexStatusWithClient returned error: %v", err)
	}
	if !result.GetSearchable() {
		t.Fatal("forIndexStatusWithClient did not return searchable result")
	}
	if client.getIndexCalls != 2 {
		t.Fatalf("GetIndex calls = %d, want 2", client.getIndexCalls)
	}
}

type mockDaemonClient struct {
	responses       []*pb.GetIndexResponse
	getIndexErr     error
	getIndexCalls   int
	blockAfterFirst bool
}

func (client *mockDaemonClient) GetIndex(ctx context.Context, _ *pb.GetIndexRequest, _ ...grpc.CallOption) (*pb.GetIndexResponse, error) {
	client.getIndexCalls++
	if client.blockAfterFirst && client.getIndexCalls > 1 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if client.getIndexErr != nil {
		return nil, client.getIndexErr
	}
	if len(client.responses) == 0 {
		return &pb.GetIndexResponse{}, nil
	}
	responseIndex := client.getIndexCalls - 1
	if responseIndex >= len(client.responses) {
		responseIndex = len(client.responses) - 1
	}
	return client.responses[responseIndex], nil
}

func (client *mockDaemonClient) Version(context.Context, *pb.VersionRequest, ...grpc.CallOption) (*pb.VersionResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) StartIndex(context.Context, *pb.StartIndexRequest, ...grpc.CallOption) (*pb.StartIndexResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) ClearIndex(context.Context, *pb.ClearIndexRequest, ...grpc.CallOption) (*pb.ClearIndexResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) CancelJob(context.Context, *pb.CancelJobRequest, ...grpc.CallOption) (*pb.CancelJobResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) SyncIndex(context.Context, *pb.SyncIndexRequest, ...grpc.CallOption) (*pb.SyncIndexResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) UpdateCodebasePolicy(context.Context, *pb.UpdateCodebasePolicyRequest, ...grpc.CallOption) (*pb.UpdateCodebasePolicyResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) ListIndexes(context.Context, *pb.ListIndexesRequest, ...grpc.CallOption) (*pb.ListIndexesResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) GetJob(context.Context, *pb.GetJobRequest, ...grpc.CallOption) (*pb.GetJobResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) ListJobs(context.Context, *pb.ListJobsRequest, ...grpc.CallOption) (*pb.ListJobsResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) WatchJobs(context.Context, *pb.WatchJobsRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[pb.WatchJobsResponse], error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) SearchCode(context.Context, *pb.SearchCodeRequest, ...grpc.CallOption) (*pb.SearchCodeResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) GraphTool(context.Context, *pb.GraphToolRequest, ...grpc.CallOption) (*pb.GraphToolResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) RegisterConversationCollection(context.Context, *pb.RegisterConversationCollectionRequest, ...grpc.CallOption) (*pb.RegisterConversationCollectionResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) SyncConversationManifest(context.Context, *pb.SyncConversationManifestRequest, ...grpc.CallOption) (*pb.SyncConversationManifestResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) UpsertConversationDocumentsStream(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[pb.UpsertConversationDocumentsChunk, pb.UpsertConversationDocumentsResponse], error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) BackfillConversationScalars(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[pb.BackfillConversationScalarsChunk, pb.BackfillConversationScalarsResponse], error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) DeleteConversation(context.Context, *pb.DeleteConversationRequest, ...grpc.CallOption) (*pb.DeleteConversationResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) SearchConversations(context.Context, *pb.SearchConversationsRequest, ...grpc.CallOption) (*pb.SearchConversationsResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) SearchWithinConversation(context.Context, *pb.SearchWithinConversationRequest, ...grpc.CallOption) (*pb.SearchWithinConversationResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) Doctor(context.Context, *pb.DoctorRequest, ...grpc.CallOption) (*pb.DoctorResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) GetStatus(context.Context, *pb.GetStatusRequest, ...grpc.CallOption) (*pb.GetStatusResponse, error) {
	return nil, errors.New("not implemented")
}

func (client *mockDaemonClient) Shutdown(context.Context, *pb.ShutdownRequest, ...grpc.CallOption) (*pb.ShutdownResponse, error) {
	return nil, errors.New("not implemented")
}
