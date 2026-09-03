package daemon

import (
	"context"
	"strings"
	"testing"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStartIndexSchedulingPolicyPersistsInitialPolicy(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	canonicalPath, err := manager.resolveCanonicalPath(repoPath)
	if err != nil {
		t.Fatalf("resolveCanonicalPath: %v", err)
	}
	discovered := newCodebaseRecord(canonicalPath)
	discovered.Status = model.CodebaseStatusDiscovered
	discovered.PolicyPendingInitialization = true
	manager.mu.Lock()
	manager.codebases[discovered.ID] = discovered
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		t.Fatalf("saveLocked: %v", err)
	}
	manager.mu.Unlock()
	server := NewGRPCServer(manager, nil)
	priority := pb.SchedulingPriority_SCHEDULING_PRIORITY_HIGH
	quiet := true
	idleAfterSeconds := int32(600)

	response, err := server.StartIndex(context.Background(), &pb.StartIndexRequest{
		Path: repoPath,
		SchedulingPolicy: &pb.SchedulingPolicyPatch{
			Priority:         &priority,
			Quiet:            &quiet,
			IdleAfterSeconds: &idleAfterSeconds,
		},
	})
	if err != nil {
		t.Fatalf("StartIndex: %v", err)
	}

	manager.mu.Lock()
	codebase := manager.codebases[response.GetCodebaseId()]
	job := manager.jobs[response.GetJobId()]
	manager.mu.Unlock()
	want := model.SchedulingPolicy{
		Priority:         model.JobPriorityHigh,
		Quiet:            true,
		IdleAfterSeconds: 600,
	}
	if codebase.SchedulingPolicy != want || codebase.PolicyPendingInitialization {
		t.Fatalf("stored policy = %+v pending=%v, want %+v and false", codebase.SchedulingPolicy, codebase.PolicyPendingInitialization, want)
	}
	if job.EffectiveSchedulingPolicy != want {
		t.Fatalf("effective policy = %+v, want %+v", job.EffectiveSchedulingPolicy, want)
	}
}

func TestSyncIndexSchedulingPolicyIsOneRunOverride(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	canonicalPath, err := manager.resolveCanonicalPath(repoPath)
	if err != nil {
		t.Fatalf("resolveCanonicalPath: %v", err)
	}
	codebase := newCodebaseRecord(canonicalPath)
	codebase.Status = model.CodebaseStatusIndexed
	codebase.EffectiveConfig = defaultIndexConfig()
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		t.Fatalf("saveLocked: %v", err)
	}
	manager.mu.Unlock()

	server := NewGRPCServer(manager, nil)
	priority := pb.SchedulingPriority_SCHEDULING_PRIORITY_LOW
	response, err := server.SyncIndex(context.Background(), &pb.SyncIndexRequest{
		Path: repoPath,
		SchedulingPolicy: &pb.SchedulingPolicyPatch{
			Priority: &priority,
		},
	})
	if err != nil {
		t.Fatalf("SyncIndex: %v", err)
	}
	job, found := manager.GetJob(response.GetJobId())
	if !found {
		t.Fatalf("GetJob(%q) did not find the sync job", response.GetJobId())
	}
	if job.EffectiveSchedulingPolicy.Priority != model.JobPriorityLow ||
		job.SchedulingOverride.Priority == nil ||
		*job.SchedulingOverride.Priority != model.JobPriorityLow {
		t.Fatalf("sync job policy = %+v override=%+v", job.EffectiveSchedulingPolicy, job.SchedulingOverride)
	}
	manager.mu.Lock()
	stored := manager.codebases[codebase.ID].SchedulingPolicy
	manager.mu.Unlock()
	if stored != model.DefaultSchedulingPolicy() {
		t.Fatalf("stored policy = %+v, want defaults", stored)
	}
}

func TestUpdateCodebasePolicyRPCValidatesBeforePathResolution(t *testing.T) {
	manager, _, _ := newTestManager(t)
	server := NewGRPCServer(manager, nil)
	priority := pb.SchedulingPriority_SCHEDULING_PRIORITY_UNSPECIFIED

	_, err := server.UpdateCodebasePolicy(
		context.Background(),
		&pb.UpdateCodebasePolicyRequest{
			Path:  "/path/that/does/not/exist",
			Patch: &pb.SchedulingPolicyPatch{Priority: &priority},
		},
	)
	if status.Code(err) != codes.InvalidArgument ||
		!strings.Contains(status.Convert(err).Message(), "priority") {
		t.Fatalf("UpdateCodebasePolicy error = %v, want invalid priority", err)
	}
}

func TestUpdateCodebasePolicyRPCReturnsUpdatedCodebase(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	canonicalPath, err := manager.resolveCanonicalPath(repoPath)
	if err != nil {
		t.Fatalf("resolveCanonicalPath: %v", err)
	}
	codebase := newCodebaseRecord(canonicalPath)
	codebase.Status = model.CodebaseStatusIndexed
	codebase.SchedulingPolicy.Quiet = true
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		t.Fatalf("saveLocked: %v", err)
	}
	manager.mu.Unlock()

	quiet := false
	server := NewGRPCServer(manager, nil)
	response, err := server.UpdateCodebasePolicy(
		context.Background(),
		&pb.UpdateCodebasePolicyRequest{
			Path:  repoPath,
			Patch: &pb.SchedulingPolicyPatch{Quiet: &quiet},
		},
	)
	if err != nil {
		t.Fatalf("UpdateCodebasePolicy: %v", err)
	}
	if response.GetCodebase().GetSchedulingPolicy().GetQuiet() {
		t.Fatalf("updated quiet = true, want false")
	}
	if response.GetCodebase().GetSchedulingPolicy().GetPriority() !=
		pb.SchedulingPriority_SCHEDULING_PRIORITY_NORMAL {
		t.Fatalf("updated priority = %v, want normal", response.GetCodebase().GetSchedulingPolicy().GetPriority())
	}
	if response.GetCodebase().GetDisplayStatus() == "" ||
		response.GetCodebase().GetGlyphToken() == "" ||
		response.GetCodebase().GetStatusLabel() == "" {
		t.Fatalf(
			"updated codebase lacks shared display tokens: %+v",
			response.GetCodebase(),
		)
	}
	if strings.TrimSpace(response.GetDisplayText()) == "" {
		t.Fatal("display text is empty")
	}
}
