package daemon

import (
	"context"
	"testing"

	"goodkind.io/lm-semantic-search/internal/model"
)

func TestUpdateCodebasePolicyAcceptsConversationCodebaseID(t *testing.T) {
	manager, _, _ := newTestManager(t)
	codebase := newCodebaseRecord(conversationCanonicalPath("clyde-conversations"))
	codebase.Kind = model.CodebaseKindDocument
	codebase.Status = model.CodebaseStatusIndexed

	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		t.Fatalf("saveLocked: %v", err)
	}
	manager.mu.Unlock()

	priority := model.JobPriorityLow
	updated, err := manager.UpdateCodebasePolicy(
		context.Background(),
		codebase.ID,
		model.SchedulingPolicyPatch{Priority: &priority},
	)
	if err != nil {
		t.Fatalf("UpdateCodebasePolicy: %v", err)
	}
	if updated.ID != codebase.ID {
		t.Fatalf("updated id = %q, want %q", updated.ID, codebase.ID)
	}
	if updated.SchedulingPolicy.Priority != model.JobPriorityLow {
		t.Fatalf(
			"updated priority = %q, want %q",
			updated.SchedulingPolicy.Priority,
			model.JobPriorityLow,
		)
	}
}
