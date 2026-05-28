package daemon

import (
	"context"

	"goodkind.io/claude-context-go/internal/model"
)

// SetCodebaseLifecycleHook plugs in the watcher (or another consumer)
// that receives hot add and remove callbacks for tracked codebases.
// Setting nil clears the hook. Safe to call any time; the wired calls
// are made outside the manager lock so a slow hook does not stall
// registration.
func (manager *Manager) SetCodebaseLifecycleHook(hook CodebaseLifecycleHook) {
	manager.lifecycleMutex.Lock()
	defer manager.lifecycleMutex.Unlock()
	manager.lifecycleHook = hook
}

func (manager *Manager) notifyCodebaseAdded(ctx context.Context, codebase model.Codebase) {
	manager.lifecycleMutex.Lock()
	hook := manager.lifecycleHook
	manager.lifecycleMutex.Unlock()
	if hook != nil {
		hook.AddCodebase(ctx, codebase)
	}
}

func (manager *Manager) notifyCodebaseRemoved(ctx context.Context, codebaseID string) {
	manager.lifecycleMutex.Lock()
	hook := manager.lifecycleHook
	manager.lifecycleMutex.Unlock()
	if hook != nil {
		hook.RemoveCodebase(ctx, codebaseID)
	}
}
