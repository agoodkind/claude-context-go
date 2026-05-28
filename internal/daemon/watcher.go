package daemon

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rjeczalik/notify"
	"goodkind.io/claude-context-go/internal/discovery"
)

const watcherEventBuffer = 4096

type watchRoot struct {
	codebaseID string
	root       string
	rules      discovery.IgnoreRules
}

// Watcher converts filesystem events under tracked codebases into per-path
// converge tasks. Each codebase tree is watched recursively with the native
// platform backend (FSEvents on macOS, inotify on Linux), so one watch covers
// a whole tree without a descriptor per file. Changed paths are enqueued into
// the coalescing queue, and ignored paths are dropped using the same rule the
// full scan applies so the watcher and a scan agree on what belongs.
type Watcher struct {
	manager *Manager
	queue   *EventQueue
	events  chan notify.EventInfo
	roots   []watchRoot
}

// NewWatcher constructs a Watcher that enqueues into queue.
func NewWatcher(manager *Manager, queue *EventQueue) *Watcher {
	return &Watcher{
		manager: manager,
		queue:   queue,
		events:  make(chan notify.EventInfo, watcherEventBuffer),
		roots:   nil,
	}
}

// Run registers a recursive watch for every tracked codebase and dispatches
// events until ctx is cancelled. Codebases tracked after Run starts are picked
// up by the periodic backstop rather than the watcher.
func (watcher *Watcher) Run(ctx context.Context) {
	watcher.roots = watcher.resolveRoots(ctx)
	registered := 0
	for _, root := range watcher.roots {
		recursivePath := filepath.Join(root.root, "...")
		if err := notify.Watch(recursivePath, watcher.events, notify.Create, notify.Remove, notify.Write, notify.Rename); err != nil {
			slog.ErrorContext(ctx, "watcher.register_failed", "component", "daemon", "subcomponent", "watcher", "root", root.root, "err", err)
			continue
		}
		registered++
	}
	slog.InfoContext(ctx, "watcher.started", "component", "daemon", "subcomponent", "watcher", "codebases", registered)
	defer notify.Stop(watcher.events)

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-watcher.events:
			watcher.dispatch(event)
		}
	}
}

func (watcher *Watcher) resolveRoots(ctx context.Context) []watchRoot {
	codebases := watcher.manager.ListIndexes(ctx)
	roots := make([]watchRoot, 0, len(codebases))
	for _, codebase := range codebases {
		rules, err := discovery.EffectiveIgnorePatterns(ctx, codebase.CanonicalPath, codebase.EffectiveConfig.IgnorePatterns)
		if err != nil {
			slog.ErrorContext(ctx, "watcher.ignore_resolve_failed", "component", "daemon", "subcomponent", "watcher", "root", codebase.CanonicalPath, "err", err)
			continue
		}
		roots = append(roots, watchRoot{codebaseID: codebase.ID, root: codebase.CanonicalPath, rules: rules})
	}
	// Longest root first so a codebase nested inside another wins the match.
	sort.Slice(roots, func(first int, second int) bool {
		return len(roots[first].root) > len(roots[second].root)
	})
	return roots
}

func (watcher *Watcher) dispatch(event notify.EventInfo) {
	path := event.Path()
	root, found := watcher.ownerOf(path)
	if !found {
		return
	}
	relativePath, err := filepath.Rel(root.root, path)
	if err != nil {
		return
	}
	relativePath = filepath.ToSlash(relativePath)
	if relativePath == "." || relativePath == "" {
		return
	}
	if info, statErr := os.Lstat(path); statErr == nil && info.IsDir() {
		// A directory event; the recursive watch already covers its files, and
		// the contained files raise their own events.
		return
	}
	if excluded, _, _ := discovery.PathIgnored(relativePath, root.rules); excluded {
		return
	}
	watcher.queue.Enqueue(root.codebaseID, relativePath)
}

func (watcher *Watcher) ownerOf(path string) (watchRoot, bool) {
	for _, root := range watcher.roots {
		if path == root.root || strings.HasPrefix(path, root.root+string(os.PathSeparator)) {
			return root, true
		}
	}
	return watchRoot{codebaseID: "", root: "", rules: discovery.IgnoreRules{Nodes: nil}}, false
}
