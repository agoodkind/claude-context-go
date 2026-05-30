package daemon

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"goodkind.io/claude-context-go/internal/model"
)

// findCodebaseByExactRoot returns the codebase whose CanonicalPath equals
// canonicalPath. Used by registration, dedup, and cancellation paths where
// two distinct codebases rooted at sibling directories must remain
// independent.
func (manager *Manager) findCodebaseByExactRoot(canonicalPath string) (model.Codebase, bool) {
	for _, codebase := range manager.codebases {
		if codebase.CanonicalPath == canonicalPath {
			return codebase, true
		}
	}
	var emptyCodebase model.Codebase
	return emptyCodebase, false
}

// findCodebasesByCoverage returns every codebase whose CanonicalPath is a
// prefix of canonicalPath, sorted longest-prefix first so the nearest
// covering codebase is index 0. Used by status, search, clear, and watcher
// dispatch where one path can fall under multiple overlapping codebases.
func (manager *Manager) findCodebasesByCoverage(canonicalPath string) []model.Codebase {
	matches := make([]model.Codebase, 0, len(manager.codebases))
	for _, codebase := range manager.codebases {
		if pathCovers(codebase.CanonicalPath, canonicalPath) {
			matches = append(matches, codebase)
		}
	}
	sort.Slice(matches, func(first int, second int) bool {
		return len(matches[first].CanonicalPath) > len(matches[second].CanonicalPath)
	})
	return matches
}

// findStrictAncestor returns the longest codebase whose CanonicalPath
// strictly prefix-covers canonicalPath (so the codebase's root is not equal
// to canonicalPath). Empty result when there is no strict ancestor. Used at
// registration time to surface an overlap hint.
func (manager *Manager) findStrictAncestor(canonicalPath string) (model.Codebase, bool) {
	var best model.Codebase
	bestLength := -1
	for _, codebase := range manager.codebases {
		if codebase.CanonicalPath == canonicalPath {
			continue
		}
		if !pathCovers(codebase.CanonicalPath, canonicalPath) {
			continue
		}
		if len(codebase.CanonicalPath) > bestLength {
			best = codebase
			bestLength = len(codebase.CanonicalPath)
		}
	}
	if bestLength < 0 {
		var empty model.Codebase
		return empty, false
	}
	return best, true
}

func canonicalizePath(requestedPath string) (string, error) {
	// Reject an empty or whitespace path before filepath.Abs, which would
	// otherwise resolve "" to the current working directory and let a caller
	// silently operate on the wrong codebase.
	if strings.TrimSpace(requestedPath) == "" {
		return "", errors.New("codebase path is required")
	}
	absolutePath, err := filepath.Abs(requestedPath)
	if err != nil {
		slog.Error("resolve absolute path failed", "path", requestedPath, "err", err)
		return "", fmt.Errorf("resolve absolute path for %s: %w", requestedPath, err)
	}
	canonicalPath, err := filepath.EvalSymlinks(absolutePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return absolutePath, nil
		}
		slog.Error("resolve symlinks failed", "path", absolutePath, "err", err)
		return "", fmt.Errorf("resolve symlinks for %s: %w", absolutePath, err)
	}
	return canonicalPath, nil
}

func pathCovers(rootPath string, targetPath string) bool {
	rootPath = filepath.Clean(rootPath)
	targetPath = filepath.Clean(targetPath)
	if rootPath == targetPath {
		return true
	}
	prefixWithSeparator := rootPath + string(filepath.Separator)
	return strings.HasPrefix(targetPath, prefixWithSeparator)
}
