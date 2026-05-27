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

func (manager *Manager) findCodebaseByPathLocked(canonicalPath string, aliasPath string) (model.Codebase, bool) {
	var bestMatch model.Codebase
	bestMatchLength := -1

	for _, codebase := range manager.codebases {
		if codebase.CanonicalPath == canonicalPath {
			return codebase, true
		}
		if pathCovers(codebase.CanonicalPath, canonicalPath) && len(codebase.CanonicalPath) > bestMatchLength {
			bestMatch = codebase
			bestMatchLength = len(codebase.CanonicalPath)
		}
		for _, alias := range codebase.Aliases {
			if alias == aliasPath || alias == canonicalPath {
				return codebase, true
			}
			if pathCovers(alias, aliasPath) && len(alias) > bestMatchLength {
				bestMatch = codebase
				bestMatchLength = len(alias)
			}
		}
	}
	if bestMatchLength >= 0 {
		return bestMatch, true
	}
	var emptyCodebase model.Codebase
	return emptyCodebase, false
}

func canonicalizePath(requestedPath string) (string, string, error) {
	absolutePath, err := filepath.Abs(requestedPath)
	if err != nil {
		slog.Error("resolve absolute path failed", "path", requestedPath, "err", err)
		return "", "", fmt.Errorf("resolve absolute path for %s: %w", requestedPath, err)
	}
	canonicalPath, err := filepath.EvalSymlinks(absolutePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return absolutePath, absolutePath, nil
		}
		slog.Error("resolve symlinks failed", "path", absolutePath, "err", err)
		return "", "", fmt.Errorf("resolve symlinks for %s: %w", absolutePath, err)
	}
	return canonicalPath, absolutePath, nil
}

func mergeAliases(existing []string, aliases ...string) []string {
	seen := map[string]struct{}{}
	merged := make([]string, 0, len(existing)+len(aliases))
	for _, alias := range existing {
		if alias == "" {
			continue
		}
		if _, found := seen[alias]; found {
			continue
		}
		seen[alias] = struct{}{}
		merged = append(merged, alias)
	}
	for _, alias := range aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		if _, found := seen[alias]; found {
			continue
		}
		seen[alias] = struct{}{}
		merged = append(merged, alias)
	}
	sort.Strings(merged)
	return merged
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
