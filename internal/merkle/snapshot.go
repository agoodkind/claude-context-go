// Package merkle captures per-file hashes for daemon-owned sync checks.
package merkle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"unicode/utf8"

	"goodkind.io/claude-context-go/internal/discovery"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/store"
)

// Snapshot stores one content hash per relative file path. ConfigDigest
// records the IgnoreDigest of the request that produced these hashes so a
// resuming run can detect when a config change has invalidated the
// checkpoint and trigger a fresh embed pass.
type Snapshot struct {
	ConfigDigest string            `json:"config_digest,omitempty"`
	Files        map[string]string `json:"files"`
}

// Capture walks a codebase and records content hashes for the tracked files.
func Capture(
	ctx context.Context,
	root string,
	indexConfig model.IndexConfig,
) (Snapshot, error) {
	discoveryResult, err := discovery.Discover(
		ctx,
		root,
		indexConfig.IgnorePatterns,
		indexConfig.Extensions,
	)
	if err != nil {
		return Snapshot{}, fmt.Errorf("discover sync files under %s: %w", root, err)
	}

	files := make(map[string]string, len(discoveryResult.Files))
	for _, path := range discoveryResult.Files {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, fmt.Errorf("capture snapshot cancelled: %w", err)
		}

		data, err := os.ReadFile(path)
		if err != nil {
			slog.ErrorContext(ctx, "read file for snapshot failed", "path", path, "err", err)
			return Snapshot{}, fmt.Errorf("read file for snapshot %s: %w", path, err)
		}

		relativePath, err := filepath.Rel(root, path)
		if err != nil {
			slog.ErrorContext(
				ctx,
				"compute snapshot relative path failed",
				"root",
				root,
				"path",
				path,
				"err",
				err,
			)
			return Snapshot{}, fmt.Errorf(
				"compute snapshot relative path for %s: %w",
				path,
				err,
			)
		}
		// Skip files the indexer will also skip so the indexer and merkle
		// agree on the file set. Otherwise every sync treats the same bad
		// file as "modified" forever and the delta loop never converges.
		if !utf8.Valid(data) {
			continue
		}
		files[relativePath] = digestBytes(data)
	}

	return Snapshot{ConfigDigest: "", Files: files}, nil
}

// WriteSnapshot persists a snapshot atomically.
func WriteSnapshot(path string, snapshot Snapshot) error {
	slog.Debug("write Merkle snapshot", "path", path, "files", len(snapshot.Files))

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		slog.Error("marshal Merkle snapshot failed", "path", path, "err", err)
		return fmt.Errorf("marshal snapshot %s: %w", path, err)
	}

	if err := store.EnsureDir(filepath.Dir(path)); err != nil {
		slog.Error("ensure Merkle directory failed", "path", path, "err", err)
		return fmt.Errorf("ensure Merkle directory for %s: %w", path, err)
	}

	tempFile, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		slog.Error("create temp Merkle snapshot failed", "path", path, "err", err)
		return fmt.Errorf("create temp snapshot %s: %w", path, err)
	}
	tempPath := tempFile.Name()

	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		_ = os.Remove(tempPath)
		slog.Error("write temp Merkle snapshot failed", "path", tempPath, "err", err)
		return fmt.Errorf("write temp snapshot %s: %w", tempPath, err)
	}
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)
		slog.Error("close temp Merkle snapshot failed", "path", tempPath, "err", err)
		return fmt.Errorf("close temp snapshot %s: %w", tempPath, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		slog.Error(
			"rename temp Merkle snapshot failed",
			"from",
			tempPath,
			"to",
			path,
			"err",
			err,
		)
		return fmt.Errorf("rename temp snapshot %s to %s: %w", tempPath, path, err)
	}
	return nil
}

// LoadSnapshotForConfig returns the snapshot at path when its ConfigDigest
// matches the requested digest. A missing, unreadable, or mismatched
// snapshot returns an empty snapshot stamped with the requested digest so
// the caller can begin writing per-file checkpoints under the new config.
func LoadSnapshotForConfig(path string, configDigest string) Snapshot {
	empty := Snapshot{ConfigDigest: configDigest, Files: map[string]string{}}
	snapshot, err := ReadSnapshot(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("load merkle snapshot failed; treating as empty", "path", path, "err", err)
		}
		return empty
	}
	if snapshot.ConfigDigest != configDigest {
		slog.Info("merkle snapshot config digest mismatch; starting fresh checkpoint", "path", path, "snapshot_digest", snapshot.ConfigDigest, "request_digest", configDigest)
		return empty
	}
	return snapshot
}

// ReadSnapshot loads one persisted snapshot.
func ReadSnapshot(path string) (Snapshot, error) {
	slog.Debug("read Merkle snapshot", "path", path)

	data, err := os.ReadFile(path)
	if err != nil {
		slog.Error("read Merkle snapshot failed", "path", path, "err", err)
		return Snapshot{}, fmt.Errorf("read snapshot %s: %w", path, err)
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		slog.Error("unmarshal Merkle snapshot failed", "path", path, "err", err)
		return Snapshot{}, fmt.Errorf("unmarshal snapshot %s: %w", path, err)
	}
	if snapshot.Files == nil {
		snapshot.Files = map[string]string{}
	}
	return snapshot, nil
}

// Diff describes the per-file changes between two snapshots.
type Diff struct {
	Added    []string // relative paths present in current, absent in prev
	Modified []string // present in both, hash differs
	Removed  []string // present in prev, absent in current
}

// Empty reports whether the diff contains zero changes.
func (diff Diff) Empty() bool {
	return len(diff.Added) == 0 && len(diff.Modified) == 0 && len(diff.Removed) == 0
}

// DiffSnapshots reports the per-file added, modified, and removed paths
// between two snapshots. The returned slices are sorted for deterministic
// downstream processing.
func DiffSnapshots(prev Snapshot, current Snapshot) Diff {
	added := []string{}
	modified := []string{}
	removed := []string{}

	for path, currentHash := range current.Files {
		previousHash, found := prev.Files[path]
		if !found {
			added = append(added, path)
			continue
		}
		if previousHash != currentHash {
			modified = append(modified, path)
		}
	}
	for path := range prev.Files {
		if _, found := current.Files[path]; !found {
			removed = append(removed, path)
		}
	}

	slices.Sort(added)
	slices.Sort(modified)
	slices.Sort(removed)
	return Diff{Added: added, Modified: modified, Removed: removed}
}

// Equal reports whether two snapshots describe the same file set and hashes.
func Equal(left Snapshot, right Snapshot) bool {
	if len(left.Files) != len(right.Files) {
		return false
	}

	keys := make([]string, 0, len(left.Files))
	for path := range left.Files {
		keys = append(keys, path)
	}
	slices.Sort(keys)

	for _, path := range keys {
		if left.Files[path] != right.Files[path] {
			return false
		}
	}
	return true
}

func digestBytes(data []byte) string {
	hashBytes := sha256.Sum256(data)
	return hex.EncodeToString(hashBytes[:])
}
