package daemon

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"goodkind.io/claude-context-go/internal/clock"
	"goodkind.io/claude-context-go/internal/model"
)

// legacyDigestForCodebase returns the canonical digest of the codebase's
// stored EffectiveConfig. The plan helpers pass this to
// merkle.LoadSnapshotForConfig so a pre-config-digest snapshot is salvaged
// when the request matches what the codebase was last indexed under.
func (manager *Manager) legacyDigestForCodebase(codebaseID string) string {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	codebase, found := manager.codebases[codebaseID]
	if !found {
		return ""
	}
	return digestIndexConfig(codebase.EffectiveConfig)
}

// digestIndexConfig hashes the indexing-relevant fields of an IndexConfig.
// The IgnoreDigest field itself is excluded from the hash input so the
// digest is stable across runs: re-hashing a stored EffectiveConfig
// produces the same value, which the merkle snapshot's ConfigDigest match
// relies on for resume.
func digestIndexConfig(indexConfig model.IndexConfig) string {
	hashable := indexConfig
	hashable.IgnoreDigest = ""
	digestBytes, err := json.Marshal(hashable)
	if err != nil {
		digest := sha256.Sum256([]byte(hashable.SplitterType))
		return "sha256:" + hex.EncodeToString(digest[:])
	}
	digest := sha256.Sum256(digestBytes)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func (manager *Manager) enrichIndexConfig(indexConfig model.IndexConfig) model.IndexConfig {
	if strings.TrimSpace(indexConfig.SplitterType) == "" {
		indexConfig.SplitterType = "ast"
	}
	if indexConfig.SplitterChunkSize == 0 {
		indexConfig.SplitterChunkSize = 2500
	}
	if indexConfig.SplitterOverlap == 0 {
		indexConfig.SplitterOverlap = 300
	}
	indexConfig.EmbeddingProvider = manager.config.EmbeddingProvider
	indexConfig.EmbeddingModel = manager.config.EmbeddingModel
	if manager.config.EmbeddingDimension > 0 {
		indexConfig.EmbeddingDimension = manager.config.EmbeddingDimension
	}
	indexConfig.VectorBackend = "milvus"
	indexConfig.Hybrid = manager.config.HybridMode
	indexConfig.Extensions = mergeDistinct(indexConfig.Extensions, manager.config.CustomExtensions)
	indexConfig.IgnorePatterns = mergeDistinct(indexConfig.IgnorePatterns, manager.config.CustomIgnorePatterns)
	return indexConfig
}

// mergeDistinct returns base + extras with duplicates removed and original
// ordering preserved.
func mergeDistinct(base []string, extras []string) []string {
	if len(extras) == 0 {
		return base
	}
	seen := make(map[string]struct{}, len(base)+len(extras))
	out := make([]string, 0, len(base)+len(extras))
	for _, value := range base {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	for _, value := range extras {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (manager *Manager) chunkPath(codebaseID string) string {
	return filepath.Join(manager.config.ChunksDir, codebaseID+".json")
}

func (manager *Manager) merklePath(codebaseID string) string {
	return filepath.Join(manager.config.MerkleDir, codebaseID+".json")
}

func newID(prefix string) string {
	randomBytes := make([]byte, 6)
	if _, err := rand.Read(randomBytes); err != nil {
		return fmt.Sprintf("%s_%d", prefix, clock.Now().UnixNano())
	}
	return fmt.Sprintf("%s_%d_%s", prefix, clock.Now().Unix(), hex.EncodeToString(randomBytes))
}
