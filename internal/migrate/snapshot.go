// Package migrate imports legacy Claude Context state into the Go daemon model.
package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"goodkind.io/claude-context-go/internal/model"
)

type legacyStatus string

const (
	legacyStatusIndexed     legacyStatus = "indexed"
	legacyStatusIndexFailed legacyStatus = "indexfailed"
	legacyStatusIndexing    legacyStatus = "indexing"
)

type legacySnapshot struct {
	FormatVersion string                    `json:"formatVersion"`
	Codebases     map[string]legacyCodebase `json:"codebases"`
	LastUpdated   string                    `json:"lastUpdated"`
}

type legacyCodebase struct {
	Status                  string   `json:"status"`
	IndexedFiles            int32    `json:"indexedFiles"`
	TotalChunks             int32    `json:"totalChunks"`
	IndexStatus             string   `json:"indexStatus"`
	IndexingPercentage      float64  `json:"indexingPercentage"`
	ErrorMessage            string   `json:"errorMessage"`
	LastAttemptedPercentage float64  `json:"lastAttemptedPercentage"`
	RequestSplitter         string   `json:"requestSplitter"`
	RequestCustomExtensions []string `json:"requestCustomExtensions"`
	RequestIgnorePatterns   []string `json:"requestIgnorePatterns"`
	LastUpdated             string   `json:"lastUpdated"`
}

// ImportLegacySnapshot reads one legacy MCP snapshot file and converts it to daemon state.
func ImportLegacySnapshot(path string) ([]model.Codebase, []model.Job, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		slog.Error("read legacy snapshot failed", "path", path, "err", err)
		return nil, nil, fmt.Errorf("read legacy snapshot %s: %w", path, err)
	}

	var snapshot legacySnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		slog.Error("unmarshal legacy snapshot failed", "path", path, "err", err)
		return nil, nil, fmt.Errorf("unmarshal legacy snapshot %s: %w", path, err)
	}

	codebases := make([]model.Codebase, 0, len(snapshot.Codebases))
	jobs := make([]model.Job, 0)

	for codebasePath, legacy := range snapshot.Codebases {
		absolutePath, err := filepath.Abs(codebasePath)
		if err != nil {
			slog.Error("resolve absolute legacy path failed", "path", codebasePath, "err", err)
			return nil, nil, fmt.Errorf("resolve absolute path for %s: %w", codebasePath, err)
		}

		codebaseID := legacyCodebaseID(absolutePath)
		updatedAt := parseTimestamp(legacy.LastUpdated)
		codebase := model.Codebase{
			ID:                codebaseID,
			CanonicalPath:     absolutePath,
			Aliases:           []string{absolutePath},
			Status:            model.CodebaseStatusNotIndexed,
			ActiveJobID:       "",
			LastSuccessfulRun: nil,
			LastFailedRun:     nil,
			UpdatedAt:         updatedAt,
			EffectiveConfig: model.IndexConfig{
				SplitterType:       defaultIfEmpty(legacy.RequestSplitter, "ast"),
				SplitterChunkSize:  2500,
				SplitterOverlap:    300,
				Extensions:         append([]string{}, legacy.RequestCustomExtensions...),
				IgnorePatterns:     append([]string{}, legacy.RequestIgnorePatterns...),
				IgnoreDigest:       "",
				EmbeddingProvider:  "",
				EmbeddingModel:     "",
				EmbeddingDimension: 0,
				VectorBackend:      "milvus",
				Hybrid:             true,
			},
			CollectionName:        legacyCollectionName(absolutePath, true),
			LegacyCollectionNames: nil,
			MerkleSnapshotPath:    "",
		}

		switch legacyStatus(legacy.Status) {
		case legacyStatusIndexed:
			codebase.Status = model.CodebaseStatusIndexed
			codebase.LastSuccessfulRun = &model.IndexRunSummary{
				IndexedFiles: legacy.IndexedFiles,
				TotalChunks:  legacy.TotalChunks,
				Status:       defaultIfEmpty(legacy.IndexStatus, "completed"),
				CompletedAt:  updatedAt,
			}
		case legacyStatusIndexFailed:
			codebase.Status = model.CodebaseStatusFailed
			codebase.LastFailedRun = &model.IndexRunFailure{
				Message:                 defaultIfEmpty(legacy.ErrorMessage, "legacy index failed"),
				LastAttemptedPercentage: int32(legacy.LastAttemptedPercentage),
				FailedAt:                updatedAt,
			}
			jobs = append(jobs, legacyFailedJob(codebase, legacy, updatedAt, "legacy_failed"))
		case legacyStatusIndexing:
			codebase.Status = model.CodebaseStatusFailed
			codebase.LastFailedRun = &model.IndexRunFailure{
				Message:                 "Indexing was interrupted during migration",
				LastAttemptedPercentage: int32(legacy.IndexingPercentage),
				FailedAt:                updatedAt,
			}
			jobs = append(jobs, legacyFailedJob(codebase, legacy, updatedAt, "legacy_interrupted"))
		default:
			codebase.Status = model.CodebaseStatusStale
		}

		codebases = append(codebases, codebase)
	}

	sort.Slice(codebases, func(i int, j int) bool {
		return codebases[i].CanonicalPath < codebases[j].CanonicalPath
	})
	return codebases, jobs, nil
}

// LegacySnapshotPath returns the well-known MCP snapshot path.
func LegacySnapshotPath() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		slog.Error("resolve user home directory failed", "err", err)
		return "", fmt.Errorf("resolve user home directory: %w", err)
	}
	return filepath.Join(homeDir, ".context", "mcp-codebase-snapshot.json"), nil
}

// SnapshotExists reports whether the legacy MCP snapshot is present.
func SnapshotExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !errors.Is(err, os.ErrNotExist)
}

func legacyFailedJob(codebase model.Codebase, legacy legacyCodebase, timestamp time.Time, reason string) model.Job {
	return model.Job{
		ID:            legacyJobID(codebase.CanonicalPath, reason),
		CodebaseID:    codebase.ID,
		RequestedPath: codebase.CanonicalPath,
		CanonicalPath: codebase.CanonicalPath,
		Client: model.ClientInfo{
			Name: "legacy-migration",
			PID:  0,
		},
		Operation: "index",
		State:     model.JobStateFailed,
		Progress: model.Progress{
			Phase:                     "failed",
			PhasePercent:              0,
			OverallPercent:            legacy.IndexingPercentage,
			FilesTotal:                0,
			FilesProcessed:            0,
			ChunksGenerated:           0,
			EmbeddingBatchesTotal:     0,
			EmbeddingBatchesCompleted: 0,
			CollectionRowsWritten:     0,
			LastEventAt:               timestamp,
			HeartbeatAt:               timestamp,
		},
		Config:      codebase.EffectiveConfig,
		StartedAt:   timestamp,
		UpdatedAt:   timestamp,
		CompletedAt: &timestamp,
		Error: &model.JobError{
			Message:   defaultIfEmpty(legacy.ErrorMessage, "legacy migration failure"),
			Retryable: false,
		},
	}
}

func legacyCollectionName(path string, hybrid bool) string {
	prefix := "code_chunks"
	if hybrid {
		prefix = "hybrid_code_chunks"
	}
	hash := sha256.Sum256([]byte(path))
	return prefix + "_" + hex.EncodeToString(hash[:])[:8]
}

func legacyCodebaseID(path string) string {
	hash := sha256.Sum256([]byte(path))
	return "cb_" + hex.EncodeToString(hash[:])[:8]
}

func legacyJobID(path string, reason string) string {
	hash := sha256.Sum256([]byte(path + ":" + reason))
	return "job_" + hex.EncodeToString(hash[:])[:8]
}

func parseTimestamp(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsedTime, err := time.Parse(time.RFC3339, value)
	if err == nil {
		return parsedTime
	}
	return time.Time{}
}

func defaultIfEmpty(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
