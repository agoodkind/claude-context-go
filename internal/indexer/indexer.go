// Package indexer walks codebases and produces file and chunk counts.
package indexer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"unicode/utf8"

	"goodkind.io/claude-context-go/internal/discovery"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/splitter"
)

// Runner executes the local discovery and splitting pipeline for one codebase.
type Runner struct {
	dispatcher *splitter.Dispatcher
}

// Result captures file and chunk totals for one indexing pass.
//
// SkippedFiles names files whose bytes are not valid UTF-8. Milvus requires
// every VarChar field to be valid UTF-8 on the gRPC wire, so embedding such
// a file marshal-fails and rolls back the entire batch. The indexer reports
// these paths to the operator.
type Result struct {
	IndexedFiles int32
	TotalChunks  int32
	Chunks       []model.StoredChunk
	FileHashes   map[string]string
	SkippedFiles []string
}

// Progress describes one visible indexing progress update.
type Progress struct {
	Phase           string
	OverallPercent  float64
	FilesTotal      int32
	FilesProcessed  int32
	ChunksGenerated int32
}

// NewRunner constructs the local indexing runner.
func NewRunner() *Runner {
	return &Runner{
		dispatcher: splitter.NewDispatcher(),
	}
}

// processedFile is the per-file output of one splitter pass. Skipped=true
// means the file's bytes are not valid UTF-8; Chunks and FileHash are then
// empty and callers add the path to Result.SkippedFiles.
type processedFile struct {
	Chunks   []model.StoredChunk
	FileHash string
	Skipped  bool
}

// processFile splits one file into chunks. When the file's bytes are not
// valid UTF-8 the splitter is skipped and Skipped=true is returned. Milvus
// rejects non-UTF-8 VarChar payloads at the gRPC marshal boundary, so the
// skip happens at the indexer boundary.
func (runner *Runner) processFile(ctx context.Context, fullPath string, relativePath string, data []byte, splitterType string) (processedFile, error) {
	if !utf8.Valid(data) {
		slog.WarnContext(ctx, "indexer.skipped_invalid_utf8", "path", relativePath, "bytes", len(data))
		return processedFile{Chunks: nil, FileHash: "", Skipped: true}, nil
	}
	splitResult, err := runner.dispatcher.SplitFileWithType(ctx, fullPath, data, splitterType)
	if err != nil {
		return processedFile{}, fmt.Errorf("split source file %s: %w", fullPath, err)
	}
	chunks := make([]model.StoredChunk, 0, len(splitResult.Chunks))
	for _, chunk := range splitResult.Chunks {
		chunks = append(chunks, model.StoredChunk{
			Content:       chunk.Content,
			RelativePath:  relativePath,
			StartLine:     safeInt32(chunk.StartLine),
			EndLine:       safeInt32(chunk.EndLine),
			Language:      chunk.Language,
			FileExtension: filepath.Ext(relativePath),
		})
	}
	return processedFile{Chunks: chunks, FileHash: digestFileBytes(data), Skipped: false}, nil
}

// Index walks the codebase and splits files into chunks.
func (runner *Runner) Index(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(Progress)) (Result, error) {
	if progress != nil {
		progress(Progress{
			Phase:           "Preparing and scanning files...",
			OverallPercent:  0,
			FilesTotal:      0,
			FilesProcessed:  0,
			ChunksGenerated: 0,
		})
	}

	discoveryResult, err := discovery.Discover(ctx, root, indexConfig.IgnorePatterns, indexConfig.Extensions)
	if err != nil {
		slog.ErrorContext(ctx, "discover source files failed", "root", root, "err", err)
		return Result{}, fmt.Errorf("discover source files under %s: %w", root, err)
	}

	totalFiles := safeInt32(len(discoveryResult.Files))
	if progress != nil {
		progress(Progress{
			Phase:           "Processing files and generating embeddings...",
			OverallPercent:  10,
			FilesTotal:      totalFiles,
			FilesProcessed:  0,
			ChunksGenerated: 0,
		})
	}

	var totalChunks int32
	var indexedCount int32
	storedChunks := make([]model.StoredChunk, 0)
	fileHashes := make(map[string]string, len(discoveryResult.Files))
	skippedFiles := []string{}
	for index, path := range discoveryResult.Files {
		if err := ctx.Err(); err != nil {
			slog.ErrorContext(ctx, "indexing cancelled before file read", "path", path, "err", err)
			return Result{}, fmt.Errorf("indexing cancelled before file read %s: %w", path, err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			slog.ErrorContext(ctx, "read source file failed", "path", path, "err", err)
			return Result{}, fmt.Errorf("read source file %s: %w", path, err)
		}
		relativePath, err := filepath.Rel(root, path)
		if err != nil {
			slog.ErrorContext(ctx, "compute relative chunk path failed", "root", root, "path", path, "err", err)
			return Result{}, fmt.Errorf("compute relative chunk path for %s: %w", path, err)
		}
		processed, err := runner.processFile(ctx, path, relativePath, data, indexConfig.SplitterType)
		if err != nil {
			slog.ErrorContext(ctx, "split source file failed", "path", path, "err", err)
			return Result{}, err
		}
		if processed.Skipped {
			skippedFiles = append(skippedFiles, relativePath)
		} else {
			totalChunks += safeInt32(len(processed.Chunks))
			indexedCount++
			fileHashes[relativePath] = processed.FileHash
			storedChunks = append(storedChunks, processed.Chunks...)
		}
		if progress != nil {
			progress(Progress{
				Phase:           "Processing files and generating embeddings...",
				OverallPercent:  calculateOverallPercent(index+1, len(discoveryResult.Files)),
				FilesTotal:      totalFiles,
				FilesProcessed:  safeInt32(index + 1),
				ChunksGenerated: totalChunks,
			})
		}
	}

	if progress != nil {
		progress(Progress{
			Phase:           "completed",
			OverallPercent:  100,
			FilesTotal:      totalFiles,
			FilesProcessed:  totalFiles,
			ChunksGenerated: totalChunks,
		})
	}

	return Result{
		IndexedFiles: indexedCount,
		TotalChunks:  totalChunks,
		Chunks:       storedChunks,
		FileHashes:   fileHashes,
		SkippedFiles: skippedFiles,
	}, nil
}

// IndexFiles processes the explicit relative-path allowlist instead of
// re-walking the codebase. Use it for delta reindex passes where the caller
// has already computed the changed-file set (via merkle.DiffSnapshots).
//
// FileHashes in the result covers only the supplied files. Callers should
// merge this map into the previous snapshot before persisting.
func (runner *Runner) IndexFiles(ctx context.Context, root string, relativePaths []string, indexConfig model.IndexConfig, progress func(Progress)) (Result, error) {
	totalFiles := safeInt32(len(relativePaths))
	if progress != nil {
		progress(Progress{
			Phase:           "Processing changed files...",
			OverallPercent:  10,
			FilesTotal:      totalFiles,
			FilesProcessed:  0,
			ChunksGenerated: 0,
		})
	}

	var totalChunks int32
	var indexedCount int32
	storedChunks := make([]model.StoredChunk, 0)
	fileHashes := make(map[string]string, len(relativePaths))
	skippedFiles := []string{}
	for index, relativePath := range relativePaths {
		if err := ctx.Err(); err != nil {
			slog.ErrorContext(ctx, "delta indexing cancelled before file read", "path", relativePath, "err", err)
			return Result{}, fmt.Errorf("delta indexing cancelled before file read %s: %w", relativePath, err)
		}
		fullPath := filepath.Join(root, relativePath)
		data, err := os.ReadFile(fullPath)
		if err != nil {
			slog.ErrorContext(ctx, "read changed file failed", "path", fullPath, "err", err)
			return Result{}, fmt.Errorf("read changed file %s: %w", fullPath, err)
		}
		processed, err := runner.processFile(ctx, fullPath, relativePath, data, indexConfig.SplitterType)
		if err != nil {
			slog.ErrorContext(ctx, "split changed file failed", "path", fullPath, "err", err)
			return Result{}, err
		}
		if processed.Skipped {
			skippedFiles = append(skippedFiles, relativePath)
		} else {
			totalChunks += safeInt32(len(processed.Chunks))
			indexedCount++
			fileHashes[relativePath] = processed.FileHash
			storedChunks = append(storedChunks, processed.Chunks...)
		}
		if progress != nil {
			progress(Progress{
				Phase:           "Processing changed files...",
				OverallPercent:  calculateOverallPercent(index+1, len(relativePaths)),
				FilesTotal:      totalFiles,
				FilesProcessed:  safeInt32(index + 1),
				ChunksGenerated: totalChunks,
			})
		}
	}

	if progress != nil {
		progress(Progress{
			Phase:           "completed",
			OverallPercent:  100,
			FilesTotal:      totalFiles,
			FilesProcessed:  totalFiles,
			ChunksGenerated: totalChunks,
		})
	}

	return Result{
		IndexedFiles: indexedCount,
		TotalChunks:  totalChunks,
		Chunks:       storedChunks,
		FileHashes:   fileHashes,
		SkippedFiles: skippedFiles,
	}, nil
}

func digestFileBytes(data []byte) string {
	hashBytes := sha256.Sum256(data)
	return hex.EncodeToString(hashBytes[:])
}

func calculateOverallPercent(processedFiles int, totalFiles int) float64 {
	if totalFiles <= 0 {
		return 100
	}
	return 10 + (float64(processedFiles)/float64(totalFiles))*90
}

func safeInt32(value int) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	if value < math.MinInt32 {
		return math.MinInt32
	}
	return int32(value)
}
