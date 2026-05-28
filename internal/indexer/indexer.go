// Package indexer walks codebases and produces file and chunk counts.
package indexer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"unicode/utf8"

	"goodkind.io/claude-context-go/internal/discovery"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/splitter"
)

// defaultMaxFileBytes caps a single file at 2 MiB before it reaches the
// splitter. Larger files are skipped so a stray generated dump or vendored
// blob cannot inflate one embedding batch into something Milvus refuses.
// Override at runtime with INDEX_MAX_FILE_BYTES (set to 0 to disable).
const defaultMaxFileBytes int64 = 2 * 1024 * 1024

// Runner executes the local discovery and splitting pipeline for one codebase.
type Runner struct {
	dispatcher   *splitter.Dispatcher
	maxFileBytes int64
}

// Result captures file and chunk totals for one indexing pass.
//
// SkippedFiles names files the indexer refused to embed. A file is skipped
// when its bytes are not valid UTF-8 (Milvus requires every VarChar field
// to be valid UTF-8 on the gRPC wire) or when its size exceeds the
// per-file cap.
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
		dispatcher:   splitter.NewDispatcher(),
		maxFileBytes: resolveMaxFileBytes(),
	}
}

// resolveMaxFileBytes reads INDEX_MAX_FILE_BYTES from the environment. An
// unset or unparseable value falls back to defaultMaxFileBytes. A value of
// 0 or below disables the cap.
func resolveMaxFileBytes() int64 {
	raw := os.Getenv("INDEX_MAX_FILE_BYTES")
	if raw == "" {
		return defaultMaxFileBytes
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return defaultMaxFileBytes
	}
	return parsed
}

// processedFile is the per-file output of one splitter pass. Skipped=true
// means the file's bytes are not valid UTF-8; Chunks and FileHash are then
// empty and callers add the path to Result.SkippedFiles. Removed=true means
// the file was absent on disk when the task ran, so the converge operation
// for this path is a removal: callers delete its rows and drop it from the
// snapshot rather than treating the absence as an error.
type processedFile struct {
	Chunks   []model.StoredChunk
	FileHash string
	Skipped  bool
	Removed  bool
}

// OneFileResult mirrors the per-file accumulator output for callers that
// drive their own iteration (the daemon's per-file delta loop).
type OneFileResult = processedFile

// IndexOne reads, gates, and splits a single file. The daemon's per-file
// delta loop calls this so the merkle snapshot can be flushed after each
// successful semantic.Reindex call.
func (runner *Runner) IndexOne(ctx context.Context, root string, relativePath string, cfg model.IndexConfig) (OneFileResult, error) {
	fullPath := filepath.Join(root, relativePath)
	if oversize, err := runner.isOversize(ctx, fullPath, relativePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			slog.DebugContext(ctx, "source file absent; converging to removal", "path", fullPath)
			return OneFileResult{Chunks: nil, FileHash: "", Skipped: false, Removed: true}, nil
		}
		return OneFileResult{Chunks: nil, FileHash: "", Skipped: false, Removed: false}, err
	} else if oversize {
		return OneFileResult{Chunks: nil, FileHash: "", Skipped: true, Removed: false}, nil
	}
	data, err := os.ReadFile(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			slog.DebugContext(ctx, "source file absent; converging to removal", "path", fullPath)
			return OneFileResult{Chunks: nil, FileHash: "", Skipped: false, Removed: true}, nil
		}
		slog.ErrorContext(ctx, "read source file failed", "path", fullPath, "err", err)
		return OneFileResult{Chunks: nil, FileHash: "", Skipped: false, Removed: false}, fmt.Errorf("read source file %s: %w", fullPath, err)
	}
	return runner.processFile(ctx, fullPath, relativePath, data, cfg.SplitterType)
}

// isOversize reports whether the file at fullPath exceeds the runner's
// per-file size cap. A cap of 0 or below disables the check.
func (runner *Runner) isOversize(ctx context.Context, fullPath string, relativePath string) (bool, error) {
	if runner.maxFileBytes <= 0 {
		return false, nil
	}
	info, err := os.Stat(fullPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.ErrorContext(ctx, "stat source file failed", "path", fullPath, "err", err)
		}
		return false, fmt.Errorf("stat source file %s: %w", fullPath, err)
	}
	if info.Size() <= runner.maxFileBytes {
		return false, nil
	}
	slog.WarnContext(ctx, "indexer.skipped_oversize", "path", relativePath, "bytes", info.Size(), "limit", runner.maxFileBytes)
	return true, nil
}

// processFile splits one file into chunks. When the file's bytes are not
// valid UTF-8 the splitter is skipped and Skipped=true is returned. Milvus
// rejects non-UTF-8 VarChar payloads at the gRPC marshal boundary, so the
// skip happens at the indexer boundary.
func (runner *Runner) processFile(ctx context.Context, fullPath string, relativePath string, data []byte, splitterType string) (processedFile, error) {
	if !utf8.Valid(data) {
		slog.WarnContext(ctx, "indexer.skipped_invalid_utf8", "path", relativePath, "bytes", len(data))
		return processedFile{Chunks: nil, FileHash: "", Skipped: true, Removed: false}, nil
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
	return processedFile{Chunks: chunks, FileHash: digestFileBytes(data), Skipped: false, Removed: false}, nil
}

// indexAccumulator collects per-file output across one indexing pass.
type indexAccumulator struct {
	totalChunks  int32
	indexedCount int32
	storedChunks []model.StoredChunk
	fileHashes   map[string]string
	skippedFiles []string
}

// ingestFile reads one file, checks size and UTF-8 gates, and routes the
// result into the accumulator. It returns false when the file was skipped.
func (runner *Runner) ingestFile(ctx context.Context, fullPath string, relativePath string, splitterType string, accumulator *indexAccumulator) error {
	if oversize, sizeErr := runner.isOversize(ctx, fullPath, relativePath); sizeErr != nil {
		if errors.Is(sizeErr, os.ErrNotExist) {
			slog.DebugContext(ctx, "source file absent during walk; excluding from rebuild", "path", fullPath)
			return nil
		}
		return sizeErr
	} else if oversize {
		accumulator.skippedFiles = append(accumulator.skippedFiles, relativePath)
		return nil
	}
	data, err := os.ReadFile(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			slog.DebugContext(ctx, "source file absent during walk; excluding from rebuild", "path", fullPath)
			return nil
		}
		slog.ErrorContext(ctx, "read source file failed", "path", fullPath, "err", err)
		return fmt.Errorf("read source file %s: %w", fullPath, err)
	}
	processed, err := runner.processFile(ctx, fullPath, relativePath, data, splitterType)
	if err != nil {
		slog.ErrorContext(ctx, "split source file failed", "path", fullPath, "err", err)
		return err
	}
	if processed.Skipped {
		accumulator.skippedFiles = append(accumulator.skippedFiles, relativePath)
		return nil
	}
	accumulator.totalChunks += safeInt32(len(processed.Chunks))
	accumulator.indexedCount++
	accumulator.fileHashes[relativePath] = processed.FileHash
	accumulator.storedChunks = append(accumulator.storedChunks, processed.Chunks...)
	return nil
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

	accumulator := &indexAccumulator{
		totalChunks:  0,
		indexedCount: 0,
		storedChunks: make([]model.StoredChunk, 0),
		fileHashes:   make(map[string]string, len(discoveryResult.Files)),
		skippedFiles: []string{},
	}
	for index, path := range discoveryResult.Files {
		if err := ctx.Err(); err != nil {
			slog.ErrorContext(ctx, "indexing cancelled before file read", "path", path, "err", err)
			return Result{}, fmt.Errorf("indexing cancelled before file read %s: %w", path, err)
		}
		relativePath, err := filepath.Rel(root, path)
		if err != nil {
			slog.ErrorContext(ctx, "compute relative chunk path failed", "root", root, "path", path, "err", err)
			return Result{}, fmt.Errorf("compute relative chunk path for %s: %w", path, err)
		}
		if err := runner.ingestFile(ctx, path, relativePath, indexConfig.SplitterType, accumulator); err != nil {
			return Result{}, err
		}
		if progress != nil {
			progress(Progress{
				Phase:           "Processing files and generating embeddings...",
				OverallPercent:  calculateOverallPercent(index+1, len(discoveryResult.Files)),
				FilesTotal:      totalFiles,
				FilesProcessed:  safeInt32(index + 1),
				ChunksGenerated: accumulator.totalChunks,
			})
		}
	}

	if progress != nil {
		progress(Progress{
			Phase:           "completed",
			OverallPercent:  100,
			FilesTotal:      totalFiles,
			FilesProcessed:  totalFiles,
			ChunksGenerated: accumulator.totalChunks,
		})
	}

	return Result{
		IndexedFiles: accumulator.indexedCount,
		TotalChunks:  accumulator.totalChunks,
		Chunks:       accumulator.storedChunks,
		FileHashes:   accumulator.fileHashes,
		SkippedFiles: accumulator.skippedFiles,
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

	accumulator := &indexAccumulator{
		totalChunks:  0,
		indexedCount: 0,
		storedChunks: make([]model.StoredChunk, 0),
		fileHashes:   make(map[string]string, len(relativePaths)),
		skippedFiles: []string{},
	}
	for index, relativePath := range relativePaths {
		if err := ctx.Err(); err != nil {
			slog.ErrorContext(ctx, "delta indexing cancelled before file read", "path", relativePath, "err", err)
			return Result{}, fmt.Errorf("delta indexing cancelled before file read %s: %w", relativePath, err)
		}
		fullPath := filepath.Join(root, relativePath)
		if err := runner.ingestFile(ctx, fullPath, relativePath, indexConfig.SplitterType, accumulator); err != nil {
			return Result{}, err
		}
		if progress != nil {
			progress(Progress{
				Phase:           "Processing changed files...",
				OverallPercent:  calculateOverallPercent(index+1, len(relativePaths)),
				FilesTotal:      totalFiles,
				FilesProcessed:  safeInt32(index + 1),
				ChunksGenerated: accumulator.totalChunks,
			})
		}
	}

	if progress != nil {
		progress(Progress{
			Phase:           "completed",
			OverallPercent:  100,
			FilesTotal:      totalFiles,
			FilesProcessed:  totalFiles,
			ChunksGenerated: accumulator.totalChunks,
		})
	}

	return Result{
		IndexedFiles: accumulator.indexedCount,
		TotalChunks:  accumulator.totalChunks,
		Chunks:       accumulator.storedChunks,
		FileHashes:   accumulator.fileHashes,
		SkippedFiles: accumulator.skippedFiles,
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
