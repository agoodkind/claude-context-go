// Package indexer walks codebases and produces file and chunk counts.
package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"

	"github.com/zilliztech/claude-context-go/internal/discovery"
	"github.com/zilliztech/claude-context-go/internal/model"
	"github.com/zilliztech/claude-context-go/internal/splitter"
)

// Runner executes the local discovery and splitting pipeline for one codebase.
type Runner struct {
	dispatcher *splitter.Dispatcher
}

// Result captures file and chunk totals for one indexing pass.
type Result struct {
	IndexedFiles int32
	TotalChunks  int32
}

// NewRunner constructs the local indexing runner.
func NewRunner() *Runner {
	return &Runner{
		dispatcher: splitter.NewDispatcher(),
	}
}

// Index walks the codebase and splits files into chunks.
func (runner *Runner) Index(ctx context.Context, root string, indexConfig model.IndexConfig) (Result, error) {
	discoveryResult, err := discovery.Discover(ctx, root, indexConfig.IgnorePatterns, indexConfig.Extensions)
	if err != nil {
		slog.ErrorContext(ctx, "discover source files failed", "root", root, "err", err)
		return Result{}, fmt.Errorf("discover source files under %s: %w", root, err)
	}

	var totalChunks int32
	for _, path := range discoveryResult.Files {
		data, err := os.ReadFile(path)
		if err != nil {
			slog.ErrorContext(ctx, "read source file failed", "path", path, "err", err)
			return Result{}, fmt.Errorf("read source file %s: %w", path, err)
		}
		splitResult, err := runner.dispatcher.SplitFile(ctx, path, data)
		if err != nil {
			slog.ErrorContext(ctx, "split source file failed", "path", path, "err", err)
			return Result{}, fmt.Errorf("split source file %s: %w", path, err)
		}
		totalChunks += safeInt32(len(splitResult.Chunks))
	}

	return Result{
		IndexedFiles: safeInt32(len(discoveryResult.Files)),
		TotalChunks:  totalChunks,
	}, nil
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
