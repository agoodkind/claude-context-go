package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/semantic"
	"goodkind.io/claude-context-go/internal/store"
)

// SearchCode performs a local ranked search over persisted chunk content.
func (manager *Manager) SearchCode(ctx context.Context, requestedPath string, query string, limit int32, extensionFilter []string) (SearchOutcome, error) {
	normalizedExtensions, err := semantic.ValidateExtensionFilter(extensionFilter)
	if err != nil {
		return SearchOutcome{}, fmt.Errorf("validate extension filter: %w", err)
	}

	codebase, activeJob, found, err := manager.GetIndex(ctx, requestedPath)
	if err != nil {
		return SearchOutcome{}, err
	}
	if !found {
		return SearchOutcome{}, errors.New("codebase not tracked: " + requestedPath)
	}

	if manager.semantic != nil && manager.semantic.Available() {
		chunks, semanticErr := manager.semantic.Search(ctx, codebase.CanonicalPath, query, limit, normalizedExtensions)
		switch {
		case semanticErr == nil:
			return SearchOutcome{
				Codebase:  codebase,
				ActiveJob: activeJob,
				Results:   semantic.DeduplicateChunks(chunks),
			}, nil
		case (errors.Is(semanticErr, semantic.ErrCollectionMissing) ||
			errors.Is(semanticErr, semantic.ErrCollectionNotReady) ||
			errors.Is(semanticErr, semantic.ErrSearchResultIncomplete)) &&
			codebase.Status == model.CodebaseStatusIndexing:
			return SearchOutcome{Codebase: codebase, ActiveJob: activeJob, Results: []model.StoredChunk{}}, nil
		case errors.Is(semanticErr, semantic.ErrCollectionMissing):
			return SearchOutcome{}, fmt.Errorf("index data for '%s' has been lost (collection not found in Milvus). Please re-index using index_codebase with force=true", codebase.CanonicalPath)
		case errors.Is(semanticErr, semantic.ErrUnavailable):
		default:
			return SearchOutcome{}, fmt.Errorf("semantic search for %s: %w", codebase.CanonicalPath, semanticErr)
		}
	}

	chunks, err := store.ReadChunks(manager.chunkPath(codebase.ID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && codebase.Status == model.CodebaseStatusIndexing {
			return SearchOutcome{Codebase: codebase, ActiveJob: activeJob, Results: []model.StoredChunk{}}, nil
		}
		slog.ErrorContext(ctx, "read chunk cache failed", "codebase_id", codebase.ID, "err", err)
		return SearchOutcome{}, fmt.Errorf("read chunk cache for %s: %w", codebase.ID, err)
	}
	return SearchOutcome{Codebase: codebase, ActiveJob: activeJob, Results: rankChunks(chunks, query, limit, normalizedExtensions)}, nil
}

func rankChunks(chunks []model.StoredChunk, query string, limit int32, extensionFilter []string) []model.StoredChunk {
	filteredChunks := make([]model.StoredChunk, 0, len(chunks))
	filterSet := map[string]struct{}{}
	for _, extension := range extensionFilter {
		filterSet[extension] = struct{}{}
	}

	queryLower := strings.ToLower(query)
	queryTerms := strings.Fields(queryLower)
	type scoredChunk struct {
		chunk model.StoredChunk
		score int
	}
	scored := make([]scoredChunk, 0, len(chunks))
	for _, chunk := range chunks {
		if len(filterSet) > 0 {
			if _, found := filterSet[chunk.FileExtension]; !found {
				continue
			}
		}

		contentLower := strings.ToLower(chunk.Content)
		score := 0
		if strings.Contains(contentLower, queryLower) {
			score += 100
		}
		for _, term := range queryTerms {
			if strings.Contains(contentLower, term) {
				score++
			}
		}
		if score == 0 {
			continue
		}
		scored = append(scored, scoredChunk{chunk: chunk, score: score})
	}

	sort.SliceStable(scored, func(i int, j int) bool {
		if scored[i].score == scored[j].score {
			if scored[i].chunk.RelativePath == scored[j].chunk.RelativePath {
				return scored[i].chunk.StartLine < scored[j].chunk.StartLine
			}
			return scored[i].chunk.RelativePath < scored[j].chunk.RelativePath
		}
		return scored[i].score > scored[j].score
	})

	maxResults := int(limit)
	if maxResults <= 0 {
		maxResults = 10
	}
	if maxResults > len(scored) {
		maxResults = len(scored)
	}
	for _, item := range scored[:maxResults] {
		filteredChunks = append(filteredChunks, item.chunk)
	}
	return filteredChunks
}
