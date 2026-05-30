package daemon

import (
	"context"

	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/semantic"
)

// semanticReader is the read-only slice of the embedding-and-vector-store
// surface the manager queries.
type semanticReader interface {
	Available() bool
	CollectionName(codebasePath string) string
	Search(ctx context.Context, codebasePath string, query string, limit int32, extensionFilter []string) ([]model.StoredChunk, error)
	Count(ctx context.Context, codebasePath string) (int32, error)
	ListCollections(ctx context.Context) ([]string, error)
	HasCollectionForPath(ctx context.Context, codebasePath string) (bool, error)
	HasStaging(ctx context.Context, codebasePath string) (bool, error)
}

// semanticWriter is the slice that mutates the live or staging collection.
type semanticWriter interface {
	Reindex(ctx context.Context, codebasePath string, addedOrModifiedChunks []model.StoredChunk, removedOrModifiedRelativePaths []string, progress func(semantic.Progress)) error
	StageReindex(ctx context.Context, codebasePath string, chunks []model.StoredChunk, removedOrModifiedRelativePaths []string, progress func(semantic.Progress)) error
	PromoteStaging(ctx context.Context, codebasePath string) error
	CopyChunks(ctx context.Context, codebasePath string, srcRelativePath string, dstRelativePath string) (int, error)
	PruneToCurrent(ctx context.Context, codebasePath string, currentRelativePaths []string) error
}

// semanticDropper is the slice that removes collections.
type semanticDropper interface {
	Drop(ctx context.Context, codebasePath string) error
	DropStaging(ctx context.Context, codebasePath string) error
}

// semanticIndex is the full embedding-and-vector-store surface the manager
// depends on. It exists so tests can substitute a fake for the Milvus-backed
// [semantic.Service]; the concrete service satisfies it. The method set is
// exactly what the daemon calls, no more.
type semanticIndex interface {
	semanticReader
	semanticWriter
	semanticDropper
}
