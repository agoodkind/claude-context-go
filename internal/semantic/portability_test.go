package semantic

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"goodkind.io/claude-context-go/internal/config"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/tshash"
)

// TestCollectionNameMatchesSharedInvariant locks the collection-name format to
// the shared contract (hybrid_code_chunks_<md5(path)[:8]>) so a Go-created
// collection stays readable by the upstream TS tool.
func TestCollectionNameMatchesSharedInvariant(t *testing.T) {
	t.Parallel()

	service := &Service{cfg: config.Config{HybridMode: true}}
	path := "/Users/example/repo"
	got := service.CollectionName(path)
	want := "hybrid_code_chunks_" + tshash.PathPrefix(path)
	if got != want {
		t.Fatalf("CollectionName = %q, want %q", got, want)
	}
}

// TestGenerateIDMatchesSharedInvariant locks the chunk-id format to the shared
// contract (chunk_<sha256(path:start:end:content)[:16]>), the same id the TS
// tool computes, so re-indexing from either tool is an idempotent upsert.
func TestGenerateIDMatchesSharedInvariant(t *testing.T) {
	t.Parallel()

	chunk := model.StoredChunk{
		Content:      "package main",
		RelativePath: "a.go",
		StartLine:    1,
		EndLine:      2,
	}
	sum := sha256.Sum256([]byte("a.go:1:2:package main"))
	want := "chunk_" + hex.EncodeToString(sum[:])[:16]
	if got := generateID(chunk, 0); got != want {
		t.Fatalf("generateID = %q, want %q", generateID(chunk, 0), want)
	}
}
