package indexer

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"goodkind.io/claude-context-go/internal/model"
)

func TestIndexUsesRequestedLangchainSplitter(t *testing.T) {
	t.Parallel()

	tempDirectory := t.TempDir()
	sourcePath := filepath.Join(tempDirectory, "main.go")
	sourceContent := []byte("package main\n\nfunc example() {}\n")
	if err := os.WriteFile(sourcePath, sourceContent, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	runner := NewRunner()
	result, err := runner.Index(context.Background(), tempDirectory, model.IndexConfig{
		SplitterType:      "langchain",
		SplitterChunkSize: 1000,
		SplitterOverlap:   200,
	}, nil)
	if err != nil {
		t.Fatalf("Index returned error: %v", err)
	}
	if result.IndexedFiles != 1 {
		t.Fatalf("Index returned indexedFiles=%d", result.IndexedFiles)
	}
	if len(result.Chunks) == 0 {
		t.Fatal("Index returned no chunks")
	}
}

// TestIndexSkipsInvalidUTF8File proves the indexer refuses to embed files
// whose bytes are not valid UTF-8. Milvus rejects such payloads at the gRPC
// marshal boundary so embedding them would roll back an entire batch.
func TestIndexSkipsInvalidUTF8File(t *testing.T) {
	t.Parallel()

	tempDirectory := t.TempDir()
	validPath := filepath.Join(tempDirectory, "valid.go")
	if err := os.WriteFile(validPath, []byte("package main\nfunc example() {}\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	invalidPath := filepath.Join(tempDirectory, "invalid.go")
	if err := os.WriteFile(invalidPath, []byte{'p', 'a', 'c', 'k', 'a', 'g', 'e', ' ', 0xff, 0xfe, '\n'}, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	runner := NewRunner()
	result, err := runner.Index(context.Background(), tempDirectory, model.IndexConfig{
		SplitterType:      "langchain",
		SplitterChunkSize: 1000,
		SplitterOverlap:   200,
	}, nil)
	if err != nil {
		t.Fatalf("Index returned error: %v", err)
	}
	if result.IndexedFiles != 1 {
		t.Fatalf("IndexedFiles = %d, want 1", result.IndexedFiles)
	}
	if result.TotalChunks == 0 {
		t.Fatal("TotalChunks = 0, want > 0 from the valid file")
	}
	if !slices.Contains(result.SkippedFiles, "invalid.go") {
		t.Fatalf("SkippedFiles = %v, want to contain invalid.go", result.SkippedFiles)
	}
	if _, found := result.FileHashes["invalid.go"]; found {
		t.Fatal("FileHashes contains invalid.go; merkle snapshot would re-flag it forever")
	}
}

// TestIndexFilesSkipsInvalidUTF8File mirrors the skip behavior on the delta
// path. A delta sync must not crash on a previously valid file that was
// edited to contain invalid bytes.
func TestIndexFilesSkipsInvalidUTF8File(t *testing.T) {
	t.Parallel()

	tempDirectory := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDirectory, "valid.go"), []byte("package main\nfunc example() {}\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tempDirectory, "invalid.go"), []byte{'p', 'a', 'c', 'k', 'a', 'g', 'e', ' ', 0xff, 0xfe, '\n'}, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	runner := NewRunner()
	result, err := runner.IndexFiles(context.Background(), tempDirectory, []string{"valid.go", "invalid.go"}, model.IndexConfig{
		SplitterType:      "langchain",
		SplitterChunkSize: 1000,
		SplitterOverlap:   200,
	}, nil)
	if err != nil {
		t.Fatalf("IndexFiles returned error: %v", err)
	}
	if result.IndexedFiles != 1 {
		t.Fatalf("IndexedFiles = %d, want 1", result.IndexedFiles)
	}
	if result.TotalChunks == 0 {
		t.Fatal("TotalChunks = 0, want > 0 from the valid file")
	}
	if !slices.Contains(result.SkippedFiles, "invalid.go") {
		t.Fatalf("SkippedFiles = %v, want to contain invalid.go", result.SkippedFiles)
	}
}
