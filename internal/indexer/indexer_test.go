package indexer

import (
	"context"
	"os"
	"path/filepath"
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
