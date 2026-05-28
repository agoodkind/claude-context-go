package semantic

import (
	"errors"
	"strings"
	"testing"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"goodkind.io/claude-context-go/internal/model"
)

// TestStagingCollectionNameStaysWithinCap proves the rebuild staging name
// keeps the suffix and never exceeds the Milvus name-length cap.
func TestStagingCollectionNameStaysWithinCap(t *testing.T) {
	t.Parallel()

	short := stagingCollectionName("code_chunks_abc123")
	if short != "code_chunks_abc123"+stagingCollectionSuffix {
		t.Fatalf("staging name = %q, want suffix appended", short)
	}

	long := stagingCollectionName(strings.Repeat("x", maxCollectionNameLength+10))
	if len(long) != maxCollectionNameLength {
		t.Fatalf("staging name length = %d, want %d", len(long), maxCollectionNameLength)
	}
	if !strings.HasSuffix(long, stagingCollectionSuffix) {
		t.Fatalf("staging name %q lost its suffix after truncation", long)
	}
}

func TestValidateExtensionFilter(t *testing.T) {
	t.Parallel()

	validExtensions, err := ValidateExtensionFilter([]string{" .go ", ".ts"})
	if err != nil {
		t.Fatalf("ValidateExtensionFilter returned error for valid input: %v", err)
	}
	if len(validExtensions) != 2 || validExtensions[0] != ".go" || validExtensions[1] != ".ts" {
		t.Fatalf("ValidateExtensionFilter returned %+v", validExtensions)
	}

	_, err = ValidateExtensionFilter([]string{".go", "bad extension"})
	if err == nil {
		t.Fatal("ValidateExtensionFilter returned nil error for invalid input")
	}
	if !strings.Contains(err.Error(), "invalid file extensions") {
		t.Fatalf("ValidateExtensionFilter returned unexpected error: %v", err)
	}
}

func TestValidateExtensionFilterAcceptsBareExtensions(t *testing.T) {
	t.Parallel()

	normalized, err := ValidateExtensionFilter([]string{"go", " ts ", "mk", "sh"})
	if err != nil {
		t.Fatalf("ValidateExtensionFilter returned error: %v", err)
	}
	want := []string{".go", ".ts", ".mk", ".sh"}
	if len(normalized) != len(want) {
		t.Fatalf("ValidateExtensionFilter returned %d entries, want %d: %+v", len(normalized), len(want), normalized)
	}
	for index, value := range normalized {
		if value != want[index] {
			t.Fatalf("ValidateExtensionFilter[%d] = %q, want %q", index, value, want[index])
		}
	}
}

func TestDeduplicateChunks(t *testing.T) {
	t.Parallel()

	inputChunks := []model.StoredChunk{
		{RelativePath: "a.go", StartLine: 10, EndLine: 20, Content: "first"},
		{RelativePath: "a.go", StartLine: 12, EndLine: 19, Content: "overlap"},
		{RelativePath: "a.go", StartLine: 30, EndLine: 35, Content: "separate"},
		{RelativePath: "b.go", StartLine: 12, EndLine: 19, Content: "other-file"},
	}

	dedupedChunks := DeduplicateChunks(inputChunks)
	if len(dedupedChunks) != 3 {
		t.Fatalf("DeduplicateChunks returned %d chunks", len(dedupedChunks))
	}
	if dedupedChunks[0].Content != "first" || dedupedChunks[1].Content != "separate" || dedupedChunks[2].Content != "other-file" {
		t.Fatalf("DeduplicateChunks returned unexpected order/content: %+v", dedupedChunks)
	}
}

func TestResultSetsToChunksReturnsIncompleteResultError(t *testing.T) {
	t.Parallel()

	_, err := resultSetsToChunks([]milvusclient.ResultSet{{ResultCount: 1}})
	if !errors.Is(err, ErrSearchResultIncomplete) {
		t.Fatalf("resultSetsToChunks returned err=%v", err)
	}
}
