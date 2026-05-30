package migrate

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"goodkind.io/claude-context-go/internal/tshash"
)

func TestLoadTSMerkleConvertsFileHashes(t *testing.T) {
	t.Parallel()

	contextRoot := t.TempDir()
	codebasePath := "/Users/example/repo"
	merkleDir := filepath.Join(contextRoot, "merkle")
	if err := os.MkdirAll(merkleDir, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	fixture := `{"fileHashes":[["a.go","hash-a"],["dir/b.go","hash-b"]],"merkleDAG":{"ignored":true}}`
	target := filepath.Join(merkleDir, tshash.FullHex(codebasePath)+".json")
	if err := os.WriteFile(target, []byte(fixture), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	snapshot, found, err := LoadTSMerkle(context.Background(), contextRoot, codebasePath)
	if err != nil {
		t.Fatalf("LoadTSMerkle returned error: %v", err)
	}
	if !found {
		t.Fatal("expected found=true for a present TS merkle")
	}
	if len(snapshot.Files) != 2 {
		t.Fatalf("Files = %d entries, want 2", len(snapshot.Files))
	}
	if snapshot.Files["a.go"] != "hash-a" || snapshot.Files["dir/b.go"] != "hash-b" {
		t.Fatalf("Files = %v, want a.go=hash-a dir/b.go=hash-b", snapshot.Files)
	}
	if snapshot.ConfigDigest != "" {
		t.Fatalf("ConfigDigest = %q, want empty (caller stamps it)", snapshot.ConfigDigest)
	}
}

func TestLoadTSMerkleAbsentReturnsNotFound(t *testing.T) {
	t.Parallel()

	contextRoot := t.TempDir()
	snapshot, found, err := LoadTSMerkle(context.Background(), contextRoot, "/Users/example/never-indexed")
	if err != nil {
		t.Fatalf("LoadTSMerkle returned error: %v", err)
	}
	if found {
		t.Fatal("expected found=false when no TS merkle exists")
	}
	if len(snapshot.Files) != 0 {
		t.Fatalf("Files = %d entries, want 0", len(snapshot.Files))
	}
}
