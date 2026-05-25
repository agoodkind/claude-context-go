package merkle

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoadSnapshotForConfigMissingFile(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	snapshot := LoadSnapshotForConfig(filepath.Join(tempDir, "missing.json"), "sha256:request")
	if snapshot.ConfigDigest != "sha256:request" {
		t.Fatalf("ConfigDigest = %q, want sha256:request", snapshot.ConfigDigest)
	}
	if len(snapshot.Files) != 0 {
		t.Fatalf("Files = %#v, want empty", snapshot.Files)
	}
}

func TestLoadSnapshotForConfigMatchingDigest(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "snapshot.json")
	stored := Snapshot{ConfigDigest: "sha256:digest-a", Files: map[string]string{"a.go": "hash-a"}}
	if err := WriteSnapshot(path, stored); err != nil {
		t.Fatalf("WriteSnapshot returned error: %v", err)
	}
	loaded := LoadSnapshotForConfig(path, "sha256:digest-a")
	if loaded.ConfigDigest != "sha256:digest-a" {
		t.Fatalf("ConfigDigest = %q", loaded.ConfigDigest)
	}
	if !reflect.DeepEqual(loaded.Files, stored.Files) {
		t.Fatalf("Files = %#v, want %#v", loaded.Files, stored.Files)
	}
}

func TestLoadSnapshotForConfigMismatchedDigest(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "snapshot.json")
	stored := Snapshot{ConfigDigest: "sha256:digest-a", Files: map[string]string{"a.go": "hash-a"}}
	if err := WriteSnapshot(path, stored); err != nil {
		t.Fatalf("WriteSnapshot returned error: %v", err)
	}
	loaded := LoadSnapshotForConfig(path, "sha256:digest-b")
	if loaded.ConfigDigest != "sha256:digest-b" {
		t.Fatalf("ConfigDigest = %q, want fresh sha256:digest-b", loaded.ConfigDigest)
	}
	if len(loaded.Files) != 0 {
		t.Fatalf("Files = %#v, want empty due to digest mismatch", loaded.Files)
	}
}

func TestDiffSnapshotsEmpty(t *testing.T) {
	t.Parallel()

	prev := Snapshot{Files: map[string]string{"a.go": "hash-a"}}
	current := Snapshot{Files: map[string]string{"a.go": "hash-a"}}
	diff := DiffSnapshots(prev, current)
	if !diff.Empty() {
		t.Fatalf("diff = %#v, want empty", diff)
	}
}

func TestDiffSnapshotsAddOnly(t *testing.T) {
	t.Parallel()

	prev := Snapshot{Files: map[string]string{"a.go": "hash-a"}}
	current := Snapshot{Files: map[string]string{
		"a.go":  "hash-a",
		"b.go":  "hash-b",
		"sub/c": "hash-c",
	}}
	diff := DiffSnapshots(prev, current)
	if !reflect.DeepEqual(diff.Added, []string{"b.go", "sub/c"}) {
		t.Fatalf("added = %#v", diff.Added)
	}
	if len(diff.Modified) != 0 || len(diff.Removed) != 0 {
		t.Fatalf("diff = %#v", diff)
	}
}

func TestDiffSnapshotsModifyOnly(t *testing.T) {
	t.Parallel()

	prev := Snapshot{Files: map[string]string{"a.go": "hash-old", "b.go": "hash-b"}}
	current := Snapshot{Files: map[string]string{"a.go": "hash-new", "b.go": "hash-b"}}
	diff := DiffSnapshots(prev, current)
	if !reflect.DeepEqual(diff.Modified, []string{"a.go"}) {
		t.Fatalf("modified = %#v", diff.Modified)
	}
	if len(diff.Added) != 0 || len(diff.Removed) != 0 {
		t.Fatalf("diff = %#v", diff)
	}
}

func TestDiffSnapshotsRemoveOnly(t *testing.T) {
	t.Parallel()

	prev := Snapshot{Files: map[string]string{"a.go": "hash-a", "old.go": "hash-old"}}
	current := Snapshot{Files: map[string]string{"a.go": "hash-a"}}
	diff := DiffSnapshots(prev, current)
	if !reflect.DeepEqual(diff.Removed, []string{"old.go"}) {
		t.Fatalf("removed = %#v", diff.Removed)
	}
	if len(diff.Added) != 0 || len(diff.Modified) != 0 {
		t.Fatalf("diff = %#v", diff)
	}
}

func TestDiffSnapshotsMixed(t *testing.T) {
	t.Parallel()

	prev := Snapshot{Files: map[string]string{
		"keep.go":   "same",
		"change.go": "old",
		"gone.go":   "old",
	}}
	current := Snapshot{Files: map[string]string{
		"keep.go":   "same",
		"change.go": "new",
		"added.go":  "fresh",
	}}
	diff := DiffSnapshots(prev, current)
	if !reflect.DeepEqual(diff.Added, []string{"added.go"}) {
		t.Fatalf("added = %#v", diff.Added)
	}
	if !reflect.DeepEqual(diff.Modified, []string{"change.go"}) {
		t.Fatalf("modified = %#v", diff.Modified)
	}
	if !reflect.DeepEqual(diff.Removed, []string{"gone.go"}) {
		t.Fatalf("removed = %#v", diff.Removed)
	}
}

func TestDiffSnapshotsBothEmpty(t *testing.T) {
	t.Parallel()

	diff := DiffSnapshots(Snapshot{Files: map[string]string{}}, Snapshot{Files: map[string]string{}})
	if !diff.Empty() {
		t.Fatalf("diff = %#v", diff)
	}
}
