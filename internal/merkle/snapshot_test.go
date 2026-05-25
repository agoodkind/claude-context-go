package merkle

import (
	"reflect"
	"testing"
)

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
