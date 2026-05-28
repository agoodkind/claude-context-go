package discovery

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir parent for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestEffectiveIgnorePatternsReadsNestedGitignore proves the discovery
// walker picks up patterns declared in a nested .gitignore and scopes them
// to the directory that owns the file. Patterns at the root and patterns
// in a subdirectory are both reported through PathIgnored verdicts.
func TestEffectiveIgnorePatternsReadsNestedGitignore(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	writeFile(t, filepath.Join(tempDir, ".gitignore"), "root-only.txt\n")
	writeFile(t, filepath.Join(tempDir, "pkg", ".gitignore"), "pkg-only.txt\n")
	writeFile(t, filepath.Join(tempDir, "pkg", "pkg-only.txt"), "x")
	writeFile(t, filepath.Join(tempDir, "pkg", "kept.go"), "x")

	rules, err := EffectiveIgnorePatterns(context.Background(), tempDir, nil)
	if err != nil {
		t.Fatalf("EffectiveIgnorePatterns returned error: %v", err)
	}
	if rules.IsEmpty() {
		t.Fatal("rules unexpectedly empty after reading two .gitignore files")
	}
	if excluded, _, _ := PathIgnored("root-only.txt", rules); !excluded {
		t.Fatal("root pattern did not exclude root-only.txt")
	}
	if excluded, _, _ := PathIgnored("pkg/pkg-only.txt", rules); !excluded {
		t.Fatal("nested pattern did not exclude pkg/pkg-only.txt")
	}
	if excluded, _, _ := PathIgnored("pkg/kept.go", rules); excluded {
		t.Fatal("nested pattern unexpectedly excluded pkg/kept.go")
	}
}

// TestPathIgnoredLastMatchWins exercises the rule that the final matching
// pattern in declaration order determines the verdict; a negation pattern
// declared after an exclusion pattern flips the result back to included.
func TestPathIgnoredLastMatchWins(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	writeFile(t, filepath.Join(tempDir, ".gitignore"), "*.txt\n!keep.txt\n")
	rules, err := EffectiveIgnorePatterns(context.Background(), tempDir, nil)
	if err != nil {
		t.Fatalf("EffectiveIgnorePatterns returned error: %v", err)
	}
	if excluded, _, _ := PathIgnored("trash.txt", rules); !excluded {
		t.Fatal("expected trash.txt to be excluded by *.txt")
	}
	if excluded, _, _ := PathIgnored("keep.txt", rules); excluded {
		t.Fatal("expected keep.txt to be re-included by !keep.txt")
	}
}

// TestPathIgnoredDirectoryExclusionBlocksNegation proves Git's
// directory-exclusion rule: once an ancestor directory is excluded by an
// unnegated pattern, a negation pattern on a descendant cannot re-include
// the file.
func TestPathIgnoredDirectoryExclusionBlocksNegation(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	writeFile(t, filepath.Join(tempDir, ".gitignore"), "dropme/\n!dropme/secret.txt\n")
	rules, err := EffectiveIgnorePatterns(context.Background(), tempDir, nil)
	if err != nil {
		t.Fatalf("EffectiveIgnorePatterns returned error: %v", err)
	}
	excluded, matched, _ := PathIgnored("dropme/secret.txt", rules)
	if !excluded {
		t.Fatalf("expected dropme/secret.txt to stay excluded under directory rule (matched=%q)", matched)
	}
}

// TestPathIgnoredReportsMatchedPatternAndSource verifies the second and
// third return of PathIgnored: the matched pattern (raw text) and the
// source label.
func TestPathIgnoredReportsMatchedPatternAndSource(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	writeFile(t, filepath.Join(tempDir, ".gitignore"), "logs/\n")
	rules, err := EffectiveIgnorePatterns(context.Background(), tempDir, nil)
	if err != nil {
		t.Fatalf("EffectiveIgnorePatterns returned error: %v", err)
	}
	excluded, matched, source := PathIgnored("logs/app.log", rules)
	if !excluded {
		t.Fatal("expected logs/app.log to be excluded")
	}
	if matched == "" {
		t.Fatal("matched pattern was empty")
	}
	if source == "" {
		t.Fatal("source label was empty")
	}
}

// TestPathIgnoredOutsideRulesReturnsFalse proves a path with no ignore
// rules tree returns (false, "", "") and never panics.
func TestPathIgnoredOutsideRulesReturnsFalse(t *testing.T) {
	t.Parallel()
	excluded, matched, source := PathIgnored("any/file", IgnoreRules{Nodes: nil})
	if excluded {
		t.Fatal("empty rules unexpectedly excluded a path")
	}
	if matched != "" || source != "" {
		t.Fatalf("empty rules returned non-empty match: pattern=%q source=%q", matched, source)
	}
}

// TestEffectiveIgnorePatternsAdditionalOverrides confirms user overrides
// are recorded in the root node alongside the built-in defaults so an
// override like "research/" is picked up without a .gitignore on disk.
func TestEffectiveIgnorePatternsAdditionalOverrides(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	rules, err := EffectiveIgnorePatterns(context.Background(), tempDir, []string{"research/"})
	if err != nil {
		t.Fatalf("EffectiveIgnorePatterns returned error: %v", err)
	}
	if excluded, _, _ := PathIgnored("research/file.txt", rules); !excluded {
		t.Fatal("override pattern did not exclude research/file.txt")
	}
}
