package daemon

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCanonicalizePathRejectsEmpty(t *testing.T) {
	t.Parallel()

	for _, requested := range []string{"", "   ", "\t"} {
		if _, err := canonicalizePath(requested); err == nil {
			t.Fatalf("canonicalizePath(%q) returned nil error; an empty path must not resolve to the cwd", requested)
		}
	}
}

func TestCanonicalizePathAcceptsNonEmpty(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if _, err := canonicalizePath(dir); err != nil {
		t.Fatalf("canonicalizePath(%q) returned error: %v", dir, err)
	}
}

func TestRequireNonEmptyReturnsInvalidArgument(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		value    string
		argument string
		pathLike bool
	}{
		{"empty path", "", "absolutePath", true},
		{"blank path", "   ", "absolutePath", true},
		{"empty query", "", "query", false},
		{"empty job id", "", "job_id", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			err := requireNonEmpty(context.Background(), testCase.value, testCase.argument, testCase.pathLike)
			if err == nil {
				t.Fatal("expected an error for an empty required field")
			}
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("status code = %v, want %v", got, codes.InvalidArgument)
			}
		})
	}
}

func TestRequireNonEmptyAllowsValue(t *testing.T) {
	t.Parallel()

	if err := requireNonEmpty(context.Background(), "/Users/x/repo", "absolutePath", true); err != nil {
		t.Fatalf("requireNonEmpty rejected a valid value: %v", err)
	}
}
