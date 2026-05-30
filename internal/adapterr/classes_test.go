package adapterr

import (
	"testing"

	"google.golang.org/grpc/codes"
)

func TestNewMissingArgumentMapsToInvalidArgument(t *testing.T) {
	t.Parallel()

	err := NewMissingArgument("query")
	if err.Class != ClassInvalidArgument {
		t.Fatalf("class = %q, want %q", err.Class, ClassInvalidArgument)
	}
	if !err.SafeForClient {
		t.Fatal("a missing-argument error should be safe for the client")
	}
	if CodeFor(err.Class) != codes.InvalidArgument {
		t.Fatalf("CodeFor(%q) = %v, want %v", err.Class, CodeFor(err.Class), codes.InvalidArgument)
	}
}

func TestInvalidPathMapsToInvalidArgument(t *testing.T) {
	t.Parallel()

	if got := CodeFor(ClassInvalidPath); got != codes.InvalidArgument {
		t.Fatalf("CodeFor(ClassInvalidPath) = %v, want %v", got, codes.InvalidArgument)
	}
}
