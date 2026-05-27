package adapterr

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"goodkind.io/gklog/correlation"
	"google.golang.org/grpc/codes"
)

func TestAdapterErrorIsMatchesSameClass(t *testing.T) {
	cause := io.EOF
	err := NewEmbedderUnreachable(cause)
	sentinel := &AdapterError{Class: ClassEmbedderUnreachable}
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is should match by class")
	}
	other := &AdapterError{Class: ClassMilvusUnavailable}
	if errors.Is(err, other) {
		t.Fatalf("errors.Is should not match different class")
	}
}

func TestAdapterErrorUnwrapExposesCause(t *testing.T) {
	cause := io.EOF
	err := NewEmbedderUnreachable(cause)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("errors.Is should reach EOF through unwrap")
	}
}

func TestCodeForCoversClasses(t *testing.T) {
	cases := map[Class]codes.Code{
		ClassNotIndexed:             codes.NotFound,
		ClassJobNotFound:            codes.NotFound,
		ClassCollectionMissing:      codes.FailedPrecondition,
		ClassCollectionNotReady:     codes.FailedPrecondition,
		ClassConflictingJob:         codes.FailedPrecondition,
		ClassMilvusUnavailable:      codes.Unavailable,
		ClassEmbedderUnreachable:    codes.Unavailable,
		ClassInvalidPath:            codes.InvalidArgument,
		ClassSearchResultIncomplete: codes.Internal,
		ClassInternal:               codes.Internal,
	}
	for class, want := range cases {
		if got := CodeFor(class); got != want {
			t.Fatalf("CodeFor(%q) = %v, want %v", class, got, want)
		}
	}
}

func TestRespondKnownReturnsHint(t *testing.T) {
	code, msg := Respond(context.Background(), NewEmbedderUnreachable(io.EOF))
	if code != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable", code)
	}
	if !strings.Contains(msg, "embedding endpoint") {
		t.Fatalf("message should mention embedding endpoint, got %q", msg)
	}
	if !strings.Contains(msg, "OPENAI_BASE_URL") {
		t.Fatalf("message should include hint, got %q", msg)
	}
}

func TestRespondUnknownSanitizesAndReferencesTrace(t *testing.T) {
	corr := correlation.New("req-1").WithIdentityAttributes(
		correlation.IdentityAttribute{Key: "job_id", Value: "job-xyz"},
	)
	ctx := correlation.WithContext(context.Background(), corr)

	code, msg := Respond(ctx, errors.New("kernel panic: stack overflow"))
	if code != codes.Internal {
		t.Fatalf("code = %v, want Internal", code)
	}
	if strings.Contains(msg, "stack overflow") {
		t.Fatalf("unknown message should be sanitized, got %q", msg)
	}
	if !strings.Contains(msg, "trace_id="+string(corr.TraceID)) {
		t.Fatalf("message should reference trace_id, got %q", msg)
	}
	if !strings.Contains(msg, "job_id=job-xyz") {
		t.Fatalf("message should reference job_id, got %q", msg)
	}
}

func TestRespondUnknownWithoutCorrelation(t *testing.T) {
	_, msg := Respond(context.Background(), errors.New("boom"))
	if msg != "internal error" {
		t.Fatalf("message = %q, want \"internal error\"", msg)
	}
}

func TestRespondMCPKnown(t *testing.T) {
	corr := correlation.New("req-1")
	ctx := correlation.WithContext(context.Background(), corr)

	err := RespondMCP(ctx, NewNotIndexed("/tmp/foo", nil))
	mcp, ok := err.(*MCPError)
	if !ok {
		t.Fatalf("expected *MCPError, got %T", err)
	}
	if mcp.Class != ClassNotIndexed {
		t.Fatalf("class = %q, want not_indexed", mcp.Class)
	}
	if mcp.Code != "not_indexed" {
		t.Fatalf("code = %q", mcp.Code)
	}
	if !strings.Contains(mcp.Message, "not indexed") {
		t.Fatalf("message should describe the error, got %q", mcp.Message)
	}
	if mcp.TraceID != string(corr.TraceID) {
		t.Fatalf("trace id mismatch")
	}
}

func TestRespondMCPUnknown(t *testing.T) {
	corr := correlation.New("req-1").WithIdentityAttributes(
		correlation.IdentityAttribute{Key: "job_id", Value: "job-1"},
	)
	ctx := correlation.WithContext(context.Background(), corr)

	err := RespondMCP(ctx, errors.New("opaque crash"))
	mcp, ok := err.(*MCPError)
	if !ok {
		t.Fatalf("expected *MCPError, got %T", err)
	}
	if mcp.Class != ClassInternal {
		t.Fatalf("class = %q, want internal", mcp.Class)
	}
	if strings.Contains(mcp.Message, "opaque") {
		t.Fatalf("unknown message should be sanitized, got %q", mcp.Message)
	}
	if mcp.TraceID == "" {
		t.Fatalf("trace_id should be set")
	}
	if mcp.JobID != "job-1" {
		t.Fatalf("job_id = %q, want job-1", mcp.JobID)
	}
}

func TestRespondPassesNil(t *testing.T) {
	if code, _ := Respond(context.Background(), nil); code != codes.OK {
		t.Fatalf("Respond(nil) code = %v, want OK", code)
	}
	if err := RespondMCP(context.Background(), nil); err != nil {
		t.Fatalf("RespondMCP(nil) = %v", err)
	}
}

func TestClassifyPreservesExistingAdapterError(t *testing.T) {
	known := NewEmbedderUnreachable(io.EOF)
	classified := classify(known)
	if classified != known {
		t.Fatalf("classify should return the same instance for AdapterError input")
	}
}
