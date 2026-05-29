package mcpserver

import (
	"slices"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	pb "goodkind.io/claude-context-go/gen/go/claudecontext/v1"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/response"
)

func TestRenderToolResponseUsesSingleTextContent(t *testing.T) {
	t.Parallel()

	result, err := renderToolResponse(response.ModeHuman, &pb.GetIndexResponse{DisplayText: "hello\nworld"})
	if err != nil {
		t.Fatalf("renderToolResponse returned error: %v", err)
	}
	if result.StructuredContent != nil {
		t.Fatalf("renderToolResponse returned unexpected structured content: %#v", result.StructuredContent)
	}
	if len(result.Content) != 1 {
		t.Fatalf("renderToolResponse returned %d content items", len(result.Content))
	}

	text, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("renderToolResponse returned unexpected content type %T", result.Content[0])
	}
	if text.Text != "hello\nworld" {
		t.Fatalf("renderToolResponse returned %q", text.Text)
	}
}

func TestRenderToolResponseSupportsJSONAndSingleLineModes(t *testing.T) {
	t.Parallel()

	message := &pb.GetIndexResponse{DisplayText: "line one\nline two", Tracked: true}

	jsonResult, err := renderToolResponse(response.ModeJSON, message)
	if err != nil {
		t.Fatalf("renderToolResponse JSON returned error: %v", err)
	}
	if len(jsonResult.Content) != 1 {
		t.Fatalf("renderToolResponse JSON returned %d content items", len(jsonResult.Content))
	}
	jsonText := jsonResult.Content[0].(mcp.TextContent).Text
	if !strings.HasPrefix(jsonText, "{") {
		t.Fatalf("renderToolResponse JSON returned %q", jsonText)
	}

	singleLineResult, err := renderToolResponse(response.ModeSingleLine, message)
	if err != nil {
		t.Fatalf("renderToolResponse single-line returned error: %v", err)
	}
	if singleLineResult.Content[0].(mcp.TextContent).Text != "line one" {
		t.Fatalf("renderToolResponse single-line returned %q", singleLineResult.Content[0].(mcp.TextContent).Text)
	}
}

func TestComputeDroppedCodebasesReportsCompletedUntrackedOnDisk(t *testing.T) {
	t.Parallel()

	jobs := []*pb.Job{
		{CanonicalPath: "/repo/dropped", State: string(model.JobStateCompleted)},
		{CanonicalPath: "/repo/tracked", State: string(model.JobStateCompleted)},
		{CanonicalPath: "/repo/gone", State: string(model.JobStateCompleted)},
		{CanonicalPath: "/repo/failed", State: string(model.JobStateFailed)},
		{CanonicalPath: "/repo/running", State: string(model.JobStateRunning)},
	}
	indexes := []*pb.Codebase{
		{CanonicalPath: "/repo/tracked"},
	}
	onDisk := map[string]bool{
		"/repo/dropped": true,
		"/repo/tracked": true,
		"/repo/gone":    false,
		"/repo/failed":  true,
		"/repo/running": true,
	}
	exists := func(path string) bool {
		return onDisk[path]
	}

	dropped := computeDroppedCodebases(jobs, indexes, exists)

	want := []string{"/repo/dropped"}
	if !slices.Equal(dropped, want) {
		t.Fatalf("computeDroppedCodebases = %v, want %v", dropped, want)
	}
}

func TestComputeDroppedCodebasesIgnoresNeverIndexed(t *testing.T) {
	t.Parallel()

	jobs := []*pb.Job{}
	indexes := []*pb.Codebase{}
	exists := func(string) bool {
		return true
	}

	dropped := computeDroppedCodebases(jobs, indexes, exists)
	if len(dropped) != 0 {
		t.Fatalf("computeDroppedCodebases = %v, want empty", dropped)
	}
}

func TestComputeDroppedCodebasesSortsAndDeduplicates(t *testing.T) {
	t.Parallel()

	jobs := []*pb.Job{
		{CanonicalPath: "/repo/b", State: string(model.JobStateCompleted)},
		{CanonicalPath: "/repo/a", State: string(model.JobStateCompleted)},
		{CanonicalPath: "/repo/a", State: string(model.JobStateCompleted)},
	}
	exists := func(string) bool {
		return true
	}

	dropped := computeDroppedCodebases(jobs, nil, exists)

	want := []string{"/repo/a", "/repo/b"}
	if !slices.Equal(dropped, want) {
		t.Fatalf("computeDroppedCodebases = %v, want %v", dropped, want)
	}
}

func TestRenderDroppedSectionStatesNoneWhenEmpty(t *testing.T) {
	t.Parallel()

	section := renderDroppedSection(nil)
	if !strings.Contains(section, "none") {
		t.Fatalf("renderDroppedSection empty = %q, want a none statement", section)
	}
}

func TestRenderDroppedSectionListsPaths(t *testing.T) {
	t.Parallel()

	section := renderDroppedSection([]string{"/repo/one", "/repo/two"})
	if !strings.Contains(section, "/repo/one") || !strings.Contains(section, "/repo/two") {
		t.Fatalf("renderDroppedSection = %q, want both paths", section)
	}
	if !strings.Contains(section, "2") {
		t.Fatalf("renderDroppedSection = %q, want a count of 2", section)
	}
}

func TestToolResultTextExtractsSingleTextContent(t *testing.T) {
	t.Parallel()

	if got := toolResultText(mcp.NewToolResultText("payload")); got != "payload" {
		t.Fatalf("toolResultText = %q, want %q", got, "payload")
	}
	if got := toolResultText(nil); got != "" {
		t.Fatalf("toolResultText(nil) = %q, want empty", got)
	}
}

func TestToolErrorResultUsesSingleTextContent(t *testing.T) {
	t.Parallel()

	result := toolErrorResult("missing")
	if !result.IsError {
		t.Fatal("toolErrorResult did not mark result as error")
	}
	if len(result.Content) != 1 {
		t.Fatalf("toolErrorResult returned %d content items", len(result.Content))
	}
	if result.Content[0].(mcp.TextContent).Text != "missing" {
		t.Fatalf("toolErrorResult returned %q", result.Content[0].(mcp.TextContent).Text)
	}
}
