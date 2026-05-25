package mcpserver

import (
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	pb "goodkind.io/claude-context-go/gen/go/claudecontext/v1"
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
