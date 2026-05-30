package mcpserver

import (
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

// callRequest builds a tool-call request whose arguments are the supplied map,
// matching how the MCP server delivers arguments to a handler.
func callRequest(arguments map[string]any) mcp.CallToolRequest {
	return mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: arguments,
		},
	}
}

func errorText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	if result == nil {
		t.Fatal("expected an error result, got nil")
	}
	if !result.IsError {
		t.Fatal("expected IsError result")
	}
	if len(result.Content) != 1 {
		t.Fatalf("expected one content item, got %d", len(result.Content))
	}
	text, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", result.Content[0])
	}
	return text.Text
}

func TestRequireNonEmptyArgFailsLoudWhenMissing(t *testing.T) {
	t.Parallel()

	_, result, ok := requireNonEmptyArg(callRequest(map[string]any{}), "absolutePath")
	if ok {
		t.Fatal("missing argument should not be accepted")
	}
	if got := errorText(t, result); !strings.Contains(got, "absolutePath is required") {
		t.Fatalf("error text = %q, want it to name absolutePath", got)
	}
}

func TestRequireNonEmptyArgFailsLoudWhenEmptyOrBlank(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"", "   ", "\t\n"} {
		_, result, ok := requireNonEmptyArg(callRequest(map[string]any{"query": value}), "query")
		if ok {
			t.Fatalf("blank argument %q should not be accepted", value)
		}
		if got := errorText(t, result); !strings.Contains(got, "query is required") {
			t.Fatalf("error text = %q, want it to name query", got)
		}
	}
}

func TestRequireNonEmptyArgAcceptsValue(t *testing.T) {
	t.Parallel()

	value, result, ok := requireNonEmptyArg(callRequest(map[string]any{"absolutePath": "/Users/x/repo"}), "absolutePath")
	if !ok {
		t.Fatal("a non-empty argument should be accepted")
	}
	if result != nil {
		t.Fatalf("expected nil error result, got %#v", result)
	}
	if value != "/Users/x/repo" {
		t.Fatalf("value = %q, want /Users/x/repo", value)
	}
}
