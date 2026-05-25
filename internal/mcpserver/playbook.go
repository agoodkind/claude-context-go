package mcpserver

import (
	"context"
	_ "embed"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

const (
	semanticSearchResourceURI  = "claude-context://semantic_search"
	semanticSearchPromptName   = "semantic_search"
	semanticSearchResourceName = "semantic_search"
)

//go:embed semantic_search.md
var semanticSearchText string

func registerSemanticSearchResource(s *server.MCPServer) {
	s.AddResource(
		mcp.Resource{
			URI:         semanticSearchResourceURI,
			Name:        semanticSearchResourceName,
			Description: "Read this guide before using search_code or index_codebase for semantic code discovery.",
			MIMEType:    "text/markdown",
		},
		func(_ context.Context, _ mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
			return []mcp.ResourceContents{
				mcp.TextResourceContents{
					URI:      semanticSearchResourceURI,
					MIMEType: "text/markdown",
					Text:     semanticSearchText,
				},
			}, nil
		},
	)
}

func registerSemanticSearchPrompt(s *server.MCPServer) {
	s.AddPrompt(
		mcp.Prompt{
			Name:        semanticSearchPromptName,
			Description: "Orientation guide for claude-context semantic search and indexing workflows.",
		},
		func(_ context.Context, _ mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{
				Description: "Claude Context semantic search playbook",
				Messages: []mcp.PromptMessage{
					{
						Role:    mcp.RoleAssistant,
						Content: mcp.NewTextContent(semanticSearchText),
					},
				},
			}, nil
		},
	)
}
