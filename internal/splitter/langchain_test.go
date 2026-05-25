package splitter

import (
	"context"
	"strings"
	"testing"
)

func TestLangchainSplitJavaScriptBreaksOnFunction(t *testing.T) {
	t.Parallel()

	content := strings.Join([]string{
		"function alpha() {",
		"  return 1;",
		"}",
		"",
		"function bravo() {",
		"  return 2;",
		"}",
		"",
		"function charlie() {",
		"  return 3;",
		"}",
	}, "\n")

	dispatcher := &Dispatcher{
		astChunkSize:      2500,
		astChunkOverlap:   0,
		fallbackChunkSize: 40,
		fallbackOverlap:   0,
	}
	result, err := dispatcher.SplitFileWithType(context.Background(), "example.js", []byte(content), "langchain")
	if err != nil {
		t.Fatalf("SplitFileWithType returned error: %v", err)
	}
	if result.Strategy != "langchain" {
		t.Fatalf("strategy = %q", result.Strategy)
	}
	if len(result.Chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d: %#v", len(result.Chunks), result.Chunks)
	}
	for _, chunk := range result.Chunks[1:] {
		if !strings.HasPrefix(strings.TrimSpace(chunk.Content), "function") {
			t.Fatalf("chunk does not start at a function boundary: %q", chunk.Content)
		}
	}
}

func TestLangchainSplitPythonBreaksOnDef(t *testing.T) {
	t.Parallel()

	content := strings.Join([]string{
		"def alpha():",
		"    return 1",
		"",
		"def bravo():",
		"    return 2",
		"",
		"def charlie():",
		"    return 3",
	}, "\n")

	dispatcher := &Dispatcher{
		astChunkSize:      2500,
		astChunkOverlap:   0,
		fallbackChunkSize: 30,
		fallbackOverlap:   0,
	}
	result, err := dispatcher.SplitFileWithType(context.Background(), "example.py", []byte(content), "langchain")
	if err != nil {
		t.Fatalf("SplitFileWithType returned error: %v", err)
	}
	if len(result.Chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(result.Chunks))
	}
	if !strings.Contains(result.Chunks[0].Content, "def alpha") {
		t.Fatalf("first chunk missing def alpha: %q", result.Chunks[0].Content)
	}
}

func TestLangchainSplitFallsBackForUnknownLanguage(t *testing.T) {
	t.Parallel()

	content := "alpha\nbravo\ncharlie\n"
	chunks := langchainSplit(content, "unknown", "example.unk", 8, 0)
	if len(chunks) == 0 {
		t.Fatal("langchainSplit returned no chunks")
	}
}

func TestCSharpASTSupported(t *testing.T) {
	t.Parallel()

	content := strings.Join([]string{
		"namespace Demo {",
		"  public class Greeter {",
		"    public string Hello() {",
		"      return \"hi\";",
		"    }",
		"  }",
		"}",
	}, "\n")

	dispatcher := NewDispatcher()
	result, err := dispatcher.SplitFile(context.Background(), "Greeter.cs", []byte(content))
	if err != nil {
		t.Fatalf("SplitFile returned error: %v", err)
	}
	if result.Strategy != "ast" {
		t.Fatalf("strategy = %q (expected ast)", result.Strategy)
	}
	if len(result.Chunks) == 0 {
		t.Fatal("expected at least one chunk")
	}
}
