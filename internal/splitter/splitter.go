// Package splitter splits code files into searchable chunks.
package splitter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"slices"
	"strings"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"
	tree_sitter_c "github.com/tree-sitter/tree-sitter-c/bindings/go"
	tree_sitter_cpp "github.com/tree-sitter/tree-sitter-cpp/bindings/go"
	tree_sitter_go "github.com/tree-sitter/tree-sitter-go/bindings/go"
	tree_sitter_java "github.com/tree-sitter/tree-sitter-java/bindings/go"
	tree_sitter_javascript "github.com/tree-sitter/tree-sitter-javascript/bindings/go"
	tree_sitter_python "github.com/tree-sitter/tree-sitter-python/bindings/go"
	tree_sitter_rust "github.com/tree-sitter/tree-sitter-rust/bindings/go"
	tree_sitter_scala "github.com/tree-sitter/tree-sitter-scala/bindings/go"
	tree_sitter_typescript "github.com/tree-sitter/tree-sitter-typescript/bindings/go"
)

// Chunk is one code chunk emitted by a splitter.
type Chunk struct {
	Content   string
	StartLine int
	EndLine   int
	Language  string
	FilePath  string
}

// Result captures the emitted chunks and which strategy produced them.
type Result struct {
	Chunks   []Chunk
	Strategy string
}

// Dispatcher routes files to AST or fallback splitting.
type Dispatcher struct {
	astChunkSize      int
	astChunkOverlap   int
	fallbackChunkSize int
	fallbackOverlap   int
}

type grammarKey string

const (
	grammarJavaScript grammarKey = "javascript"
	grammarJS         grammarKey = "js"
	grammarTypeScript grammarKey = "typescript"
	grammarTS         grammarKey = "ts"
	grammarPython     grammarKey = "python"
	grammarPy         grammarKey = "py"
	grammarJava       grammarKey = "java"
	grammarCPP        grammarKey = "cpp"
	grammarCXX        grammarKey = "c++"
	grammarC          grammarKey = "c"
	grammarGo         grammarKey = "go"
	grammarRust       grammarKey = "rust"
	grammarRS         grammarKey = "rs"
	grammarScala      grammarKey = "scala"
)

var splittableNodeTypes = map[string][]string{
	"javascript": {"function_declaration", "arrow_function", "class_declaration", "method_definition", "export_statement"},
	"typescript": {"function_declaration", "arrow_function", "class_declaration", "method_definition", "export_statement", "interface_declaration", "type_alias_declaration"},
	"python":     {"function_definition", "class_definition", "decorated_definition", "async_function_definition"},
	"java":       {"method_declaration", "class_declaration", "interface_declaration", "constructor_declaration"},
	"cpp":        {"function_definition", "class_specifier", "namespace_definition", "declaration"},
	"go":         {"function_declaration", "method_declaration", "type_declaration", "var_declaration", "const_declaration"},
	"rust":       {"function_item", "impl_item", "struct_item", "enum_item", "trait_item", "mod_item"},
	"csharp":     {"method_declaration", "class_declaration", "interface_declaration", "struct_declaration", "enum_declaration"},
	"scala":      {"method_declaration", "class_declaration", "interface_declaration", "constructor_declaration"},
}

var extensionLanguages = map[string]string{
	".ts":    "typescript",
	".tsx":   "typescript",
	".js":    "javascript",
	".jsx":   "javascript",
	".py":    "python",
	".java":  "java",
	".cpp":   "cpp",
	".c":     "c",
	".h":     "c",
	".hpp":   "cpp",
	".cs":    "csharp",
	".go":    "go",
	".rs":    "rust",
	".php":   "php",
	".rb":    "ruby",
	".swift": "swift",
	".kt":    "kotlin",
	".scala": "scala",
	".m":     "objective-c",
	".mm":    "objective-c",
	".dart":  "dart",
	".sol":   "solidity",
	".ipynb": "jupyter",
}

// NewDispatcher constructs a splitter dispatcher using the current defaults.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		astChunkSize:      2500,
		astChunkOverlap:   300,
		fallbackChunkSize: 1000,
		fallbackOverlap:   200,
	}
}

// SplitFile splits one file into chunks using AST or fallback behavior.
func (dispatcher *Dispatcher) SplitFile(ctx context.Context, path string, content []byte) (Result, error) {
	language := languageForPath(path)
	astChunks, err := dispatcher.tryAST(ctx, content, language, path)
	if err == nil {
		return Result{Chunks: astChunks, Strategy: "ast"}, nil
	}

	slog.WarnContext(ctx, "fall back to character splitter", "path", path, "language", language, "err", err)
	return Result{
		Chunks:   dispatcher.characterSplit(string(content), language, path, dispatcher.fallbackChunkSize, dispatcher.fallbackOverlap),
		Strategy: "fallback",
	}, nil
}

func (dispatcher *Dispatcher) tryAST(ctx context.Context, content []byte, language string, path string) ([]Chunk, error) {
	grammar, supported := grammarForLanguage(language)
	if !supported {
		slog.WarnContext(ctx, "language is unsupported by AST splitter", "path", path, "language", language)
		return nil, fmt.Errorf("language %s not supported by AST splitter", language)
	}

	parser := tree_sitter.NewParser()
	defer parser.Close()

	if err := parser.SetLanguage(grammar); err != nil {
		slog.ErrorContext(ctx, "set parser language failed", "path", path, "language", language, "err", err)
		return nil, fmt.Errorf("set parser language for %s: %w", language, err)
	}

	tree := parser.Parse(content, nil)
	if tree == nil {
		slog.ErrorContext(ctx, "parse returned nil tree", "path", path, "language", language, "err", errors.New("nil tree"))
		return nil, fmt.Errorf("parse returned nil tree for %s", path)
	}
	defer tree.Close()

	rootNode := tree.RootNode()
	if rootNode == nil {
		slog.ErrorContext(ctx, "parse produced no root node", "path", path, "language", language, "err", errors.New("nil root node"))
		return nil, fmt.Errorf("parse produced no root node for %s", path)
	}

	chunks := make([]Chunk, 0)
	dispatcher.collectASTChunks(rootNode, content, language, path, &chunks)
	if len(chunks) == 0 {
		fallbackChunk := Chunk{
			Content:   string(content),
			StartLine: 1,
			EndLine:   lineCount(string(content)),
			Language:  language,
			FilePath:  path,
		}
		return dispatcher.refineChunks([]Chunk{fallbackChunk}, dispatcher.astChunkSize, dispatcher.astChunkOverlap), nil
	}

	return dispatcher.refineChunks(chunks, dispatcher.astChunkSize, dispatcher.astChunkOverlap), nil
}

func (dispatcher *Dispatcher) collectASTChunks(node *tree_sitter.Node, content []byte, language string, path string, chunks *[]Chunk) {
	if node == nil {
		return
	}
	if slices.Contains(splittableNodeTypes[language], node.Kind()) {
		startByte := node.StartByte()
		endByte := node.EndByte()
		nodeText := strings.TrimSpace(string(content[startByte:endByte]))
		if nodeText != "" {
			startPosition := node.StartPosition()
			endPosition := node.EndPosition()
			*chunks = append(*chunks, Chunk{
				Content:   nodeText,
				StartLine: safeInt(startPosition.Row) + 1,
				EndLine:   safeInt(endPosition.Row) + 1,
				Language:  language,
				FilePath:  path,
			})
		}
	}

	for i := range node.ChildCount() {
		dispatcher.collectASTChunks(node.Child(i), content, language, path, chunks)
	}
}

func (dispatcher *Dispatcher) refineChunks(chunks []Chunk, chunkSize int, overlap int) []Chunk {
	refined := make([]Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		if len(chunk.Content) <= chunkSize {
			refined = append(refined, chunk)
			continue
		}
		refined = append(refined, dispatcher.splitLargeChunk(chunk, chunkSize)...)
	}
	return addOverlap(refined, overlap)
}

func (dispatcher *Dispatcher) splitLargeChunk(chunk Chunk, chunkSize int) []Chunk {
	lines := strings.Split(chunk.Content, "\n")
	subChunks := make([]Chunk, 0)
	currentChunk := ""
	currentStartLine := chunk.StartLine
	currentLineCount := 0

	for i, line := range lines {
		lineWithNewline := line
		if i < len(lines)-1 {
			lineWithNewline += "\n"
		}

		if len(currentChunk)+len(lineWithNewline) > chunkSize && currentChunk != "" {
			subChunks = append(subChunks, Chunk{
				Content:   strings.TrimSpace(currentChunk),
				StartLine: currentStartLine,
				EndLine:   currentStartLine + currentLineCount - 1,
				Language:  chunk.Language,
				FilePath:  chunk.FilePath,
			})
			currentChunk = lineWithNewline
			currentStartLine = chunk.StartLine + i
			currentLineCount = 1
			continue
		}

		currentChunk += lineWithNewline
		currentLineCount++
	}

	if strings.TrimSpace(currentChunk) != "" {
		subChunks = append(subChunks, Chunk{
			Content:   strings.TrimSpace(currentChunk),
			StartLine: currentStartLine,
			EndLine:   currentStartLine + currentLineCount - 1,
			Language:  chunk.Language,
			FilePath:  chunk.FilePath,
		})
	}
	return subChunks
}

func (dispatcher *Dispatcher) characterSplit(content string, language string, path string, chunkSize int, overlap int) []Chunk {
	chunks := make([]Chunk, 0)
	lines := strings.Split(content, "\n")
	currentChunk := ""
	currentStartLine := 1
	currentLineCount := 0

	for i, line := range lines {
		lineWithNewline := line
		if i < len(lines)-1 {
			lineWithNewline += "\n"
		}

		if len(currentChunk)+len(lineWithNewline) > chunkSize && currentChunk != "" {
			chunks = append(chunks, Chunk{
				Content:   currentChunk,
				StartLine: currentStartLine,
				EndLine:   currentStartLine + currentLineCount - 1,
				Language:  language,
				FilePath:  path,
			})

			currentChunk = lineWithNewline
			currentStartLine = i + 1
			currentLineCount = 1
			continue
		}

		currentChunk += lineWithNewline
		currentLineCount++
	}

	if strings.TrimSpace(currentChunk) != "" {
		chunks = append(chunks, Chunk{
			Content:   currentChunk,
			StartLine: currentStartLine,
			EndLine:   currentStartLine + currentLineCount - 1,
			Language:  language,
			FilePath:  path,
		})
	}

	return addOverlap(chunks, overlap)
}

func addOverlap(chunks []Chunk, overlap int) []Chunk {
	if len(chunks) <= 1 || overlap <= 0 {
		return chunks
	}
	result := make([]Chunk, 0, len(chunks))
	for index, chunk := range chunks {
		if index == 0 {
			result = append(result, chunk)
			continue
		}
		previousChunk := chunks[index-1]
		overlapText := previousChunk.Content
		if len(overlapText) > overlap {
			overlapText = overlapText[len(overlapText)-overlap:]
		}
		chunk.Content = overlapText + "\n" + chunk.Content
		chunk.StartLine = max(1, chunk.StartLine-lineCount(overlapText))
		result = append(result, chunk)
	}
	return result
}

func grammarForLanguage(language string) (*tree_sitter.Language, bool) {
	switch grammarKey(strings.ToLower(language)) {
	case grammarJavaScript, grammarJS:
		return tree_sitter.NewLanguage(tree_sitter_javascript.Language()), true
	case grammarTypeScript, grammarTS:
		return tree_sitter.NewLanguage(tree_sitter_typescript.LanguageTypescript()), true
	case grammarPython, grammarPy:
		return tree_sitter.NewLanguage(tree_sitter_python.Language()), true
	case grammarJava:
		return tree_sitter.NewLanguage(tree_sitter_java.Language()), true
	case grammarCPP, grammarCXX:
		return tree_sitter.NewLanguage(tree_sitter_cpp.Language()), true
	case grammarC:
		return tree_sitter.NewLanguage(tree_sitter_c.Language()), true
	case grammarGo:
		return tree_sitter.NewLanguage(tree_sitter_go.Language()), true
	case grammarRust, grammarRS:
		return tree_sitter.NewLanguage(tree_sitter_rust.Language()), true
	case grammarScala:
		return tree_sitter.NewLanguage(tree_sitter_scala.Language()), true
	default:
		return nil, false
	}
}

func languageForPath(path string) string {
	extension := filepath.Ext(path)
	language, found := extensionLanguages[extension]
	if !found {
		return "text"
	}
	return language
}

func lineCount(text string) int {
	if text == "" {
		return 0
	}
	return len(strings.Split(text, "\n"))
}

func safeInt(value uint) int {
	if value > math.MaxInt {
		return math.MaxInt
	}
	return int(value)
}
