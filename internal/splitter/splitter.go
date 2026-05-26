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
	"unicode/utf8"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"
	tree_sitter_csharp "github.com/tree-sitter/tree-sitter-c-sharp/bindings/go"
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

const splitterTypeLangchain = "langchain"

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
	grammarCSharp     grammarKey = "csharp"
	grammarCS         grammarKey = "cs"
)

var splittableNodeTypes = map[string][]string{
	"javascript": {"function_declaration", "arrow_function", "class_declaration", "method_definition", "export_statement"},
	"typescript": {"function_declaration", "arrow_function", "class_declaration", "method_definition", "export_statement", "interface_declaration", "type_alias_declaration"},
	"python":     {"function_definition", "class_definition", "decorated_definition", "async_function_definition"},
	"java":       {"method_declaration", "class_declaration", "interface_declaration", "constructor_declaration"},
	"cpp":        {"function_definition", "class_specifier", "namespace_definition", "declaration"},
	"go":         {"function_declaration", "method_declaration", "type_declaration", "var_declaration", "const_declaration"},
	"rust":       {"function_item", "impl_item", "struct_item", "enum_item", "trait_item", "mod_item"},
	"csharp":     {"method_declaration", "class_declaration", "interface_declaration", "struct_declaration", "record_declaration", "enum_declaration", "namespace_declaration", "constructor_declaration", "property_declaration"},
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
	return dispatcher.SplitFileWithType(ctx, path, content, "")
}

// SplitFileWithType splits one file using the requested splitter type.
func (dispatcher *Dispatcher) SplitFileWithType(ctx context.Context, path string, content []byte, splitterType string) (Result, error) {
	language := languageForPath(path)
	if strings.EqualFold(strings.TrimSpace(splitterType), splitterTypeLangchain) {
		return Result{
			Chunks:   langchainSplit(string(content), language, path, dispatcher.fallbackChunkSize, dispatcher.fallbackOverlap),
			Strategy: splitterTypeLangchain,
		}, nil
	}

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

	flush := func() {
		if strings.TrimSpace(currentChunk) == "" {
			return
		}
		subChunks = append(subChunks, Chunk{
			Content:   strings.TrimSpace(currentChunk),
			StartLine: currentStartLine,
			EndLine:   currentStartLine + currentLineCount - 1,
			Language:  chunk.Language,
			FilePath:  chunk.FilePath,
		})
	}

	for i, line := range lines {
		lineWithNewline := line
		if i < len(lines)-1 {
			lineWithNewline += "\n"
		}

		// A single line longer than chunkSize needs mid-line hard-splits;
		// minified JS, generated code, and JSON dumps routinely produce
		// these. Without this branch the AST chunk stays oversize and
		// Milvus rejects it for exceeding the VarChar max length.
		if len(lineWithNewline) > chunkSize {
			flush()
			currentChunk = ""
			currentLineCount = 0
			for _, piece := range hardSplit(lineWithNewline, chunkSize) {
				subChunks = append(subChunks, Chunk{
					Content:   strings.TrimSpace(piece),
					StartLine: chunk.StartLine + i,
					EndLine:   chunk.StartLine + i,
					Language:  chunk.Language,
					FilePath:  chunk.FilePath,
				})
			}
			currentStartLine = chunk.StartLine + i + 1
			continue
		}

		if len(currentChunk)+len(lineWithNewline) > chunkSize && currentChunk != "" {
			flush()
			currentChunk = lineWithNewline
			currentStartLine = chunk.StartLine + i
			currentLineCount = 1
			continue
		}

		currentChunk += lineWithNewline
		currentLineCount++
	}

	flush()
	return subChunks
}

func (dispatcher *Dispatcher) characterSplit(content string, language string, path string, chunkSize int, overlap int) []Chunk {
	chunks := make([]Chunk, 0)
	lines := strings.Split(content, "\n")
	currentChunk := ""
	currentStartLine := 1
	currentLineCount := 0

	flush := func() {
		if strings.TrimSpace(currentChunk) == "" {
			return
		}
		chunks = append(chunks, Chunk{
			Content:   currentChunk,
			StartLine: currentStartLine,
			EndLine:   currentStartLine + currentLineCount - 1,
			Language:  language,
			FilePath:  path,
		})
	}

	for i, line := range lines {
		lineWithNewline := line
		if i < len(lines)-1 {
			lineWithNewline += "\n"
		}

		// Mid-line hard-split for lines longer than chunkSize keeps
		// minified or single-line generated files within the per-chunk
		// byte budget. Without it the line stays a single oversize
		// chunk that Milvus refuses to accept.
		if len(lineWithNewline) > chunkSize {
			flush()
			currentChunk = ""
			currentLineCount = 0
			for _, piece := range hardSplit(lineWithNewline, chunkSize) {
				chunks = append(chunks, Chunk{
					Content:   piece,
					StartLine: i + 1,
					EndLine:   i + 1,
					Language:  language,
					FilePath:  path,
				})
			}
			currentStartLine = i + 2
			continue
		}

		if len(currentChunk)+len(lineWithNewline) > chunkSize && currentChunk != "" {
			flush()
			currentChunk = lineWithNewline
			currentStartLine = i + 1
			currentLineCount = 1
			continue
		}

		currentChunk += lineWithNewline
		currentLineCount++
	}

	flush()

	return addOverlap(chunks, overlap)
}

func addOverlap(chunks []Chunk, overlap int) []Chunk {
	if len(chunks) <= 1 || overlap <= 0 {
		return chunks
	}
	result := make([]Chunk, 0, len(chunks))
	result = append(result, chunks[0])
	for index := 1; index < len(chunks); index++ {
		chunk := chunks[index]
		previousChunk := chunks[index-1]
		overlapText := previousChunk.Content
		if len(overlapText) > overlap {
			cut := alignToRuneStart(overlapText, len(overlapText)-overlap)
			overlapText = overlapText[cut:]
		}
		chunk.Content = overlapText + "\n" + chunk.Content
		chunk.StartLine = max(1, chunk.StartLine-lineCount(overlapText))
		result = append(result, chunk)
	}
	return result
}

// alignToRuneStart returns the smallest index >= offset where s[i] starts a
// valid UTF-8 codepoint. Byte-offset slicing into UTF-8 strings without this
// alignment can yield chunk content that begins with a continuation byte,
// which Milvus rejects at the gRPC marshal boundary for VarChar fields.
func alignToRuneStart(s string, offset int) int {
	if offset <= 0 {
		return 0
	}
	if offset >= len(s) {
		return len(s)
	}
	for offset < len(s) && !utf8.RuneStart(s[offset]) {
		offset++
	}
	return offset
}

// alignDownToRuneStart returns the largest index <= offset where s[i] is the
// start of a codepoint (or i == 0). Used to trim a chunk's tail back to the
// last whole codepoint before a byte-offset boundary.
func alignDownToRuneStart(s string, offset int) int {
	if offset <= 0 {
		return 0
	}
	if offset >= len(s) {
		return len(s)
	}
	for offset > 0 && !utf8.RuneStart(s[offset]) {
		offset--
	}
	return offset
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
	case grammarCSharp, grammarCS:
		return tree_sitter.NewLanguage(tree_sitter_csharp.Language()), true
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
