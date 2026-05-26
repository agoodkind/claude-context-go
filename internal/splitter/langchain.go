package splitter

import (
	"strings"
	"unicode/utf8"
)

// langchainLanguage is the normalized key used to look up separator tables.
type langchainLanguage string

const (
	langchainLangJSFamily langchainLanguage = "js-family"
	langchainLangPython   langchainLanguage = "python"
	langchainLangJava     langchainLanguage = "java"
	langchainLangCFamily  langchainLanguage = "c-family"
	langchainLangGo       langchainLanguage = "go"
	langchainLangRust     langchainLanguage = "rust"
	langchainLangPHP      langchainLanguage = "php"
	langchainLangRuby     langchainLanguage = "ruby"
	langchainLangSwift    langchainLanguage = "swift"
	langchainLangScala    langchainLanguage = "scala"
	langchainLangCSharp   langchainLanguage = "csharp"
	langchainLangHTML     langchainLanguage = "html"
	langchainLangMarkdown langchainLanguage = "markdown"
	langchainLangLatex    langchainLanguage = "latex"
	langchainLangSolidity langchainLanguage = "solidity"
	langchainLangDefault  langchainLanguage = ""
)

// languageKeyAliases maps caller-facing language strings to the normalized key.
var languageKeyAliases = map[string]langchainLanguage{
	"javascript": langchainLangJSFamily,
	"js":         langchainLangJSFamily,
	"typescript": langchainLangJSFamily,
	"ts":         langchainLangJSFamily,
	"jsx":        langchainLangJSFamily,
	"tsx":        langchainLangJSFamily,
	"python":     langchainLangPython,
	"py":         langchainLangPython,
	"java":       langchainLangJava,
	"cpp":        langchainLangCFamily,
	"c++":        langchainLangCFamily,
	"c":          langchainLangCFamily,
	"go":         langchainLangGo,
	"rust":       langchainLangRust,
	"rs":         langchainLangRust,
	"php":        langchainLangPHP,
	"ruby":       langchainLangRuby,
	"rb":         langchainLangRuby,
	"swift":      langchainLangSwift,
	"scala":      langchainLangScala,
	"csharp":     langchainLangCSharp,
	"cs":         langchainLangCSharp,
	"html":       langchainLangHTML,
	"markdown":   langchainLangMarkdown,
	"md":         langchainLangMarkdown,
	"latex":      langchainLangLatex,
	"tex":        langchainLangLatex,
	"solidity":   langchainLangSolidity,
	"sol":        langchainLangSolidity,
}

// langchainSeparatorTable maps normalized language keys to their separator
// chain. Tables mirror LangChain's RecursiveCharacterTextSplitter.fromLanguage
// so chunk boundaries fall on function, class, or other natural code seams
// before breaking on blank lines, line breaks, words, and finally individual
// characters. Source: langchain-js src/text_splitter.ts.
var langchainSeparatorTable = map[langchainLanguage][]string{
	langchainLangJSFamily: {
		"\nfunction ", "\nconst ", "\nlet ", "\nvar ", "\nclass ",
		"\nif ", "\nfor ", "\nwhile ", "\nswitch ", "\ncase ", "\ndefault ",
		"\n\n", "\n", " ", "",
	},
	langchainLangPython: {
		"\nclass ", "\ndef ", "\n\tdef ",
		"\n\n", "\n", " ", "",
	},
	langchainLangJava: {
		"\nclass ", "\npublic ", "\nprotected ", "\nprivate ", "\nstatic ",
		"\nif ", "\nfor ", "\nwhile ", "\nswitch ", "\ncase ",
		"\n\n", "\n", " ", "",
	},
	langchainLangCFamily: {
		"\nclass ", "\nvoid ", "\nint ", "\nfloat ", "\ndouble ",
		"\nif ", "\nfor ", "\nwhile ", "\nswitch ", "\ncase ",
		"\n\n", "\n", " ", "",
	},
	langchainLangGo: {
		"\nfunc ", "\nvar ", "\nconst ", "\ntype ",
		"\nif ", "\nfor ", "\nswitch ", "\ncase ",
		"\n\n", "\n", " ", "",
	},
	langchainLangRust: {
		"\nfn ", "\nconst ", "\nlet ",
		"\nif ", "\nwhile ", "\nfor ", "\nloop ", "\nmatch ",
		"\n\n", "\n", " ", "",
	},
	langchainLangPHP: {
		"\nfunction ", "\nclass ",
		"\nif ", "\nforeach ", "\nwhile ", "\ndo ", "\nswitch ", "\ncase ",
		"\n\n", "\n", " ", "",
	},
	langchainLangRuby: {
		"\ndef ", "\nclass ",
		"\nif ", "\nunless ", "\nwhile ", "\nfor ", "\ndo ", "\nbegin ", "\nrescue ", "\ncase ",
		"\n\n", "\n", " ", "",
	},
	langchainLangSwift: {
		"\nfunc ", "\nclass ", "\nstruct ", "\nenum ", "\nactor ",
		"\nif ", "\nfor ", "\nwhile ", "\ndo ", "\nswitch ", "\ncase ",
		"\n\n", "\n", " ", "",
	},
	langchainLangScala: {
		"\nclass ", "\nobject ", "\ndef ", "\nval ", "\nvar ",
		"\nif ", "\nfor ", "\nwhile ", "\nmatch ", "\ncase ",
		"\n\n", "\n", " ", "",
	},
	langchainLangCSharp: {
		"\ninterface ", "\nenum ", "\nimplements ", "\ndelegate ", "\nevent ",
		"\nclass ", "\nabstract ", "\npublic ", "\nprotected ", "\nprivate ", "\nstatic ", "\nreturn ",
		"\nif ", "\ncontinue ", "\nfor ", "\nforeach ", "\nwhile ", "\nswitch ", "\nbreak ", "\ncase ",
		"\n\n", "\n", " ", "",
	},
	langchainLangHTML: {
		"<body", "<div", "<p", "<br", "<li", "<h1", "<h2", "<h3", "<h4", "<h5", "<h6",
		"<span", "<table", "<tr", "<td", "<th", "<ul", "<ol", "<header", "<footer", "<nav",
		"<head", "<style", "<script", "<meta", "<title", "",
	},
	langchainLangMarkdown: {
		"\n## ", "\n### ", "\n#### ", "\n##### ", "\n###### ",
		"```\n\n", "\n\n***\n\n", "\n\n---\n\n", "\n\n___\n\n",
		"\n\n", "\n", " ", "",
	},
	langchainLangLatex: {
		"\n\\chapter{", "\n\\section{", "\n\\subsection{", "\n\\subsubsection{",
		"\n\\begin{enumerate}", "\n\\begin{itemize}", "\n\\begin{description}", "\n\\begin{list}",
		"\n\\begin{quote}", "\n\\begin{quotation}", "\n\\begin{verse}", "\n\\begin{verbatim}",
		"\n\\begin{align}", "$$", "$", " ", "",
	},
	langchainLangSolidity: {
		"\npragma ", "\nusing ",
		"\ncontract ", "\ninterface ", "\nlibrary ",
		"\nconstructor ", "\ntype ", "\nfunction ", "\nevent ", "\nmodifier ", "\nerror ",
		"\nstruct ", "\nenum ",
		"\nif ", "\nfor ", "\nwhile ", "\ndo while ", "\nassembly ",
		"\n\n", "\n", " ", "",
	},
	langchainLangDefault: {"\n\n", "\n", " ", ""},
}

// langchainSeparators returns the prioritized separator list for a language.
func langchainSeparators(language string) []string {
	key, found := languageKeyAliases[strings.ToLower(language)]
	if !found {
		key = langchainLangDefault
	}
	return langchainSeparatorTable[key]
}

// recursiveSplit splits content using the language-aware separator chain.
func recursiveSplit(content string, separators []string, chunkSize int) []string {
	if chunkSize <= 0 || len(content) <= chunkSize {
		if content == "" {
			return nil
		}
		return []string{content}
	}

	separator, remaining := chooseSeparator(content, separators)
	splits := splitOnSeparator(content, separator)

	chunks := make([]string, 0, len(splits))
	for _, piece := range splits {
		if piece == "" {
			continue
		}
		if len(piece) <= chunkSize {
			chunks = append(chunks, piece)
			continue
		}
		if len(remaining) == 0 {
			chunks = append(chunks, hardSplit(piece, chunkSize)...)
			continue
		}
		chunks = append(chunks, recursiveSplit(piece, remaining, chunkSize)...)
	}
	return mergeAdjacent(chunks, separator, chunkSize)
}

func chooseSeparator(content string, separators []string) (string, []string) {
	for i, sep := range separators {
		if sep == "" {
			return sep, separators[i+1:]
		}
		if strings.Contains(content, sep) {
			return sep, separators[i+1:]
		}
	}
	return "", nil
}

func splitOnSeparator(content string, separator string) []string {
	if separator == "" {
		out := make([]string, 0, len(content))
		for _, r := range content {
			out = append(out, string(r))
		}
		return out
	}
	parts := strings.Split(content, separator)
	if len(parts) == 1 {
		return parts
	}
	out := make([]string, 0, len(parts))
	for i, part := range parts {
		if i == 0 {
			out = append(out, part)
			continue
		}
		out = append(out, separator+part)
	}
	return out
}

func hardSplit(content string, chunkSize int) []string {
	out := make([]string, 0, (len(content)+chunkSize-1)/chunkSize)
	start := 0
	for start < len(content) {
		end := min(start+chunkSize, len(content))
		if end < len(content) {
			end = alignDownToRuneStart(content, end)
			if end <= start {
				_, size := utf8.DecodeRuneInString(content[start:])
				end = start + size
			}
		}
		out = append(out, content[start:end])
		start = end
	}
	return out
}

func mergeAdjacent(pieces []string, separator string, chunkSize int) []string {
	if len(pieces) == 0 {
		return pieces
	}
	merged := make([]string, 0, len(pieces))
	current := pieces[0]
	for _, piece := range pieces[1:] {
		candidate := current + piece
		if len(candidate) <= chunkSize {
			current = candidate
			continue
		}
		merged = append(merged, current)
		current = piece
	}
	merged = append(merged, current)
	_ = separator
	return merged
}

// langchainSplit produces chunks for a file using the language-aware
// recursive splitter. The returned chunks preserve start/end line numbers
// computed from the original content offsets.
func langchainSplit(content string, language string, path string, chunkSize int, overlap int) []Chunk {
	if content == "" {
		return nil
	}
	separators := langchainSeparators(language)
	pieces := recursiveSplit(content, separators, chunkSize)

	chunks := make([]Chunk, 0, len(pieces))
	cursor := 0
	for _, piece := range pieces {
		if piece == "" {
			continue
		}
		index := max(strings.Index(content[cursor:], piece), 0)
		startByte := cursor + index
		endByte := startByte + len(piece)
		startLine := byteOffsetToLine(content, startByte)
		endLine := byteOffsetToLine(content, endByte-1)
		chunks = append(chunks, Chunk{
			Content:   piece,
			StartLine: startLine,
			EndLine:   endLine,
			Language:  language,
			FilePath:  path,
		})
		cursor = endByte
	}
	return addOverlap(chunks, overlap)
}

func byteOffsetToLine(content string, offset int) int {
	if offset < 0 {
		return 1
	}
	if offset >= len(content) {
		offset = len(content) - 1
	}
	line := 1
	for i := 0; i <= offset && i < len(content); i++ {
		if content[i] == '\n' {
			line++
		}
	}
	return line
}
