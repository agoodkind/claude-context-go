// Package discovery finds code files using the current Claude Context rules.
package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// defaultIgnorePatterns is the denylist applied to every codebase. It
// covers VCS internals, dependency and build output, editor scratch, OS
// metadata, common binary file extensions, package lockfiles, and minified
// or generated bundles. Discovery walks every other file regardless of
// extension; binary content is filtered later by the indexer's UTF-8 check.
var defaultIgnorePatterns = []string{
	"node_modules/",
	"dist/",
	"build/",
	"out/",
	"target/",
	"vendor/",
	"coverage/",
	".nyc_output/",
	".vscode/",
	".idea/",
	".git/",
	".svn/",
	".hg/",
	".cache/",
	".next/",
	".nuxt/",
	".turbo/",
	".parcel-cache/",
	".pnpm-store/",
	".yarn/",
	".gradle/",
	".terraform/",
	".direnv/",
	"__pycache__/",
	".pytest_cache/",
	".mypy_cache/",
	".ruff_cache/",
	".tox/",
	".venv/",
	"venv/",
	"logs/",
	"tmp/",
	"temp/",
	".DS_Store",
	".env",
	".env.*",
	"*.swp",
	"*.swo",
	"*.log",
	"*.local",
	"*.min.js",
	"*.min.css",
	"*.min.map",
	"*.bundle.js",
	"*.bundle.css",
	"*.chunk.js",
	"*.vendor.js",
	"*.polyfills.js",
	"*.runtime.js",
	"*.map",
	// Image formats.
	"*.png",
	"*.jpg",
	"*.jpeg",
	"*.gif",
	"*.ico",
	"*.webp",
	"*.bmp",
	"*.tiff",
	"*.tif",
	"*.heic",
	"*.avif",
	// Document and archive binaries.
	"*.pdf",
	"*.doc",
	"*.docx",
	"*.xls",
	"*.xlsx",
	"*.ppt",
	"*.pptx",
	"*.zip",
	"*.tar",
	"*.tgz",
	"*.gz",
	"*.bz2",
	"*.xz",
	"*.7z",
	"*.rar",
	"*.dmg",
	"*.iso",
	// Native, compiled, and packaged binaries.
	"*.exe",
	"*.dll",
	"*.so",
	"*.dylib",
	"*.o",
	"*.obj",
	"*.a",
	"*.lib",
	"*.class",
	"*.jar",
	"*.war",
	"*.ear",
	"*.wasm",
	"*.pyc",
	"*.pyo",
	"*.pyd",
	// Fonts.
	"*.ttf",
	"*.otf",
	"*.woff",
	"*.woff2",
	"*.eot",
	// Audio and video.
	"*.mp3",
	"*.mp4",
	"*.mov",
	"*.avi",
	"*.mkv",
	"*.webm",
	"*.wav",
	"*.flac",
	"*.ogg",
	// Databases and embedded blobs.
	"*.sqlite",
	"*.sqlite3",
	"*.db",
	"*.bin",
	// Package lockfiles.
	"package-lock.json",
	"yarn.lock",
	"pnpm-lock.yaml",
	"Cargo.lock",
	"Gemfile.lock",
	"composer.lock",
	"poetry.lock",
	"go.sum",
}

// Constant labels for the source field on a pattern; surface in PathIgnored
// output so callers can name the matched pattern's origin.
const (
	patternSourceBuiltin  = "<built-in>"
	patternSourceOverride = "<override>"
	patternSourceGlobal   = "<global>"
)

// Result is one discovery pass over a codebase root.
type Result struct {
	Files          []string
	IgnorePatterns []string
	Extensions     []string
}

// IgnoreRules captures the resolved ignore decision tree for one codebase.
// The discovery package walks the codebase tree at resolution time, reading
// every nested .gitignore plus the built-in defaults, repo-level ignore
// files, the global ~/.context/.contextignore, and any user overrides. The
// resulting rules are then evaluated with PathIgnored.
//
// Each entry in Nodes maps a directory relative path (slash-separated, root
// is "") to the patterns declared directly in that directory's .gitignore
// (or, for the root entry, the merged defaults and overrides). Patterns
// inside a node are stored in declaration order so last-match-wins is
// preserved.
type IgnoreRules struct {
	// Nodes is the per-directory pattern table. The key is the directory
	// path relative to the codebase root; the empty key holds the root
	// node's patterns.
	Nodes map[string][]ignorePattern
}

// ignorePattern is one parsed entry from a .gitignore-style source. Pattern
// is the raw text (including any leading '!' for negation); Negation is
// true when the entry starts with '!'; Source names the file the pattern
// came from so PathIgnored can surface it.
type ignorePattern struct {
	Pattern  string
	Negation bool
	Source   string
}

// Patterns returns the root-node pattern list as a slice of plain strings
// so legacy callers that consume a flat denylist still work.
func (rules IgnoreRules) Patterns() []string {
	if len(rules.Nodes) == 0 {
		return nil
	}
	root := rules.Nodes[""]
	patterns := make([]string, 0, len(root))
	for _, entry := range root {
		patterns = append(patterns, entry.Pattern)
	}
	return patterns
}

// Flatten returns every pattern in the rules tree as a single deduplicated
// list. The flat list is used by callers (notably the indexer's discovery
// pass and the merkle config digest) that need a stable denylist before
// the per-directory tree walk is available.
func (rules IgnoreRules) Flatten() []string {
	if len(rules.Nodes) == 0 {
		return nil
	}
	merged := make([]string, 0)
	for _, entries := range rules.Nodes {
		for _, entry := range entries {
			merged = append(merged, entry.Pattern)
		}
	}
	return dedupStrings(merged)
}

// IsEmpty reports whether the rule tree has no nodes.
func (rules IgnoreRules) IsEmpty() bool {
	return len(rules.Nodes) == 0
}

// Discover walks a codebase root and returns every file that survives the
// ignore-pattern denylist. Extensions in the request are honored for
// backwards compatibility but no longer gate inclusion; the binary content
// gate now lives in the indexer's UTF-8 check.
func Discover(ctx context.Context, root string, additionalIgnorePatterns []string, additionalExtensions []string) (Result, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		slog.ErrorContext(ctx, "resolve absolute root failed", "root", root, "err", err)
		return Result{}, fmt.Errorf("resolve absolute root %s: %w", root, err)
	}

	rules, err := EffectiveIgnorePatterns(ctx, absoluteRoot, additionalIgnorePatterns)
	if err != nil {
		return Result{}, err
	}
	effectiveExtensions := normalizeExtensions(additionalExtensions)

	files := []string{}
	if err := walkFiles(ctx, absoluteRoot, absoluteRoot, rules, &files); err != nil {
		return Result{}, err
	}
	slices.Sort(files)

	return Result{
		Files:          files,
		IgnorePatterns: rules.Flatten(),
		Extensions:     effectiveExtensions,
	}, nil
}

// EffectiveIgnorePatterns resolves the ignore rule tree for root by reading
// every nested .gitignore file, the built-in defaults, repo-level ignore
// files at the root, the global ~/.context/.contextignore, and any
// user-supplied overrides. The returned tree is evaluated by PathIgnored.
func EffectiveIgnorePatterns(ctx context.Context, root string, additionalIgnorePatterns []string) (IgnoreRules, error) {
	nodes := map[string][]ignorePattern{}
	rootPatterns := make([]ignorePattern, 0, len(defaultIgnorePatterns)+len(additionalIgnorePatterns))
	for _, pattern := range defaultIgnorePatterns {
		rootPatterns = append(rootPatterns, parsePattern(pattern, patternSourceBuiltin))
	}
	for _, pattern := range additionalIgnorePatterns {
		rootPatterns = append(rootPatterns, parsePattern(pattern, patternSourceOverride))
	}

	if err := walkGitignore(ctx, root, "", nodes); err != nil {
		return IgnoreRules{}, err
	}

	// Repo-level .gitignore at the root is folded into rootPatterns rather
	// than into a "" node entry from the walk so the digest of the root
	// node stays stable across reloads.
	if existing, found := nodes[""]; found {
		rootPatterns = append(rootPatterns, existing...)
		delete(nodes, "")
	}

	if globalPatterns, err := loadGlobalContextIgnore(ctx); err == nil {
		rootPatterns = append(rootPatterns, globalPatterns...)
	}

	nodes[""] = rootPatterns

	return IgnoreRules{Nodes: nodes}, nil
}

// PathIgnored reports whether relativePath is excluded by the ignore rule
// tree. The first return is the verdict. The second return is the matched
// pattern (raw, including any leading '!') when excluded. The third return
// is the source label (the .gitignore file's path within the codebase, the
// global ignore path, or one of the built-in/override labels).
//
// Evaluation walks rules from the root node down through every ancestor
// directory of the file, applying last-match-wins inside each node. Git's
// directory-exclusion rule applies: a negation pattern on a file cannot
// re-include the file when any ancestor directory was excluded by a
// non-negation match.
func PathIgnored(relativePath string, rules IgnoreRules) (bool, string, string) {
	normalized := strings.Trim(strings.ReplaceAll(relativePath, "\\", "/"), "/")
	if normalized == "" || rules.IsEmpty() {
		return false, "", ""
	}
	parts := strings.Split(normalized, "/")

	var lastExcluded bool
	var lastPattern, lastSource string
	var ancestorExcluded bool
	var ancestorPattern, ancestorSource string

	visit := func(scopeDir string, entries []ignorePattern) {
		pathInScope := normalized
		if scopeDir != "" {
			pathInScope = strings.TrimPrefix(normalized, scopeDir+"/")
		}
		if pathInScope == "" || (pathInScope == normalized && scopeDir != "") {
			return
		}
		for _, entry := range entries {
			cleaned := entry.Pattern
			if entry.Negation {
				cleaned = strings.TrimPrefix(cleaned, "!")
			}
			if cleaned == "" {
				continue
			}
			if patternMatchesFile(pathInScope, cleaned) {
				lastExcluded = !entry.Negation
				lastPattern = entry.Pattern
				lastSource = entry.Source
				continue
			}
			if !entry.Negation && patternMatchesAncestor(pathInScope, cleaned) {
				ancestorExcluded = true
				ancestorPattern = entry.Pattern
				ancestorSource = entry.Source
			}
		}
	}

	visit("", rules.Nodes[""])
	for index := range parts[:len(parts)-1] {
		dirRel := strings.Join(parts[:index+1], "/")
		entries, found := rules.Nodes[dirRel]
		if !found {
			continue
		}
		visit(dirRel, entries)
	}

	if lastExcluded {
		return true, lastPattern, lastSource
	}
	if ancestorExcluded {
		return true, ancestorPattern, ancestorSource
	}
	return false, "", ""
}

func walkFiles(ctx context.Context, root string, current string, rules IgnoreRules, files *[]string) error {
	if err := ctx.Err(); err != nil {
		slog.ErrorContext(ctx, "walk cancelled", "path", current, "err", err)
		return fmt.Errorf("walk cancelled at %s: %w", current, err)
	}

	entries, err := os.ReadDir(current)
	if err != nil {
		slog.ErrorContext(ctx, "read directory failed", "path", current, "err", err)
		return fmt.Errorf("read directory %s: %w", current, err)
	}

	for _, entry := range entries {
		fullPath := filepath.Join(current, entry.Name())
		relativePath, err := filepath.Rel(root, fullPath)
		if err != nil {
			slog.ErrorContext(ctx, "compute relative path failed", "root", root, "path", fullPath, "err", err)
			return fmt.Errorf("compute relative path for %s: %w", fullPath, err)
		}
		if excluded, _, _ := PathIgnored(relativePath, rules); excluded {
			continue
		}

		if entry.IsDir() {
			if err := walkFiles(ctx, root, fullPath, rules, files); err != nil {
				return err
			}
			continue
		}

		*files = append(*files, fullPath)
	}
	return nil
}

// walkGitignore reads every .gitignore file under root (recursively) and
// records its patterns in the per-directory node table.
func walkGitignore(ctx context.Context, root string, relativeDir string, nodes map[string][]ignorePattern) error {
	currentDir := filepath.Join(root, relativeDir)
	entries, err := os.ReadDir(currentDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		slog.ErrorContext(ctx, "read directory for ignore walk failed", "path", currentDir, "err", err)
		return fmt.Errorf("read directory %s: %w", currentDir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			// Cheap default-pruning so the gitignore walk does not
			// recurse into vendor/, node_modules/, .git/, etc. The
			// PathIgnored evaluator still has the authoritative tree;
			// this is only a discovery-time speedup.
			if isDefaultExcludedDir(name) {
				continue
			}
			nextRelative := name
			if relativeDir != "" {
				nextRelative = filepath.ToSlash(filepath.Join(relativeDir, name))
			}
			if err := walkGitignore(ctx, root, nextRelative, nodes); err != nil {
				return err
			}
			continue
		}
		if !strings.HasPrefix(name, ".") || !strings.HasSuffix(name, "ignore") {
			continue
		}
		full := filepath.Join(currentDir, name)
		raw, readErr := readIgnoreFile(ctx, full)
		if readErr != nil {
			return readErr
		}
		source := name
		if relativeDir != "" {
			source = filepath.ToSlash(filepath.Join(relativeDir, name))
		}
		parsed := make([]ignorePattern, 0, len(raw))
		for _, pattern := range raw {
			parsed = append(parsed, parsePattern(pattern, source))
		}
		nodes[relativeDir] = append(nodes[relativeDir], parsed...)
	}
	return nil
}

func loadGlobalContextIgnore(ctx context.Context) ([]ignorePattern, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		slog.ErrorContext(ctx, "resolve user home directory failed", "err", err)
		return nil, fmt.Errorf("resolve user home directory: %w", err)
	}
	globalPath := filepath.Join(homeDir, ".context", ".contextignore")
	if _, statErr := os.Stat(globalPath); statErr != nil {
		if !os.IsNotExist(statErr) {
			slog.WarnContext(ctx, "stat global contextignore failed; ignoring", "path", globalPath, "err", statErr)
		}
		return nil, nil
	}
	raw, readErr := readIgnoreFile(ctx, globalPath)
	if readErr != nil {
		return nil, readErr
	}
	parsed := make([]ignorePattern, 0, len(raw))
	for _, pattern := range raw {
		parsed = append(parsed, parsePattern(pattern, patternSourceGlobal))
	}
	return parsed, nil
}

func readIgnoreFile(ctx context.Context, path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		slog.ErrorContext(ctx, "read ignore file failed", "path", path, "err", err)
		return nil, fmt.Errorf("read ignore file %s: %w", path, err)
	}

	lines := strings.Split(string(data), "\n")
	patterns := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		patterns = append(patterns, trimmed)
	}
	return patterns, nil
}

func parsePattern(raw string, source string) ignorePattern {
	negation := strings.HasPrefix(raw, "!")
	return ignorePattern{Pattern: raw, Negation: negation, Source: source}
}

// patternMatchesFile reports whether pattern matches the file at
// pathInScope as a direct match (not an ancestor-directory match). A
// directory pattern (one ending in "/") never matches a file directly; it
// only matches the file's ancestor directories, so this returns false for
// directory patterns.
func patternMatchesFile(pathInScope string, pattern string) bool {
	normalizedPattern := strings.ReplaceAll(pattern, "\\", "/")
	if strings.HasSuffix(normalizedPattern, "/") {
		return false
	}
	cleanPattern := strings.Trim(normalizedPattern, "/")
	cleanPath := strings.Trim(strings.ReplaceAll(pathInScope, "\\", "/"), "/")
	if cleanPath == "" || cleanPattern == "" {
		return false
	}
	if strings.HasPrefix(normalizedPattern, "/") || strings.Contains(cleanPattern, "/") {
		return simpleGlobMatch(cleanPath, cleanPattern)
	}
	return simpleGlobMatch(filepath.Base(cleanPath), cleanPattern)
}

// patternMatchesAncestor reports whether pattern matches any ancestor
// directory of pathInScope. Directory patterns and base-name patterns both
// route through this helper; the equality decides whether an ancestor was
// excluded for Git's directory-exclusion rule.
func patternMatchesAncestor(pathInScope string, pattern string) bool {
	normalizedPattern := strings.ReplaceAll(pattern, "\\", "/")
	cleanPattern := strings.Trim(normalizedPattern, "/")
	cleanPath := strings.Trim(strings.ReplaceAll(pathInScope, "\\", "/"), "/")
	if cleanPath == "" || cleanPattern == "" {
		return false
	}
	parts := strings.Split(cleanPath, "/")
	if len(parts) <= 1 {
		return false
	}
	containsSlash := strings.Contains(cleanPattern, "/")
	isRootAnchored := strings.HasPrefix(normalizedPattern, "/")
	for index := 1; index < len(parts); index++ {
		ancestorPath := strings.Join(parts[:index], "/")
		if isRootAnchored || containsSlash {
			if simpleGlobMatch(ancestorPath, cleanPattern) {
				return true
			}
			continue
		}
		if simpleGlobMatch(parts[index-1], cleanPattern) {
			return true
		}
	}
	return false
}

// defaultExcludedDirs lets the gitignore walker skip well-known dependency
// and tooling directories without consulting the full ignore rules. The
// PathIgnored evaluator still has the authoritative tree; this is only a
// discovery-time speedup so a 50k-file node_modules does not blow up the
// recursive read.
var defaultExcludedDirs = map[string]struct{}{
	".git":          {},
	".hg":           {},
	".svn":          {},
	"node_modules":  {},
	"vendor":        {},
	"dist":          {},
	"build":         {},
	"out":           {},
	"target":        {},
	".cache":        {},
	".next":         {},
	".nuxt":         {},
	".turbo":        {},
	".parcel-cache": {},
	".pnpm-store":   {},
	".yarn":         {},
	".gradle":       {},
	".terraform":    {},
	".direnv":       {},
	"__pycache__":   {},
	".pytest_cache": {},
	".mypy_cache":   {},
	".ruff_cache":   {},
	".tox":          {},
	".venv":         {},
	"venv":          {},
}

func isDefaultExcludedDir(name string) bool {
	_, found := defaultExcludedDirs[name]
	return found
}

func simpleGlobMatch(text string, pattern string) bool {
	quoted := strings.NewReplacer(
		".", "\\.",
		"+", "\\+",
		"^", "\\^",
		"$", "\\$",
		"(", "\\(",
		")", "\\)",
		"[", "\\[",
		"]", "\\]",
		"{", "\\{",
		"}", "\\}",
		"|", "\\|",
	).Replace(pattern)
	regexPattern := "^" + strings.ReplaceAll(quoted, "*", ".*") + "$"
	matched, _ := filepath.Match(regexPattern, text)
	if matched {
		return true
	}
	return wildcardMatch(text, pattern)
}

func wildcardMatch(text string, pattern string) bool {
	textIndex := 0
	patternIndex := 0
	starIndex := -1
	matchIndex := 0

	for textIndex < len(text) {
		if patternIndex < len(pattern) && (pattern[patternIndex] == text[textIndex] || pattern[patternIndex] == '*') {
			if pattern[patternIndex] == '*' {
				starIndex = patternIndex
				matchIndex = textIndex
				patternIndex++
				continue
			}
			textIndex++
			patternIndex++
			continue
		}
		if starIndex != -1 {
			patternIndex = starIndex + 1
			matchIndex++
			textIndex = matchIndex
			continue
		}
		return false
	}

	for patternIndex < len(pattern) && pattern[patternIndex] == '*' {
		patternIndex++
	}

	return patternIndex == len(pattern)
}

func normalizeExtensions(extensions []string) []string {
	result := make([]string, 0, len(extensions))
	for _, extension := range extensions {
		trimmed := strings.TrimSpace(extension)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(trimmed, ".") {
			trimmed = "." + trimmed
		}
		result = append(result, trimmed)
	}
	return dedupStrings(result)
}

func dedupStrings(values []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, found := seen[value]; found {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
