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

var defaultSupportedExtensions = []string{
	".ts", ".tsx", ".js", ".jsx", ".py", ".java", ".cpp", ".c", ".h", ".hpp",
	".cs", ".go", ".rs", ".php", ".rb", ".swift", ".kt", ".scala", ".m", ".mm",
	".dart", ".sol", ".md", ".markdown", ".ipynb",
}

var defaultIgnorePatterns = []string{
	"node_modules/**",
	"dist/**",
	"build/**",
	"out/**",
	"target/**",
	"coverage/**",
	".nyc_output/**",
	".vscode/**",
	".idea/**",
	"*.swp",
	"*.swo",
	".git/**",
	".svn/**",
	".hg/**",
	".cache/**",
	"__pycache__/**",
	".pytest_cache/**",
	"logs/**",
	"tmp/**",
	"temp/**",
	"*.log",
	".env",
	".env.*",
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
	"node_modules",
	".git",
	".svn",
	".hg",
	"build",
	"dist",
	"out",
	"target",
	".vscode",
	".idea",
	"__pycache__",
	".pytest_cache",
	"coverage",
	".nyc_output",
	"logs",
	"tmp",
	"temp",
}

// Result is one discovery pass over a codebase root.
type Result struct {
	Files          []string
	IgnorePatterns []string
	Extensions     []string
}

// Discover applies the current ignore and extension rules to one codebase root.
func Discover(ctx context.Context, root string, additionalIgnorePatterns []string, additionalExtensions []string) (Result, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		slog.ErrorContext(ctx, "resolve absolute root failed", "root", root, "err", err)
		return Result{}, fmt.Errorf("resolve absolute root %s: %w", root, err)
	}

	effectiveIgnorePatterns, err := loadIgnorePatterns(ctx, absoluteRoot, additionalIgnorePatterns)
	if err != nil {
		return Result{}, err
	}
	effectiveExtensions := normalizeExtensions(additionalExtensions)
	effectiveExtensions = append(dedupStrings(defaultSupportedExtensions), effectiveExtensions...)
	effectiveExtensions = dedupStrings(effectiveExtensions)

	files := []string{}
	if err := walkFiles(ctx, absoluteRoot, absoluteRoot, effectiveIgnorePatterns, effectiveExtensions, &files); err != nil {
		return Result{}, err
	}
	slices.Sort(files)

	return Result{
		Files:          files,
		IgnorePatterns: effectiveIgnorePatterns,
		Extensions:     effectiveExtensions,
	}, nil
}

func walkFiles(ctx context.Context, root string, current string, ignorePatterns []string, extensions []string, files *[]string) error {
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
		if shouldIgnore(relativePath, ignorePatterns) {
			continue
		}

		if entry.IsDir() {
			if err := walkFiles(ctx, root, fullPath, ignorePatterns, extensions, files); err != nil {
				return err
			}
			continue
		}

		extension := filepath.Ext(entry.Name())
		if slices.Contains(extensions, extension) {
			*files = append(*files, fullPath)
		}
	}
	return nil
}

func loadIgnorePatterns(ctx context.Context, root string, additionalIgnorePatterns []string) ([]string, error) {
	ignorePatterns := append([]string{}, defaultIgnorePatterns...)
	ignorePatterns = append(ignorePatterns, additionalIgnorePatterns...)

	ignoreFiles, err := os.ReadDir(root)
	if err != nil {
		slog.ErrorContext(ctx, "read root directory for ignore files failed", "root", root, "err", err)
		return nil, fmt.Errorf("read root directory %s: %w", root, err)
	}
	for _, entry := range ignoreFiles {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, ".") || !strings.HasSuffix(name, "ignore") {
			continue
		}
		patterns, err := readIgnoreFile(ctx, filepath.Join(root, name))
		if err != nil {
			return nil, err
		}
		ignorePatterns = append(ignorePatterns, patterns...)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		slog.ErrorContext(ctx, "resolve user home directory failed", "err", err)
		return nil, fmt.Errorf("resolve user home directory: %w", err)
	}
	globalIgnorePath := filepath.Join(homeDir, ".context", ".contextignore")
	if _, err := os.Stat(globalIgnorePath); err == nil {
		patterns, readErr := readIgnoreFile(ctx, globalIgnorePath)
		if readErr != nil {
			return nil, readErr
		}
		ignorePatterns = append(ignorePatterns, patterns...)
	}

	return dedupStrings(ignorePatterns), nil
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

func shouldIgnore(relativePath string, ignorePatterns []string) bool {
	for part := range strings.SplitSeq(relativePath, string(filepath.Separator)) {
		if strings.HasPrefix(part, ".") {
			return true
		}
	}

	normalizedPath := strings.Trim(strings.ReplaceAll(relativePath, "\\", "/"), "/")
	if normalizedPath == "" {
		return false
	}
	for _, pattern := range ignorePatterns {
		if patternMatch(normalizedPath, pattern) {
			return true
		}
	}
	return false
}

func patternMatch(path string, pattern string) bool {
	cleanPath := strings.Trim(strings.ReplaceAll(path, "\\", "/"), "/")
	normalizedPattern := strings.ReplaceAll(pattern, "\\", "/")
	cleanPattern := strings.Trim(normalizedPattern, "/")
	isRootAnchored := strings.HasPrefix(normalizedPattern, "/")
	isDirectoryPattern := strings.HasSuffix(normalizedPattern, "/")

	if cleanPath == "" || cleanPattern == "" {
		return false
	}

	if isDirectoryPattern {
		if isRootAnchored {
			return simpleGlobMatch(cleanPath, cleanPattern) || strings.HasPrefix(cleanPath, cleanPattern+"/")
		}
		return matchesDirectoryPattern(cleanPath, cleanPattern)
	}

	if isRootAnchored || strings.Contains(cleanPattern, "/") {
		return simpleGlobMatch(cleanPath, cleanPattern)
	}

	return simpleGlobMatch(filepath.Base(cleanPath), cleanPattern)
}

func matchesDirectoryPattern(path string, pattern string) bool {
	pathParts := strings.Split(path, "/")
	dirPartCount := len(strings.Split(pattern, "/"))
	for i := 0; i <= len(pathParts)-dirPartCount; i++ {
		candidate := strings.Join(pathParts[i:i+dirPartCount], "/")
		if simpleGlobMatch(candidate, pattern) {
			return true
		}
	}
	return false
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
