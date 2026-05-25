package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestParseCommaSeparated(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		input  string
		expect []string
	}{
		{"empty", "", nil},
		{"whitespace only", "  \t  ", nil},
		{"single", "alpha", []string{"alpha"}},
		{"multi", "alpha,bravo,charlie", []string{"alpha", "bravo", "charlie"}},
		{"trimmed", "  alpha , bravo ,charlie  ", []string{"alpha", "bravo", "charlie"}},
		{"skips blanks", "alpha,,bravo,", []string{"alpha", "bravo"}},
	}
	for _, tc := range cases {
		got := parseCommaSeparated(tc.input)
		if !reflect.DeepEqual(got, tc.expect) {
			t.Errorf("parseCommaSeparated(%q) = %#v want %#v", tc.input, got, tc.expect)
		}
	}
}

func TestLoadContextEnvFileSetsMissingKeys(t *testing.T) {
	tempDir := t.TempDir()
	envPath := filepath.Join(tempDir, ".env")
	contents := `
# comment
EMBEDDING_PROVIDER=OpenAI
EMBEDDING_MODEL="text-embedding-3-small"
EMPTYLINE=

OPENAI_API_KEY='sk-fromfile'
`
	if err := os.WriteFile(envPath, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	t.Setenv("OPENAI_API_KEY", "sk-from-process")
	t.Setenv("EMBEDDING_PROVIDER", "")
	if err := os.Unsetenv("EMBEDDING_PROVIDER"); err != nil {
		t.Fatalf("Unsetenv returned error: %v", err)
	}
	if err := os.Unsetenv("EMBEDDING_MODEL"); err != nil {
		t.Fatalf("Unsetenv returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Unsetenv("EMBEDDING_PROVIDER")
		_ = os.Unsetenv("EMBEDDING_MODEL")
		_ = os.Unsetenv("EMPTYLINE")
	})

	loadContextEnvFile(envPath)

	if got := os.Getenv("EMBEDDING_PROVIDER"); got != "OpenAI" {
		t.Errorf("EMBEDDING_PROVIDER = %q want OpenAI", got)
	}
	if got := os.Getenv("EMBEDDING_MODEL"); got != "text-embedding-3-small" {
		t.Errorf("EMBEDDING_MODEL = %q want text-embedding-3-small (quotes stripped)", got)
	}
	if got := os.Getenv("OPENAI_API_KEY"); got != "sk-from-process" {
		t.Errorf("OPENAI_API_KEY = %q want sk-from-process (process env wins over .env file)", got)
	}
}

func TestDefaultReadsCustomExtensionAndIgnoreEnvVars(t *testing.T) {
	tempState := t.TempDir()
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)
	t.Setenv("CLAUDE_CONTEXTD_STATE_ROOT", tempState)
	t.Setenv("CUSTOM_EXTENSIONS", ".toml,.yaml")
	t.Setenv("CUSTOM_IGNORE_PATTERNS", "vendor/**, third_party/**")

	cfg, err := Default()
	if err != nil {
		t.Fatalf("Default returned error: %v", err)
	}
	if !reflect.DeepEqual(cfg.CustomExtensions, []string{".toml", ".yaml"}) {
		t.Errorf("CustomExtensions = %#v", cfg.CustomExtensions)
	}
	if !reflect.DeepEqual(cfg.CustomIgnorePatterns, []string{"vendor/**", "third_party/**"}) {
		t.Errorf("CustomIgnorePatterns = %#v", cfg.CustomIgnorePatterns)
	}
}
