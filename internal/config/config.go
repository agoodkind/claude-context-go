// Package config resolves daemon runtime paths and settings.
package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	defaultStateDirName = ".contextd"
	defaultSocketName   = "claude-contextd.sock"
	defaultLogFileName  = "claude-contextd.log"
	defaultSyncInterval = 300000
	defaultSyncLockAge  = 600000
)

type embeddingProvider string

const embeddingProviderOpenAI embeddingProvider = "OpenAI"

// Config describes daemon runtime paths on the local machine.
type Config struct {
	StateRoot    string
	SocketPath   string
	RegistryPath string
	JobsPath     string
	EventsPath   string
	LogsDir      string
	LogPath      string
	MerkleDir    string
	LocksDir     string
	SocketsDir   string
	ChunksDir    string
	ContextRoot  string

	EmbeddingProvider      string
	EmbeddingModel         string
	EmbeddingBatchSize     int
	EmbeddingDimension     int32
	OpenAIAPIKey           string
	OpenAIBaseURL          string
	CustomExtensions       []string
	CustomIgnorePatterns   []string
	MilvusAddress          string
	MilvusToken            string
	CollectionNameOverride string
	HybridMode             bool
	BackgroundSyncEnabled  bool
	SyncIntervalMS         int
	TriggerWatcherEnabled  bool
	SyncLockStaleMS        int
}

type persistedConfig struct {
	EmbeddingProvider      string `json:"embeddingProvider"`
	EmbeddingModel         string `json:"embeddingModel"`
	EmbeddingBatchSize     int    `json:"embeddingBatchSize"`
	EmbeddingDimension     int32  `json:"embeddingDimension"`
	OpenAIAPIKey           string `json:"openaiApiKey"`
	OpenAIBaseURL          string `json:"openaiBaseUrl"`
	MilvusAddress          string `json:"milvusAddress"`
	MilvusToken            string `json:"milvusToken"`
	CollectionNameOverride string `json:"collectionNameOverride"`
	HybridMode             *bool  `json:"hybridMode"`
}

// Default returns the daemon configuration derived from the local environment.
func Default() (Config, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		slog.Error("resolve user home directory failed", "err", err)
		return Config{}, fmt.Errorf("resolve user home directory: %w", err)
	}

	loadContextEnvFile(filepath.Join(homeDir, ".context", ".env"))

	stateRoot := filepath.Join(homeDir, defaultStateDirName)
	stateRoot = envOrDefault("CLAUDE_CONTEXTD_STATE_ROOT", stateRoot)
	socketsDir := filepath.Join(stateRoot, "sockets")
	logsDir := filepath.Join(stateRoot, "logs")
	contextRoot := filepath.Join(homeDir, ".context")

	socketPath := envOrDefault("CLAUDE_CONTEXTD_SOCKET_PATH", filepath.Join(socketsDir, defaultSocketName))
	logPath := envOrDefault("CLAUDE_CONTEXTD_LOG_PATH", filepath.Join(logsDir, defaultLogFileName))

	fileConfig := readPersistedConfig(filepath.Join(stateRoot, "config.json"))

	defaultProvider := envOrDefault("EMBEDDING_PROVIDER", string(embeddingProviderOpenAI))
	if defaultProvider == string(embeddingProviderOpenAI) && fileConfig.EmbeddingProvider != "" {
		defaultProvider = fileConfig.EmbeddingProvider
	}

	defaultModel := fileConfig.EmbeddingModel
	if defaultModel == "" {
		defaultModel = envOrDefault("EMBEDDING_MODEL", "text-embedding-3-small")
	}

	return Config{
		StateRoot:              stateRoot,
		SocketPath:             socketPath,
		RegistryPath:           filepath.Join(stateRoot, "registry.json"),
		JobsPath:               filepath.Join(stateRoot, "jobs.jsonl"),
		EventsPath:             filepath.Join(stateRoot, "events.jsonl"),
		LogsDir:                logsDir,
		LogPath:                logPath,
		MerkleDir:              filepath.Join(stateRoot, "merkle"),
		LocksDir:               filepath.Join(stateRoot, "locks"),
		SocketsDir:             socketsDir,
		ChunksDir:              filepath.Join(stateRoot, "chunks"),
		ContextRoot:            contextRoot,
		EmbeddingProvider:      envOrDefault("EMBEDDING_PROVIDER", defaultProvider),
		EmbeddingModel:         envOrDefault("EMBEDDING_MODEL", defaultModel),
		EmbeddingBatchSize:     envIntOrDefault("EMBEDDING_BATCH_SIZE", intOrDefault(fileConfig.EmbeddingBatchSize, 32)),
		EmbeddingDimension:     envInt32OrDefault("EMBEDDING_DIMENSION", fileConfig.EmbeddingDimension),
		OpenAIAPIKey:           envOrDefault("OPENAI_API_KEY", fileConfig.OpenAIAPIKey),
		OpenAIBaseURL:          envOrDefault("OPENAI_BASE_URL", fileConfig.OpenAIBaseURL),
		CustomExtensions:       parseCommaSeparated(os.Getenv("CUSTOM_EXTENSIONS")),
		CustomIgnorePatterns:   parseCommaSeparated(os.Getenv("CUSTOM_IGNORE_PATTERNS")),
		MilvusAddress:          envOrDefault("MILVUS_ADDRESS", fileConfig.MilvusAddress),
		MilvusToken:            envOrDefault("MILVUS_TOKEN", fileConfig.MilvusToken),
		CollectionNameOverride: envOrDefault("CODE_CHUNKS_COLLECTION_NAME_OVERRIDE", fileConfig.CollectionNameOverride),
		HybridMode:             envBoolOrDefault("HYBRID_MODE", boolOrDefault(fileConfig.HybridMode, true)),
		BackgroundSyncEnabled:  envBoolOrDefault("CLAUDE_CONTEXT_BACKGROUND_SYNC", true),
		SyncIntervalMS:         envIntOrDefault("CLAUDE_CONTEXT_SYNC_INTERVAL_MS", defaultSyncInterval),
		TriggerWatcherEnabled:  envBoolOrDefault("CLAUDE_CONTEXT_TRIGGER_WATCHER", true),
		SyncLockStaleMS:        envIntOrDefault("CLAUDE_CONTEXT_SYNC_LOCK_STALE_MS", defaultSyncLockAge),
	}, nil
}

func envOrDefault(name string, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

func envIntOrDefault(name string, fallback int) int {
	rawValue := os.Getenv(name)
	if rawValue == "" {
		return fallback
	}

	parsedValue, err := strconv.Atoi(rawValue)
	if err != nil {
		return fallback
	}
	return parsedValue
}

func envInt32OrDefault(name string, fallback int32) int32 {
	rawValue := os.Getenv(name)
	if rawValue == "" {
		return fallback
	}

	parsedValue, err := strconv.ParseInt(rawValue, 10, 32)
	if err != nil {
		return fallback
	}
	return int32(parsedValue)
}

func envBoolOrDefault(name string, fallback bool) bool {
	rawValue := os.Getenv(name)
	if rawValue == "" {
		return fallback
	}

	parsedValue, err := strconv.ParseBool(rawValue)
	if err != nil {
		return fallback
	}
	return parsedValue
}

func readPersistedConfig(path string) persistedConfig {
	data, err := os.ReadFile(path)
	if err != nil {
		var cfg persistedConfig
		return cfg
	}

	var cfg persistedConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		slog.Error("read persisted daemon config failed", "path", path, "err", err)
		var emptyConfig persistedConfig
		return emptyConfig
	}
	return cfg
}

func intOrDefault(value int, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

func boolOrDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

// loadContextEnvFile loads KEY=VALUE pairs from ~/.context/.env (or any path
// supplied by the caller). It only sets keys that are not already present in
// the process environment so explicit env-var overrides win. Lines starting
// with '#' and blank lines are ignored.
func loadContextEnvFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		equalsIndex := strings.IndexByte(trimmed, '=')
		if equalsIndex <= 0 {
			continue
		}
		key := strings.TrimSpace(trimmed[:equalsIndex])
		value := strings.TrimSpace(trimmed[equalsIndex+1:])
		if key == "" {
			continue
		}
		if (strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`)) ||
			(strings.HasPrefix(value, `'`) && strings.HasSuffix(value, `'`)) {
			value = value[1 : len(value)-1]
		}
		if _, alreadySet := os.LookupEnv(key); alreadySet {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			slog.Error("set env from .context/.env failed", "key", key, "err", err)
		}
	}
}

// parseCommaSeparated returns a trimmed, non-empty list from a comma-separated
// string. Returns nil for empty input so the field cleanly distinguishes
// "unset" from "explicit empty list".
func parseCommaSeparated(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
