// Package config resolves daemon runtime paths and settings.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

const (
	defaultStateDirName = ".contextd"
	defaultSocketName   = "claude-contextd.sock"
	defaultLogFileName  = "claude-contextd.log"
)

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
}

// Default returns the daemon configuration derived from the local environment.
func Default() (Config, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		slog.Error("resolve user home directory failed", "err", err)
		return Config{}, fmt.Errorf("resolve user home directory: %w", err)
	}

	stateRoot := filepath.Join(homeDir, defaultStateDirName)
	stateRoot = envOrDefault("CLAUDE_CONTEXTD_STATE_ROOT", stateRoot)
	socketsDir := filepath.Join(stateRoot, "sockets")
	logsDir := filepath.Join(stateRoot, "logs")

	socketPath := envOrDefault("CLAUDE_CONTEXTD_SOCKET_PATH", filepath.Join(socketsDir, defaultSocketName))
	logPath := envOrDefault("CLAUDE_CONTEXTD_LOG_PATH", filepath.Join(logsDir, defaultLogFileName))

	return Config{
		StateRoot:    stateRoot,
		SocketPath:   socketPath,
		RegistryPath: filepath.Join(stateRoot, "registry.json"),
		JobsPath:     filepath.Join(stateRoot, "jobs.jsonl"),
		EventsPath:   filepath.Join(stateRoot, "events.jsonl"),
		LogsDir:      logsDir,
		LogPath:      logPath,
		MerkleDir:    filepath.Join(stateRoot, "merkle"),
		LocksDir:     filepath.Join(stateRoot, "locks"),
		SocketsDir:   socketsDir,
	}, nil
}

func envOrDefault(name string, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}
