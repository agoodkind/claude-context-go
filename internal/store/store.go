// Package store persists daemon state to local JSON and JSONL files.
package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/zilliztech/claude-context-go/internal/model"
)

// EnsureDir creates a directory tree when it is missing.
func EnsureDir(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		slog.Error("create directory failed", "path", path, "err", err)
		return fmt.Errorf("create directory %s: %w", path, err)
	}
	return nil
}

// ReadRegistry reads the persisted codebase registry file.
func ReadRegistry(path string) (model.RegistryFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		slog.Error("read registry file failed", "path", path, "err", err)
		return model.RegistryFile{}, fmt.Errorf("read registry file %s: %w", path, err)
	}

	var registry model.RegistryFile
	if err := json.Unmarshal(data, &registry); err != nil {
		slog.Error("unmarshal registry file failed", "path", path, "err", err)
		return model.RegistryFile{}, fmt.Errorf("unmarshal registry file %s: %w", path, err)
	}
	return registry, nil
}

// WriteRegistry atomically replaces the persisted codebase registry file.
func WriteRegistry(path string, registry model.RegistryFile) error {
	slog.Info("write registry", "path", path, "codebases", len(registry.Codebases))

	data, err := json.MarshalIndent(registry, "", "  ")
	if err != nil {
		slog.Error("marshal registry file failed", "path", path, "err", err)
		return fmt.Errorf("marshal registry file %s: %w", path, err)
	}

	if err := EnsureDir(filepath.Dir(path)); err != nil {
		return err
	}

	tempFile, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		slog.Error("create temp registry file failed", "dir", filepath.Dir(path), "err", err)
		return fmt.Errorf("create temp registry file in %s: %w", filepath.Dir(path), err)
	}
	tempPath := tempFile.Name()

	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		slog.Error("write temp registry file failed", "path", tempPath, "err", err)
		return fmt.Errorf("write temp registry file %s: %w", tempPath, err)
	}
	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		slog.Error("close temp registry file failed", "path", tempPath, "err", err)
		return fmt.Errorf("close temp registry file %s: %w", tempPath, err)
	}
	if err := os.Chmod(tempPath, 0o644); err != nil {
		os.Remove(tempPath)
		slog.Error("chmod temp registry file failed", "path", tempPath, "err", err)
		return fmt.Errorf("chmod temp registry file %s: %w", tempPath, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		slog.Error("rename temp registry file failed", "from", tempPath, "to", path, "err", err)
		return fmt.Errorf("rename temp registry file %s to %s: %w", tempPath, path, err)
	}
	return nil
}

// AppendJobEvent appends one job event to the JSONL journal.
func AppendJobEvent(path string, event model.JobEvent) error {
	if err := EnsureDir(filepath.Dir(path)); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		slog.Error("open jobs journal failed", "path", path, "err", err)
		return fmt.Errorf("open jobs journal %s: %w", path, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(event); err != nil {
		slog.Error("append jobs journal failed", "path", path, "err", err)
		return fmt.Errorf("append jobs journal %s: %w", path, err)
	}
	return nil
}

// ReadJobEvents replays the JSONL journal into a latest-by-id map.
func ReadJobEvents(path string) (map[string]model.Job, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]model.Job{}, nil
		}
		slog.Error("open jobs journal failed", "path", path, "err", err)
		return nil, fmt.Errorf("open jobs journal %s: %w", path, err)
	}
	defer file.Close()

	jobs := map[string]model.Job{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event model.JobEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			slog.Error("unmarshal jobs journal line failed", "path", path, "err", err)
			return nil, fmt.Errorf("unmarshal jobs journal line in %s: %w", path, err)
		}
		jobs[event.Job.ID] = event.Job
	}
	if err := scanner.Err(); err != nil {
		slog.Error("scan jobs journal failed", "path", path, "err", err)
		return nil, fmt.Errorf("scan jobs journal %s: %w", path, err)
	}
	return jobs, nil
}
