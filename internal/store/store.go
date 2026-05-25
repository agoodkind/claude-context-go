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

	"goodkind.io/claude-context-go/internal/model"
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

// ReadChunks reads one persisted codebase chunk file.
func ReadChunks(path string) ([]model.StoredChunk, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		slog.Error("read chunk file failed", "path", path, "err", err)
		return nil, fmt.Errorf("read chunk file %s: %w", path, err)
	}

	var chunks []model.StoredChunk
	if err := json.Unmarshal(data, &chunks); err != nil {
		slog.Error("unmarshal chunk file failed", "path", path, "err", err)
		return nil, fmt.Errorf("unmarshal chunk file %s: %w", path, err)
	}
	return chunks, nil
}

// WriteChunks atomically replaces one persisted codebase chunk file.
func WriteChunks(path string, chunks []model.StoredChunk) error {
	slog.Info("write chunk file", "path", path, "chunks", len(chunks))

	data, err := json.MarshalIndent(chunks, "", "  ")
	if err != nil {
		slog.Error("marshal chunk file failed", "path", path, "err", err)
		return fmt.Errorf("marshal chunk file %s: %w", path, err)
	}
	if err := EnsureDir(filepath.Dir(path)); err != nil {
		return err
	}

	tempFile, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		slog.Error("create temp chunk file failed", "dir", filepath.Dir(path), "err", err)
		return fmt.Errorf("create temp chunk file in %s: %w", filepath.Dir(path), err)
	}
	tempPath := tempFile.Name()
	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		slog.Error("write temp chunk file failed", "path", tempPath, "err", err)
		return fmt.Errorf("write temp chunk file %s: %w", tempPath, err)
	}
	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		slog.Error("close temp chunk file failed", "path", tempPath, "err", err)
		return fmt.Errorf("close temp chunk file %s: %w", tempPath, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		slog.Error("rename temp chunk file failed", "from", tempPath, "to", path, "err", err)
		return fmt.Errorf("rename temp chunk file %s to %s: %w", tempPath, path, err)
	}
	return nil
}

// RemoveFile deletes one persisted daemon file when it exists.
func RemoveFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Error("remove persisted file failed", "path", path, "err", err)
		return fmt.Errorf("remove persisted file %s: %w", path, err)
	}
	return nil
}
