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

	"goodkind.io/lm-semantic-search/internal/model"
)

// syncFile stays replaceable so tests can verify that file data and its
// directory entry reach durable storage in the required order.
var syncFile = (*os.File).Sync

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

	return replaceFileAtomically(path, "registry file", func(file *os.File) error {
		if _, writeErr := file.Write(data); writeErr != nil {
			return fmt.Errorf("write registry bytes: %w", writeErr)
		}
		return nil
	})
}

// replaceFileAtomically is the single home for durably replacing a whole file.
// It writes a temporary file beside the target, flushes both that file and the
// directory entry, then renames, so a crash leaves either the old contents or
// the new ones and never a truncated file. description names the file in errors
// and logs, so each caller reads naturally without forking this logic.
func replaceFileAtomically(
	path string,
	description string,
	writeContents func(file *os.File) error,
) error {
	if err := EnsureDir(filepath.Dir(path)); err != nil {
		return err
	}

	tempFile, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		slog.Error(
			"create temp file failed",
			"component", "store",
			"description", description,
			"dir", filepath.Dir(path),
			"err", err,
		)
		return fmt.Errorf("create temp %s in %s: %w", description, filepath.Dir(path), err)
	}
	tempPath := tempFile.Name()

	if err := writeTempFileContents(tempFile, tempPath, description, writeContents); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		slog.Error(
			"rename temp file failed",
			"component", "store",
			"description", description,
			"from", tempPath,
			"to", path,
			"err", err,
		)
		return fmt.Errorf("rename temp %s %s to %s: %w", description, tempPath, path, err)
	}
	return syncFileDirectory(filepath.Dir(path), description)
}

// writeTempFileContents fills the temporary file and makes it durable before
// the rename can unlink whatever the target already held.
func writeTempFileContents(
	tempFile *os.File,
	tempPath string,
	description string,
	writeContents func(file *os.File) error,
) error {
	failTemp := func(stage string, err error) error {
		tempFile.Close()
		os.Remove(tempPath)
		slog.Error(
			stage+" temp file failed",
			"component", "store",
			"description", description,
			"path", tempPath,
			"err", err,
		)
		return fmt.Errorf("%s temp %s %s: %w", stage, description, tempPath, err)
	}

	if err := writeContents(tempFile); err != nil {
		return failTemp("write", err)
	}
	if err := syncFile(tempFile); err != nil {
		return failTemp("sync", err)
	}
	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		slog.Error(
			"close temp file failed",
			"component", "store",
			"description", description,
			"path", tempPath,
			"err", err,
		)
		return fmt.Errorf("close temp %s %s: %w", description, tempPath, err)
	}
	if err := os.Chmod(tempPath, 0o644); err != nil {
		os.Remove(tempPath)
		slog.Error(
			"chmod temp file failed",
			"component", "store",
			"description", description,
			"path", tempPath,
			"err", err,
		)
		return fmt.Errorf("chmod temp %s %s: %w", description, tempPath, err)
	}
	return nil
}

// syncFileDirectory flushes the directory entry so the rename itself survives a
// host crash, not just the bytes the renamed file holds.
func syncFileDirectory(directoryPath string, description string) error {
	directoryFile, err := os.Open(directoryPath)
	if err != nil {
		slog.Error(
			"open directory failed",
			"component", "store",
			"description", description,
			"path", directoryPath,
			"err", err,
		)
		return fmt.Errorf("open %s directory %s: %w", description, directoryPath, err)
	}
	syncErr := syncFile(directoryFile)
	closeErr := directoryFile.Close()
	if syncErr != nil {
		slog.Error(
			"sync directory failed",
			"component", "store",
			"description", description,
			"path", directoryPath,
			"err", syncErr,
		)
		return fmt.Errorf("sync %s directory %s: %w", description, directoryPath, syncErr)
	}
	if closeErr != nil {
		slog.Error(
			"close directory failed",
			"component", "store",
			"description", description,
			"path", directoryPath,
			"err", closeErr,
		)
		return fmt.Errorf("close %s directory %s: %w", description, directoryPath, closeErr)
	}
	return nil
}

// AppendJobEvent appends one job event to the JSONL journal.
func AppendJobEvent(path string, event model.JobEvent) error {
	return appendJobEvent(path, event, false)
}

// AppendJobEventSync appends one job event and synchronizes it before returning.
func AppendJobEventSync(path string, event model.JobEvent) error {
	return appendJobEvent(path, event, true)
}

func appendJobEvent(path string, event model.JobEvent, sync bool) error {
	if err := EnsureDir(filepath.Dir(path)); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		slog.Error("open jobs journal failed", "path", path, "err", err)
		return fmt.Errorf("open jobs journal %s: %w", path, err)
	}
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(event); err != nil {
		slog.Error("append jobs journal failed", "path", path, "err", err)
		encodeErr := fmt.Errorf("append jobs journal %s: %w", path, err)
		if closeErr := file.Close(); closeErr != nil {
			return errors.Join(encodeErr, fmt.Errorf("close jobs journal %s: %w", path, closeErr))
		}
		return encodeErr
	}
	if sync {
		if err := syncFile(file); err != nil {
			slog.Error("sync jobs journal failed", "path", path, "err", err)
			syncErr := fmt.Errorf("sync jobs journal %s: %w", path, err)
			if closeErr := file.Close(); closeErr != nil {
				return errors.Join(syncErr, fmt.Errorf("close jobs journal %s: %w", path, closeErr))
			}
			return syncErr
		}
	}
	if err := file.Close(); err != nil {
		slog.Error("close jobs journal failed", "path", path, "err", err)
		return fmt.Errorf("close jobs journal %s: %w", path, err)
	}
	if sync {
		if err := syncFileDirectory(filepath.Dir(path), "jobs journal"); err != nil {
			return err
		}
	}
	return nil
}

// WriteJobEvents atomically replaces the JSONL journal so a failed rewrite
// leaves the previous journal intact.
func WriteJobEvents(path string, events []model.JobEvent) error {
	slog.Info(
		"write jobs journal",
		"component",
		"store",
		"subcomponent",
		"journal",
		"path",
		path,
		"events",
		len(events),
	)

	return replaceFileAtomically(path, "jobs journal", func(file *os.File) error {
		encoder := json.NewEncoder(file)
		for _, event := range events {
			if err := encoder.Encode(event); err != nil {
				slog.Error(
					"encode job event for journal failed",
					"component",
					"store",
					"subcomponent",
					"journal",
					"path",
					path,
					"job_id",
					event.Job.ID,
					"err",
					err,
				)
				return fmt.Errorf("encode job event %s: %w", event.Job.ID, err)
			}
		}
		return nil
	})
}

// ReadJobEventsLatest replays the JSONL journal and keeps each complete latest
// event so compaction preserves its event name and timestamp.
func ReadJobEventsLatest(path string) (map[string]model.JobEvent, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]model.JobEvent{}, nil
		}
		slog.Error("open jobs journal failed", "path", path, "err", err)
		return nil, fmt.Errorf("open jobs journal %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	events := map[string]model.JobEvent{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event model.JobEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			slog.Error("unmarshal jobs journal line failed", "path", path, "err", err)
			return nil, fmt.Errorf("unmarshal jobs journal line in %s: %w", path, err)
		}
		events[event.Job.ID] = event
	}
	if err := scanner.Err(); err != nil {
		slog.Error("scan jobs journal failed", "path", path, "err", err)
		return nil, fmt.Errorf("scan jobs journal %s: %w", path, err)
	}
	return events, nil
}

// ReadJobEvents projects the latest complete journal events to their jobs so
// existing callers keep the same result type.
func ReadJobEvents(path string) (map[string]model.Job, error) {
	events, err := ReadJobEventsLatest(path)
	if err != nil {
		return nil, err
	}
	jobs := make(map[string]model.Job, len(events))
	for id, event := range events {
		jobs[id] = event.Job
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
