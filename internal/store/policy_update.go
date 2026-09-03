package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"goodkind.io/lm-semantic-search/internal/model"
)

const policyUpdateFilename = "policy-update.json"

// ErrPolicyUpdateMarkerMayExist reports that an atomic marker replacement
// failed after, or ambiguously around, the rename boundary.
var ErrPolicyUpdateMarkerMayExist = errors.New("policy update marker may exist")

// PolicyUpdatePath returns the transaction marker beside the registry.
func PolicyUpdatePath(registryPath string) string {
	return filepath.Join(filepath.Dir(registryPath), policyUpdateFilename)
}

// ReadPolicyUpdate reads one pending policy-update transaction.
func ReadPolicyUpdate(path string) (model.PolicyUpdateTransaction, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return model.PolicyUpdateTransaction{}, fmt.Errorf(
				"read policy update marker %s: %w",
				path,
				err,
			)
		}
		slog.Error("read policy update marker failed", "path", path, "err", err)
		return model.PolicyUpdateTransaction{}, fmt.Errorf(
			"read policy update marker %s: %w",
			path,
			err,
		)
	}

	var transaction model.PolicyUpdateTransaction
	if err := json.Unmarshal(data, &transaction); err != nil {
		slog.Error("unmarshal policy update marker failed", "path", path, "err", err)
		return model.PolicyUpdateTransaction{}, fmt.Errorf(
			"unmarshal policy update marker %s: %w",
			path,
			err,
		)
	}
	return transaction, nil
}

// WritePolicyUpdate atomically persists one policy-update transaction.
func WritePolicyUpdate(path string, transaction model.PolicyUpdateTransaction) error {
	data, err := json.MarshalIndent(transaction, "", "  ")
	if err != nil {
		wrappedErr := fmt.Errorf("marshal policy update marker %s: %w", path, err)
		slog.Error("marshal policy update marker failed", "path", path, "err", wrappedErr)
		return wrappedErr
	}
	replaceErr := replaceFileAtomically(
		path,
		"policy update marker",
		func(file *os.File) error {
			if _, writeErr := file.Write(data); writeErr != nil {
				return fmt.Errorf("write policy update marker bytes: %w", writeErr)
			}
			return nil
		},
	)
	if replaceErr == nil {
		return nil
	}
	currentData, readErr := os.ReadFile(path)
	if errors.Is(readErr, os.ErrNotExist) {
		return replaceErr
	}
	if readErr == nil && bytes.Equal(currentData, data) {
		return errors.Join(ErrPolicyUpdateMarkerMayExist, replaceErr)
	}
	if readErr != nil {
		return errors.Join(
			ErrPolicyUpdateMarkerMayExist,
			replaceErr,
			fmt.Errorf("inspect policy update marker %s: %w", path, readErr),
		)
	}
	return errors.Join(
		ErrPolicyUpdateMarkerMayExist,
		replaceErr,
		fmt.Errorf("policy update marker %s contains different data", path),
	)
}

// RemovePolicyUpdate removes the marker and synchronizes its directory entry.
func RemovePolicyUpdate(path string) error {
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syncFileDirectory(filepath.Dir(path), "policy update marker removal")
		}
		slog.Error("remove policy update marker failed", "path", path, "err", err)
		return fmt.Errorf("remove policy update marker %s: %w", path, err)
	}
	return syncFileDirectory(filepath.Dir(path), "policy update marker removal")
}
