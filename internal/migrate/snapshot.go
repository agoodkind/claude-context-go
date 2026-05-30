// Package migrate reads the upstream TypeScript adapter's on-disk bookkeeping
// so the daemon can adopt a codebase the TS tool already indexed. Every read
// here is strictly read-only: the daemon never writes the TS snapshot or the TS
// merkle files. The shared Milvus collection, not these files, is the portable
// index.
package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"goodkind.io/claude-context-go/internal/merkle"
	"goodkind.io/claude-context-go/internal/tshash"
)

// tsMerkleFile mirrors the shape the TS synchronizer writes to
// ~/.context/merkle/<md5(path)>.json. Only fileHashes is consumed; the
// merkleDAG field is the TS-internal tree and is ignored.
type tsMerkleFile struct {
	FileHashes [][]string `json:"fileHashes"`
}

// TSMerklePath returns the absolute path of the TS merkle file for a codebase,
// which the TS synchronizer names by the full MD5 of the codebase path under
// the context root's merkle directory.
func TSMerklePath(contextRoot string, codebasePath string) string {
	return filepath.Join(contextRoot, "merkle", tshash.FullHex(codebasePath)+".json")
}

// LoadTSMerkle reads the TS merkle baseline for a codebase and converts its
// fileHashes pairs into a merkle.Snapshot whose Files map holds one
// content hash per relative path. found is false when the TS tool never wrote a
// merkle for this path, in which case the caller starts from an empty baseline.
// The returned snapshot has no ConfigDigest; the caller stamps the adopting
// codebase's digest before persisting it.
//
// The TS per-file hash is sha256 of the raw file content, the same hash
// merkle.Capture computes, so a seeded baseline lets the delta sync re-embed
// only files that changed since the TS index rather than the whole corpus.
func LoadTSMerkle(ctx context.Context, contextRoot string, codebasePath string) (merkle.Snapshot, bool, error) {
	path := TSMerklePath(contextRoot, codebasePath)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return merkle.Snapshot{ConfigDigest: "", Files: nil, Inodes: nil}, false, nil
		}
		slog.WarnContext(ctx, "read TS merkle failed", "path", path, "err", err)
		return merkle.Snapshot{ConfigDigest: "", Files: nil, Inodes: nil}, false, fmt.Errorf("read TS merkle %s: %w", path, err)
	}

	var parsed tsMerkleFile
	if err := json.Unmarshal(data, &parsed); err != nil {
		slog.WarnContext(ctx, "parse TS merkle failed", "path", path, "err", err)
		return merkle.Snapshot{ConfigDigest: "", Files: nil, Inodes: nil}, false, fmt.Errorf("parse TS merkle %s: %w", path, err)
	}

	files := make(map[string]string, len(parsed.FileHashes))
	for _, pair := range parsed.FileHashes {
		if len(pair) != 2 {
			continue
		}
		relativePath := pair[0]
		hash := pair[1]
		if relativePath == "" || hash == "" {
			continue
		}
		files[relativePath] = hash
	}

	return merkle.Snapshot{ConfigDigest: "", Files: files, Inodes: nil}, true, nil
}
