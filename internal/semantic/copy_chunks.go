package semantic

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"goodkind.io/claude-context-go/internal/model"
)

// CopyChunks rewrites the relativePath column on every existing chunk row
// for srcRelativePath so the same vectors are addressable under
// dstRelativePath. Used by the converge decision table to handle a rename
// or hardlink without re-embedding.
//
// Milvus does not support an in-place column update on a primary-keyed
// row (the primary key in this schema is derived from relativePath), so
// CopyChunks queries the source rows, computes new IDs and rows for the
// destination path, deletes the source rows, then inserts the new rows.
// The dense vector is preserved across the copy; the sparse vector, when
// the collection is hybrid, is re-derived by the BM25 function from the
// preserved content so no embedding API call is issued.
func (service *Service) CopyChunks(ctx context.Context, codebasePath string, srcRelativePath string, dstRelativePath string) (int, error) {
	if !service.Available() {
		return 0, ErrUnavailable
	}
	if srcRelativePath == dstRelativePath {
		return 0, nil
	}
	collectionName := service.CollectionName(codebasePath)
	hasCollection, err := service.milvus.HasCollection(ctx, milvusclient.NewHasCollectionOption(collectionName))
	if err != nil {
		slog.ErrorContext(ctx, "check Milvus collection for copy failed", "collection", collectionName, "err", err)
		return 0, fmt.Errorf("check Milvus collection %s: %w", collectionName, err)
	}
	if !hasCollection {
		return 0, ErrCollectionMissing
	}

	chunks, vectors, err := service.fetchChunksForPath(ctx, collectionName, srcRelativePath)
	if err != nil {
		return 0, err
	}
	if len(chunks) == 0 {
		return 0, nil
	}

	rewritten := make([]model.StoredChunk, 0, len(chunks))
	for _, chunk := range chunks {
		chunkCopy := chunk
		chunkCopy.RelativePath = dstRelativePath
		rewritten = append(rewritten, chunkCopy)
	}

	if err := service.deleteByRelativePaths(ctx, collectionName, []string{srcRelativePath}); err != nil {
		return 0, err
	}
	if err := service.insertBatch(ctx, collectionName, rewritten, vectors); err != nil {
		return 0, err
	}
	slog.InfoContext(ctx, "semantic.copy_chunks", "collection", collectionName, "src", srcRelativePath, "dst", dstRelativePath, "rows", len(rewritten))
	return len(rewritten), nil
}

// fetchChunksForPath retrieves every chunk row for relativePath including
// the dense vector so CopyChunks can reinsert under a new key without
// re-embedding the content.
func (service *Service) fetchChunksForPath(ctx context.Context, collectionName string, relativePath string) ([]model.StoredChunk, [][]float32, error) {
	expression := fmt.Sprintf(`%s == %q`, relativePathFieldName, relativePath)
	outputFields := []string{
		idFieldName,
		contentFieldName,
		relativePathFieldName,
		startLineFieldName,
		endLineFieldName,
		fileExtensionFieldName,
		metadataFieldName,
		denseVectorFieldName,
	}
	queryOption := milvusclient.NewQueryOption(collectionName).WithFilter(expression).WithOutputFields(outputFields...)
	resultSet, err := service.milvus.Query(ctx, queryOption)
	if err != nil {
		slog.ErrorContext(ctx, "query chunks for copy failed", "collection", collectionName, "path", relativePath, "err", err)
		return nil, nil, fmt.Errorf("query chunks for %s in %s: %w", relativePath, collectionName, err)
	}
	if resultSet.ResultCount == 0 {
		return nil, nil, nil
	}

	contentColumn := resultSet.GetColumn(contentFieldName)
	startLineColumn := resultSet.GetColumn(startLineFieldName)
	endLineColumn := resultSet.GetColumn(endLineFieldName)
	fileExtensionColumn := resultSet.GetColumn(fileExtensionFieldName)
	metadataColumn := resultSet.GetColumn(metadataFieldName)
	vectorColumn := resultSet.GetColumn(denseVectorFieldName)
	if contentColumn == nil || startLineColumn == nil || endLineColumn == nil || fileExtensionColumn == nil || vectorColumn == nil {
		return nil, nil, ErrSearchResultIncomplete
	}

	chunks := make([]model.StoredChunk, 0, resultSet.ResultCount)
	vectors := make([][]float32, 0, resultSet.ResultCount)
	for rowIndex := range resultSet.ResultCount {
		contentValue, contentErr := contentColumn.GetAsString(rowIndex)
		if contentErr != nil {
			return nil, nil, fmt.Errorf("read content column at %d: %w", rowIndex, contentErr)
		}
		startLineValue, startLineErr := startLineColumn.GetAsInt64(rowIndex)
		if startLineErr != nil {
			return nil, nil, fmt.Errorf("read start_line column at %d: %w", rowIndex, startLineErr)
		}
		endLineValue, endLineErr := endLineColumn.GetAsInt64(rowIndex)
		if endLineErr != nil {
			return nil, nil, fmt.Errorf("read end_line column at %d: %w", rowIndex, endLineErr)
		}
		fileExtensionValue, fileExtensionErr := fileExtensionColumn.GetAsString(rowIndex)
		if fileExtensionErr != nil {
			return nil, nil, fmt.Errorf("read file_extension column at %d: %w", rowIndex, fileExtensionErr)
		}
		languageValue := ""
		if metadataColumn != nil {
			metadataValue, metadataErr := metadataColumn.GetAsString(rowIndex)
			if metadataErr == nil {
				languageValue = decodeMetadataLanguage(metadataValue)
			}
		}
		vector, vectorErr := vectorAt(vectorColumn, rowIndex)
		if vectorErr != nil {
			return nil, nil, fmt.Errorf("read vector column at %d: %w", rowIndex, vectorErr)
		}

		chunks = append(chunks, model.StoredChunk{
			Content:       contentValue,
			RelativePath:  relativePath,
			StartLine:     safeInt32FromInt64(startLineValue),
			EndLine:       safeInt32FromInt64(endLineValue),
			Language:      languageValue,
			FileExtension: fileExtensionValue,
		})
		vectors = append(vectors, vector)
	}
	return chunks, vectors, nil
}

// vectorAt extracts one float-vector row from a Milvus result column. The
// client's typed Column surface exposes the row through Get(int); for a
// dense FloatVector column the returned value is entity.FloatVector,
// which is just a []float32 with a named type.
func vectorAt(vectorColumn column.Column, rowIndex int) ([]float32, error) {
	raw, err := vectorColumn.Get(rowIndex)
	if err != nil {
		slog.Error("read vector row failed", "row", rowIndex, "err", err)
		return nil, fmt.Errorf("read vector row %d: %w", rowIndex, err)
	}
	switch typed := raw.(type) {
	case entity.FloatVector:
		out := make([]float32, len(typed))
		copy(out, typed)
		return out, nil
	case []float32:
		out := make([]float32, len(typed))
		copy(out, typed)
		return out, nil
	}
	err = fmt.Errorf("unexpected vector row type %T", raw)
	slog.Error("vector row type unexpected", "row", rowIndex, "err", err)
	return nil, err
}
