// Package semantic implements embedding and Milvus-backed code indexing.
package semantic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"goodkind.io/claude-context-go/internal/adapterr"
	"goodkind.io/claude-context-go/internal/config"
	"goodkind.io/claude-context-go/internal/embedding"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/spans"
	"goodkind.io/claude-context-go/internal/tshash"
)

// Milvus field names match the upstream TS schema at
// packages/core/src/vectordb/milvus-vectordb.ts so the Go daemon reads and
// writes the same collections the TS adapter does. The names are camelCase
// because that is what the TS adapter wrote.
const (
	maxCollectionNameLength = 255
	denseVectorFieldName    = "vector"
	sparseVectorFieldName   = "sparse_vector"
	contentFieldName        = "content"
	relativePathFieldName   = "relativePath"
	startLineFieldName      = "startLine"
	endLineFieldName        = "endLine"
	fileExtensionFieldName  = "fileExtension"
	metadataFieldName       = "metadata"
	idFieldName             = "id"
)

// Progress reports semantic indexing progress after chunk extraction.
type Progress struct {
	Phase                     string
	OverallPercent            float64
	EmbeddingBatchesTotal     int32
	EmbeddingBatchesCompleted int32
	CollectionRowsWritten     int32
}

// Service owns the embedding provider and Milvus client for semantic search.
type Service struct {
	cfg       config.Config
	embedder  embedding.Provider
	milvus    *milvusclient.Client
	available bool
}

// NewService constructs the semantic search runtime.
func NewService(ctx context.Context, cfg config.Config) (*Service, error) {
	if strings.TrimSpace(cfg.MilvusAddress) == "" {
		return &Service{
			cfg:       cfg,
			embedder:  nil,
			milvus:    nil,
			available: false,
		}, nil
	}

	embedder, err := embedding.NewProvider(cfg)
	if err != nil {
		slog.ErrorContext(ctx, "create embedding provider failed", "provider", cfg.EmbeddingProvider, "err", err)
		return nil, fmt.Errorf("create embedding provider: %w", err)
	}

	clientConfig := &milvusclient.ClientConfig{
		Address: cfg.MilvusAddress,
		APIKey:  cfg.MilvusToken,
	}
	client, err := milvusclient.New(ctx, clientConfig)
	if err != nil {
		slog.ErrorContext(ctx, "connect to Milvus failed", "address", cfg.MilvusAddress, "err", err)
		return nil, fmt.Errorf("connect to Milvus at %s: %w", cfg.MilvusAddress, err)
	}

	return &Service{
		cfg:       cfg,
		embedder:  embedder,
		milvus:    client,
		available: true,
	}, nil
}

// Close shuts down external resources held by the semantic service.
func (service *Service) Close(ctx context.Context) error {
	if service == nil || service.milvus == nil {
		return nil
	}
	if err := service.milvus.Close(ctx); err != nil {
		slog.ErrorContext(ctx, "close Milvus client failed", "err", err)
		return fmt.Errorf("close Milvus client: %w", err)
	}
	return nil
}

// Available reports whether semantic indexing is configured.
func (service *Service) Available() bool {
	return service != nil && service.available
}

// CollectionName matches the TypeScript collection naming contract at
// packages/core/src/context.ts:275 so the Go daemon reads and writes the
// same Milvus collections as the upstream TS adapter.
func (service *Service) CollectionName(codebasePath string) string {
	prefix := "code_chunks"
	if service.cfg.HybridMode {
		prefix = "hybrid_code_chunks"
	}

	normalizedPath, err := filepath.Abs(codebasePath)
	if err != nil {
		normalizedPath = codebasePath
	}
	pathHash := tshash.PathPrefix(normalizedPath)

	override := strings.TrimSpace(service.cfg.CollectionNameOverride)
	if override == "" {
		return prefix + "_" + pathHash
	}

	hashSuffix := "_" + pathHash
	maxReadableLength := maxCollectionNameLength - len(prefix) - 1 - len(hashSuffix)
	sanitized := sanitizeCollectionSuffix(override)
	if len(sanitized) > maxReadableLength {
		sanitized = sanitized[:maxReadableLength]
	}
	if sanitized == "" {
		sanitized = "custom"
	}
	return prefix + "_" + sanitized + hashSuffix
}

// Replace drops and rebuilds one codebase collection from chunk data.
func (service *Service) Replace(ctx context.Context, codebasePath string, chunks []model.StoredChunk, progress func(Progress)) (err error) {
	ctx, done := spans.Open(ctx, "semantic.replace")
	defer done(&err)

	if !service.Available() {
		return nil
	}
	if len(chunks) == 0 {
		return nil
	}
	chunks = service.guardrailExpand(ctx, codebasePath, chunks, "replace")

	collectionName := service.CollectionName(codebasePath)
	if err := service.dropIfExists(ctx, collectionName); err != nil {
		return err
	}

	batchSize := service.cfg.EmbeddingBatchSize
	if batchSize <= 0 {
		batchSize = 32
	}

	totalBatches := (len(chunks) + batchSize - 1) / batchSize
	var writtenRows int32
	collectionReady := false

	for batchIndex := range totalBatches {
		start := batchIndex * batchSize
		end := min(start+batchSize, len(chunks))

		chunkBatch := chunks[start:end]
		textBatch := make([]string, 0, len(chunkBatch))
		for _, chunk := range chunkBatch {
			textBatch = append(textBatch, chunk.Content)
		}

		vectors, err := service.embedder.EmbedBatch(ctx, textBatch)
		if err != nil {
			slog.ErrorContext(ctx, "embed batch failed", "err", err)
			return adapterr.NewEmbedderUnreachable(err)
		}
		if len(vectors) != len(chunkBatch) {
			slog.ErrorContext(ctx, "embedding batch returned unexpected vector count", "want", len(chunkBatch), "got", len(vectors), "err", errors.New("vector count mismatch"))
			return fmt.Errorf("embedding batch returned %d vectors for %d chunks", len(vectors), len(chunkBatch))
		}

		if !collectionReady {
			dimension := len(vectors[0])
			if err := service.createCollection(ctx, collectionName, dimension); err != nil {
				return err
			}
			collectionReady = true
		}

		if err := service.insertBatch(ctx, collectionName, chunkBatch, vectors); err != nil {
			return err
		}

		writtenRows += safeInt32FromInt(len(chunkBatch))
		if progress != nil {
			progress(Progress{
				Phase:                     "Generating embeddings and writing to Milvus...",
				OverallPercent:            90 + (float64(batchIndex+1)/float64(totalBatches))*10,
				EmbeddingBatchesTotal:     safeInt32FromInt(totalBatches),
				EmbeddingBatchesCompleted: safeInt32FromInt(batchIndex + 1),
				CollectionRowsWritten:     writtenRows,
			})
		}
	}
	return nil
}

// Reindex applies a per-file delta against an existing collection.
//
// removedOrModifiedRelativePaths is deleted via Milvus expression
// `relative_path == "<escaped>"`. The chunk batch is then embedded and
// inserted in the same batched flow Replace uses. Reindex returns
// ErrCollectionMissing if the collection no longer exists so callers can
// fall back to a full Replace.
func (service *Service) Reindex(ctx context.Context, codebasePath string, addedOrModifiedChunks []model.StoredChunk, removedOrModifiedRelativePaths []string, progress func(Progress)) (err error) {
	ctx, done := spans.Open(ctx, "semantic.reindex")
	defer done(&err)

	if !service.Available() {
		return nil
	}

	collectionName := service.CollectionName(codebasePath)
	hasCollection, err := service.milvus.HasCollection(ctx, milvusclient.NewHasCollectionOption(collectionName))
	if err != nil {
		slog.ErrorContext(ctx, "check Milvus collection for reindex failed", "collection", collectionName, "err", err)
		return fmt.Errorf("check Milvus collection %s: %w", collectionName, err)
	}
	if !hasCollection {
		return ErrCollectionMissing
	}

	if len(removedOrModifiedRelativePaths) > 0 {
		if err := service.deleteByRelativePaths(ctx, collectionName, removedOrModifiedRelativePaths); err != nil {
			return err
		}
	}

	if len(addedOrModifiedChunks) == 0 {
		return nil
	}
	addedOrModifiedChunks = service.guardrailExpand(ctx, codebasePath, addedOrModifiedChunks, "reindex")

	batchSize := service.cfg.EmbeddingBatchSize
	if batchSize <= 0 {
		batchSize = 32
	}

	totalBatches := (len(addedOrModifiedChunks) + batchSize - 1) / batchSize
	var writtenRows int32
	for batchIndex := range totalBatches {
		start := batchIndex * batchSize
		end := min(start+batchSize, len(addedOrModifiedChunks))

		chunkBatch := addedOrModifiedChunks[start:end]
		textBatch := make([]string, 0, len(chunkBatch))
		for _, chunk := range chunkBatch {
			textBatch = append(textBatch, chunk.Content)
		}

		vectors, err := service.embedder.EmbedBatch(ctx, textBatch)
		if err != nil {
			slog.ErrorContext(ctx, "embed reindex batch failed", "err", err)
			return adapterr.NewEmbedderUnreachable(err)
		}
		if len(vectors) != len(chunkBatch) {
			slog.ErrorContext(ctx, "reindex embedding batch returned unexpected vector count", "want", len(chunkBatch), "got", len(vectors), "err", errors.New("vector count mismatch"))
			return fmt.Errorf("reindex embedding batch returned %d vectors for %d chunks", len(vectors), len(chunkBatch))
		}

		if err := service.insertBatch(ctx, collectionName, chunkBatch, vectors); err != nil {
			return err
		}

		writtenRows += safeInt32FromInt(len(chunkBatch))
		if progress != nil {
			progress(Progress{
				Phase:                     "Reindexing changed files...",
				OverallPercent:            90 + (float64(batchIndex+1)/float64(totalBatches))*10,
				EmbeddingBatchesTotal:     safeInt32FromInt(totalBatches),
				EmbeddingBatchesCompleted: safeInt32FromInt(batchIndex + 1),
				CollectionRowsWritten:     writtenRows,
			})
		}
	}
	return nil
}

// PruneToCurrent removes rows whose relativePath is outside the provided
// set of current files. Use it after a streaming reindex to drop chunks
// left over from files that no longer exist on disk.
func (service *Service) PruneToCurrent(ctx context.Context, codebasePath string, currentRelativePaths []string) error {
	if !service.Available() {
		return nil
	}
	if len(currentRelativePaths) == 0 {
		return nil
	}
	collectionName := service.CollectionName(codebasePath)
	hasCollection, err := service.milvus.HasCollection(ctx, milvusclient.NewHasCollectionOption(collectionName))
	if err != nil {
		slog.ErrorContext(ctx, "check Milvus collection for prune failed", "collection", collectionName, "err", err)
		return fmt.Errorf("check Milvus collection %s: %w", collectionName, err)
	}
	if !hasCollection {
		return ErrCollectionMissing
	}

	quoted := make([]string, 0, len(currentRelativePaths))
	for _, path := range currentRelativePaths {
		quoted = append(quoted, `"`+escapeMilvusString(path)+`"`)
	}
	expression := fmt.Sprintf(`%s not in [%s]`, relativePathFieldName, strings.Join(quoted, ","))

	if _, err := service.milvus.Delete(ctx, milvusclient.NewDeleteOption(collectionName).WithExpr(expression)); err != nil {
		slog.ErrorContext(ctx, "prune orphans failed", "collection", collectionName, "current_count", len(currentRelativePaths), "err", err)
		return fmt.Errorf("prune orphans from %s: %w", collectionName, err)
	}
	return nil
}

// deleteByRelativePaths removes existing chunks for the given relative paths.
// Paths are escaped to be safe inside the Milvus filter expression.
func (service *Service) deleteByRelativePaths(ctx context.Context, collectionName string, relativePaths []string) error {
	if len(relativePaths) == 0 {
		return nil
	}

	quoted := make([]string, 0, len(relativePaths))
	for _, path := range relativePaths {
		quoted = append(quoted, `"`+escapeMilvusString(path)+`"`)
	}
	expression := fmt.Sprintf(`%s in [%s]`, relativePathFieldName, strings.Join(quoted, ","))

	if _, err := service.milvus.Delete(ctx, milvusclient.NewDeleteOption(collectionName).WithExpr(expression)); err != nil {
		slog.ErrorContext(ctx, "delete by relative path failed", "collection", collectionName, "count", len(relativePaths), "err", err)
		return fmt.Errorf("delete from %s by relative path: %w", collectionName, err)
	}
	return nil
}

// escapeMilvusString matches the TS escape rule at context.ts:412 and quotes
// any backslash or double quote in a filter string.
func escapeMilvusString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

// Search executes semantic or hybrid search against the configured collection.
func (service *Service) Search(ctx context.Context, codebasePath string, query string, limit int32, extensionFilter []string) ([]model.StoredChunk, error) {
	if !service.Available() {
		return nil, ErrUnavailable
	}

	collectionName := service.CollectionName(codebasePath)
	hasCollection, err := service.milvus.HasCollection(ctx, milvusclient.NewHasCollectionOption(collectionName))
	if err != nil {
		slog.ErrorContext(ctx, "check Milvus collection failed", "collection", collectionName, "err", err)
		return nil, fmt.Errorf("check Milvus collection %s: %w", collectionName, err)
	}
	if !hasCollection {
		return nil, ErrCollectionMissing
	}

	queryVector, err := service.embedder.Embed(ctx, query)
	if err != nil {
		slog.ErrorContext(ctx, "embed query failed", "err", err)
		return nil, fmt.Errorf("embed query: %w", err)
	}

	searchLimit := int(limit)
	if searchLimit <= 0 {
		searchLimit = 10
	}
	filterExpr := buildExtensionFilter(extensionFilter)

	outputFields := []string{
		contentFieldName,
		relativePathFieldName,
		startLineFieldName,
		endLineFieldName,
		fileExtensionFieldName,
		metadataFieldName,
	}

	if service.cfg.HybridMode {
		denseRequest := milvusclient.NewAnnRequest(denseVectorFieldName, maxInt(searchLimit, 10), entity.FloatVector(queryVector))
		sparseRequest := milvusclient.NewAnnRequest(sparseVectorFieldName, maxInt(searchLimit, 10), entity.Text(query))
		if filterExpr != "" {
			denseRequest = denseRequest.WithFilter(filterExpr)
			sparseRequest = sparseRequest.WithFilter(filterExpr)
		}
		resultSets, err := service.milvus.HybridSearch(ctx, milvusclient.NewHybridSearchOption(
			collectionName,
			searchLimit,
			denseRequest,
			sparseRequest,
		).WithReranker(milvusclient.NewRRFReranker()).WithOutputFields(outputFields...))
		if err != nil {
			slog.ErrorContext(ctx, "hybrid search failed", "collection", collectionName, "err", err)
			if strings.Contains(err.Error(), "collection not loaded") {
				return nil, ErrCollectionNotReady
			}
			return nil, fmt.Errorf("hybrid search collection %s: %w", collectionName, err)
		}
		return resultSetsToChunks(resultSets)
	}

	searchOption := milvusclient.NewSearchOption(
		collectionName,
		searchLimit,
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField(denseVectorFieldName).WithOutputFields(outputFields...)
	if filterExpr != "" {
		searchOption = searchOption.WithFilter(filterExpr)
	}

	resultSets, err := service.milvus.Search(ctx, searchOption)
	if err != nil {
		slog.ErrorContext(ctx, "dense search failed", "collection", collectionName, "err", err)
		if strings.Contains(err.Error(), "collection not loaded") {
			return nil, ErrCollectionNotReady
		}
		return nil, fmt.Errorf("search collection %s: %w", collectionName, err)
	}
	return resultSetsToChunks(resultSets)
}

// Drop removes one semantic index collection.
func (service *Service) Drop(ctx context.Context, codebasePath string) error {
	if !service.Available() {
		return nil
	}
	return service.dropIfExists(ctx, service.CollectionName(codebasePath))
}

// Count returns the current row count for one semantic collection.
func (service *Service) Count(ctx context.Context, codebasePath string) (int32, error) {
	if !service.Available() {
		return 0, ErrUnavailable
	}

	stats, err := service.milvus.GetCollectionStats(ctx, milvusclient.NewGetCollectionStatsOption(service.CollectionName(codebasePath)))
	if err != nil {
		slog.ErrorContext(ctx, "get collection stats failed", "collection", service.CollectionName(codebasePath), "err", err)
		return 0, fmt.Errorf("get collection stats: %w", err)
	}

	rowCount, found := stats["row_count"]
	if !found {
		slog.ErrorContext(ctx, "collection stats missing row_count", "collection", service.CollectionName(codebasePath), "err", errors.New("missing row_count"))
		return 0, errors.New("milvus collection stats missing row_count")
	}
	parsedCount, err := strconv.ParseInt(rowCount, 10, 32)
	if err != nil {
		slog.ErrorContext(ctx, "parse row count failed", "row_count", rowCount, "err", err)
		return 0, fmt.Errorf("parse row_count %q: %w", rowCount, err)
	}
	return int32(parsedCount), nil
}

// ListCollections returns the current semantic collection names from Milvus.
func (service *Service) ListCollections(ctx context.Context) ([]string, error) {
	if !service.Available() {
		return nil, ErrUnavailable
	}
	collections, err := service.milvus.ListCollections(ctx, milvusclient.NewListCollectionOption())
	if err != nil {
		slog.ErrorContext(ctx, "list Milvus collections failed", "err", err)
		return nil, fmt.Errorf("list Milvus collections: %w", err)
	}
	return collections, nil
}

// HasCollectionForPath reports whether Milvus has the collection for the
// given codebase path.
func (service *Service) HasCollectionForPath(ctx context.Context, codebasePath string) (bool, error) {
	if !service.Available() {
		return false, ErrUnavailable
	}
	collectionName := service.CollectionName(codebasePath)
	hasCollection, err := service.milvus.HasCollection(ctx, milvusclient.NewHasCollectionOption(collectionName))
	if err != nil {
		slog.ErrorContext(ctx, "check Milvus collection presence failed", "collection", collectionName, "err", err)
		return false, fmt.Errorf("check Milvus collection %s: %w", collectionName, err)
	}
	return hasCollection, nil
}

func (service *Service) dropIfExists(ctx context.Context, collectionName string) error {
	hasCollection, err := service.milvus.HasCollection(ctx, milvusclient.NewHasCollectionOption(collectionName))
	if err != nil {
		slog.ErrorContext(ctx, "check Milvus collection before drop failed", "collection", collectionName, "err", err)
		return fmt.Errorf("check Milvus collection %s: %w", collectionName, err)
	}
	if !hasCollection {
		return nil
	}
	if err := service.milvus.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName)); err != nil {
		slog.ErrorContext(ctx, "drop Milvus collection failed", "collection", collectionName, "err", err)
		return fmt.Errorf("drop Milvus collection %s: %w", collectionName, err)
	}
	return nil
}

func (service *Service) createCollection(ctx context.Context, collectionName string, dimension int) error {
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName(idFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(512).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(contentFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(65535).WithEnableAnalyzer(true).WithEnableMatch(true)).
		WithField(entity.NewField().WithName(relativePathFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(1024)).
		WithField(entity.NewField().WithName(startLineFieldName).WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(endLineFieldName).WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(fileExtensionFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(32)).
		WithField(entity.NewField().WithName(metadataFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(65535)).
		WithField(entity.NewField().WithName(denseVectorFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dimension)))

	indexOptions := []milvusclient.CreateIndexOption{
		milvusclient.NewCreateIndexOption(collectionName, denseVectorFieldName, index.NewAutoIndex(entity.COSINE)),
	}

	if service.cfg.HybridMode {
		schema = schema.
			WithField(entity.NewField().WithName(sparseVectorFieldName).WithDataType(entity.FieldTypeSparseVector)).
			WithFunction(entity.NewFunction().WithName("bm25").WithType(entity.FunctionTypeBM25).WithInputFields(contentFieldName).WithOutputFields(sparseVectorFieldName))
		indexOptions = append(indexOptions, milvusclient.NewCreateIndexOption(collectionName, sparseVectorFieldName, index.NewSparseInvertedIndex(entity.BM25, 0.2)))
	}

	if err := service.milvus.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema).WithIndexOptions(indexOptions...)); err != nil {
		slog.ErrorContext(ctx, "create Milvus collection failed", "collection", collectionName, "err", err)
		return fmt.Errorf("create Milvus collection %s: %w", collectionName, err)
	}
	loadTask, err := service.milvus.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(collectionName))
	if err != nil {
		slog.ErrorContext(ctx, "load Milvus collection failed", "collection", collectionName, "err", err)
		return fmt.Errorf("load Milvus collection %s: %w", collectionName, err)
	}
	if err := loadTask.Await(ctx); err != nil {
		slog.ErrorContext(ctx, "await Milvus collection load failed", "collection", collectionName, "err", err)
		return fmt.Errorf("await Milvus collection load %s: %w", collectionName, err)
	}
	return nil
}

func (service *Service) insertBatch(ctx context.Context, collectionName string, chunks []model.StoredChunk, vectors [][]float32) (err error) {
	ctx, done := spans.Open(ctx, "semantic.insertBatch")
	defer done(&err)

	insertOption := milvusclient.NewColumnBasedInsertOption(collectionName)

	ids := make([]string, 0, len(chunks))
	contents := make([]string, 0, len(chunks))
	relativePaths := make([]string, 0, len(chunks))
	startLines := make([]int64, 0, len(chunks))
	endLines := make([]int64, 0, len(chunks))
	fileExtensions := make([]string, 0, len(chunks))
	metadataValues := make([]string, 0, len(chunks))

	sanitizedCount := 0
	for index, chunk := range chunks {
		content, contentChanged := sanitizeUTF8(chunk.Content)
		relativePath, pathChanged := sanitizeUTF8(chunk.RelativePath)
		fileExtension, extChanged := sanitizeUTF8(chunk.FileExtension)
		metadataValue, metaChanged := sanitizeUTF8(encodeMetadata(chunk))
		if contentChanged || pathChanged || extChanged || metaChanged {
			sanitizedCount++
			slog.WarnContext(ctx, "semantic.sanitized_invalid_utf8", "relative_path", chunk.RelativePath, "start_line", chunk.StartLine, "end_line", chunk.EndLine, "content_changed", contentChanged, "path_changed", pathChanged, "extension_changed", extChanged, "metadata_changed", metaChanged)
		}
		ids = append(ids, generateID(chunk, index))
		contents = append(contents, content)
		relativePaths = append(relativePaths, relativePath)
		startLines = append(startLines, int64(chunk.StartLine))
		endLines = append(endLines, int64(chunk.EndLine))
		fileExtensions = append(fileExtensions, fileExtension)
		metadataValues = append(metadataValues, metadataValue)
	}
	if sanitizedCount > 0 {
		slog.WarnContext(ctx, "semantic.insertBatch sanitized chunks before Milvus marshal", "collection", collectionName, "sanitized", sanitizedCount, "batch_size", len(chunks))
	}

	insertOption = insertOption.
		WithVarcharColumn(idFieldName, ids).
		WithVarcharColumn(contentFieldName, contents).
		WithVarcharColumn(relativePathFieldName, relativePaths).
		WithInt64Column(startLineFieldName, startLines).
		WithInt64Column(endLineFieldName, endLines).
		WithVarcharColumn(fileExtensionFieldName, fileExtensions).
		WithVarcharColumn(metadataFieldName, metadataValues).
		WithFloatVectorColumn(denseVectorFieldName, len(vectors[0]), vectors)

	if _, err := service.milvus.Insert(ctx, insertOption); err != nil {
		slog.ErrorContext(ctx, "insert Milvus batch failed", "collection", collectionName, "err", err)
		return fmt.Errorf("insert Milvus batch into %s: %w", collectionName, err)
	}
	return nil
}

func resultSetsToChunks(resultSets []milvusclient.ResultSet) ([]model.StoredChunk, error) {
	if len(resultSets) == 0 {
		return []model.StoredChunk{}, nil
	}

	resultSet := resultSets[0]
	if resultSet.ResultCount == 0 {
		return []model.StoredChunk{}, nil
	}
	contentColumn := resultSet.GetColumn(contentFieldName)
	relativePathColumn := resultSet.GetColumn(relativePathFieldName)
	startLineColumn := resultSet.GetColumn(startLineFieldName)
	endLineColumn := resultSet.GetColumn(endLineFieldName)
	fileExtensionColumn := resultSet.GetColumn(fileExtensionFieldName)
	metadataColumn := resultSet.GetColumn(metadataFieldName)
	if contentColumn == nil || relativePathColumn == nil || startLineColumn == nil || endLineColumn == nil || fileExtensionColumn == nil {
		return nil, ErrSearchResultIncomplete
	}

	chunks := make([]model.StoredChunk, 0, resultSet.ResultCount)
	for index := range resultSet.ResultCount {
		contentValue, err := contentColumn.GetAsString(index)
		if err != nil {
			slog.Error("read content column failed", "index", index, "err", err)
			return nil, fmt.Errorf("read content column at %d: %w", index, err)
		}
		relativePathValue, err := relativePathColumn.GetAsString(index)
		if err != nil {
			slog.Error("read relative path column failed", "index", index, "err", err)
			return nil, fmt.Errorf("read relative path column at %d: %w", index, err)
		}
		startLineValue, err := startLineColumn.GetAsInt64(index)
		if err != nil {
			slog.Error("read start line column failed", "index", index, "err", err)
			return nil, fmt.Errorf("read start line column at %d: %w", index, err)
		}
		endLineValue, err := endLineColumn.GetAsInt64(index)
		if err != nil {
			slog.Error("read end line column failed", "index", index, "err", err)
			return nil, fmt.Errorf("read end line column at %d: %w", index, err)
		}
		fileExtensionValue, err := fileExtensionColumn.GetAsString(index)
		if err != nil {
			slog.Error("read file extension column failed", "index", index, "err", err)
			return nil, fmt.Errorf("read file extension column at %d: %w", index, err)
		}
		languageValue := ""
		if metadataColumn != nil {
			metadataValue, metadataErr := metadataColumn.GetAsString(index)
			if metadataErr == nil {
				languageValue = decodeMetadataLanguage(metadataValue)
			}
		}

		chunks = append(chunks, model.StoredChunk{
			Content:       contentValue,
			RelativePath:  relativePathValue,
			StartLine:     safeInt32FromInt64(startLineValue),
			EndLine:       safeInt32FromInt64(endLineValue),
			Language:      languageValue,
			FileExtension: fileExtensionValue,
		})
	}
	return chunks, nil
}

// chunkMetadata mirrors the JSON shape the TS adapter writes into the
// Milvus `metadata` field. The Go daemon adds a language hint so search
// results can resurface the splitter-derived language without a dedicated
// column.
type chunkMetadata struct {
	Language string `json:"language,omitempty"`
}

func encodeMetadata(chunk model.StoredChunk) string {
	if chunk.Language == "" {
		return "{}"
	}
	encoded, err := json.Marshal(chunkMetadata{Language: chunk.Language})
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func decodeMetadataLanguage(metadata string) string {
	if metadata == "" {
		return ""
	}
	var parsed chunkMetadata
	if err := json.Unmarshal([]byte(metadata), &parsed); err != nil {
		return ""
	}
	return parsed.Language
}

// sanitizeUTF8 returns a copy of value with invalid UTF-8 byte sequences
// replaced by the Unicode replacement character. Milvus rejects VarChar
// payloads with invalid UTF-8 at the gRPC marshal boundary, so any chunk
// content that survives the file-level skip but still slices through a
// multi-byte codepoint (for example from a tree-sitter byte-offset
// boundary) gets repaired here. The second return value reports whether
// the input needed repair so callers can log the event.
func sanitizeUTF8(value string) (string, bool) {
	if utf8.ValidString(value) {
		return value, false
	}
	return strings.ToValidUTF8(value, "�"), true
}

// milvusVarcharMaxBytes mirrors the schema's WithMaxLength(65535) for VarChar
// fields. Chunks longer than this fail the Milvus insert with "length of
// varchar field content exceeds max length". The splitter is supposed to
// keep every chunk under chunk_size (default 2500), so an oversize chunk
// at insert time signals a splitter regression. The expansion in
// expandOversizeChunks turns the splitter regression into multiple rows
// rather than a dropped insert, so no content is lost.
const milvusVarcharMaxBytes = 65000

// guardrailExpand wraps expandOversizeChunks with logging. Each oversize
// chunk hitting this path signals an upstream splitter regression that
// emitted content longer than chunk_size. The log carries the codebase
// path, the operation that requested the embed, and the relative path of
// the offending file so the regression can be diagnosed without losing
// the data.
func (service *Service) guardrailExpand(ctx context.Context, codebasePath string, chunks []model.StoredChunk, operation string) []model.StoredChunk {
	ctx, done := spans.Open(ctx, "semantic.guardrailExpand")
	defer done(nil)

	expanded, changed := expandOversizeChunks(chunks)
	if !changed {
		return chunks
	}
	offenders := make([]string, 0)
	for _, chunk := range chunks {
		if len(chunk.Content) > milvusVarcharMaxBytes {
			offenders = append(offenders, fmt.Sprintf("%s:%d-%d (%d bytes)", chunk.RelativePath, chunk.StartLine, chunk.EndLine, len(chunk.Content)))
		}
	}
	slog.WarnContext(ctx, "semantic.expanded_oversize_chunks", "codebase_path", codebasePath, "operation", operation, "expanded_from", len(chunks), "expanded_to", len(expanded), "max_bytes", milvusVarcharMaxBytes, "offenders", offenders)
	return expanded
}

// expandOversizeChunks returns a list where any chunk over
// milvusVarcharMaxBytes has been split into multiple chunks aligned to
// codepoint boundaries. The boolean reports whether any expansion
// happened so the caller can log the upstream regression.
func expandOversizeChunks(chunks []model.StoredChunk) ([]model.StoredChunk, bool) {
	expanded := false
	for _, chunk := range chunks {
		if len(chunk.Content) > milvusVarcharMaxBytes {
			expanded = true
			break
		}
	}
	if !expanded {
		return chunks, false
	}
	out := make([]model.StoredChunk, 0, len(chunks))
	for _, chunk := range chunks {
		if len(chunk.Content) <= milvusVarcharMaxBytes {
			out = append(out, chunk)
			continue
		}
		for _, piece := range splitForVarchar(chunk.Content) {
			child := chunk
			child.Content = piece
			out = append(out, child)
		}
	}
	return out, true
}

// splitForVarchar cuts value into sub-strings of at most
// milvusVarcharMaxBytes bytes, each ending on a UTF-8 codepoint boundary.
func splitForVarchar(value string) []string {
	out := make([]string, 0, (len(value)+milvusVarcharMaxBytes-1)/milvusVarcharMaxBytes)
	start := 0
	for start < len(value) {
		end := start + milvusVarcharMaxBytes
		if end >= len(value) {
			out = append(out, value[start:])
			break
		}
		for end > start && !utf8.RuneStart(value[end]) {
			end--
		}
		if end == start {
			_, size := utf8.DecodeRuneInString(value[start:])
			end = start + size
		}
		out = append(out, value[start:end])
		start = end
	}
	return out
}

// generateID matches the TS chunk-ID format at packages/core/src/context.ts:1067.
func generateID(chunk model.StoredChunk, _ int) string {
	hashInput := fmt.Sprintf("%s:%d:%d:%s", chunk.RelativePath, chunk.StartLine, chunk.EndLine, chunk.Content)
	sum := sha256.Sum256([]byte(hashInput))
	return "chunk_" + hex.EncodeToString(sum[:])[:16]
}

func buildExtensionFilter(extensionFilter []string) string {
	cleanedExtensions := make([]string, 0, len(extensionFilter))
	for _, extension := range normalizeExtensionFilter(extensionFilter) {
		trimmedExtension := strings.TrimSpace(extension)
		if trimmedExtension == "" {
			continue
		}
		cleanedExtensions = append(cleanedExtensions, fmt.Sprintf("%q", trimmedExtension))
	}
	if len(cleanedExtensions) == 0 {
		return ""
	}
	return fileExtensionFieldName + " in [" + strings.Join(cleanedExtensions, ", ") + "]"
}

func sanitizeCollectionSuffix(value string) string {
	var builder strings.Builder
	for _, runeValue := range value {
		switch {
		case runeValue >= 'A' && runeValue <= 'Z':
			builder.WriteRune(runeValue)
		case runeValue >= 'a' && runeValue <= 'z':
			builder.WriteRune(runeValue)
		case runeValue >= '0' && runeValue <= '9':
			builder.WriteRune(runeValue)
		default:
			builder.WriteByte('_')
		}
	}
	return builder.String()
}

func maxInt(values ...int) int {
	currentMax := 0
	for _, value := range values {
		if value > currentMax {
			currentMax = value
		}
	}
	return currentMax
}

// ValidateExtensionFilter returns the normalized extension list or an error if any entry is invalid.
func ValidateExtensionFilter(extensionFilter []string) ([]string, error) {
	cleanedExtensions := normalizeExtensionFilter(extensionFilter)
	invalidExtensions := make([]string, 0)
	for _, extension := range cleanedExtensions {
		if !isValidExtension(extension) {
			invalidExtensions = append(invalidExtensions, extension)
		}
	}
	if len(invalidExtensions) > 0 {
		err := fmt.Errorf("invalid file extensions in extensionFilter: %v. Use proper extensions like '.ts', '.py'", invalidExtensions)
		slog.Error("validate extension filter failed", "err", err)
		return nil, err
	}
	return cleanedExtensions, nil
}

// DeduplicateChunks removes overlapping results from the same file.
func DeduplicateChunks(chunks []model.StoredChunk) []model.StoredChunk {
	keptChunks := make([]model.StoredChunk, 0, len(chunks))

	for _, chunk := range chunks {
		hasOverlap := false
		for _, existingChunk := range keptChunks {
			if existingChunk.RelativePath != chunk.RelativePath {
				continue
			}
			overlapStart := maxInt32(existingChunk.StartLine, chunk.StartLine)
			overlapEnd := minInt32(existingChunk.EndLine, chunk.EndLine)
			if overlapStart > overlapEnd {
				continue
			}
			overlapSize := overlapEnd - overlapStart + 1
			chunkSize := chunk.EndLine - chunk.StartLine + 1
			if chunkSize > 0 && float64(overlapSize)/float64(chunkSize) > 0.5 {
				hasOverlap = true
				break
			}
		}
		if !hasOverlap {
			keptChunks = append(keptChunks, chunk)
		}
	}

	return keptChunks
}

// normalizeExtensionFilter trims whitespace and prepends a leading dot when
// missing so the filter matches the dot-prefixed values that [filepath.Ext]
// writes into the file_extension column.
func normalizeExtensionFilter(extensionFilter []string) []string {
	cleanedExtensions := make([]string, 0, len(extensionFilter))
	for _, extension := range extensionFilter {
		trimmedExtension := strings.TrimSpace(extension)
		if trimmedExtension == "" {
			continue
		}
		if !strings.HasPrefix(trimmedExtension, ".") {
			trimmedExtension = "." + trimmedExtension
		}
		cleanedExtensions = append(cleanedExtensions, trimmedExtension)
	}
	return cleanedExtensions
}

func isValidExtension(extension string) bool {
	if !strings.HasPrefix(extension, ".") || len(extension) <= 1 {
		return false
	}
	for _, runeValue := range extension {
		if unicode.IsSpace(runeValue) {
			return false
		}
	}
	return true
}

func maxInt32(left int32, right int32) int32 {
	if left > right {
		return left
	}
	return right
}

func minInt32(left int32, right int32) int32 {
	if left < right {
		return left
	}
	return right
}

func safeInt32FromInt(value int) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	if value < math.MinInt32 {
		return math.MinInt32
	}
	return int32(value)
}

func safeInt32FromInt64(value int64) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	if value < math.MinInt32 {
		return math.MinInt32
	}
	return int32(value)
}
