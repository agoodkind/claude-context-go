package semantic

import "goodkind.io/claude-context-go/internal/adapterr"

// ErrUnavailable reports that the semantic backend is not configured.
var ErrUnavailable error = newSentinel(
	adapterr.ClassMilvusUnavailable,
	"semantic backend is unavailable",
	"milvus_unavailable",
	"verify MILVUS_ADDRESS and that the milvus process is reachable",
)

// ErrCollectionMissing reports that the semantic collection does not exist yet.
var ErrCollectionMissing error = newSentinel(
	adapterr.ClassCollectionMissing,
	"semantic collection is missing",
	"collection_missing",
	"re-run index_codebase to recreate the collection",
)

// ErrCollectionNotReady reports that the semantic collection exists but cannot be searched yet.
var ErrCollectionNotReady error = newSentinel(
	adapterr.ClassCollectionNotReady,
	"semantic collection is not ready",
	"collection_not_ready",
	"retry in a few seconds while the collection loads",
)

// ErrSearchResultIncomplete reports that Milvus returned a result set without the requested fields.
var ErrSearchResultIncomplete error = newSentinel(
	adapterr.ClassSearchResultIncomplete,
	"semantic search result is incomplete",
	"search_result_incomplete",
	"retry the query; the daemon will refetch missing fields",
)

func newSentinel(class adapterr.Class, message, code, hint string) error {
	return &adapterr.AdapterError{
		Class:         class,
		Message:       message,
		Code:          code,
		Hint:          hint,
		Cause:         nil,
		SafeForClient: true,
	}
}
