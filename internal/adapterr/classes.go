package adapterr

import "google.golang.org/grpc/codes"

// Class is the closed-set classification for an [AdapterError].
type Class string

// Class constants.
const (
	// ClassNotIndexed reports a path that the daemon does not yet
	// track as an indexed codebase.
	ClassNotIndexed Class = "not_indexed"

	// ClassCollectionMissing reports that the Milvus collection for
	// the codebase does not exist.
	ClassCollectionMissing Class = "collection_missing"

	// ClassCollectionNotReady reports that the Milvus collection
	// exists but is still loading or otherwise unavailable to serve
	// reads or writes.
	ClassCollectionNotReady Class = "collection_not_ready"

	// ClassSearchResultIncomplete reports that a semantic search
	// returned without all requested fields.
	ClassSearchResultIncomplete Class = "search_result_incomplete"

	// ClassMilvusUnavailable reports that the Milvus client is not
	// configured or cannot reach the configured address.
	ClassMilvusUnavailable Class = "milvus_unavailable"

	// ClassEmbedderUnreachable reports that the configured embedding
	// endpoint refused or timed out the request.
	ClassEmbedderUnreachable Class = "embedder_unreachable"

	// ClassInvalidPath reports a path argument that fails validation
	// (empty, malformed, or otherwise unusable).
	ClassInvalidPath Class = "invalid_path"

	// ClassInvalidArgument reports a required non-path argument that is
	// missing or empty (for example a search query or a job id).
	ClassInvalidArgument Class = "invalid_argument"

	// ClassConflictingJob reports that another job already owns the
	// effective indexing operation for this codebase.
	ClassConflictingJob Class = "conflicting_job"

	// ClassJobNotFound reports a job id that the daemon does not
	// track.
	ClassJobNotFound Class = "job_not_found"

	// ClassInternal is the catch-all class for unknown errors. The
	// message is sanitized at the boundary; the operator finds the
	// real cause in the daemon log by grepping trace_id.
	ClassInternal Class = "internal_error"
)

// CodeFor maps a class to its gRPC status code.
func CodeFor(class Class) codes.Code {
	switch class {
	case ClassNotIndexed, ClassJobNotFound:
		return codes.NotFound
	case ClassCollectionMissing, ClassCollectionNotReady, ClassConflictingJob:
		return codes.FailedPrecondition
	case ClassMilvusUnavailable, ClassEmbedderUnreachable:
		return codes.Unavailable
	case ClassInvalidPath, ClassInvalidArgument:
		return codes.InvalidArgument
	case ClassSearchResultIncomplete, ClassInternal:
		return codes.Internal
	default:
		return codes.Unknown
	}
}

// NewNotIndexed reports an operation against a codebase the daemon
// does not yet track.
func NewNotIndexed(path string, cause error) *AdapterError {
	return &AdapterError{
		Class:         ClassNotIndexed,
		Message:       "codebase " + quote(path) + " is not indexed",
		Code:          "not_indexed",
		Hint:          "run the index_codebase tool against this path first",
		Cause:         cause,
		SafeForClient: true,
	}
}

// NewEmbedderUnreachable reports a transient or fatal failure
// reaching the configured embedding endpoint.
func NewEmbedderUnreachable(cause error) *AdapterError {
	return &AdapterError{
		Class:         ClassEmbedderUnreachable,
		Message:       "embedding endpoint is unreachable",
		Code:          "embedder_unreachable",
		Hint:          "verify OPENAI_BASE_URL and that the endpoint serves the OpenAI embeddings API",
		Cause:         cause,
		SafeForClient: true,
	}
}

// NewInvalidPath reports a path argument that fails validation.
func NewInvalidPath(message string, cause error) *AdapterError {
	return &AdapterError{
		Class:         ClassInvalidPath,
		Message:       message,
		Code:          "invalid_path",
		Hint:          "pass an absolute path to a directory",
		Cause:         cause,
		SafeForClient: true,
	}
}

// NewMissingArgument reports a required argument that the caller left
// empty or omitted. name is the argument the operation needs, so the
// message points the caller at the exact field to supply.
func NewMissingArgument(name string) *AdapterError {
	return &AdapterError{
		Class:         ClassInvalidArgument,
		Message:       "missing required argument " + quote(name),
		Code:          "invalid_argument",
		Hint:          "supply a non-empty " + name,
		Cause:         nil,
		SafeForClient: true,
	}
}

// NewConflictingJob reports a duplicate indexing request the daemon
// rejects in favor of an in-flight job.
func NewConflictingJob(message string, cause error) *AdapterError {
	return &AdapterError{
		Class:         ClassConflictingJob,
		Message:       message,
		Code:          "conflicting_job",
		Hint:          "wait for the existing job to complete or cancel it before retrying",
		Cause:         cause,
		SafeForClient: true,
	}
}

// NewJobNotFound reports an operation against an unknown job id.
func NewJobNotFound(jobID string) *AdapterError {
	return &AdapterError{
		Class:         ClassJobNotFound,
		Message:       "job " + quote(jobID) + " not found",
		Code:          "job_not_found",
		Hint:          "list jobs with list_indexing_jobs to find the current id",
		Cause:         nil,
		SafeForClient: true,
	}
}

// NewInternal wraps an unknown error. The message is recorded in the
// daemon log; the boundary replaces it with a sanitized envelope.
func NewInternal(message string, cause error) *AdapterError {
	return &AdapterError{
		Class:         ClassInternal,
		Message:       message,
		Code:          "internal_error",
		Hint:          "",
		Cause:         cause,
		SafeForClient: false,
	}
}

func quote(value string) string {
	return "\"" + value + "\""
}
