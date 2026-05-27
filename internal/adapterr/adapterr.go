// Package adapterr is the daemon's structured error boundary. Known
// failure classes carry a clear message, a stable code, and a hint
// the operator can act on. Unknown failures wrap the underlying error
// behind a sanitized public message and a reference to the daemon
// logs so the operator can grep by trace_id.
package adapterr

// AdapterError is the typed error every daemon path returns from its
// external boundary. Construct via the per-class helpers in this
// package; the [Class] discriminates known classes from the catch-all
// [ClassInternal].
type AdapterError struct {
	// Class is the closed-set classification used to map the error to
	// a gRPC code and to decide whether the message is safe to show
	// the client. Required.
	Class Class

	// Message is the human-readable description. Shown to the client
	// only when SafeForClient is true; otherwise it lives in the
	// daemon log.
	Message string

	// Code is a stable machine-readable identifier (snake_case) that
	// clients use for programmatic routing. Required for known
	// classes; "internal_error" for [ClassInternal].
	Code string

	// Hint is operator-facing guidance appended after the message in
	// the safe-for-client envelope. Empty when no action is needed.
	Hint string

	// Cause is the underlying error this error wraps. Recoverable
	// via [errors.Unwrap]. May be nil.
	Cause error

	// SafeForClient reports whether Message can be shown verbatim.
	// False means the boundary sanitizes the message and replaces it
	// with an "internal error" reference to the daemon logs.
	SafeForClient bool
}

// Error renders the class plus message and, when present, the cause.
func (e *AdapterError) Error() string {
	if e == nil {
		return "<nil adapter error>"
	}
	out := string(e.Class) + ": " + e.Message
	if e.Cause != nil {
		out += ": " + e.Cause.Error()
	}
	return out
}

// Unwrap exposes [AdapterError.Cause] for [errors.Is] and [errors.As].
func (e *AdapterError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Is reports class equality so a typed sentinel matches every
// constructor call that produces the same class.
func (e *AdapterError) Is(target error) bool {
	if e == nil {
		return target == nil
	}
	other, ok := target.(*AdapterError)
	if !ok {
		return false
	}
	return e.Class == other.Class
}

// MCPError is the MCP-shaped error returned by [RespondMCP]. The
// Class, Code, TraceID, and JobID fields let tool callers route on
// structured fields. Message holds the safe-for-client envelope.
type MCPError struct {
	Class   Class
	Code    string
	Message string
	TraceID string
	JobID   string
}

// Error returns the safe-for-client message.
func (e *MCPError) Error() string {
	if e == nil {
		return "<nil mcp error>"
	}
	return e.Message
}
