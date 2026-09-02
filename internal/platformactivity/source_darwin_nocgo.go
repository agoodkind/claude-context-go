//go:build darwin && !cgo

package platformactivity

import "context"

// New returns an unavailable source when the native macOS bridge cannot build.
func New(context.Context) Source {
	return NewUnavailable("input activity unavailable")
}
