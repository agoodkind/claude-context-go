//go:build darwin && !cgo

package platformactivity

import "goodkind.io/lm-semantic-search/internal/model"

// New returns an unavailable source when the native macOS bridge cannot build.
func New() Source {
	return NewUnavailable(model.SchedulingReasonActivityUnavailable)
}
