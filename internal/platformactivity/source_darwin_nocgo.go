//go:build darwin && !cgo

package platformactivity

// New returns an unavailable source when the native macOS bridge cannot build.
func New() Source {
	return NewUnavailable("input activity unavailable")
}
