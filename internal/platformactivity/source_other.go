//go:build !darwin && !linux

package platformactivity

// New returns unavailable activity on unsupported platforms.
func New() Source {
	return NewUnavailable("input activity unavailable")
}
