//go:build !darwin && !linux

package platformactivity

import "context"

// New returns unavailable activity on unsupported platforms.
func New(context.Context) Source {
	return NewUnavailable("input activity unavailable")
}
