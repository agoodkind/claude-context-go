// Package platformactivity reports host input and thermal activity for quiet work.
package platformactivity

import (
	"context"
	"time"
)

// Snapshot is one input-idle and thermal-safety observation.
type Snapshot struct {
	InputAvailable   bool
	InputIdleFor     time.Duration
	InputReason      string
	ThermalAvailable bool
	ThermalUnsafe    bool
	ThermalReason    string
}

// Source samples host activity without returning transport errors.
type Source interface {
	Sample(context.Context) Snapshot
	Close()
}
