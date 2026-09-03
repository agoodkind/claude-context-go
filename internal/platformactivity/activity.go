// Package platformactivity reports host input and thermal activity for quiet work.
package platformactivity

import (
	"context"
	"time"

	"goodkind.io/lm-semantic-search/internal/model"
)

// Snapshot is one input-idle and thermal-safety observation.
type Snapshot struct {
	InputAvailable   bool
	InputIdleFor     time.Duration
	InputReason      model.SchedulingReason
	ThermalAvailable bool
	ThermalUnsafe    bool
	ThermalReason    model.SchedulingReason
}

// Source samples host activity without returning transport errors.
type Source interface {
	Sample(context.Context) Snapshot
	Close()
}
