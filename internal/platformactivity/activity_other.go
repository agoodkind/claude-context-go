package platformactivity

import (
	"context"

	"goodkind.io/lm-semantic-search/internal/model"
)

type unavailableSource struct {
	reason model.SchedulingReason
}

// NewUnavailable returns a source whose input and thermal signals stay unavailable.
func NewUnavailable(reason model.SchedulingReason) Source {
	return &unavailableSource{reason: reason}
}

func (source *unavailableSource) Sample(context.Context) Snapshot {
	return Snapshot{
		InputAvailable:   false,
		InputIdleFor:     0,
		InputReason:      source.reason,
		ThermalAvailable: false,
		ThermalUnsafe:    false,
		ThermalReason:    source.reason,
	}
}

func (source *unavailableSource) Close() {}
