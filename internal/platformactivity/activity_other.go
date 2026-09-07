package platformactivity

import "context"

const unavailableSourceReason = "platform activity source not installed"

type unavailableSource struct {
	reason string
}

// New returns the fallback source until a platform-specific source replaces it.
func New() Source {
	return NewUnavailable(unavailableSourceReason)
}

// NewUnavailable returns a source whose input and thermal signals stay unavailable.
func NewUnavailable(reason string) Source {
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
