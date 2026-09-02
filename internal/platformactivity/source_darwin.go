//go:build darwin && cgo

package platformactivity

/*
#cgo LDFLAGS: -framework CoreGraphics -framework Foundation
#include "activity_bridge_darwin.h"
*/
import "C"

import (
	"context"
	"math"
	"time"
)

const (
	inputUnavailableReason = "input activity unavailable"
	thermalUnsafeReason    = "thermal state unsafe"
)

type darwinSource struct {
	unavailable Source
}

type nativeActivityResult struct {
	idleSeconds      float64
	inputAvailable   bool
	thermalAvailable bool
	thermalUnsafe    bool
}

var readNativeActivity = readNativeActivityBridge

// New returns the native macOS activity source.
func New() Source {
	return &darwinSource{unavailable: NewUnavailable(inputUnavailableReason)}
}

func (source *darwinSource) Sample(ctx context.Context) Snapshot {
	result := readNativeActivity()
	idleFor, inputAvailable := durationForIdleSeconds(result)
	snapshot := Snapshot{
		InputAvailable:   inputAvailable,
		InputIdleFor:     0,
		InputReason:      "",
		ThermalAvailable: result.thermalAvailable,
		ThermalUnsafe:    result.thermalAvailable && result.thermalUnsafe,
		ThermalReason:    "",
	}
	if snapshot.InputAvailable {
		snapshot.InputIdleFor = idleFor
	} else {
		snapshot.InputReason = source.unavailable.Sample(ctx).InputReason
	}
	if snapshot.ThermalUnsafe {
		snapshot.ThermalReason = thermalUnsafeReason
	}
	return snapshot
}

func (source *darwinSource) Close() {
	source.unavailable.Close()
}

func durationForIdleSeconds(result nativeActivityResult) (time.Duration, bool) {
	if !result.inputAvailable || math.IsNaN(result.idleSeconds) ||
		math.IsInf(result.idleSeconds, 0) || result.idleSeconds < 0 {
		return 0, false
	}
	idleNanoseconds := result.idleSeconds * float64(time.Second)
	maximumSafeNanoseconds := math.Nextafter(float64(math.MaxInt64), 0)
	if idleNanoseconds > maximumSafeNanoseconds {
		return 0, false
	}
	return time.Duration(idleNanoseconds), true
}

func readNativeActivityBridge() nativeActivityResult {
	result := C.lms_activity_read()
	return nativeActivityResult{
		idleSeconds:      float64(result.idle_seconds),
		inputAvailable:   result.input_available != 0,
		thermalAvailable: result.thermal_available != 0,
		thermalUnsafe:    result.thermal_unsafe != 0,
	}
}
