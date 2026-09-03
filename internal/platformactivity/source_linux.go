//go:build linux

package platformactivity

import (
	"context"
	"math"
	"os"
	"time"

	"goodkind.io/lm-semantic-search/internal/model"
)

const (
	inputActiveReason      = model.SchedulingReasonUserActive
	inputUnavailableReason = model.SchedulingReasonActivityUnavailable
	linuxThermalRoot       = "/sys/class/thermal"
	thermalUnsafeReason    = model.SchedulingReasonThermalSafety
)

type sessionActivity struct {
	UID                    uint32
	Remote                 bool
	Active                 bool
	Class                  string
	IdleHint               bool
	IdleSinceMonotonicUsec uint64
}

type loginSessionReader interface {
	Read(context.Context) ([]sessionActivity, error)
	Close()
}

type monotonicClock func() (uint64, error)

type linuxSource struct {
	sessions     loginSessionReader
	thermalRoot  string
	monotonicNow monotonicClock
	fallback     Snapshot
}

// New returns the native Linux activity source.
func New() Source {
	sessions, err := newLogin1SessionReader()
	if err != nil {
		sessions = nil
	}
	return newLinuxSource(sessions, linuxThermalRoot, readMonotonicUsec)
}

func newLinuxSource(
	sessions loginSessionReader,
	thermalRoot string,
	monotonicNow monotonicClock,
) Source {
	return &linuxSource{
		sessions:     sessions,
		thermalRoot:  thermalRoot,
		monotonicNow: monotonicNow,
		fallback:     NewUnavailable(inputUnavailableReason).Sample(context.Background()),
	}
}

func (source *linuxSource) Sample(ctx context.Context) Snapshot {
	thermal := readThermalActivity(source.thermalRoot)
	snapshot := source.fallback
	snapshot.ThermalAvailable = thermal.available
	snapshot.ThermalUnsafe = thermal.available && thermal.unsafe
	if thermal.available {
		snapshot.ThermalReason = ""
	}
	if snapshot.ThermalUnsafe {
		snapshot.ThermalReason = thermalUnsafeReason
	}
	if source.sessions == nil {
		return snapshot
	}
	sessions, err := source.sessions.Read(ctx)
	if err != nil {
		return snapshot
	}
	monotonicNowUsec, err := source.monotonicNow()
	if err != nil {
		return snapshot
	}
	currentUID, available := currentUserID()
	if !available {
		return snapshot
	}
	idleFor, active, available := selectedSessionIdleFor(
		sessions,
		currentUID,
		monotonicNowUsec,
	)
	if !available {
		return snapshot
	}
	snapshot.InputAvailable = true
	snapshot.InputIdleFor = idleFor
	snapshot.InputReason = ""
	if active {
		snapshot.InputReason = inputActiveReason
	}
	return snapshot
}

func (source *linuxSource) Close() {
	if source.sessions != nil {
		source.sessions.Close()
	}
}

func selectedSessionIdleFor(
	sessions []sessionActivity,
	currentUID uint32,
	monotonicNowUsec uint64,
) (time.Duration, bool, bool) {
	selectedCount := 0
	minimumIdleUsec := uint64(math.MaxUint64)
	active := false
	maximumDurationUsec := uint64(math.MaxInt64 / int64(time.Microsecond))
	for _, session := range sessions {
		if session.UID != currentUID || session.Remote || !session.Active || session.Class != "user" {
			continue
		}
		selectedCount++
		if session.IdleSinceMonotonicUsec > monotonicNowUsec {
			return 0, false, false
		}
		idleUsec := monotonicNowUsec - session.IdleSinceMonotonicUsec
		if idleUsec > maximumDurationUsec {
			return 0, false, false
		}
		if idleUsec < minimumIdleUsec {
			minimumIdleUsec = idleUsec
		}
		if !session.IdleHint {
			active = true
		}
	}
	if selectedCount == 0 {
		return 0, false, false
	}
	if active {
		return 0, true, true
	}
	idleFor, available := durationFromMicroseconds(minimumIdleUsec)
	return idleFor, false, available
}

func currentUserID() (uint32, bool) {
	uid := os.Getuid()
	if uid < 0 {
		return 0, false
	}
	if uint64(uid) > math.MaxUint32 {
		return 0, false
	}
	return uint32(uid), true
}

func durationFromMicroseconds(microseconds uint64) (time.Duration, bool) {
	maximumDurationUsec := uint64(math.MaxInt64 / int64(time.Microsecond))
	if microseconds > maximumDurationUsec {
		return 0, false
	}
	return time.Duration(int64(microseconds)) * time.Microsecond, true
}
