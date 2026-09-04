//go:build linux

package platformactivity

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/coreos/go-systemd/v22/login1"
	"golang.org/x/sys/unix"
)

const microsecondsPerSecond = uint64(1_000_000)

type login1SessionReader struct {
	connection *login1.Conn
}

func newLogin1SessionReader() (loginSessionReader, error) {
	connection, err := login1.New()
	if err != nil {
		return nil, fmt.Errorf("connect to login1: %w", err)
	}
	return &login1SessionReader{connection: connection}, nil
}

func (reader *login1SessionReader) Read(ctx context.Context) ([]sessionActivity, error) {
	sessions, err := reader.connection.ListSessionsContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("list login1 sessions: %w", err)
	}
	currentUID, available := currentUserID()
	if !available {
		return nil, fmt.Errorf("read current user ID")
	}
	activities := make([]sessionActivity, 0, len(sessions))
	for _, session := range sessions {
		if session.UID != currentUID {
			continue
		}
		properties, err := reader.connection.GetSessionPropertiesContext(ctx, session.Path)
		if err != nil {
			return nil, fmt.Errorf("read login1 session %s: %w", session.ID, err)
		}
		propertyValues := make(map[string]any, len(properties))
		for name, property := range properties {
			propertyValues[name] = property.Value()
		}
		activity, err := sessionActivityFromProperties(session.UID, propertyValues)
		if err != nil {
			return nil, fmt.Errorf("read login1 session %s: %w", session.ID, err)
		}
		activities = append(activities, activity)
	}
	return activities, nil
}

func (reader *login1SessionReader) Close() {
	reader.connection.Close()
}

func sessionActivityFromProperties(
	uid uint32,
	properties map[string]any,
) (sessionActivity, error) {
	remote, ok := properties["Remote"].(bool)
	if !ok {
		return sessionActivity{}, errorsForSessionProperty("Remote")
	}
	active, ok := properties["Active"].(bool)
	if !ok {
		return sessionActivity{}, errorsForSessionProperty("Active")
	}
	class, ok := properties["Class"].(string)
	if !ok {
		return sessionActivity{}, errorsForSessionProperty("Class")
	}
	activity := sessionActivity{
		UID:                    uid,
		Remote:                 remote,
		Active:                 active,
		Class:                  class,
		IdleHint:               false,
		IdleSinceMonotonicUsec: 0,
	}
	if remote || !active || class != "user" {
		return activity, nil
	}
	idleHint, ok := properties["IdleHint"].(bool)
	if !ok {
		return sessionActivity{}, errorsForSessionProperty("IdleHint")
	}
	idleSince, ok := properties["IdleSinceHintMonotonic"].(uint64)
	if !ok {
		return sessionActivity{}, errorsForSessionProperty("IdleSinceHintMonotonic")
	}
	activity.IdleHint = idleHint
	activity.IdleSinceMonotonicUsec = idleSince
	return activity, nil
}

func errorsForSessionProperty(name string) error {
	return fmt.Errorf("login1 property %s has an invalid variant", name)
}

func readMonotonicUsec() (uint64, error) {
	var current unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &current); err != nil {
		return 0, fmt.Errorf("read monotonic clock: %w", err)
	}
	if current.Sec < 0 || current.Nsec < 0 || current.Nsec >= int64(time.Second) {
		return 0, fmt.Errorf("read monotonic clock: invalid timespec")
	}
	seconds := uint64(current.Sec)
	microseconds := uint64(current.Nsec) / uint64(time.Microsecond)
	if seconds > (math.MaxUint64-microseconds)/microsecondsPerSecond {
		return 0, fmt.Errorf("read monotonic clock: duration overflow")
	}
	return seconds*microsecondsPerSecond + microseconds, nil
}
