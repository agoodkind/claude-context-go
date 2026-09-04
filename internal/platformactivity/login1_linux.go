//go:build linux

package platformactivity

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/coreos/go-systemd/v22/login1"
	"github.com/godbus/dbus/v5"
	"golang.org/x/sys/unix"
)

const microsecondsPerSecond = uint64(1_000_000)

type login1SessionReader struct {
	connection *login1.Conn
}

type login1SessionProperties struct {
	remote                 bool
	active                 bool
	class                  string
	idleHint               bool
	idleSinceMonotonicUsec uint64
}

type login1PropertyVariants map[string]dbus.Variant

const (
	login1RemoteProperty                 = "Remote"
	login1ActiveProperty                 = "Active"
	login1ClassProperty                  = "Class"
	login1IdleHintProperty               = "IdleHint"
	login1IdleSinceMonotonicUsecProperty = "IdleSinceHintMonotonic"
)

func newLogin1SessionReader() (loginSessionReader, error) {
	connection, err := login1.New()
	if err != nil {
		slog.Error("connect to login1", "error", err)
		return nil, fmt.Errorf("connect to login1: %w", err)
	}
	return &login1SessionReader{connection: connection}, nil
}

func (reader *login1SessionReader) Read(ctx context.Context) ([]sessionActivity, error) {
	sessions, err := reader.connection.ListSessionsContext(ctx)
	if err != nil {
		slog.Error("list login1 sessions", "error", err)
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
			slog.Error("read login1 session properties", "error", err)
			return nil, fmt.Errorf("read login1 session %s: %w", session.ID, err)
		}
		parsedProperties, err := login1SessionPropertiesFromVariants(
			login1PropertyVariants(properties),
		)
		if err != nil {
			slog.Error("parse login1 session properties", "error", err)
			return nil, fmt.Errorf("read login1 session %s: %w", session.ID, err)
		}
		activity := sessionActivityFromProperties(session.UID, parsedProperties)
		activities = append(activities, activity)
	}
	return activities, nil
}

func (reader *login1SessionReader) Close() {
	reader.connection.Close()
}

func sessionActivityFromProperties(
	uid uint32,
	properties login1SessionProperties,
) sessionActivity {
	activity := sessionActivity{
		UID:                    uid,
		Remote:                 properties.remote,
		Active:                 properties.active,
		Class:                  properties.class,
		IdleHint:               false,
		IdleSinceMonotonicUsec: 0,
	}
	if properties.remote || !properties.active || properties.class != "user" {
		return activity
	}
	activity.IdleHint = properties.idleHint
	activity.IdleSinceMonotonicUsec = properties.idleSinceMonotonicUsec
	return activity
}

func login1SessionPropertiesFromVariants(
	properties login1PropertyVariants,
) (login1SessionProperties, error) {
	remote, err := login1BooleanProperty(properties, login1RemoteProperty)
	if err != nil {
		return login1SessionProperties{}, err
	}
	active, err := login1BooleanProperty(properties, login1ActiveProperty)
	if err != nil {
		return login1SessionProperties{}, err
	}
	class, err := login1StringProperty(properties, login1ClassProperty)
	if err != nil {
		return login1SessionProperties{}, err
	}
	parsed := login1SessionProperties{remote: remote, active: active, class: class}
	if remote || !active || class != "user" {
		return parsed, nil
	}
	idleHint, err := login1BooleanProperty(properties, login1IdleHintProperty)
	if err != nil {
		return login1SessionProperties{}, err
	}
	idleSinceMonotonicUsec, err := login1Uint64Property(
		properties,
		login1IdleSinceMonotonicUsecProperty,
	)
	if err != nil {
		return login1SessionProperties{}, err
	}
	parsed.idleHint = idleHint
	parsed.idleSinceMonotonicUsec = idleSinceMonotonicUsec
	return parsed, nil
}

func login1BooleanProperty(properties login1PropertyVariants, name string) (bool, error) {
	value, found := properties[name]
	if !found {
		return false, errorsForSessionProperty(name)
	}
	parsed, ok := value.Value().(bool)
	if !ok {
		return false, errorsForSessionProperty(name)
	}
	return parsed, nil
}

func login1StringProperty(properties login1PropertyVariants, name string) (string, error) {
	value, found := properties[name]
	if !found {
		return "", errorsForSessionProperty(name)
	}
	parsed, ok := value.Value().(string)
	if !ok {
		return "", errorsForSessionProperty(name)
	}
	return parsed, nil
}

func login1Uint64Property(properties login1PropertyVariants, name string) (uint64, error) {
	value, found := properties[name]
	if !found {
		return 0, errorsForSessionProperty(name)
	}
	parsed, ok := value.Value().(uint64)
	if !ok {
		return 0, errorsForSessionProperty(name)
	}
	return parsed, nil
}

func errorsForSessionProperty(name string) error {
	return fmt.Errorf("login1 property %s has an invalid variant", name)
}

func readMonotonicUsec() (uint64, error) {
	var current unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &current); err != nil {
		slog.Error("read monotonic clock", "error", err)
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
