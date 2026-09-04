//go:build linux

package platformactivity

import (
	"context"
	"errors"
	"math"
	"os"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"

	"goodkind.io/lm-semantic-search/internal/model"
)

type stubLoginSessionReader struct {
	sessions   []sessionActivity
	err        error
	closeCalls int
}

func (reader *stubLoginSessionReader) Read(context.Context) ([]sessionActivity, error) {
	return reader.sessions, reader.err
}

func (reader *stubLoginSessionReader) Close() {
	reader.closeCalls++
}

func TestLinuxActivitySelectsLocalActiveUserLoginSessions(t *testing.T) {
	currentUID := uint32(os.Getuid())
	reader := &stubLoginSessionReader{sessions: []sessionActivity{
		{UID: currentUID, Active: true, Class: "user", IdleHint: true, IdleSinceMonotonicUsec: 100},
		{UID: currentUID, Active: true, Class: "user", IdleHint: true, IdleSinceMonotonicUsec: 200},
		{UID: currentUID, Remote: true, Active: true, Class: "user", IdleHint: false, IdleSinceMonotonicUsec: 300},
		{UID: currentUID + 1, Active: true, Class: "user", IdleHint: false, IdleSinceMonotonicUsec: 300},
		{UID: currentUID, Class: "user", IdleHint: false, IdleSinceMonotonicUsec: 300},
		{UID: currentUID, Active: true, Class: "manager", IdleHint: false, IdleSinceMonotonicUsec: 300},
	}}
	source := newLinuxSource(context.Background(), reader, t.TempDir(), func() (uint64, error) {
		return 500, nil
	})

	snapshot := source.Sample(context.Background())

	if !snapshot.InputAvailable {
		t.Fatal("InputAvailable = false, want true")
	}
	if snapshot.InputIdleFor != 300*time.Microsecond {
		t.Fatalf("InputIdleFor = %s, want 300us", snapshot.InputIdleFor)
	}
}

func TestLoginSessionRequiresEverySelectedSessionIdle(t *testing.T) {
	currentUID := uint32(os.Getuid())
	reader := &stubLoginSessionReader{sessions: []sessionActivity{
		{UID: currentUID, Active: true, Class: "user", IdleHint: true, IdleSinceMonotonicUsec: 100},
		{UID: currentUID, Active: true, Class: "user", IdleHint: false, IdleSinceMonotonicUsec: 200},
	}}
	source := newLinuxSource(context.Background(), reader, t.TempDir(), func() (uint64, error) {
		return 500, nil
	})

	snapshot := source.Sample(context.Background())

	if !snapshot.InputAvailable {
		t.Fatal("InputAvailable = false, want true")
	}
	if snapshot.InputIdleFor != 0 {
		t.Fatalf("InputIdleFor = %s, want 0", snapshot.InputIdleFor)
	}
	if snapshot.InputReason != model.SchedulingReasonUserActive {
		t.Fatalf("InputReason = %q, want user active", snapshot.InputReason)
	}
}

func TestLoginSessionNoSelectedSessionIsUnavailable(t *testing.T) {
	reader := &stubLoginSessionReader{}
	source := newLinuxSource(context.Background(), reader, t.TempDir(), func() (uint64, error) {
		return 500, nil
	})

	snapshot := source.Sample(context.Background())

	assertInputUnavailable(t, snapshot)
}

func TestLinuxActivityFallbackExplainsUnavailableThermalState(t *testing.T) {
	source := newLinuxSource(context.Background(), nil, t.TempDir(), func() (uint64, error) {
		return 0, nil
	})

	snapshot := source.Sample(context.Background())

	if snapshot.ThermalAvailable {
		t.Fatal("ThermalAvailable = true, want false")
	}
	if snapshot.ThermalReason != inputUnavailableReason {
		t.Fatalf(
			"ThermalReason = %q, want %q",
			snapshot.ThermalReason,
			inputUnavailableReason,
		)
	}
}

func TestLoginSessionBusFailureIsUnavailable(t *testing.T) {
	reader := &stubLoginSessionReader{err: errors.New("system bus unavailable")}
	source := newLinuxSource(context.Background(), reader, t.TempDir(), func() (uint64, error) {
		return 500, nil
	})

	snapshot := source.Sample(context.Background())

	assertInputUnavailable(t, snapshot)
}

func TestLoginSessionInvalidMonotonicTimeIsUnavailable(t *testing.T) {
	reader := &stubLoginSessionReader{sessions: []sessionActivity{{
		UID:                    uint32(os.Getuid()),
		Active:                 true,
		Class:                  "user",
		IdleHint:               true,
		IdleSinceMonotonicUsec: 501,
	}}}
	source := newLinuxSource(context.Background(), reader, t.TempDir(), func() (uint64, error) {
		return 500, nil
	})

	snapshot := source.Sample(context.Background())

	assertInputUnavailable(t, snapshot)
}

func TestLoginSessionDurationOverflowIsUnavailable(t *testing.T) {
	reader := &stubLoginSessionReader{sessions: []sessionActivity{{
		UID:                    uint32(os.Getuid()),
		Active:                 true,
		Class:                  "user",
		IdleHint:               true,
		IdleSinceMonotonicUsec: 0,
	}}}
	overflowUsec := uint64(math.MaxInt64/int64(time.Microsecond)) + 1
	source := newLinuxSource(context.Background(), reader, t.TempDir(), func() (uint64, error) {
		return overflowUsec, nil
	})

	snapshot := source.Sample(context.Background())

	assertInputUnavailable(t, snapshot)
}

func TestLoginSessionInvalidPropertiesAreRejected(t *testing.T) {
	valid := login1PropertyVariants{
		login1RemoteProperty:                 dbus.MakeVariant(false),
		login1ActiveProperty:                 dbus.MakeVariant(true),
		login1ClassProperty:                  dbus.MakeVariant("user"),
		login1IdleHintProperty:               dbus.MakeVariant(true),
		login1IdleSinceMonotonicUsecProperty: dbus.MakeVariant(uint64(100)),
	}
	testCases := map[string]struct {
		name  string
		value dbus.Variant
	}{
		"remote":    {name: login1RemoteProperty, value: dbus.MakeVariant("false")},
		"active":    {name: login1ActiveProperty, value: dbus.MakeVariant(uint64(1))},
		"class":     {name: login1ClassProperty, value: dbus.MakeVariant(true)},
		"idle hint": {name: login1IdleHintProperty, value: dbus.MakeVariant("true")},
		"idle timestamp": {
			name:  login1IdleSinceMonotonicUsecProperty,
			value: dbus.MakeVariant(int64(100)),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			properties := make(login1PropertyVariants, len(valid))
			for propertyName, value := range valid {
				properties[propertyName] = value
			}
			properties[testCase.name] = testCase.value

			if _, err := login1SessionPropertiesFromVariants(properties); err == nil {
				t.Fatal("login1SessionPropertiesFromVariants returned no error")
			}
		})
	}
}

func TestLoginSessionDoesNotRequireIdlePropertiesForUnselectedSession(t *testing.T) {
	properties := login1PropertyVariants{
		login1RemoteProperty:                 dbus.MakeVariant(true),
		login1ActiveProperty:                 dbus.MakeVariant(true),
		login1ClassProperty:                  dbus.MakeVariant("user"),
		login1IdleHintProperty:               dbus.MakeVariant("invalid"),
		login1IdleSinceMonotonicUsecProperty: dbus.MakeVariant("invalid"),
	}

	parsed, err := login1SessionPropertiesFromVariants(properties)
	if err != nil {
		t.Fatalf("login1SessionPropertiesFromVariants: %v", err)
	}
	activity := sessionActivityFromProperties(501, parsed)
	if !activity.Remote {
		t.Fatal("Remote = false, want true")
	}
}

func TestLinuxActivityCloseClosesLoginConnection(t *testing.T) {
	reader := &stubLoginSessionReader{}
	source := newLinuxSource(context.Background(), reader, t.TempDir(), func() (uint64, error) {
		return 0, nil
	})

	source.Close()

	if reader.closeCalls != 1 {
		t.Fatalf("close calls = %d, want 1", reader.closeCalls)
	}
}

func assertInputUnavailable(t *testing.T, snapshot Snapshot) {
	t.Helper()
	if snapshot.InputAvailable {
		t.Fatal("InputAvailable = true, want false")
	}
	if snapshot.InputIdleFor != 0 {
		t.Fatalf("InputIdleFor = %s, want 0", snapshot.InputIdleFor)
	}
	if snapshot.InputReason != model.SchedulingReasonActivityUnavailable {
		t.Fatalf("InputReason = %q, want activity unavailable", snapshot.InputReason)
	}
}
