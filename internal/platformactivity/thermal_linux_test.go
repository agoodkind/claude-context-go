//go:build linux

package platformactivity

import (
	"os"
	"path/filepath"
	"testing"
)

func TestThermalZoneHotTripReportsSafe(t *testing.T) {
	thermalRoot := t.TempDir()
	writeThermalValue(t, thermalRoot, "thermal_zone0/temp", "70000\n")
	writeThermalValue(t, thermalRoot, "thermal_zone0/trip_point_0_type", "hot\n")
	writeThermalValue(t, thermalRoot, "thermal_zone0/trip_point_0_temp", "80000\n")

	reading := readThermalActivity(thermalRoot)

	if !reading.available {
		t.Fatal("available = false, want true")
	}
	if reading.unsafe {
		t.Fatal("unsafe = true, want false")
	}
}

func TestThermalZoneCriticalTripReportsUnsafeAtThreshold(t *testing.T) {
	thermalRoot := t.TempDir()
	writeThermalValue(t, thermalRoot, "thermal_zone0/temp", "90000")
	writeThermalValue(t, thermalRoot, "thermal_zone0/trip_point_2_type", "critical")
	writeThermalValue(t, thermalRoot, "thermal_zone0/trip_point_2_temp", "90000")

	reading := readThermalActivity(thermalRoot)

	if !reading.available {
		t.Fatal("available = false, want true")
	}
	if !reading.unsafe {
		t.Fatal("unsafe = false, want true")
	}
}

func TestThermalZonePairsTripTypeAndTemperatureByIndex(t *testing.T) {
	thermalRoot := t.TempDir()
	writeThermalValue(t, thermalRoot, "thermal_zone0/temp", "85000")
	writeThermalValue(t, thermalRoot, "thermal_zone0/trip_point_0_type", "hot")
	writeThermalValue(t, thermalRoot, "thermal_zone0/trip_point_1_temp", "80000")

	reading := readThermalActivity(thermalRoot)

	if reading.available {
		t.Fatal("available = true, want false")
	}
	if reading.unsafe {
		t.Fatal("unsafe = true, want false")
	}
}

func TestThermalZoneMalformedValuesAreUnavailable(t *testing.T) {
	testCases := map[string]struct {
		current string
		trip    string
	}{
		"current temperature": {current: "warm", trip: "80000"},
		"trip temperature":    {current: "70000", trip: "hot"},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			thermalRoot := t.TempDir()
			writeThermalValue(t, thermalRoot, "thermal_zone0/temp", testCase.current)
			writeThermalValue(t, thermalRoot, "thermal_zone0/trip_point_0_type", "hot")
			writeThermalValue(t, thermalRoot, "thermal_zone0/trip_point_0_temp", testCase.trip)

			reading := readThermalActivity(thermalRoot)

			if reading.available {
				t.Fatal("available = true, want false")
			}
			if reading.unsafe {
				t.Fatal("unsafe = true, want false")
			}
		})
	}
}

func TestThermalZoneWithoutUsableTripPairIsUnavailable(t *testing.T) {
	thermalRoot := t.TempDir()
	writeThermalValue(t, thermalRoot, "thermal_zone0/temp", "70000")
	writeThermalValue(t, thermalRoot, "thermal_zone0/trip_point_0_type", "passive")
	writeThermalValue(t, thermalRoot, "thermal_zone0/trip_point_0_temp", "65000")

	reading := readThermalActivity(thermalRoot)

	if reading.available {
		t.Fatal("available = true, want false")
	}
	if reading.unsafe {
		t.Fatal("unsafe = true, want false")
	}
}

func TestThermalZoneUsesAnyUsableHotOrCriticalTrip(t *testing.T) {
	thermalRoot := t.TempDir()
	writeThermalValue(t, thermalRoot, "thermal_zone0/temp", "malformed")
	writeThermalValue(t, thermalRoot, "thermal_zone0/trip_point_0_type", "hot")
	writeThermalValue(t, thermalRoot, "thermal_zone0/trip_point_0_temp", "80000")
	writeThermalValue(t, thermalRoot, "thermal_zone1/temp", "85000")
	writeThermalValue(t, thermalRoot, "thermal_zone1/trip_point_0_type", "critical")
	writeThermalValue(t, thermalRoot, "thermal_zone1/trip_point_0_temp", "80000")

	reading := readThermalActivity(thermalRoot)

	if !reading.available {
		t.Fatal("available = false, want true")
	}
	if !reading.unsafe {
		t.Fatal("unsafe = false, want true")
	}
}

func writeThermalValue(t *testing.T, root string, relativePath string, value string) {
	t.Helper()
	path := filepath.Join(root, relativePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create thermal directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatalf("write thermal value: %v", err)
	}
}
