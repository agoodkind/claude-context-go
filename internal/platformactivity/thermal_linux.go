//go:build linux

package platformactivity

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type thermalActivity struct {
	available bool
	unsafe    bool
}

func readThermalActivity(root string) thermalActivity {
	zonePaths, err := filepath.Glob(filepath.Join(root, "thermal_zone*"))
	if err != nil {
		return thermalActivity{available: false, unsafe: false}
	}
	reading := thermalActivity{available: false, unsafe: false}
	for _, zonePath := range zonePaths {
		currentTemperature, err := readThermalInteger(filepath.Join(zonePath, "temp"))
		if err != nil {
			continue
		}
		entries, err := os.ReadDir(zonePath)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasPrefix(name, "trip_point_") || !strings.HasSuffix(name, "_type") {
				continue
			}
			tripTypeBytes, err := os.ReadFile(filepath.Join(zonePath, name))
			if err != nil {
				continue
			}
			tripType := strings.TrimSpace(string(tripTypeBytes))
			if tripType != "hot" && tripType != "critical" {
				continue
			}
			tripTemperatureName := strings.TrimSuffix(name, "_type") + "_temp"
			tripTemperature, err := readThermalInteger(
				filepath.Join(zonePath, tripTemperatureName),
			)
			if err != nil {
				continue
			}
			reading.available = true
			if currentTemperature >= tripTemperature {
				reading.unsafe = true
			}
		}
	}
	return reading
}

func readThermalInteger(path string) (int64, error) {
	value, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read thermal value %s: %w", path, err)
	}
	reading, err := strconv.ParseInt(strings.TrimSpace(string(value)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse thermal value %s: %w", path, err)
	}
	return reading, nil
}
