// Package clock centralizes wall-clock access for testable runtime code.
package clock

import "time"

// Now returns the current UTC timestamp.
func Now() time.Time {
	return time.Now().UTC()
}
