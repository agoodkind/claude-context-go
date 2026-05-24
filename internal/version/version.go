// Package version holds build metadata stamped by the Go build pipeline.
package version

var (
	// Commit is the source revision used for this build.
	Commit = "dev"
	// Version is the semantic version or describe string for this build.
	Version = "dev"
	// Dirty reports whether the source tree was dirty during build.
	Dirty = "false"
	// BuildTime is the UTC build timestamp.
	BuildTime = ""
)
