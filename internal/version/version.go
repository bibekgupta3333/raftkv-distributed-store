// Package version holds build-stamped version info (set with -ldflags).
package version

import "fmt"

// Version and GitSHA are stamped at build time via -ldflags -X.
var (
	Version = "dev"
	GitSHA  = "unknown"
)

// String returns "<name> <version> (<sha>)".
func String(name string) string {
	return fmt.Sprintf("%s %s (%s)", name, Version, GitSHA)
}
