// Package web holds the status-page assets. Templates are embedded so cmd/web
// compiles to a single self-contained binary.
package web

import "embed"

//go:embed templates/*.html
var Templates embed.FS
