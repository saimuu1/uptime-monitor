// Package migrations embeds the goose SQL files so cmd/migrate can apply them
// from inside a container without the source tree present.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
