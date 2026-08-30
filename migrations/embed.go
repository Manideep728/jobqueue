// Package migrations embeds the SQL migration files into the binary.
//
// Embedding means the server can migrate its own database on startup with no
// separate tool and no files to ship alongside the binary -- which is what makes
// `docker compose up` work with nothing else installed on the host.
package migrations

import "embed"

// FS holds every .sql file in this directory, applied in filename order.
//
//go:embed *.sql
var FS embed.FS
