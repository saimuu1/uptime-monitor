// Command migrate applies the embedded goose migrations and exits. It runs as a
// one-shot step (in docker-compose and on the cloud box) so the schema is in
// place before the other services start. Idempotent: safe to run every boot.
package main

import (
	"database/sql"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/pressly/goose/v3"

	"github.com/saimuu1/uptime-monitor/internal/env"
	"github.com/saimuu1/uptime-monitor/migrations"
)

func main() {
	db, err := sql.Open("pgx", env.DBURL())
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		log.Fatalf("dialect: %v", err)
	}
	if err := goose.Up(db, "."); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	log.Print("migrations up to date")
}
