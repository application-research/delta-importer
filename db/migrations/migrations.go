package migrations

import (
	"database/sql"
	"embed"

	"github.com/pressly/goose/v3"
	log "github.com/sirupsen/logrus"
)

////go:embed *.sql
var EmbedMigrations embed.FS

func Migrate(sqldb *sql.DB) error {
	goose.SetBaseFS(EmbedMigrations)

	if err := goose.SetDialect("sqlite3"); err != nil {
		return err
	}

	beforeVer, err := goose.GetDBVersion(sqldb)
	if err != nil {
		return err
	}

	if err := goose.Up(sqldb, "."); err != nil {
		return err
	}

	afterVer, err := goose.GetDBVersion(sqldb)
	if err != nil {
		return err
	}

	if beforeVer != afterVer {
		log.Warn("delta-importer sqlite3 migrated", "previous", beforeVer, "current", afterVer)
	}

	return nil
}
