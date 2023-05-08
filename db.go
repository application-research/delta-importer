package main

import (
	"database/sql"
	"fmt"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

type diDB struct {
	db *sql.DB
}

const dbSchema = `
	/* imported_deals */
	CREATE TABLE IF NOT EXISTS imported_deals (
		id integer PRIMARY KEY AUTOINCREMENT,
		deal_uuid VARCHAR(255),
		comm_p VARCHAR(255) NOT NULL,
		state VARCHAR(255) NOT NULL,
		mode VARCHAR(255) NOT NULL,
		message TEXT,
		created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
`

type DbImportedDeal struct {
	Id          int    `json:"id"`
	DealUuid    string `json:"deal_uuid"`
	CommP       string `json:"comm_p"`
	State       string `json:"state"`
	Mode        Mode   `json:"mode"`
	Message     string `json:"message"`
	CreatedDate string `json:"created_date"`
}

const (
	PENDING = "pending"
	SUCCESS = "success"
	FAILURE = "failed"
)

func OpenDiDB(root string) (*diDB, error) {
	log.Debugf("using database file at %s", filepath.Join(root, "./delta-importer.db"))
	db, err := sql.Open("sqlite3", filepath.Join(root, "./delta-importer.db"))
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	_, err = db.Exec(fmt.Sprintf(dbSchema))
	if err != nil {
		return nil, fmt.Errorf("exec schema: %w", err)
	}

	return &diDB{
		db: db,
	}, nil
}

func (d *diDB) InsertDeal(dealUuid string, commP string, success bool, mode Mode) error {
	var state string
	if success {
		state = PENDING
	} else {
		state = FAILURE
	}
	_, err := d.db.Exec("INSERT INTO imported_deals (deal_uuid, comm_p, state, mode) VALUES (?, ?, ?, ?)", dealUuid, commP, state, mode)

	if err != nil {
		return fmt.Errorf("insert deal: %w", err)
	}
	return nil
}

type DealStats struct {
	TotalImported int
}

func (d *diDB) GetDealStats() (DealStats, error) {
	var stats DealStats
	err := d.db.QueryRow("SELECT COUNT(*) FROM imported_deals").Scan(&stats.TotalImported)
	if err != nil {
		return DealStats{}, fmt.Errorf("get deal stats: %w", err)
	}
	return stats, nil
}
