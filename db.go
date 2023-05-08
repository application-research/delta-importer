package main

import (
	"database/sql"
	"fmt"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
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
		size BIGINT,
		message TEXT,
		created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
`

type DbImportedDeal struct {
	Id          int    `json:"id"`
	DealUuid    string `json:"deal_uuid"`
	CommP       string `json:"comm_p"`
	State       string `json:"state"`
	Mode        Mode   `json:"mode"`
	Size        int64  `json:"size"`
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

func (d *diDB) InsertDeal(dealUuid string, commP string, success bool, mode Mode, message string, size int64) error {
	var state string
	if success {
		state = PENDING
	} else {
		state = FAILURE
	}
	_, err := d.db.Exec("INSERT INTO imported_deals (deal_uuid, comm_p, state, mode, message, size) VALUES (?, ?, ?, ?, ?, ?)", dealUuid, commP, state, mode, message, size)

	if err != nil {
		return fmt.Errorf("insert deal: %w", err)
	}
	return nil
}

type DealStats struct {
	TotalImported Stat
	Pending       Stat
	Success       Stat
	Failed        Stat
}

type Stat struct {
	Count uint
	Bytes uint
}

func (d *diDB) GetDealStats() (DealStats, error) {
	var stats DealStats
	err := d.db.QueryRow("SELECT COUNT(*), SUM(size) FROM imported_deals").Scan(&stats.TotalImported.Count, &stats.TotalImported.Bytes)
	if err != nil {
		return stats, fmt.Errorf("get total deal stats: %w", err)
	}

	err = d.db.QueryRow("SELECT COUNT(*), SUM(size) FROM imported_deals WHERE state = 'PENDING'").Scan(&stats.Pending.Count, &stats.Pending.Bytes)
	if err != nil {
		return stats, fmt.Errorf("get pending deal stats: %w", err)
	}

	err = d.db.QueryRow("SELECT COUNT(*), SUM(size) FROM imported_deals WHERE state = 'SUCCESS'").Scan(&stats.Success.Count, &stats.Success.Bytes)
	if err != nil {
		return stats, fmt.Errorf("get success deal stats: %w", err)
	}

	err = d.db.QueryRow("SELECT COUNT(*), SUM(size) FROM imported_deals WHERE state = 'FAILED'").Scan(&stats.Failed.Count, &stats.Failed.Bytes)
	if err != nil {
		return stats, fmt.Errorf("get failed deal stats: %w", err)
	}

	return stats, nil
}
