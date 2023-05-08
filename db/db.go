package db

import (
	"database/sql"
	_ "embed"
	"fmt"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
)

type DIDB struct {
	db *sql.DB
}

type DbImportedDeal struct {
	Id          int    `json:"id"`
	DealUuid    string `json:"deal_uuid"`
	CommP       string `json:"comm_p"`
	State       string `json:"state"`
	Mode        string `json:"mode"`
	Size        int64  `json:"size"`
	Message     string `json:"message"`
	CreatedDate string `json:"created_date"`
}

const (
	PENDING = "pending"
	SUCCESS = "success"
	FAILURE = "failed"
)

func OpenDIDB(root string) (*DIDB, error) {
	log.Debugf("using database file at %s", filepath.Join(root, "./delta-importer.db"))
	db, err := sql.Open("sqlite3", filepath.Join(root, "./delta-importer.db"))
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	err = setUpDBTables(db)
	if err != nil {
		return nil, fmt.Errorf("set up db tables: %w", err)
	}

	return &DIDB{
		db: db,
	}, nil
}

// Create the initial DB tables to set up a brand new db
func setUpDBTables(db *sql.DB) error {
	//go:embed create_db.sql
	var dbSchema string

	_, err := db.Exec(fmt.Sprintf(dbSchema))
	if err != nil {
		return err
	}

	return nil
}

// Store a deal in the DI database
func (d *DIDB) InsertDeal(dealUuid string, commP string, success bool, mode string, message string, size int64) error {
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

func (d *DIDB) GetDealStats() (DealStats, error) {
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
