package db

import (
	"database/sql"
	_ "embed"
	"fmt"
	"path/filepath"
	"time"

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
	PENDING = "PENDING"
	SUCCESS = "SUCCESS"
	FAILURE = "FAILED"
)

//go:embed create_db.sql
var dbSchema string

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
	TotalImported Stat      `json:"total_imported"`
	Pending       Stat      `json:"pending"`
	Success       Stat      `json:"success"`
	Failure       Stat      `json:"failure"`
	LastImported  time.Time `json:"last_imported_time"`
}

type Stat struct {
	Count sql.NullInt64 `json:"count"`
	Bytes sql.NullInt64 `json:"bytes"`
}

func (d *DIDB) GetDealStats() (DealStats, error) {
	var stats DealStats
	err := d.db.QueryRow(`
		SELECT
		  COUNT(*) AS total_imported_count,
		  SUM(size) AS total_imported_bytes,
		  COUNT(*) FILTER (WHERE state = $1) AS pending_count,
		  SUM(size) FILTER (WHERE state = $1) AS pending_bytes,
		  COUNT(*) FILTER (WHERE state = $2) AS success_count,
		  SUM(size) FILTER (WHERE state = $2) AS success_bytes,
		  COUNT(*) FILTER (WHERE state = $3) AS failed_count,
		  SUM(size) FILTER (WHERE state = $3) AS failed_bytes
		FROM
		  imported_deals`, PENDING, SUCCESS, FAILURE).
		Scan(&stats.TotalImported.Count, &stats.TotalImported.Bytes,
			&stats.Pending.Count, &stats.Pending.Bytes,
			&stats.Success.Count, &stats.Success.Bytes,
			&stats.Failure.Count, &stats.Failure.Bytes)
	if err != nil {
		return stats, fmt.Errorf("get deal stats: %w", err)
	}

	err = d.db.QueryRow(`
		SELECT 
			created_date
		FROM
			imported_deals
		ORDER BY crated_date
		LIMIT 1
	`).Scan(&stats.LastImported)
	if err != nil {
		return stats, fmt.Errorf("get last imported deal date: %w", err)
	}

	return stats, nil
}
