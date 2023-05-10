package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/application-research/delta-importer/daemon/api"
	"github.com/application-research/delta-importer/db"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

func RunDaemon(cctx *cli.Context) error {
	cfg, err := CreateConfig(cctx)
	if err != nil {
		return err
	}

	if cfg.Debug {
		log.SetLevel(log.DebugLevel)
	}

	logFileLocation := cfg.Log
	if logFileLocation != "" {
		f, err := os.OpenFile(logFileLocation, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			log.Errorf("error accessing the specified log file: %s", err)
		} else {
			log.SetOutput(f)
			log.Debugf("set log output to %s", logFileLocation)
		}
	} else {
		log.Infof("log file not specified. outputting logs only to terminal")
	}

	ds := ReadInDatasetsFromFile(filepath.Join(cfg.DataDir + "/datasets.json"))
	log.Debugf("datasets: %+v", ds)

	db, err := db.OpenDIDB(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("error opening db: %w", err)
	}

	api.InitializeEchoRouterConfig(db, cfg.Port)

	for {
		log.Debugf("running import...")
		importer(cfg, db, ds)
		time.Sleep(time.Second * time.Duration(cfg.Interval))
	}
}
