package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/application-research/delta-importer/daemon/api"
	"github.com/application-research/delta-importer/db"
	didb "github.com/application-research/delta-importer/db"
	svc "github.com/application-research/delta-importer/services"
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

	go api.InitializeEchoRouterConfig(db, cfg.Port)

	for {
		log.Debugf("running import...")
		importer(cfg, db, ds)
		time.Sleep(time.Second * time.Duration(cfg.Interval))
	}
}

func reconcileImportedDeals(cfg Config, db *db.DIDB) {
	pendingDeals, err := db.GetDeals(didb.PENDING)

	if err != nil {
		log.Errorf("error getting pending deals: %s", err)
	}

	boost, err := svc.NewBoostConnection(cfg.BoostAddress, cfg.BoostPort, cfg.BoostGqlPort, cfg.BoostAPIKey)
	if err != nil {
		log.Errorf("error creating boost connection: %s", err.Error())
		return
	}
	defer boost.Close()

	for _, d := range *pendingDeals {
		deal, err := boost.GetDeal(d.DealUuid)
		if err != nil {
			log.Errorf("error getting deal %s: %s", d.DealUuid, err)
			continue
		}

		switch {
		case deal.Message == "Sealer: Proving":
			err = db.UpdateDeal(d.DealUuid, didb.SUCCESS, "")
			if err != nil {
				log.Errorf("error updating deal %s status to success: %s", d.DealUuid, err)
			}
		case strings.HasPrefix(deal.Message, "Error"):
			err = db.UpdateDeal(d.DealUuid, didb.FAILURE, "")
			if err != nil {
				log.Errorf("error updating deal %s status to failed: %s", d.DealUuid, err)
			}
		default:
			// No change, still pending
			log.Debugf("deal %s has status %s", d.DealUuid, deal.Message)
		}
	}

}
