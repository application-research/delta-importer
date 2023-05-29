package daemon

import (
	"strings"
	"time"

	"github.com/application-research/delta-importer/db"
	didb "github.com/application-research/delta-importer/db"
	svc "github.com/application-research/delta-importer/services"
	log "github.com/sirupsen/logrus"
)

type DealReconciler struct {
	cfg      Config
	db       *db.DIDB
	interval uint
}

func NewDealReconciler(cfg Config, db *db.DIDB) *DealReconciler {
	return &DealReconciler{
		cfg:      cfg,
		db:       db,
		interval: cfg.Interval * 5, // Reconcile every 5 deal imports, should be reasonable
	}
}

func (dr *DealReconciler) Run() {
	for {
		dr.reconcileImportedDeals()
		time.Sleep(time.Second * time.Duration(dr.interval))
	}
}

func (dr *DealReconciler) reconcileImportedDeals() {
	pendingDeals, err := dr.db.GetDeals(didb.PENDING)

	if err != nil {
		log.Errorf("error getting pending deals: %s", err)
	}

	boost, err := svc.NewBoostConnection(dr.cfg.BoostAddress, dr.cfg.BoostPort, dr.cfg.BoostGqlPort, dr.cfg.BoostAPIKey, dr.cfg.StagingDir)
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
			err = dr.db.UpdateDeal(d.DealUuid, didb.SUCCESS, "")
			if err != nil {
				log.Errorf("error updating deal %s status to success: %s", d.DealUuid, err)
			}
		case strings.HasPrefix(deal.Message, "Error"):
			err = dr.db.UpdateDeal(d.DealUuid, didb.FAILURE, "")
			if err != nil {
				log.Errorf("error updating deal %s status to failed: %s", d.DealUuid, err)
			}
		default:
			// No change, still pending
			log.Debugf("deal %s has status %s", d.DealUuid, deal.Message)
		}
	}

}
