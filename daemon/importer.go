package daemon

import (
	"context"
	"time"

	"github.com/application-research/delta-importer/db"
	svc "github.com/application-research/delta-importer/services"
	util "github.com/application-research/delta-importer/util"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

func importer(cfg Config, db *db.DIDB, datasets map[string]Dataset) {
	// We construct a new Boost connection at each run of the importer, as this is resilient in case boost is down/restarts
	// It will simply re-connect upon the next run of the importer
	boost, err := svc.NewBoostConnection(cfg.BoostAddress, cfg.BoostPort, cfg.BoostGqlPort, cfg.BoostAPIKey)
	if err != nil {
		log.Errorf("error creating boost connection: %s", err.Error())
		return
	}
	defer boost.Close()

	inProgress := boost.GetDealsInPipeline()

	if cfg.MaxConcurrent != 0 && len(inProgress) >= int(cfg.MaxConcurrent) {
		log.Infof("skipping import job as there are already %d deals in progress (max_concurrent is %d)", len(inProgress), cfg.MaxConcurrent)
		return
	}

	log.Debugf("found %d deals in sealing pipeline", len(inProgress))

	var importResult *svc.ImportResult

	// Attempt to import a deal for each dataset in order - if any dataset fails, go to the next one
	for _, ds := range datasets {
		log.Debugf("searching for a deal for dataset %s", ds.Dataset)

		switch cfg.Mode {
		case ModePullDataset:
			importResult = importerPullDataset(cfg, ds, boost)
		case ModePullCID:
			importResult = importerPullCid(cfg, ds, boost)
		default:
			importResult = importerDefault(cfg, ds, boost)
		}

		if importResult != nil {
			db.InsertDeal(importResult.DealUuid, importResult.CommP, importResult.Successful, string(cfg.Mode), importResult.Message, importResult.FileSize)
		}

		if importResult != nil && importResult.Successful {
			break
		}
	}
}

var cidsAlreadyAttempted = make(map[string]bool)

func importerDefault(cfg Config, ds Dataset, boost *svc.BoostConnection) *svc.ImportResult {
	toImport := boost.GetDealsAwaitingImport(ds.Address)

	if len(toImport) == 0 {
		log.Debugf("skipping dataset %s : no deals awaiting import", ds.Dataset)
		return nil
	}

	log.Debugf("%d deals awaiting import for dataset %s", len(toImport), ds.Dataset)

	// Start with the last (oldest) deal
	i := len(toImport)
	// keep trying until we successfully manage to import a deal
	// this should usually simply take the first one, import it, and then return
	for i > 0 {
		i = i - 1
		deal := toImport[i]

		// Don't attempt more than once
		if cidsAlreadyAttempted[deal.PieceCid] {
			continue
		}
		cidsAlreadyAttempted[deal.PieceCid] = true

		if deal.StartEpoch.IntoUnix() < time.Now().Add(MIN_SEALING_TIME).Unix() {
			log.Debugf("skipping deal %s as it would be past the start epoch when sealing completes", deal.ID)
			continue
		}

		// See if we have failed this CID before with mismatched commP
		otherDeals := boost.GetDealsForContent(deal.PieceCid)
		if otherDeals.HasMismatchedCommPErrors() {
			log.Debugf("skipping import of %s as there are mismatched CommP errors for it", deal.PieceCid)
			continue
		}

		filename := ds.GenerateCarFileName(deal.PieceCid)
		if filename == "" {
			log.Errorf("could not find carfile name for dataset %s for CID %s", ds.Dataset, deal.PieceCid)
			continue
		}

		if !util.FileExists(filename) {
			log.Errorf("could not find carfile %s for dataset %s for CID %s", filename, ds.Dataset, deal.PieceCid)
			continue
		}

		id, err := uuid.Parse(deal.ID)
		if err != nil {
			log.Errorf("could not parse uuid " + deal.ID)
			continue
		}

		importResult := boost.ImportCar(context.Background(), filename, deal.PieceCid, id)
		return &importResult
	}

	log.Infof("attempted all deals for for dataset %s, none could be imported", ds.Dataset)
	return nil
}

func importerPullDataset(cfg Config, ds Dataset, boost *svc.BoostConnection) *svc.ImportResult {
	ddm := svc.NewDDMApi(cfg.DDMURL, cfg.DDMToken)

	log.Infof("requesting deal for dataset %s", ds.Dataset)
	pieceCid, err := ddm.RequestDealForDataset(ds.Dataset)
	if err != nil {
		log.Errorf("error requesting deal for dataset %s: %s", ds.Dataset, err.Error())
		return nil
	}
	if pieceCid == "" {
		log.Errorf("no deal returned for dataset %s", ds.Dataset)
		return nil
	}

	// Successfully requested a deal - wait for it to show up in Boost
	readyToImport, err := boost.WaitForDeal(pieceCid)
	if err != nil {
		log.Errorf("error waiting for deal for dataset %s: %s", ds.Dataset, err.Error())
		return nil
	}

	// * Note: We don't need to check HasMismatchedCommPErrors here, as this should result in a newly requested deal. DDM should not allow multiple re-deals if it had been previously dealt

	// Deal has been made with boost - import it
	// There may be several deals matching the pieceCid, but we only want to import one - take the first one
	deal := readyToImport[0]
	filename := ds.GenerateCarFileName(pieceCid)

	if !util.FileExists(filename) {
		log.Debugf("could not find carfile %s for dataset %s for CID %s", filename, ds.Dataset, pieceCid)
		return nil
	}

	id, err := uuid.Parse(deal.ID)
	if err != nil {
		log.Errorf("could not parse uuid " + deal.ID)
		return nil
	}

	importResult := boost.ImportCar(context.Background(), filename, pieceCid, id)
	return &importResult
}

func importerPullCid(cfg Config, ds Dataset, boost *svc.BoostConnection) *svc.ImportResult {
	ddm := svc.NewDDMApi(cfg.DDMURL, cfg.DDMToken)
	carFilePaths := ds.CarFilePaths()

	ds.PopulateAlreadyImportedCids(boost)

	if len(carFilePaths) == 0 {
		log.Debugf("skipping dataset %s : no car files found", ds.Dataset)
		return nil
	}

	log.Debugf("%d car files found for dataset %s", len(carFilePaths), ds.Dataset)

	for _, carFilePath := range carFilePaths {
		// Assume files are named as <cidFromFilename>.car
		cidFromFilename := util.FileNameFromPath(carFilePath)

		if ds.IsCidAlreadyImported(cidFromFilename) {
			log.Debugf("skipping import of %s as it's already been imported previously", cidFromFilename)
			continue
		}

		// Don't attempt any given carfile import more than once
		if cidsAlreadyAttempted[cidFromFilename] {
			continue
		}
		cidsAlreadyAttempted[cidFromFilename] = true

		// See if we have failed this CID before with mismatched commP
		otherDeals := boost.GetDealsForContent(cidFromFilename)
		if otherDeals.HasMismatchedCommPErrors() {
			log.Debugf("skipping import of %s as there are mismatched CommP errors for it", cidFromFilename)
			continue
		}

		log.Infof("requesting deal for dataset %s, cid %s", ds.Dataset, cidFromFilename)
		pieceCid, err := ddm.RequestDealForCid(cidFromFilename)
		if err != nil {
			log.Errorf("error requesting deal for cid %s: %s", cidFromFilename, err.Error())
			return nil
		}
		if pieceCid == "" {
			log.Errorf("no deal returned for dataset %s", ds.Dataset)
			return nil
		}

		// Successfully requested a deal - wait for it to show up in Boost
		readyToImport, err := boost.WaitForDeal(pieceCid)
		if err != nil {
			log.Errorf("error waiting for deal for dataset %s: %s", ds.Dataset, err.Error())
			return nil
		}

		// Deal has been made with boost - import it
		// There may be several deals matching the pieceCid, but we only want to import one - take the first one
		deal := readyToImport[0]

		// This should not happen as we just read the file, but check anyway in case the file has been deleted very recently
		if !util.FileExists(carFilePath) {
			log.Errorf("could not find carfile %s for dataset %s for CID %s. it must have been deleted", carFilePath, ds.Dataset, pieceCid)
			return nil
		}

		id, err := uuid.Parse(deal.ID)
		if err != nil {
			log.Errorf("could not parse uuid " + deal.ID)
			return nil
		}

		importResult := boost.ImportCar(context.Background(), carFilePath, pieceCid, id)
		return &importResult
	}

	log.Infof("attempted to import all carfiles for dataset %d, but none could be imported", ds.Dataset)
	return nil
}
