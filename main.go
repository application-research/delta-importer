package main

import (
	"context"
	"os"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name: "Delta Importer",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "boost-url",
				Usage:       "192.168.1.1",
				DefaultText: "http://localhost",
				Value:       "http://localhost",
				EnvVars:     []string{"BOOST_URL"},
			},
			&cli.StringFlag{
				Name:     "boost-auth-token",
				Usage:    "eyJ....XXX",
				Required: true,
				EnvVars:  []string{"BOOST_AUTH_TOKEN"},
			},
			&cli.StringFlag{
				Name:        "boost-gql-port",
				Usage:       "8080",
				DefaultText: "8080",
				Value:       "8080",
				EnvVars:     []string{"BOOST_GQL_PORT"},
			},
			&cli.StringFlag{
				Name:        "boost-port",
				Usage:       "1288",
				DefaultText: "1288",
				Value:       "1288",
				EnvVars:     []string{"BOOST_PORT"},
			},
			&cli.StringFlag{
				Name:        "datasets",
				Usage:       "filename for the datasets configuration file",
				Value:       "datasets.json",
				DefaultText: "datasets.json",
				EnvVars:     []string{"DATASETS"},
			},
			&cli.IntFlag{
				Name:    "max_concurrent",
				Usage:   "stop importing if # of deals in sealing pipeline are above this threshold. 0 = unlimited.",
				EnvVars: []string{"MAX_CONCURRENT"},
			},
			&cli.IntFlag{
				Name:     "interval",
				Usage:    "interval, in seconds, to re-run the importer",
				Required: true,
				EnvVars:  []string{"INTERVAL"},
			},
			&cli.StringFlag{
				Name:    "ddm-api",
				Usage:   "url of ddm api (required only for pull modes)",
				EnvVars: []string{"DDM_API"},
			},
			&cli.StringFlag{
				Name:    "ddm-token",
				Usage:   "dc002354-9acb-4f1d-bdec-b21bf4c2f36d",
				EnvVars: []string{"DDM_TOKEN"},
			},
			&cli.StringFlag{
				Name:        "mode",
				Usage:       "mode of operation (default | pull-dataset | pull-cid)",
				Value:       "default",
				DefaultText: "default",
				EnvVars:     []string{"MODE"},
			},
			&cli.BoolFlag{
				Name:    "debug",
				Usage:   "set to enable debug logging output",
				EnvVars: []string{"DEBUG"},
			},
		},

		Action: func(cctx *cli.Context) error {
			logo := `Δ 𝔻𝕖𝕝𝕥𝕒  𝕀𝕞𝕡𝕠𝕣𝕥𝕖𝕣`
			log.Info(logo)

			cfg, err := CreateConfig(cctx)
			if err != nil {
				return err
			}

			if cfg.Debug {
				log.SetLevel(log.DebugLevel)
			}

			ds := ReadInDatasetsFromFile(cfg.DatasetsFilename)
			log.Debugf("datasets: %+v", ds)

			for {
				log.Debugf("running import...")
				importer(cfg, ds)
				time.Sleep(time.Second * time.Duration(cfg.Interval))
			}
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func importer(cfg Config, datasets map[string]Dataset) {
	// We construct a new Boost connection at each run of the importer, as this is resilient in case boost is down/restarts
	// It will simply re-connect upon the next run of the importer
	boost, err := NewBoostConnection(cfg.BoostAddress, cfg.BoostPort, cfg.BoostGqlPort, cfg.BoostAPIKey)
	if err != nil {
		log.Errorf("error creating boost connection: %s", err.Error())
		return
	}
	defer boost.close()

	inProgress := boost.GetDealsInPipeline()

	if cfg.MaxConcurrent != 0 && len(inProgress) >= int(cfg.MaxConcurrent) {
		log.Debugf("skipping import job as there are already %d deals in progress (max_concurrent is %d)", len(inProgress), cfg.MaxConcurrent)
		return
	}

	log.Debugf("found %d deals in sealing pipeline", len(inProgress))
	successfulImport := false

	// Attempt to import a deal for each dataset in order - if any dataset fails, go to the next one
	for _, ds := range datasets {
		if ds.Skip {
			continue
		}

		log.Debugf("searching for a deal for dataset %s", ds.Dataset)

		switch cfg.Mode {
		case ModePullDataset:
			successfulImport = importerPullDataset(cfg, ds, boost)
		case ModePullCID:
			successfulImport = importerPullCid(cfg, ds, boost)
		default:
			successfulImport = importerDefault(cfg, ds, boost)
		}

		if successfulImport {
			break
		}
	}
}

var cidsAlreadyAttempted = make(map[string]bool)

func importerDefault(cfg Config, ds Dataset, boost *BoostConnection) bool {
	toImport := boost.GetDealsAwaitingImport(ds.Address)

	if len(toImport) == 0 {
		log.Debugf("skipping dataset %s : no deals awaiting import", ds.Dataset)
		return false
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

		// See if we have failed this CID before with mismatched commP
		otherDeals := boost.GetDealsForContent(deal.PieceCid)
		if HasMismatchedCommPErrors(otherDeals) {
			log.Debugf("skipping import of %s as there are mismatched CommP errors for it", deal.PieceCid)
			continue
		}

		filename := ds.GenerateCarFileName(deal.PieceCid)
		if filename == "" {
			log.Debugf("could not find carfile name for dataset %s for CID %s", ds.Dataset, deal.PieceCid)
			continue
		}

		if !FileExists(filename) {
			log.Debugf("could not find carfile %s for dataset %s for CID %s", filename, ds.Dataset, deal.PieceCid)
			continue
		}

		id, err := uuid.Parse(deal.ID)
		if err != nil {
			log.Errorf("could not parse uuid " + deal.ID)
			continue
		}

		succesfulImport := boost.ImportCar(context.Background(), filename, id)
		if succesfulImport {
			return true
		} else {
			log.Debugf("error importing car file for dataset %s", ds.Dataset)
			return false
		}
	}

	log.Errorf("attempted all deals for for dataset %s, none could be imported", ds.Dataset)
	return false
}

func importerPullDataset(cfg Config, ds Dataset, boost *BoostConnection) bool {
	ddm := NewDDMApi(cfg.DDMURL, cfg.DDMToken)

	pieceCid, err := ddm.RequestDealForDataset(ds.Dataset)
	if err != nil {
		log.Debugf("error requesting deal for dataset %s: %s", ds.Dataset, err.Error())
		return false
	}
	if pieceCid == "" {
		log.Debugf("no deal returned for dataset %s", ds.Dataset)
		return false
	}

	// Successfully requested a deal - wait for it to show up in Boost
	readyToImport, err := boost.WaitForDeal(pieceCid)
	if err != nil {
		log.Debugf("error waiting for deal for dataset %s: %s", ds.Dataset, err.Error())
		return false
	}

	// * Note: We don't need to check HasMismatchedCommPErrors here, as this should result in a newly requested deal. DDM should not allow multiple re-deals if it had been previously dealt

	// Deal has been made with boost - import it
	// There may be several deals matching the pieceCid, but we only want to import one - take the first one
	deal := readyToImport[0]
	filename := ds.GenerateCarFileName(pieceCid)

	if !FileExists(filename) {
		log.Debugf("could not find carfile %s for dataset %s for CID %s", filename, ds.Dataset, pieceCid)
		return false
	}

	id, err := uuid.Parse(deal.ID)
	if err != nil {
		log.Errorf("could not parse uuid " + deal.ID)
		return false
	}

	succesfulImport := boost.ImportCar(context.Background(), filename, id)
	if succesfulImport {
		return true
	} else {
		return false
	}
}

func importerPullCid(cfg Config, ds Dataset, boost *BoostConnection) bool {
	ddm := NewDDMApi(cfg.DDMURL, cfg.DDMToken)
	carFilePaths := ds.CarFilePaths()

	if len(carFilePaths) == 0 {
		log.Debugf("skipping dataset %s : no car files found", ds.Dataset)
		return false
	}

	log.Debugf("%d car files found for dataset %s", len(carFilePaths), ds.Dataset)

	for _, carFilePath := range carFilePaths {
		// Assume files are named as <cidFromFilename>.car
		cidFromFilename := FileNameFromPath(carFilePath)

		// Don't attempt any given carfile import more than once
		if cidsAlreadyAttempted[cidFromFilename] {
			continue
		}
		cidsAlreadyAttempted[cidFromFilename] = true

		// See if we have failed this CID before with mismatched commP
		otherDeals := boost.GetDealsForContent(cidFromFilename)
		if HasMismatchedCommPErrors(otherDeals) {
			log.Debugf("skipping import of %s as there are mismatched CommP errors for it", cidFromFilename)
			continue
		}

		pieceCid, err := ddm.RequestDealForCid(cidFromFilename)
		if err != nil {
			log.Debugf("error requesting deal for cid %s: %s", cidFromFilename, err.Error())
			return false
		}
		if pieceCid == "" {
			log.Debugf("no deal returned for dataset %s", ds.Dataset)
			return false
		}

		// Successfully requested a deal - wait for it to show up in Boost
		readyToImport, err := boost.WaitForDeal(pieceCid)
		if err != nil {
			log.Debugf("error waiting for deal for dataset %s: %s", ds.Dataset, err.Error())
			return false
		}

		// Deal has been made with boost - import it
		// There may be several deals matching the pieceCid, but we only want to import one - take the first one
		deal := readyToImport[0]

		// This should not happen as we just read the file, but check anyway in case the file has been deleted very recently
		if !FileExists(carFilePath) {
			log.Debugf("could not find carfile %s for dataset %s for CID %s. it must have been deleted", carFilePath, ds.Dataset, pieceCid)
			return false
		}

		id, err := uuid.Parse(deal.ID)
		if err != nil {
			log.Errorf("could not parse uuid " + deal.ID)
			return false
		}

		succesfulImport := boost.ImportCar(context.Background(), carFilePath, id)
		if succesfulImport {
			return true
		} else {
			return false
		}
	}

	log.Debugf("attempted to import all carfiles for dataset %d, but none could be imported", ds.Dataset)
	return false
}
