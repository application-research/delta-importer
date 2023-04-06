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
			log.Info("Starting Delta Dataset Importer")

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
	switch cfg.Mode {
	case ModePullDataset:
		// importerPullDataset(cfg, datasets)
	case ModePullCID:
		// importerPullCid(cfg, datasets)
	default:
		importerDefault(cfg, datasets)
	}
}

var alreadyAttempted = make(map[string]bool)

func importerDefault(cfg Config, datasets map[string]Dataset) {
	boost, err := NewBoostConnection(cfg.BoostAddress, cfg.BoostPort, cfg.BoostGqlPort, cfg.BoostAPIKey)
	if err != nil {
		log.Errorf("error creating boost connection: %s", err.Error())
		return
	}

	inProgress := boost.GetDealsInPipeline()

	if cfg.MaxConcurrent != 0 && len(inProgress) >= int(cfg.MaxConcurrent) {
		log.Debugf("skipping import as there are %d deals in progress (max_concurrent is %d)", len(inProgress), cfg.MaxConcurrent)
		return
	}

	toImport := boost.GetDealsAwaitingImport()

	log.Printf("%d deals awaiting import and %d deals in progress\n", len(toImport), len(inProgress))

	if len(toImport) == 0 {
		log.Debugf("nothing to do, no deals awaiting import")
		return
	}

	// Start with the last (oldest) deal
	i := len(toImport)
	// keep trying until we successfully manage to import a deal
	// this should usually simply take the first one, import it, and then return
	for i > 0 {
		i = i - 1
		deal := toImport[i]

		// Don't attempt more than once
		if alreadyAttempted[deal.PieceCid] {
			continue
		}
		alreadyAttempted[deal.PieceCid] = true

		// Check to see if the client address is in the dataset map
		thisDataset, ok := datasets[deal.ClientAddress]
		if !ok {
			log.Debugf("skipping import of %s as it is not in the datasets config", deal.PieceCid)
			continue
		}
		otherDeals := boost.GetDealsForContent(deal.PieceCid)
		if HasFailedDeals(otherDeals) {
			log.Debugf("skipping import of %s as there are mismatched CommP errors for it", deal.PieceCid)
			continue
		}

		filename := thisDataset.GenerateCarFileName(deal.PieceCid)
		if filename == "" {
			log.Debugf("could not find carfile name for dataset %s for CID %s", thisDataset.Dataset, deal.PieceCid)
			continue
		}

		if !FileExists(filename) {
			log.Debugf("could not find carfile %s for dataset %s for CID %s", filename, thisDataset.Dataset, deal.PieceCid)
			continue
		}

		id, err := uuid.Parse(deal.ID)
		if err != nil {
			log.Errorf("could not parse uuid " + deal.ID)
			continue
		}

		log.Debugf("importing uuid %v from %v\n", id, filename)
		boost.ImportCar(context.Background(), filename, id)
		break
	}
}
