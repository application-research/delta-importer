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
				Name:     "boost-url",
				Usage:    "192.168.1.1",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "boost-auth-token",
				Usage:    "eyJ....XXX",
				Required: true,
			},
			&cli.StringFlag{
				Name:        "boost-gql-port",
				Usage:       "8080",
				DefaultText: "8080",
			},
			&cli.StringFlag{
				Name:        "boost-port",
				Usage:       "1288",
				DefaultText: "1288",
			},
			&cli.StringFlag{
				Name:        "datasets",
				Usage:       "filename for the datasets configuration file",
				Value:       "datasets.json",
				DefaultText: "datasets.json",
			},
			&cli.IntFlag{
				Name:  "max_concurrent",
				Usage: "stop importing if # of deals in sealing pipeline are above this threshold. 0 = unlimited.",
			},
			&cli.IntFlag{
				Name:     "interval",
				Usage:    "interval, in seconds, to re-run the importer",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "ddm-api",
				Usage: "url of ddm api (required only for pull modes)",
			},
			&cli.StringFlag{
				Name:  "ddm-token",
				Usage: "dc002354-9acb-4f1d-bdec-b21bf4c2f36d",
			},
			&cli.StringFlag{
				Name:        "mode",
				Usage:       "mode of operation (default | pull-dataset | pull-cid)",
				Value:       "default",
				DefaultText: "default",
			},
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "set to enable debug logging output",
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

			for {
				log.Debugf("running import")
				importer(boost_address, boost_port, gql_port, boost_api_key, base_directory, max_concurrent)
				time.Sleep(time.Second * time.Duration(interval))
			}
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

var alreadyAttempted = make(map[string]bool)

func importer(boost_address string, boost_port string, gql_port string, boost_api_key string, base_directory string, max_concurrent int) {
	boost, err := NewBoostConnection(boost_address, boost_port, gql_port, boost_api_key)
	if err != nil {
		log.Fatal(err)
	}

	inProgress := boost.GetDealsInPipeline()

	if max_concurrent != 0 && len(inProgress) >= max_concurrent {
		log.Debugf("skipping import as there are %d deals in progress (max_concurrent is %d)", len(inProgress), max_concurrent)
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

		otherDeals := boost.GetDealsForContent(deal.PieceCid)
		if HasFailedDeals(otherDeals) {
			log.Debugf("skipping import of %s as there are mismatched CommP errors for it", deal.PieceCid)
			continue
		}

		filename := GenerateCarFileName(base_directory, deal.PieceCid, deal.ClientAddress)
		if filename == "" {
			continue
		}

		if !FileExists(filename) {
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
