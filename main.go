package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var Commit string
var Version string

var Reset = "\033[0m"
var Purple = "\033[35m"
var Cyan = "\033[36m"
var Gray = "\033[37m"
var White = "\033[97m"
var Red = "\033[31m"
var Green = "\033[32m"

func main() {

	app := &cli.App{
		Name:    "Delta Importer",
		Usage:   "A tool to automate the ingestion of offline/import deals into boost",
		Version: fmt.Sprintf("%s+git.%s\n", Version, Commit),
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
			&cli.StringFlag{
				Name:    "log",
				Usage:   "log file to write to",
				EnvVars: []string{"LOG"},
			},
			&cli.StringFlag{
				Name:        "dir",
				Usage:       "directory to store local files in",
				Value:       "~/.delta/importer",
				DefaultText: "~/.delta/importer",
				EnvVars:     []string{"DELTA_DIR"},
			},
			&cli.BoolFlag{
				Name:    "debug",
				Usage:   "set to enable debug logging output",
				EnvVars: []string{"DEBUG"},
			},
		},

		Action: func(cctx *cli.Context) error {
			logo := `Δ 𝔻𝕖𝕝𝕥𝕒  𝕀𝕞𝕡𝕠𝕣𝕥𝕖𝕣`
			fmt.Println(Purple + logo + Reset)
			fmt.Println("Version: " + Version + " (git." + Commit + ")")
			fmt.Printf("\n\n")
			fmt.Println("Running in " + Red + cctx.String("mode") + Reset + " mode")
			fmt.Println("Imports every " + Green + cctx.String("interval") + Reset + " seconds, until max-concurrent of " + Cyan + cctx.String("max_concurrent") + Reset + " is reached")
			fmt.Println("Using data dir in " + Gray + cctx.String("delta-dir") + Reset)

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

			ds := ReadInDatasetsFromFile(filepath.Join(cfg.DataDir + "topology.json"))
			log.Debugf("datasets: %+v", ds)

			db, err := OpenDiDB(cfg.DataDir)
			if err != nil {
				return fmt.Errorf("error opening db: %w", err)
			}

			for {
				log.Debugf("running import...")
				importer(cfg, db, ds)
				time.Sleep(time.Second * time.Duration(cfg.Interval))
			}
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func importer(cfg Config, db *diDB, datasets map[string]Dataset) {
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
		log.Infof("skipping import job as there are already %d deals in progress (max_concurrent is %d)", len(inProgress), cfg.MaxConcurrent)
		return
	}

	log.Debugf("found %d deals in sealing pipeline", len(inProgress))

	var importResult *ImportResult

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
			db.InsertDeal(importResult.DealUuid, importResult.CommP, importResult.Successful, cfg.Mode)
		}

		if importResult != nil && importResult.Successful {
			break
		}
	}
}

var cidsAlreadyAttempted = make(map[string]bool)

func importerDefault(cfg Config, ds Dataset, boost *BoostConnection) *ImportResult {
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
		if HasMismatchedCommPErrors(otherDeals) {
			log.Debugf("skipping import of %s as there are mismatched CommP errors for it", deal.PieceCid)
			continue
		}

		filename := ds.GenerateCarFileName(deal.PieceCid)
		if filename == "" {
			log.Errorf("could not find carfile name for dataset %s for CID %s", ds.Dataset, deal.PieceCid)
			continue
		}

		if !FileExists(filename) {
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

func importerPullDataset(cfg Config, ds Dataset, boost *BoostConnection) *ImportResult {
	ddm := NewDDMApi(cfg.DDMURL, cfg.DDMToken)

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

	if !FileExists(filename) {
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

func importerPullCid(cfg Config, ds Dataset, boost *BoostConnection) *ImportResult {
	ddm := NewDDMApi(cfg.DDMURL, cfg.DDMToken)
	carFilePaths := ds.CarFilePaths()

	ds.PopulateAlreadyImportedCids(boost)

	if len(carFilePaths) == 0 {
		log.Debugf("skipping dataset %s : no car files found", ds.Dataset)
		return nil
	}

	log.Debugf("%d car files found for dataset %s", len(carFilePaths), ds.Dataset)

	for _, carFilePath := range carFilePaths {
		// Assume files are named as <cidFromFilename>.car
		cidFromFilename := FileNameFromPath(carFilePath)

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
		if HasMismatchedCommPErrors(otherDeals) {
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
		if !FileExists(carFilePath) {
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
