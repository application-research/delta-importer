package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/application-research/delta-importer/db"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var Commit string
var Version string

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
			fmt.Println("Using data dir in " + Gray + cctx.String("dir") + Reset)

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
