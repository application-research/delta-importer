package main

import (
	"context"
	"os"
	"regexp"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/urfave/cli/v2"
)

var DATASET_MAP map[string]string

func main() {
	var boost_address string
	var boost_api_key string
	var base_directory string
	var debug bool = false
	var gql_port = "8080"
	var boost_port = "1288"
	var max_concurrent = 0
	var interval = 0

	app := &cli.App{
		Name: "Delta Importer",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "boost",
				Usage:       "192.168.1.1",
				Required:    true,
				Destination: &boost_address,
			},
			&cli.StringFlag{
				Name:        "key",
				Usage:       "eyJ....XXX",
				Required:    true,
				Destination: &boost_api_key,
			},
			&cli.StringFlag{
				Name:        "dir",
				Usage:       "/home/filecoin/path/to/mount",
				Required:    true,
				Destination: &base_directory,
			},
			&cli.StringFlag{
				Name:        "gql",
				Usage:       "8080",
				DefaultText: "8080",
				Destination: &gql_port,
			},
			&cli.StringFlag{
				Name:        "port",
				Usage:       "1288",
				DefaultText: "1288",
				Destination: &boost_port,
			},
			&cli.IntFlag{
				Name:        "max_concurrent",
				Usage:       "stop importing if # of deals in AP or PC1 are above this threshold. 0 = unlimited.",
				Destination: &max_concurrent,
			},
			&cli.IntFlag{
				Name:        "interval",
				Usage:       "interval, in seconds, to re-run the importer",
				Required:    true,
				Destination: &interval,
			},
			&cli.BoolFlag{
				Name:        "debug",
				Usage:       "set to enable debug logging output",
				Destination: &debug,
			},
		},

		Action: func(cctx *cli.Context) error {
			log.Info("Starting Dataset Importer")

			viper.AddConfigPath(".")
			viper.SetConfigType("json")
			viper.SetConfigName("datasets")

			if err := viper.ReadInConfig(); err != nil {
				if _, ok := err.(viper.ConfigFileNotFoundError); ok {
					log.Fatalf("missing config file datasets.json. see readme for info.")
				} else {
					log.Fatalf("config file could not be read: %s", err)
				}
			}

			if debug {
				log.SetLevel(log.DebugLevel)
			}

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

func importer(boost_address string, boost_port string, gql_port string, boost_api_key string, base_directory string, max_concurrent int) {
	boost, err := NewBoostConnection(boost_address, boost_port, gql_port, boost_api_key)
	if err != nil {
		log.Fatal(err)
	}

	d := boost.GetDeals()
	inProgress := d.InProgress()

	if max_concurrent != 0 && len(inProgress) >= max_concurrent {
		log.Debugf("skipping import as there are %d deals in progress (max_concurrent is %d)", len(inProgress), max_concurrent)
		return
	}

	toImport := d.AwaitingImport()

	log.Debugf("%d deals awaiting import and %d deals in progress\n", len(toImport), len(inProgress))

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

		otherDeals := boost.GetDealsForContent(deal.PieceCid)
		if hasFailedDeals(otherDeals) {
			log.Debugf("skipping import of %s as there are failed deals for it", deal.PieceCid)
			continue
		}

		filename := generateCarFileName(base_directory, deal.PieceCid, deal.ClientAddress)
		if filename == "" {
			continue
		}

		if !carExists(filename) {
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

func carExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		log.Tracef("error finding car file %s: %s", path, err)
		return false
	}
	return true
}

// Mapping from client address -> dataset slug -> find in the folder
func generateCarFileName(base_directory string, pieceCid string, sourceAddr string) string {
	datasetSlug := viper.GetString(sourceAddr)
	if datasetSlug == "" {
		log.Errorf("unrecognized dataset from addr %s\n", sourceAddr)
		return ""
	}

	return base_directory + "/" + datasetSlug + "/" + pieceCid + ".car"
}

// checks if there are failed deals in a given array of deals
func hasFailedDeals(ds []Deal) bool {
	failed := false
	re, err := regexp.Compile(`.*commp mismatch.*`)
	if err != nil {
		log.Error("could not compile regex: " + err.Error())
		return false
	}

	for _, d := range ds {
		isCommpMismatch := re.MatchString(d.Err)

		if isCommpMismatch {
			failed = true
			break
		}
	}

	return failed

}
