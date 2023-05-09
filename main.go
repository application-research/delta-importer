package main

import (
	"fmt"
	"os"

	dmn "github.com/application-research/delta-importer/daemon"
	"github.com/application-research/delta-importer/util"
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
				Usage:       "ip address of boost",
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
				Usage:       "graphql port for boost",
				DefaultText: "8080",
				Value:       "8080",
				EnvVars:     []string{"BOOST_GQL_PORT"},
			},
			&cli.StringFlag{
				Name:        "boost-port",
				Usage:       "rpc port for boost",
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
				Usage:   "auth token for pull-modes (self-service in DDM)",
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
			fmt.Println(util.Purple + logo + util.Reset)
			fmt.Println("Version: " + Version + " (git." + Commit + ")")
			fmt.Printf("\n\n")
			fmt.Println("Running in " + util.Red + cctx.String("mode") + util.Reset + " mode")
			fmt.Println("Imports every " + util.Green + cctx.String("interval") + util.Reset + " seconds, until max-concurrent of " + util.Cyan + cctx.String("max_concurrent") + util.Reset + " is reached")
			fmt.Println("Using data dir in " + util.Gray + cctx.String("dir") + util.Reset)

			return dmn.RunDaemon(cctx)
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
