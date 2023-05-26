package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	dmn "github.com/application-research/delta-importer/daemon"
	"github.com/application-research/delta-importer/db"
	"github.com/application-research/delta-importer/util"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v2"
)

func SetupCommands() []*cli.Command {
	var commands []*cli.Command

	/* daemon command */
	commands = append(commands, &cli.Command{
		Name:    "daemon",
		Aliases: []string{"d"},
		Usage:   "run the delta-importer daemon to continuously import deals",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "boost-url",
				Usage:       "ip address of boost",
				DefaultText: "http://localhost",
				Value:       "http://localhost",
				EnvVars:     []string{"BOOST_URL"},
			},
			&cli.UintFlag{
				Name:        "port",
				Usage:       "port to run the daemon's API on",
				DefaultText: "1313",
				Value:       1313,
				EnvVars:     []string{"DI_PORT"},
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
				Name:    "staging-dir",
				Usage:   "directory to use for carfile staging",
				EnvVars: []string{"STAGING_DIR"},
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
			fmt.Printf("\n--\n")
			fmt.Println("Running in " + util.Red + cctx.String("mode") + util.Reset + " mode")
			fmt.Println("Imports every " + util.Green + cctx.String("interval") + util.Reset + " seconds, until max-concurrent of " + util.Cyan + cctx.String("max_concurrent") + util.Reset + " is reached")
			fmt.Println("Using data dir in " + util.Gray + cctx.String("dir") + util.Reset)
			fmt.Println("Importer API is available at" + util.Red + " 127.0.0.1:" + cctx.String("port") + util.Reset)

			return dmn.RunDaemon(cctx)
		},
	})

	/* stats command */
	commands = append(commands, &cli.Command{
		Name:  "stats",
		Usage: "get stats about imported deals",
		Flags: CLIConnectFlags,
		Action: func(cctx *cli.Context) error {
			c, err := NewCmdProcessor(cctx)
			if err != nil {
				return err
			}

			res, closer, err := c.MakeRequest("GET", "/api/v1/stats", nil)
			if err != nil {
				return fmt.Errorf("command failed %s", err)
			}
			defer closer()

			var statsJson db.DealStats
			err = json.Unmarshal(res, &statsJson)
			if err != nil {
				return fmt.Errorf("failed to parse %s", err)
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"State", "Count", "Bytes"})
			t.AppendRows([]table.Row{
				{"Success", statsJson.Success.Count.Int64, util.BytesToReadable(statsJson.Success.Bytes.Int64)},
				{"Failure", statsJson.Failure.Count.Int64, util.BytesToReadable(statsJson.Failure.Bytes.Int64)},
				{"Pending", statsJson.Pending.Count.Int64, util.BytesToReadable(statsJson.Pending.Bytes.Int64)},
			})
			t.AppendSeparator()
			t.AppendRows([]table.Row{
				{"Total", statsJson.TotalImported.Count.Int64, util.BytesToReadable(statsJson.TotalImported.Bytes.Int64)},
			})
			t.AppendFooter(table.Row{"", "Last Import Time", statsJson.LastImported})
			t.SetStyle(table.StyleColoredDark)
			t.Render()

			return nil
		},
	})

	return commands
}
