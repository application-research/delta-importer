# Δ Importer
https://delta.store

## What is it?
- Delta Importer is a tool designed to be run on the Storage Provider infrastructure.
- It facilitates automation of import deals - that is, importing .car files from the filesystem that match the CID of deal proposals sent to the provider.
- It integrates with Delta-DM (Dataset Manager) to request deals from the self-service API, facilitating a fully automated dealmaking & deal ingestion pipeline.
- It has multiple modes of operation, covering a variety of different data ingestion strategies
- It’s designed from the ground up to be high performance, written in Go. It has tuneable import frequency/concurrent maximum to optimize for sealing throughput
- Only one instance of Delta Importer is required per instance of Boost 

## Project Goals
> We intend to make the deal ingestion process fully automated, intelligent and streamlined, such that there is no functional difference between End-to-end (Online) and Import (Offline) deals.
> This will allow large-scale providers to easily and efficiently onboard large datasets, where the data transfer is decoupled from the dealmaking process.

# Requirements
- Go v1.19+
- Rust (needed to build filecoin-ffi)
- [Boost v1.6.0+](https://github.com/filecoin-project/boost)

> Assumption: all carfiles to import are named `<pieceCID>.car` , which matches the PieceCID of the deal made with Boost.
> This obviates the need for a File<>Deal mapping, as the importer can simply scan the filesystem for a file matching the PieceCID of the deal.

# Installation

Perform the following steps from a user account with `root` privileges. Note: Once installed, the `delta-importer` binary can be run from any user account.

Build from Source
1. Clone `git clone https://github.com/application-research/delta-importer.git` 
2. `make all`
3. `make install`

This will install the `delta-importer` binary to `/usr/local/bin`. Test it out by running `delta-importer --help`.

# Usage

`delta-importer`

```
NAME:
   delta-importer - An application to facilitate importing deals into a Filecoin Storage Provider

USAGE:
   delta-importer [global options] command [command options] [arguments...]

COMMANDS:
   daemon, d  run the delta-importer daemon to continuously import deals
   stats      get stats about imported deals
   help, h    Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h     show help
   --version, -v  print the version
```


## Running the Importer Daemon

### Configuration

By default, `delta-importer` stores all its local data in the `~/delta/importer` directory for the currently running user. If it does not exist, the tool will attempt to create the directory structure on first launch of the `daemon` command. This can be changed using the `--dir` flag or `DELTA_DIR` environment variable.

### Command-Line Operation
Delta Importer daemon requires a few configuration options to be set. These can be set via environment variables, or via command line flags.

Below is an example shell script to launch the importer daemon, running in **default** mode, and importing a new deal every **260** seconds, until a maximum of **175** deals are active in the sealing pipeline (AP+PC1+PC2+C2).

```bash
delta-importer daemon \
  --boost-url 10.10.10.20 \
  --boost-gql-port 8080 \
  --boost-port 1288 \
  --boost-auth-token XXX.YYY.ZZZ \ 
  --max_concurrent 175 \ 
  --interval 260 \ 
  --mode default  
```

> Hint: Obtain the `boost-auth-token` by running the `boostd auth create-token --perm admin` command on your Boost node.

> Hint: The `--interval` and `--max_concurrent` flags are used to tweak the importer's speed. These parameters should be carefully tuned to match the provider's sealing throughput and available bandwidth. The example provided above is a good starting point for a provider with approximately 10TiB/day of sealing throughput.

### datasets.json
The `datasets.json` file is required to be present in the `delta-importer` data directory (defaults to `~/delta/importer/`). This file maintains a mapping between client `wallets` (i.e, who is making deals) with a `dataset slug` (identifier), and a directory to search for CAR files to import.

Example `datasets.json`
```json
[
  {
    "dataset": "radiant-ml",
    "address": "f1p3l3wgnfukemmaupqecwcoqp7fcgjcqgqcq7rja",
    "dir": "/mnt/delta-datasets/radiant-poc",
    "ignore": false
  },
  {
    "dataset": "cancer-imaging-archive",
    "address": "f1p3l3wgnfukemmaupqecwcoqp7fcgjcqgqcq7rja",
    "dir": "/mnt/delta-datasets/cancer-imaging-archive",
    "ignore": true
  }
]
```

This `datasets.json` file will be processed in order, preferring deals with the first dataset in the list. 

Using the above example,
- If a deal is found for `radiant-ml`, the importer will scan the `/mnt/delta-datasets/radiant-poc` directory for a CAR file matching the PieceCID of the deal. 
- If a match is found, the importer will import the data. 
- If no match is found, the importer will move on to the next dataset in the list, and attempt to import data for that dataset.

Set the `ignore` flag to `true` to skip a dataset. This is useful if you want to speed-up the import loop by disabling a dataset from being imported (ex. if datacap has been exhausted, or data transfer is not complete yet)

### Operational Modes
Delta-Importer can be ran in three modes:

1. **Default (Boost Scanning) Mode**: This is the default mode. 
`--mode default // not required`
In this mode, Delta Importer will scan Boost for deals awaiting import, and automatically match them to CAR files on the filesystem and import them.

2. **Pull Mode - Dataset**
`--mode pull-dataset`
In this mode, the Delta Importer will request deals from the DDM self-service API per-dataset, before attempting to import them. 

3. **Pull Mode - CID**
`--mode pull-cid`
In this mode, the Delta Importer will scan the filesystem for CAR files, and make requests to the DDM self-service API for each carfile.
It will check Boost to ensure duplicate deals are not requested.

When using in either `Pull Mode`, the `--ddm-api` and `--ddm-token` flags are required. These indicate the DDM API endpoint and the API token to use when making deal requests to the DDM API. Contact your DDM administrator for these parameters.

*example pull mode (Dataset) configuration*
```bash
delta-importer \
--boost-url 10.32.32.20 \
--boost-gql-port 8080
--boost-port 1288 \
--boost-auth-token XXX.YYY.ZZZ \ 
--max_concurrent 160 \
--interval 220 \
--mode pull-dataset \
--ddm-api http://ddm-api.delta.store/api/v1/self-service \
--ddm-token 4b28d311-8be6-48d7-801f-dcb6a87ad49d \
--debug 2>&1 | tee ~/import_log_delta.txt
```

## Other commands

Run `delta-importer stats` to get a table showing statistics on imported deal data.