# Δ Importer
This is a tool designed to be run on the Storage Provider infrastructure. It facilitates *import* deals - that is, importing `CAR` files from the filesystem that match the CID of deal proposals sent to the provider.

# Requirements
- Go v1.19+
- Rust (needed to build filecoin-ffi)
- [Boost v1.6.0+](https://github.com/filecoin-project/boost)

Assumption: all carfiles to import are named `<pieceCID>.car`, and are stored in folders at `/<dir>/<dataset-slug>/<pieceCID>.car`.
where `<dir>` is specified in the run flags, and `<dataset-slug>` is specified in the `datasets.json` file. See below for more information.

# Installation

Building from Source
1. Clone `git clone https://github.com/application-research/delta-importer.git` 
2. `make all`
3. `make install`


# Usage

`delta-importer`

```
NAME:
   Delta Importer 

USAGE:
   delta-importer [global options] command [command options] [arguments...]

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --boost value           192.168.1.1
   --debug                 set to enable debug logging output (default: false)
   --dir value             /home/filecoin/path/to/mount
   --gql value             8080 (default: 8080)
   --help, -h              show help (default: false)
   --interval value        interval, in seconds, to re-run the importer (default: 0)
   --key value             eyJ....XXX
   --max_concurrent value  stop importing if # of deals in AP or PC1 are above this threshold. 0 = unlimited. (default: 0)
   --port value            1288 (default: 1288)
```


## Modes
Delta-Importer can be ran in three modes:

1. **Default (Boost Scanning) Mode**: This is the default mode. 
`--mode default // not required`
In this mode, Delta Importer will scan Boost for deals awaiting import, and automatically match them to CAR files on the filesystem and import them.

2. **Pull Mode - Dataset**
`--mode pull-dataset`
In this mode, the Delta Importer will request deals from the DDM self-service API per-dataset, before attempting to import them. 

1. **Pull Mode - CID**
`--mode pull-cid`
In this mode, the Delta Importer will scan the filesystem for CAR files, and make requests to the DDM self-service API for each carfile.
It will check Boost to ensure duplicate deals are not requested


## Example run
The following command will import a deal every 240 seconds, until there are 80 deals currently in the AP/PC1 state. Then, it will stop untill some deals clear out. 

`delta-importer --boost 192.168.1.1 --port 1288 --gql 8080 --key eyJ...XXX --dir /home/filecoin/datasets --interval 240 --max_concurrent 80 --debug`


## Datasets Config
You must provide the tool with a file named `datasets.json` , in the same directory that the command is being executed from. This file maintains a mapping between client `wallets` (i.e, who is making deals) with a `dataset slug`. This dataset slug is appended to the `--dir` flag when importing data. 

For example, given a `datasets.json` that looks like this:
```json
{
   "f1234": "test-dataset"
}
```

And a `--dir` flag or `/home/filecoin/datasets`, 

Then, when an offline deal comes in from address `f1234`, the importer will search in the directory `/home/filecoin/datasets/test-dataset` for a CAR file `<pieceCID>.car` to import. 

You can find an example of the datasets file in `sample_datasets.json`