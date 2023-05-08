
# Delta Importer Set-Up guide 

This guide is intended for storage providers who wish to replicate datasets from [Delta Dataset Manager](https://github.com/application-research/delta-dm). 

## Prerequisites

The following software must be present on your machine
- [Go v1.19+](https://golang.org/doc/install)
- [Rust v1.65+](https://www.rust-lang.org/tools/install)
- [Boost v1.6.0+](https://github.com/filecoin-project/boost)*

*Note: Currently, you must run Delta Importer on the same server as Boost, as it needs direct access to the carfiles.

Before beginning, you should have received the following information from the data onboarder:
- **Provider Key** 
- **DDM API URL**
- **datasets.json** file


Additionally, you should have the carfiles available locally for the datasets you intend to replicate. One method of setting this up is with rclone - checkout the [Rclone Configuration for Delta Importer](https://docs.fw.services/fw-services/delta-tech-stack/getting-started-with-delta-importer/rclone-configuration-for-delta-importer) guide for more information.


## SP Set-up

## DI Install
Follow the steps to install [Delta Importer](https://github.com/application-research/delta-importer) on your Boost server.

```bash
git clone https://github.com/application-research/delta-importer.git
make all
make install
```

## DI Configuration

### datasets.json file
Take the `datasets.json` file provided to you, and modify the `dir` fields to point to the directory where the carfiles are located for each dataset.

```jsonc
[
  {
    "dataset": "sucho-s3",
    "address": "f1p3l3wgnfukemmaupqecwcoqp7fcgjcqgqcq7rja",
    "dir": "/mnt/ehi_remote/delta-datasets/sucho-s3", // <-- Modify this line 
    "ignore": false
  }
  ... 
]
```

### launch script
Create a launch script, e.g. `delta-import.sh` to easily start up delta importer. 

```bash
#!/bin/bash
clear
delta-importer \
  --boost-url 127.0.0.1 \
  --boost-port 1288 \
  --boost-gql-port 8080 \
  --boost-auth-token eyJ.eyX.xx\
  --max_concurrent 140 \
  --interval 270 \
  --mode pull-cid \
  --ddm-api https://delta-dm.estuary.tech/api/v1/self-service \
  --ddm-token 4b28d311-8be6-48d7-801f-dcb6a87ad49d \
  --datasets /path/to/datasets.json \
  --log /var/log/delta-importer.log 
```

**Parameters**

- `boost-url` is the ip address of the boost node. It can be found in boost's `config.toml`, under `RemoteListenAddress`
- `boost-port` is the rpc port of the boost node (default 1288) it can be found in boost's `config.toml`, under `RemoteListenAddress`
- `boost-gql-port` is the graphql port of the boost node (default 8080). It can be found in boost's `config.toml` under `[Graphql]` -> `Port`
- `boost-auth-token` can be acquired by running `boostd auth create-token --perm admin` on the boost node
- `max_concurrent` is the maximum number of deals to allow in the sealing pipeline at once. This depends on your sealing rate, and includes sectors in all states (AP/PC1/PC2/C1/C2).
- `interval` is the interval in seconds between deal importing deals. This depends on your sealing rate. Example - 86400 seconds (1 day) / 270 seconds = 320 sectors per day for ~10TiB/day sealing rate.
- `mode` is the mode of operation. `pull-cid` will search the local path for carfiles, request deals for them, then attempt to import them. You can read more about the different modes in the [Delta Importer README](https://github.com/application-research/delta-importer#operational-modes)
- `ddm-api` is the url of the DDM instance self-service API endpoint
- `ddm-token` is the Provider Key provided to you by the data onboarder
- `datasets` is the path to the datasets.json file
- `log` is the path to write log output to

**Note** This can also be accomplished with a systemd file (i.e, `/etc/systemd/system/delta-importer.service`). Here is an example systemd file:
  
```ini
[Unit]
Description=Delta Importer
After=network-online.target
Requires=network-online.target

[Service]
ExecStart=/usr/local/bin/delta-importer \
          --boost-url 127.0.0.1 \
          --boost-port 1288 \
          --boost-gql-port 8080 \
          --boost-auth-token eyJ.eyX.xx\
          --max_concurrent 140 \
          --interval 270 \
          --mode pull-cid \
          --ddm-api https://delta-dm.estuary.tech/api/v1/self-service \
          --ddm-token 4b28d311-8be6-48d7-801f-dcb6a87ad49d \
          --datasets /path/to/datasets.json \
          --log /var/log/delta-importer.log
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Starting Delta Importer

Run the script or start the service to begin importing deals.

```bash
./delta-import.sh
```

or 
  
```bash
systemctl start delta-importer
```