# Migration Guide - V1.x to V2.x

Follow these steps if migrating from Delta Importer v1.x to v2.x. 

- Checkout, build and install the latest version of Delta Importer v2.x
ex)
```bash
git checkout v2.0.0
make build
make install
```

- Create the `delta-importer` data directory. By default, this should be at `~/delta/importer`, for the user running the binary.

```bash
mkdir -p ~/.delta/importer
```

- Move your existing `datasets.json` file into the newly created `delta-importer` data directory.

```bash
mv /path/to/datasets.json ~/.delta/importer/datasets.json
```

- Modify your `start script` or `service` file to include the `daemon` command. For example,

```bash
 delta-importer daemon \
  [ ... rest of flags are unchanged ]
```

**Done!**


**Hint**: If you'd like to put the data directory elsewhere, you can pass the `--dir` flag to `delta-importer daemon` to specify the location of the data directory. For example,

```bash
 delta-importer daemon --dir /path/to/data/dir \
  [ ... rest of flags are unchanged ]
```