# Migration Guide - V2.0 to V2.1
This version results a very small breaking change in the `datasets.json` file. The `address` field is now parsed as an array/list, so you must enclose it in `[]` brackets. It does not need to contain multiple addresses, one will still work fine.

This change allows one-many mapping between datasets and wallets, as may be the case when ingesting a dataset where the datacap allocation is split across multiple wallets. 

To change this, edit the `datasets.json` file:


OLD *~/.delta/importer/datasets.json*
```jsonc
[
  {
    "dataset": "radiant-ml",
    "address": "f1p3l3wgnfukemmaupqecwcoqp7fcgjcqgqcq7rja",
    "dir": "/mnt/delta-datasets/radiant-poc",
    "ignore": false
  }
]
```

Change to:

NEW *~/.delta/importer/datasets.json*
```jsonc
[
  {
    "dataset": "radiant-ml",
    "address": ["f1p3l3wgnfukemmaupqecwcoqp7fcgjcqgqcq7rja"], // <- address is now enclosed in [] brackets
    "dir": "/mnt/delta-datasets/radiant-poc",
    "ignore": false
  }
]
```