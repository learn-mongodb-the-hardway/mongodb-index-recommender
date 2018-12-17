---
title: "Commandline Examples"
date: 2018-10-23T09:34:11+02:00
weight: 2
draft: false
---

## Perform Index Recommendation on Two collections

In this simple example we are going to extract two simple collection Schemas.

```bash
java -jar .\{{% jarname %}} mongodb://localhost:27017 --namespace appdb.users --namespace appdb.items --output-directory ./ --format txt
```

Let's break down the command line. This will connect to the MongoDB server at the host `localhost` and port `27017`. It specifies
that the output format will be `txt` which is the `Index Recommendation Engine Textual Format`. Finally we will write the index recommendation `.txt` and
the shell `.js` files to the directory specified `./`

At the end of a successful execution we have two files.

```bash
./appdb_2018_10_18_08_51.txt
./appdb_2018_10_18_08_51.js
```

Each file is made up of the following sections `<db name>_<timestamp>.<txt|js>`.