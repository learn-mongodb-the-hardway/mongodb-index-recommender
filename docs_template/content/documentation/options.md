---
title: "Commandline Options"
date: 2018-10-23T09:34:11+02:00
weight: 1
draft: false
---

This section documents the `command line` options and how they work.

## General Options

The general options control the main functions of the tool. These are broken up into the two
main functions of `--extract` and `--apply` which extracts schemas from MongoDB or applies an existing 
schema to MongoDB.

| Option | Multiple | Description |
| ---- | ---- | --- |
| --extract | - | Extract `Schemas` from MongoDB |
| --apply | - | Apply a MongoDB format JSON Schema to a MongoDB collection |
| -h, --help | - | Display command line help information |
| --version | - | Display the command line version information |

## Logging Options

Allows for setting the logging options for the tool.

| Options | Multiple | Description |
| --- | --- | --- |
| --logpath | - | The path of the file we wish to log too |
| --quiet | - | Turn off all logging |
| -v, --verbosity | Yes | Set the verbosity of the logging |

## Connection Options

The connection options are options for the MongoDB connectivity.

| Options | Multiple | Description |
| --- | --- | --- |
| --uri | - | [The MongoDB Connection URI](https://docs.mongodb.com/manual/reference/connection-string/) |

## Extract Options

Outlines all options available when extracting schemas from MongoDB.

| Options | Multiple | Values | Description |
| --- | --- | --- | --- |
| --format | - | `schema`, `mongodb-schema-v4` | Set the output format of the Schema extraction. |
| --mergeDocuments | - | - | When the schema extractor locates multiple Document schemas it merges them into a single Schema. |
| --namespace | Yes | - | Add a namespace to extract the schema from. format `<db.collection:sampleSize (int = 0 means all)>`, ex: `db1.coll:1000` |
| --output-directory | - | - | Output directory for the extracted schemas, ex: `--output-directory ./` |

## Apply Options

Outlines the all options available when applying a `mongodb-schema-v4` Schema to a MongoDB collection.

| Options | Multiple | Values | Description |
| --- | --- | --- | --- |
| --schema | YES | - | Specify a schema to apply, format `<db.collection:file>`, ex: `db1.coll:./quickstart_users_2018_10_18_08_51.json` |
| --validationAction | - | `ERROR`, `WARN` | Specify the MongoDB Schema Validation Action. |
| --validationLevel | - | `STRICT`, `MODERATE` | Specify the MongoDB Schema Validation Level. |
