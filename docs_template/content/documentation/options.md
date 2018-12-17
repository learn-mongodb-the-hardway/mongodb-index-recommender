---
title: "Commandline Options"
date: 2018-10-23T09:34:11+02:00
weight: 1
draft: false
---

This section documents the `command line` options and how they work.

## General Options

The general options control the main functions of the tool.

| Option | Multiple | Description |
| ---- | ---- | --- |
| `-h`, `--help` | - | Display command line help information |
| `--skip-queries-shape-explain-plan-execution` | - | Skip attempt to use detected queries shapes to detect additional information |
| `--skip-read-profile-collection` | - | Skip attempting to queries the profile collection |
| `--version` | - | Display the command line version information |

> Notes
>
> The `--skip-queries-shape-explain-plan-execution` disables the index recommendation engines
> attempts to grab the `query explain plan` for an existing compound index shape from `MongoDB` to be able to determine
> if the index is a multikey index.
>
> The `--skip-read-profile-collection` disables the index recommendation engine's attempt to
> read the `system.profile` collection to extract query shapes to create candidate indexes
> for the recommendation engine.

## Logging Options

Allows for setting the logging options for the tool.

| Options | Multiple | Description |
| --- | --- | --- |
| `--logpath` <LOGPATH> | - | The path of the file we wish to log too |
| `--quiet` | - | Turn off all logging |
| `-v`, `--verbosity` | Yes | Set the verbosity of the logging |

## Connection Options

The connection options are options for the MongoDB connectivity.

| Options | Multiple | Description |
| --- | --- | --- |
| `--uri` <URI> | - | [The MongoDB Connection URI](https://docs.mongodb.com/manual/reference/connection-string/) |

## Extract Options

Outlines all options available when extracting schemas from MongoDB.

| Options | Multiple | Values | Description |
| --- | --- | --- | --- |
| `--format` <FORMAT>` | - | `txt`, `json` | Set the output format of the Schema extraction, ex: [--output txt] |
| `--mongolog` <MONGOLOG> | Yes | - | One or more MongoDB logs to parse for analysis. |
| `--namespace` <NAMESPACE> | Yes | - | Add a namespace to analyse indexes for, format <db.collection>, ex: `db1.coll` |
| `--output-directory` <OUTPUT_DIRECTORY> | - | - | Output directory for the extracted schemas, ex: `--output-directory ./` |

## Statistics Options

Outlines the all options available collection statistics.

| Options | Multiple | Values | Description |
| --- | --- | --- | --- |
| `--bucket-resolution` <BUCKET_RESOLUTION> | - | `millisecond`, `second`, `minute`, `hour`, `day` | The statistics gathering resolution (millisecond, second, minute, hour, day) |
