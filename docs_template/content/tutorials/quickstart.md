---
title: "QuickStart"
date: 2018-10-16T10:53:09+02:00
draft: false
---

Welcome to the QuickStart guide. In this guide we are going to cover how to use the tool to extract a list of optimized indexes
for a MongoDB collection. We are going to showcase how the tool can extract index candidates from the `system.
profile` collection, merge it with existing collection indexes and optimize the indexes.

1. Preload some dummy data into a running MongoDB instance.
    - Create an existing index.
2. Run the tool against MongoDB to extract Recommended list of indexes for the collection.
3. Apply the Index recommendations to the collection.
4. Validate that the indexes are present on the collections.

## Pre-Requisities

This QuickStart makes the assumption that you have a `MongoDB instance` running on `localhost` on port
`27017` that you can connect to with the `mongo` shell.

We also assume you have `Java 8` installed on your computer and that the `java` command is available from you command line prompt.

Finally we assume you have downloaded the latest release of the Schema Extraction Tool. In our case that is
the `{{% jarname %}}` file.

## Preloading Data

First connect to the MongoDB instance using the `mongo` shell from your commandline with the following command.

```shell
mongo
```

Next lets switch to the `quickstart` database that we will use in this example.

```shell
> use quickstart
```

If we already have the `quickstart` database and the collections `users` and `sights` we need to drop the collections.

```shell
> db.users.drop();
> db.sights.drop();
```

Next we are going to turn on database profiling support so we can capture all the database queries.

```shell
> db.setProfilingLevel(2, { slowms: 0 })
```

Next create a basic index on the `users` collection.

```shell
> db.users.createIndex({ name: 1 });
```

Now lets insert a dummy document for each of the collections `sights` and `users`.

```bash
> db.users.insertOne({_id: 1, name: 'peter', address: { street: 'smiths road 16', city: 'london', country: 'uk' }})
```

Now lets insert a dummy document for each of the collections `sights` and `users`.

```bash
> db.sights.insertOne({_id: 1, user_id: 1, address: { street: 'smiths road 16', city: 'london', country: 'uk' }, name: "Peters house" })
```

Next prime the `system.profile` collection by executing some query operations.

```bash
> db.users.find({name: 'peter'});
> db.users.aggregate([{ $match: { name: 'peter', city: "london" } }, { $lookup: { from: "sights", localField: "_id", foreignField: "user_id", as: "sights" }} ])
```

Finally turn of profiling.

```shell
> db.setProfilingLevel(0, { slowms: 100 })
```

We now have two simple collections primed with some sample data and operations to run the index recommendation tool.

## Running the Index Recommendation Tool

In this step we are going to run the Index Recommendation Tool to generate the MongoDB index recommendations based on the
previously entered information.

This tool makes the assumption that you have a `Java 8` or higher installed to be able to run the
tool from the commandline. You need to be able to run the `java` command on the command line to
execute the tool.

Let's run the tool to extract the index recommendations

```bash
java -jar ./{{% jarname %}} --extract --uri mongodb://localhost:27017 --namespace quickstart.users --namespace quickstart.sights --format txt --output-directory ./
```

This will run the index recommendation engine and output the recommended indexes to a file for each db. In our case
we will get two files written to disk. The first file contains a textual summary of the indexes recommended and ends with `.txt`.
The other file ends in `.js` and contains a `mongo shell` script that will apply the index recommendations.

```
./quickstart_1544608465048.txt
./quickstart_1544608465048.js
```

The `quickstart_1544608465048.txt` contains a textual representation of the recommended indexes, their statistics
and what indexes where removed for the creation of each of the recommended indexes.

```text
db: quickstart

  collection: users

    notes:

      [number of documents < 10000]: The number of documents in the collection is 1,
        which is relatively low, meaning that it's possible that using indexes might not provide
        much of a performance improvement over collection scans, especially if the collection
        fits in memory

    statistics:

      document count: 1
      index count: 2
      read/write ratio: 2.0
      ops:
        finds: 2
        updates: 0
        removes: 0
        inserts: 1

    indexes:

      [Id]:
        name: _id_
        unique: true
        sparse: false
        statistics:
          count: 0
          since: Mon Dec 17 10:15:43 CET 2018

      <Compound Key>:
        name: name_1_city_1
        fields:
          field:
            key: name
            direction: UNKNOWN
          field:
            key: city
            direction: UNKNOWN
        unique: false
        sparse: false
        statistics:
          count: 1
        shapes:
          shape:
            filter: [{"$match":{"name":true,"city":true}},{"$lookup":{"from":true,"localField":true,"foreignField":true,"as":true}}]
            count: 1
        removed indexes:
          [Single Key]:
            name: name_1
            field:
              key: name
              direction: ASCENDING

  collection: sights

    notes:

      [number of documents < 10000]: The number of documents in the collection is 1,
        which is relatively low, meaning that it's possible that using indexes might not provide
        much of a performance improvement over collection scans, especially if the collection
        fits in memory

    statistics:

      document count: 1
      index count: 1
      read/write ratio: 0.0
      ops:
        finds: 0
        updates: 0
        removes: 0
        inserts: 1

    indexes:

      [Id]:
        name: _id_
        unique: true
        sparse: false
        statistics:
          count: 0
          since: Mon Dec 17 10:15:43 CET 2018

      <Single Key>:
        name: user_id_1
        field:
          key: user_id
          direction: UNKNOWN
        unique: false
        sparse: false
        statistics:
          count: 0
```

Let's break down the output and what it means. For each collection we get a section that contains a `notes`, `statistics` and `indexes`
section.

```text
notes:

  [number of documents < 10000]: The number of documents in the collection is 1,
    which is relatively low, meaning that it's possible that using indexes might not provide
    much of a performance improvement over collection scans, especially if the collection
    fits in memory
```

The `notes` section contains any notes generated by the recommendation engine outlining some hints about the aggregated information.

```text
statistics:

  document count: 1
  index count: 1
  read/write ratio: 0.0
  ops:
    finds: 0
    updates: 0
    removes: 0
    inserts: 1
```

The `statistics` section contains `collection` specific information. The following fields are tracked.

| Field | Description |
| --- | --- |
| document count | The number of documents in the collection. |
| index count | The number of `existing` indexes in the collection.|
| read/write ratio | The ratio of reads to writes on the collection.|
| ops: finds | The total number of finds executed against the collection as extracted from collection stats.|
| ops: updates | The total number of updates executed against the collection as extracted from collection stats.|
| ops: removes | The total number of removes executed against the collection as extracted from collection stats.|
| ops: inserts | The total number of inserts executed against the collection as extracted from collection stats.|

The `indexes` section contains all the information about the recommended indexes for the given `collection`.

```text
indexes:

  [Id]:
    name: _id_
    unique: true
    sparse: false
    statistics:
      count: 0
      since: Mon Dec 17 10:15:43 CET 2018

  <Compound Key>:
    name: name_1_city_1
    fields:
      field:
        key: name
        direction: UNKNOWN
      field:
        key: city
        direction: UNKNOWN
    unique: false
    sparse: false
    statistics:
      count: 1
    shapes:
      shape:
        filter: [{"$match":{"name":true,"city":true}},{"$lookup":{"from":true,"localField":true,"foreignField":true,"as":true}}]
        count: 1
    removed indexes:
      [Single Key]:
        name: name_1
        field:
          key: name
          direction: ASCENDING
```

Each recommended index contains the following information parts.

- The index type (in this case the second index is a Compound Index).
- The index name.
- The fields contained in the index (this will vary by the index type recommended).
- If the index is `unique`.
- If the index is `sparsse`.
- Index specific statistics (either collected from index statistics or query shapes found)
    - Number of counts the index was accessed
    - If the information was collected from index statistics, the date since the statistic was collected.
- Query shapes that underpin the index recommendation.
- The `indexes` that were removed due to this index recommendation.

The `quickstart_1544608465048.js` contains a MongoDB shell script that will apply the index recommendations.

```js
// Select the database [quickstart]
var db = db.getSiblingDB("quickstart");

// Drop and create indexes for collection users
db.users.dropIndex("name_1");
db.users.createIndex({ "name" : 1, "city" : 1 }, { "name" : "name_1_city_1", "unique" : false, "sparse" : false, "background" : true });

// Drop and create indexes for collection sights
db.sights.createIndex({ "user_id" : 1 }, { "name" : "user_id_1", "unique" : false, "sparse" : false, "background" : true });
```

This script will drop any non-needed indexes and then create the recommended indexes. It's provided to simplify the
application of the index recommendations to your MongoDB instance. 

## Applying the Recommended indexes to MongoDB

To apply the recommended indexes to MongoDB we can execute the `quickstart_1544608465048.js` generated script against MongoDB. We
do this using the `mongo` shell taking advantage of that it allows us to execute a script.

```text
> mongo ./quickstart_1544608465048.js
```

Once the script has run we can verify that the indexes were correctly applied to MongoDB.

## Validate the Indexes on MongoDB

Let's connect to the MongoDB instance running on `localhost` at port `27017`.

```bash
mongo
```

Next switch to the `quickstart` database.

```bash
> use quickstart
```

Now lets view the indexes for the collection `users`.

```bash
> db.users.getIndexes()
```

This returns a the following list of indexes for the `users` collection.

```json
[
        {
                "v" : 2,
                "key" : {
                        "_id" : 1
                },
                "name" : "_id_",
                "ns" : "quickstart.users"
        },
        {
                "v" : 2,
                "key" : {
                        "name" : 1,
                        "city" : 1
                },
                "name" : "name_1_city_1",
                "ns" : "quickstart.users",
                "sparse" : false,
                "background" : true
        }
]
```



```json
[
        {
                "v" : 2,
                "key" : {
                        "_id" : 1
                },
                "name" : "_id_",
                "ns" : "quickstart.sights"
        },
        {
                "v" : 2,
                "key" : {
                        "user_id" : 1
                },
                "name" : "user_id_1",
                "ns" : "quickstart.sights",
                "sparse" : false,
                "background" : true
        }
]
```

as we can see both of the indexes for the `users`

