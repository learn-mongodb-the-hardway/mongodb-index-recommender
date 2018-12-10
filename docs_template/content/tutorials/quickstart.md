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
java -jar ./{{% jarname %}} --extract --uri mongodb://localhost:27017 --namespace quickstart.users --namespace quickstart.sights --format txt
```







This will create a schema file for each of the collections.

```bash
./quickstart_users_2018_10_18_08_51.json
./quickstart_sights_2018_10_18_08_51.json
```

The file name has the following format (<db>_<collection>_<timestamp>.json). It includes a timestamp to
make it easier to keep track of when the schema was extracted.

## Applying the Schema to MongoDB

We can also use the tool to apply the files to the MongoDB Collections. It is important to note that only the `mongodb-schema-v4` format jsons can be
used with MongoDB to validate documents.

To apply the the two json files above we execute the following command.

```bash
java -jar ./{{% jarname %}} --apply --uri mongodb://localhost:27017 --schema quickstart.users:./quickstart_users_2018_10_18_08_51.json --schema quickstart.sights:./quickstart_sights_2018_10_18_08_51.json --validationLevel strict --validationAction error
```

This will successfully add the two generated validation schemas to their respective collections setting the validation level
to `strict` and the validation action to `error`. Finally lets validate that the schemas where in fact applied to the database.

## Validate the Schemas on MongoDB

Let's connect to the MongoDB instance running on `localhost` at port `27017`.

```bash
mongo
```

Next let's validate that the schema was applied to the `users` collection. First switch to the `quickstart` database.

```bash
> use quickstart
```

Now lets view the information stored about the collection `users`.

```bash
> db.getCollectionInfos({name: "users"})
```

This returns a result that looks somewhat like.

```json
[
    {
        "name" : "users",
        "type" : "collection",
        "options" : {
            "validator" : {
                "$jsonSchema" : {
                    "description" : "quickstart.users MongoDB Schema",
                    "bsonType" : "object",
                    "required" : [
                        "_id",
                        "name",
                        "address"
                    ],
                    "properties" : {
                        "_id" : {
                            "bsonType" : "double"
                        },
                        "name" : {
                            "bsonType" : "string"
                        },
                        "address" : {
                            "bsonType" : "object",
                            "required" : [
                                "street",
                                "city",
                                "country"
                            ],
                            "properties" : {
                                "street" : {
                                    "bsonType" : "string"
                                },
                                "city" : {
                                    "bsonType" : "string"
                                },
                                "country" : {
                                    "bsonType" : "string"
                                }
                            }
                        }
                    }
                }
            },
            "validationLevel" : "strict",
            "validationAction" : "error"
        },
        "info" : {
            "readOnly" : false,
            "uuid" : UUID("9125017a-d353-4326-8f16-fd62e2391b85")
        },
        "idIndex" : {
            "v" : 2,
            "key" : {
                "_id" : 1
            },
            "name" : "_id_",
            "ns" : "quickstart.users"
        }
    }
]
```

as we can see the `MongoDB JSON Schema` was correctly applied to the collection. If we look at the `sights` collection.

```bash
> db.getCollectionInfos({name: "sights"})
```

We get the following result

```json
[
    {
        "name" : "sights",
        "type" : "collection",
        "options" : {
            "validator" : {
                "$jsonSchema" : {
                    "description" : "quickstart.sights MongoDB Schema",
                    "bsonType" : "object",
                    "required" : [
                        "_id",
                        "user_id",
                        "address",
                        "name"
                    ],
                    "properties" : {
                        "_id" : {
                            "bsonType" : "double"
                        },
                        "user_id" : {
                            "bsonType" : "double"
                        },
                        "address" : {
                            "bsonType" : "object",
                            "required" : [
                                "street",
                                "city",
                                "country"
                            ],
                            "properties" : {
                                "street" : {
                                    "bsonType" : "string"
                                },
                                "city" : {
                                    "bsonType" : "string"
                                },
                                "country" : {
                                    "bsonType" : "string"
                                }
                            }
                        },
                        "name" : {
                            "bsonType" : "string"
                        }
                    }
                }
            },
            "validationLevel" : "strict",
            "validationAction" : "error"
        },
        "info" : {
            "readOnly" : false,
            "uuid" : UUID("1e43a53f-063e-4d7e-b20e-e5563279fb8e")
        },
        "idIndex" : {
            "v" : 2,
            "key" : {
                "_id" : 1
            },
            "name" : "_id_",
            "ns" : "quickstart.sights"
        }
    }
]
```

We can see that both schemas are correctly set on the respective collections. This concludes the Quick start tutorial.