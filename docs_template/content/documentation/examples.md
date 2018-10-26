---
title: "Commandline Examples"
date: 2018-10-23T09:34:11+02:00
weight: 2
draft: false
---

## Extract Two Collection Schemas

In this simple example we are going to extract two simple collection Schemas.

```bash
java -jar .\{{% jarname %}} --extract --uri mongodb://localhost:27017 --format mongodb-schema-v4 --namespace appdb.users:0 --namespace appdb.items:1000 --output-directory ./
```

Let's break down the command line. This will connect to the MongoDB server at the host `localhost` and port `27017`. It specifies
that the output format will be `mongodb-schema-v4` which is the `MongoDB Json Schema format`. Finally we will write a Schema `.json`
file to the directory specified `./`

At the end of a successful execution we have two Schema json files.

```bash
./appdb_users_2018_10_18_08_51.json
./appdb_items_2018_10_18_08_51.json
```

Each file is made up of the following sections `<db name>_<collection name>_<timestamp>.json`.

## Apply a Schema to MongoDB

In this simple example we are going to apply a `MongodB Json Schema` Schema to a collection.

```bash
java -jar .\{{% jarname %}} --apply --uri mongodb://localhost:27017 --schema appdb.users:./appdb_users_2018_10_18_08_51.json --validationLevel strict --validationAction error
```

Let's break down the command line. This will connect to the MongoDB server at the host `localhost` and port `27017`. It specifies
`--schema` as `appdb.users:./appdb_users_2018-10-18T08:51Z.json`. Lets break down the schema options. It has the following format
`<db name>.<collection name>:<MongoDB Json Schema file path>`. In our case we are going to apply the Schema in the `appdb_users_2018-10-18T08:51Z.json`
to the MongoDB database `appdb` and collection `users`.

The parameter `--validationLevel` sets the level of validation used by MongoDB. The possible levels supported are `strict` and `moderate`.
The parameter `--validationAction` sets the action of MongoDB on a validation error. Two possible values are `error` and `warn`.