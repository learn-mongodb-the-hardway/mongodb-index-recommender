---
title: "Frequently Asked Questions"
date: 2018-12-17T10:53:09+02:00
draft: false
---
# Important Information

This section contains additional information needed to succesfully use the tool.

## My MongoDB Log Entries are being Redacted

MongoDB only logs up to 10000 bytes per log line captured by default. This means queries using large
`$in` blocks might be redacted and cannot successfully be parsed by the Index recommendation engine.

If you find this is the case for your logged statements you can increase the size by starting `mongod`
with the following extra parameter `maxLogSizeKB` where the value is the max number of `KiloBytes` a single
log line can be before it gets truncated. The default value as of `MongoDB 4.X` is `10`.

There are two ways to change this value. One is setting the value on the `mongod` command line.

```text
> ./mongod --dbpath ./mydb --setParameter maxLogSizeKB=20
```

The other is by modifying the parameter at runtime. Start the `mongo` shell.

```
> ./mongo
```

Next set the value of the parameter.

```
> db.adminCommand({setParameter: 1, maxLogSizeKB: 25});
```

You can read back the value to confirm it was correctly set by executing the following command.

```
> db.adminCommand({getParameter: 1, maxLogSizeKB: 1});
```