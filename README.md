# mongodb-index-recommender (WIP)

[![Build Status][travis-img]][travis-url]

# MongoDB Index Recommendation Tool

| Page | Link |
| --- | --- |
| Documentation | [https://learn-mongodb-the-hardway.github.io/mongodb-index-recommender/](https://learn-mongodb-the-hardway.github.io/mongodb-index-recommender/)|
| History | [docs_template/content/HISTORY.md](docs_template/content/HISTORY.md) |

The MongoDB Index Recommendation Tool lets you analyze your existing MongoDB indexes as well as analyze your query operations from the `system.profile` or
MongoDB log files to create an optimized list of indexes for each collection.

## Features

* `Optimize Indexes:`
    * Consume Existing Indexes from your MongoDB collection
    * Process `system.profile` and/or MongoDB logs and extract candidate indexes based on your read operations including
        - Query operations
        - Update operations
        - Deletes operations
        - Aggregation operations
        - Count operations
        - Group operations
        - Distinct operations
    * Optimize indexes by coalescing the existing indexes and candidate indexes into a minimized list of indexes.

[travis-img]: https://travis-ci.org/learn-mongodb-the-hardway/mongodb-index-recommender.svg?branch=master
[travis-url]: https://travis-ci.org/learn-mongodb-the-hardway/mongodb-index-recommender?branch=master