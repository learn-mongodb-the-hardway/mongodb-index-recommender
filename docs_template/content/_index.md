# MongoDB Index Recommendation Tool

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

## History
```bash
{{% readfile file="HISTORY.md" markdown="false" %}}
```
