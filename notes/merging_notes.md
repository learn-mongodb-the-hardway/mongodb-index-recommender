## Sources of Index Candidates
- Existing Collection Indexes
    + Grab from MongoDB
    + Test if index is Multikey or not
        - Create Query and sort by Key values and explore the query plan to establish the index.
- Parse Queries stored in the profile collection for the MongoDB Collection
    + Extract an Index candidate based on the fields used and method
        - Single field index
            - Attempt to establish if resulting document would create a Multikey or a SingleField index
        - Compound field index
            - Attempt to establish if resulting document would create a Multikey or a Compound index
- Sample Collection Counts
    + Low document count might mean we do not apply Indexes
- Keep Query shape statistics
    + Allow us to keep an idea of what queries are important and how big they are
    + Track keys in queries as well as values to allow us to see ranges of values
            
## Coalesce Index Candidates
With a list of Index Candidate Coalesce the indexes to the minimum amount of indexes needed.