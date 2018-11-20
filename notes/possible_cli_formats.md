index-recommender
    --namespace db1.users                       // What namespace to analyze (multiple allowed)
    --uri mongodb://localhost:27017             // MongoDB connection URI
    --mongolog /logs/mongolog.log               // One or more mongologs to parse for analysis
    --format (json/txt)                         // Results output format
    --output-directory ./                       // Results output directory
    --skip-read-profile-collection              // Skip attempting to query the profile collection
    --skip-query-shape-explain-plan-execution   // Skip attempt to use detected query shapes to detect additional information
    --bucket-resolution                         // The statistics gathering resolution (millisecond, second, minute, hour, day)
