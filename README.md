# Decide harvester consumer service

This service aims to handle delta consumer events (dumps, delta file).

It downloads the delta or dump file and make it available to the next steps.

This service reacts to deltas and must be used within the context of the harvester.

## Usage

Add the following to your docker-compose file:

```yml
harvester-consumer-service:
  image: lblod/poc-decide-harvester-consumer-service
  environment:
            DCR_START_FROM_DELTA_TIMESTAMP: 2025-09-01T00:00:00
            DCR_SYNC_BASE_URL: https://lokaalbeslist-harvester-1.s.redhost.be/
            DCR_SYNC_FILES_PATH: /sync/besluiten/files
            DCR_SYNC_DATASET_SUBJECT: http://data.lblod.info/datasets/delta-producer/dumps/lblod-harvester/BesluitenCacheGraphDump
            LANDING_GRAPH: "http://mu.semte.ch/graphs/oslo-decisions"
            HTTP_MAX_QUERY_SIZE_BYTES: 120000
            DCR_BATCH_SIZE: 2000
            HIGH_LOAD_DATABASE_ENDPOINT: http://triplestore:8890/sparql
   links:
    - database:database
```

Add the delta rule:

```json
{
  "match": {
    "predicate": {
      "type": "uri",
      "value": "http://www.w3.org/ns/adms#status"
    },
    "object": {
      "type": "uri",
      "value": "http://redpencil.data.gift/id/concept/JobStatus/scheduled"
    }
  },
  "callback": {
    "method": "POST",
    "url": "http://harvester-consumer-service/delta"
  },
  "options": {
    "resourceFormat": "v0.0.1",
    "gracePeriod": 1000,
    "ignoreFromSelf": true,
    "foldEffectiveChanges": true
  }
}
```
