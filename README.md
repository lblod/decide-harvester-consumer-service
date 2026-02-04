# Decide harvester consumer service

## About
This service consumes Decide harvester tasks and ingests data from a remote producer stack. It reacts to `task:Task` deltas: when a task becomes `adms:status = scheduled`, the service loads the task, determines whether it is an initial sync or delta sync, and ingests the corresponding dump or delta files into the `LANDING_GRAPH`.

Initial sync tasks ingest the latest full dump into `LANDING_GRAPH` and **do not** produce a result graph. Delta sync tasks ingest deltas into `LANDING_GRAPH` and also write inserts to a **temporary result graph**, which is recorded on the task.

## How it works
- A delta notification marks a task as `scheduled`.
- The service loads the task and inspects its remote data object to determine the task type (`initial-sync` or `delta`).
- Initial sync:
  - Download latest dump distribution.
  - Stream-parse and ingest all triples into `LANDING_GRAPH`.
  - No result graph is recorded on the task.
- Delta sync:
  - Fetch unconsumed delta files since the latest timestamp.
  - Apply deletes + inserts to `LANDING_GRAPH`.
  - Also write inserts into a new temporary result graph.
  - Link the temporary result graph to the task via `task:resultsContainer / task:hasGraph`.

## Usage

Add the following to your docker-compose file:

```yml
harvester-consumer-service:
  image: lblod/decide-harvester-consumer-service
  environment:
    SYNC_BASE_URL: https://lokaalbeslist-harvester-1.s.redhost.be/
    SYNC_FILES_PATH: /sync/besluiten/files
    SYNC_DATASET_SUBJECT: http://data.lblod.info/datasets/delta-producer/dumps/lblod-harvester/BesluitenCacheGraphDump
    LANDING_GRAPH: "http://mu.semte.ch/graphs/oslo-decisions"
    OPERATION_URI: http://lblod.data.gift/id/jobs/concept/TaskOperation/oslo-eli/consume
    HIGH_LOAD_DATABASE_ENDPOINT: http://triplestore:8890/sparql
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

## Configuration

| Environment variable            | Description                                                         | Default                                                                |
| ------------------------------- | ------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `HIGH_LOAD_DATABASE_ENDPOINT`   | SPARQL endpoint used for reads/writes of data (i.e. non-task data). | `http://database:8890/sparql`                                          |
| `LANDING_GRAPH`                 | Graph where full dumps and deltas are ingested.                     | `http://mu.semte.ch/graphs/oslo-decisions`                             |
| `SYNC_BASE_URL`                 | Base URL of the remote harvester producer.                          | unset (required)                                                       |
| `SYNC_FILES_PATH`               | Path used to fetch delta files.                                     | `/sync/files`                                                          |
| `SYNC_DATASET_SUBJECT`          | Dataset subject used to select the latest dump.                     | unset (required)                                                       |
| `START_FROM_DELTA_TIMESTAMP`    | If set, deltas are fetched starting from this timestamp.            | unset (optional)                                                       |
| `HTTP_MAX_QUERY_SIZE_BYTES`     | Max SPARQL query size used by batching logic.                       | `60000`                                                                |
| `BATCH_SIZE`                    | Batch size for streaming insert/delete operations.                  | `100`                                                                  |
| `OPERATION_URI`                 | Only tasks with `task:operation` set to this URI are handled.       | `http://lblod.data.gift/id/jobs/concept/TaskOperation/decide-consumer` |

## Notes
- This service is intended to run inside a harvester stack.
- Initial sync tasks do not record a result graph; delta tasks do.
