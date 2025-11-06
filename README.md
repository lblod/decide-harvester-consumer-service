# harvesting scanning cve service

This service aims to handle delta consumer events (dumps, delta file).

It downloads the delta or dump file and make it available to the next steps

This service reacts to deltas and must be used within the context of the LBLOD harvester.

Here's an example of scheduled job one can create to trigger the task (notice `<http://www.semanticdesktop.org/ontologies/2007/01/19/nie#url>

"nbittich/api-backend"`).

```turtle
    <http://redpencil.data.gift/id/scheduled-task/b44aaf38-740a-43ad-a965-c382f0e5994d>
        a       <http://redpencil.data.gift/vocabularies/tasks/ScheduledTask>;
        <http://mu.semte.ch/vocabularies/core/uuid>
                "b44aaf38-740a-43ad-a965-c382f0e5994d";
        <http://purl.org/dc/terms/created>
                "2025-04-11T12:25:58.290Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
        <http://purl.org/dc/terms/isPartOf>
                <http://redpencil.data.gift/id/scheduled-job/443ccbe6-0e80-464e-bfea-4d3d20b376a5>;
        <http://purl.org/dc/terms/modified>
                "2025-04-11T12:25:58.290Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
        <http://redpencil.data.gift/vocabularies/tasks/index>
                0;
        <http://redpencil.data.gift/vocabularies/tasks/inputContainer>
                <http://redpencil.data.gift/id/data-container/2bdcef45-55a9-4ab6-b241-2a4d37c1b824>;
        <http://redpencil.data.gift/vocabularies/tasks/operation>
                <http://lblod.data.gift/id/jobs/concept/TaskOperation/scanCVE> .

<http://redpencil.data.gift/id/remote-file/01f7285e-6536-4226-96af-43e067ab8759>
        a       <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#RemoteDataObject>;
        <http://mu.semte.ch/vocabularies/core/uuid>
                "01f7285e-6536-4226-96af-43e067ab8759";
        <http://purl.org/dc/terms/created>
                "2025-04-11T12:25:58.290Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
        <http://purl.org/dc/terms/creator>
                <http://lblod.data.gift/services/job-self-service>;
        <http://purl.org/dc/terms/modified>
                "2025-04-11T12:25:58.290Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
        <http://redpencil.data.gift/vocabularies/http/requestHeader>
                <http://data.lblod.info/request-headers/accept/text/html>;
        <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#url>
                "nbittich/api-backend" .

<http://redpencil.data.gift/id/cron-schedule/d0481113-d276-4c6e-9f7f-bade24eeaa50>
        a       <http://redpencil.data.gift/vocabularies/tasks/CronSchedule>;
        <http://mu.semte.ch/vocabularies/core/uuid>
                "d0481113-d276-4c6e-9f7f-bade24eeaa50";
        <http://schema.org/repeatFrequency>
                "*/5 * * * *" .

<http://redpencil.data.gift/id/data-container/2bdcef45-55a9-4ab6-b241-2a4d37c1b824>
        a       <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#DataContainer>;
        <http://mu.semte.ch/vocabularies/core/uuid>
                "2bdcef45-55a9-4ab6-b241-2a4d37c1b824";
        <http://redpencil.data.gift/vocabularies/tasks/hasHarvestingCollection>
                <http://redpencil.data.gift/id/harvesting-container/8dd50770-f05e-400f-923e-85f5963a718a> .

<http://redpencil.data.gift/id/scheduled-job/443ccbe6-0e80-464e-bfea-4d3d20b376a5>
        a       <http://vocab.deri.ie/cogs#ScheduledJob>;
        <http://mu.semte.ch/vocabularies/core/uuid>
                "443ccbe6-0e80-464e-bfea-4d3d20b376a5";
        <http://purl.org/dc/terms/created>
                "2025-04-11T12:25:58.290Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
        <http://purl.org/dc/terms/creator>
                <http://lblod.data.gift/services/job-self-service>;
        <http://purl.org/dc/terms/modified>
                "2025-04-11T12:25:58.290Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
        <http://purl.org/dc/terms/title>
                "nbittich/api-backend";
        <http://redpencil.data.gift/vocabularies/tasks/operation>
                <http://lblod.data.gift/id/jobs/concept/JobOperation/cveHarvesting>;
        <http://redpencil.data.gift/vocabularies/tasks/schedule>
                <http://redpencil.data.gift/id/cron-schedule/d0481113-d276-4c6e-9f7f-bade24eeaa50> .

<http://redpencil.data.gift/id/harvesting-container/8dd50770-f05e-400f-923e-85f5963a718a>
        a       <http://lblod.data.gift/vocabularies/harvesting/HarvestingCollection>;
        <http://mu.semte.ch/vocabularies/core/uuid>
                "8dd50770-f05e-400f-923e-85f5963a718a";
        <http://purl.org/dc/terms/creator>
                <http://lblod.data.gift/services/job-self-service>;
        <http://purl.org/dc/terms/hasPart>
                <http://redpencil.data.gift/id/remote-file/01f7285e-6536-4226-96af-43e067ab8759> .
```

## Usage

Add the following to your docker-compose file:

```yml
cve-scanner-service:
  image: lblod/harvesting-scanning-cve-service
  links:
    - database:database
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
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
    "url": "http://cve-scanner-service/delta"
  },
  "options": {
    "resourceFormat": "v0.0.1",
    "gracePeriod": 1000,
    "ignoreFromSelf": true,
    "foldEffectiveChanges": true
  }
}
```
