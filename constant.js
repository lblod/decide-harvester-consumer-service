export const OPERATION_URI =
  process.env.OPERATION_URI ||
  "http://lblod.data.gift/id/jobs/concept/TaskOperation/decide-consumer";
export const TYPE_INITIAL_SYNC = "http://mu.semte.ch/vocabularies/ext/decide-consumer/initial-sync";
export const TYPE_DELTA_FILES = "http://mu.semte.ch/vocabularies/ext/decide-consumer/delta";

export const STATUS_BUSY = "http://redpencil.data.gift/id/concept/JobStatus/busy";
export const STATUS_SCHEDULED = "http://redpencil.data.gift/id/concept/JobStatus/scheduled";
export const STATUS_SUCCESS = "http://redpencil.data.gift/id/concept/JobStatus/success";
export const STATUS_FAILED = "http://redpencil.data.gift/id/concept/JobStatus/failed";

export const JOB_TYPE = "http://vocab.deri.ie/cogs#Job";
export const TASK_TYPE = "http://redpencil.data.gift/vocabularies/tasks/Task";
export const ERROR_TYPE = "http://open-services.net/ns/core#Error";
export const ERROR_URI_PREFIX = "http://redpencil.data.gift/id/jobs/error/";
export const connectionOptions = {
  // scope: "http://services.redpencil.io/decide-consumer-service"
};
export const PREFIXES = `
  PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
  PREFIX terms: <http://purl.org/dc/terms/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  PREFIX dbpedia: <http://dbpedia.org/resource/>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX oslc: <http://open-services.net/ns/core#>
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
`;

export const HIGH_LOAD_DATABASE_ENDPOINT =
  process.env.HIGH_LOAD_DATABASE_ENDPOINT || "http://database:8890/sparql";
export const LANDING_GRAPH =
  process.env.LANDING_GRAPH || "http://mu.semte.ch/graphs/oslo-decisions";

export const PUBLISHER_URI =
  process.env.PUBLISHER_URI || "http://data.lblod.info/services/decide-harvester-consumer-service";

export const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 100;

// delta consumer related

export const DUMPFILE_FOLDER = process.env.DUMPFILE_FOLDER || "consumer/deltas";
if (!process.env.SYNC_BASE_URL) throw `Expected 'SYNC_BASE_URL' to be provided.`;
export const SYNC_BASE_URL = process.env.SYNC_BASE_URL;
export const SYNC_FILES_PATH = process.env.SYNC_FILES_PATH || "/sync/files";
export const GET_FILE_PATH = process.env.GET_FILE_PATH || "/files/:id";
export const DOWNLOAD_FILE_PATH = process.env.DOWNLOAD_FILE_PATH || GET_FILE_PATH + "/download";
export const DOWNLOAD_FILE_ENDPOINT = `${SYNC_BASE_URL}${DOWNLOAD_FILE_PATH}`;
export const SYNC_DATASET_PATH = process.env.SYNC_DATASET_PATH || "/datasets";
if (!process.env.SYNC_DATASET_SUBJECT)
  throw `Expected 'SYNC_DATASET_SUBJECT' to be provided by default.`;
export const SYNC_DATASET_SUBJECT = process.env.SYNC_DATASET_SUBJECT;

export const GET_FILE_ENDPOINT = `${SYNC_BASE_URL}${GET_FILE_PATH}`;
export const SYNC_FILES_ENDPOINT = `${SYNC_BASE_URL}${SYNC_FILES_PATH}`;
export const SYNC_DATASET_ENDPOINT = `${SYNC_BASE_URL}${SYNC_DATASET_PATH}`;

export const START_FROM_DELTA_TIMESTAMP = process.env.START_FROM_DELTA_TIMESTAMP;
export const DELTA_FILE_FOLDER = process.env.DELTA_FILE_FOLDER || "/tmp/";
