export const INGEST_INTERVAL = process.env.INGEST_INTERVAL_MS || 60000;
export const MU_APPLICATION_GRAPH = process.env.MU_APPLICATION_GRAPH || 'http://mu.semte.ch/application';
export const SYNC_BASE_URL = process.env.SYNC_BASE_URL || 'https://api.loket.lblod.info';
export const SYNC_FILES_PATH = process.env.SYNC_FILES_PATH || '/sync/mandatarissen/files';
export const DUMPFILE_FOLDER = process.env.DUMPFILE_FOLDER || 'dumpfiles';
export const SYNC_FILES_ENDPOINT = `${SYNC_BASE_URL}${SYNC_FILES_PATH}`;
export const SYNC_DATASET_PATH = process.env.SYNC_DATASET_PATH || '/datasets';
export const SYNC_DATASET_ENDPOINT = `${SYNC_BASE_URL}${SYNC_DATASET_PATH}`;

if(!process.env.SYNC_DATASET_SUBJECT)
  throw `Expected 'SYNC_DATASET_SUBJECT' to be provided.`;
export const SYNC_DATASET_SUBJECT = process.env.SYNC_DATASET_SUBJECT;

export const DOWNLOAD_FILE_PATH = process.env.DOWNLOAD_FILE_PATH || '/files/:id/download';
export const DOWNLOAD_FILE_ENDPOINT = `${SYNC_BASE_URL}${DOWNLOAD_FILE_PATH}`;
export const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 100;
export const JOBS_GRAPH = process.env.JOBS_GRAPH || 'http://mu.semte.ch/graphs/system/jobs';

export const MU_CALL_SCOPE_ID_INITIAL_SYNC = process.env.MU_CALL_SCOPE_ID_INITIAL_SYNC || 'http://redpencil.data.gift/id/concept/muScope/deltas/consumer/initialSync';

if(!process.env.JOB_CREATOR_URI)
  throw `Expected 'JOB_CREATOR_URI' to be provided.`;
export const JOB_CREATOR_URI = process.env.JOB_CREATOR_URI;

if(!process.env.INITIAL_SYNC_JOB_OPERATION)
  throw `Expected 'INITIAL_SYNC_JOB_OPERATION' to be provided.`;
export const INITIAL_SYNC_JOB_OPERATION = process.env.INITIAL_SYNC_JOB_OPERATION;

//mainly for debugging
export const DISABLE_INITIAL_SYNC = process.env.DISABLE_INITIAL_SYNC == 'true' ? true : false;
export const DISABLE_DELTA_INGEST = process.env.DISABLE_DELTA_INGEST == 'true' ? true : false;

export const INITIAL_SYNC_TASK_OPERATION = 'http://redpencil.data.gift/id/jobs/concept/TaskOperation/deltas/consumer/initialSyncing';
export const JOB_URI_PREFIX = 'http://redpencil.data.gift/id/job/';
export const TASK_URI_PREFIX = 'http://redpencil.data.gift/id/task/';
export const JOB_TYPE = 'http://vocab.deri.ie/cogs#Job';
export const TASK_TYPE = 'http://redpencil.data.gift/vocabularies/tasks/Task';
export const ERROR_TYPE= 'http://open-services.net/ns/core#Error';
export const DELTA_ERROR_TYPE = 'http://redpencil.data.gift/vocabularies/deltas/Error';
export const ERROR_URI_PREFIX = 'http://redpencil.data.gift/id/jobs/error/';

export const STATUS_BUSY = 'http://redpencil.data.gift/id/concept/JobStatus/busy';
export const STATUS_SCHEDULED = 'http://redpencil.data.gift/id/concept/JobStatus/scheduled';
export const STATUS_FAILED = 'http://redpencil.data.gift/id/concept/JobStatus/failed';
export const STATUS_CANCELED = 'http://redpencil.data.gift/id/concept/JobStatus/canceled';
export const STATUS_SUCCESS = 'http://redpencil.data.gift/id/concept/JobStatus/success';

export const PREFIXES = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX oslc: <http://open-services.net/ns/core#>
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX dbpedia: <http://dbpedia.org/resource/>
`;
