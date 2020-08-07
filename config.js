const INGEST_INTERVAL = process.env.INGEST_INTERVAL_MS || 60000;
const MU_APPLICATION_GRAPH = process.env.MU_APPLICATION_GRAPH || 'http://mu.semte.ch/application';
const SYNC_BASE_URL = process.env.SYNC_BASE_URL || 'https://api.loket.lblod.info';
const SYNC_FILES_PATH = process.env.SYNC_FILES_PATH || '/sync/mandatarissen/files';
const SYNC_FILES_ENDPOINT = `${SYNC_BASE_URL}${SYNC_FILES_PATH}`;
const DOWNLOAD_FILE_PATH = process.env.DOWNLOAD_FILE_PATH || '/files/:id/download';
const DOWNLOAD_FILE_ENDPOINT = `${SYNC_BASE_URL}${DOWNLOAD_FILE_PATH}`;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 100;

export {
  INGEST_INTERVAL,
  MU_APPLICATION_GRAPH,
  SYNC_BASE_URL,
  SYNC_FILES_ENDPOINT,
  DOWNLOAD_FILE_ENDPOINT
}
