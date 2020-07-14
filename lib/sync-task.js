import fs from 'fs-extra';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import mu, { sparqlEscapeDateTime, uuid } from 'mu';

import { MU_APPLICATION_GRAPH } from '../config';

const TASK_NOT_STARTED_STATUS = 'http://lblod.data.gift/mandatendatabank-consumer-sync-task-statuses/not-started';
const TASK_ONGOING_STATUS = 'http://lblod.data.gift/mandatendatabank-consumer-sync-task-statuses/ongoing';
const TASK_SUCCESS_STATUS = 'http://lblod.data.gift/mandatendatabank-consumer-sync-task-statuses/success';
const TASK_FAILED_STATUS = 'http://lblod.data.gift/mandatendatabank-consumer-sync-task-statuses/failure';

class SyncTask {
  constructor({ uri, since, until, created, status }) {
    /** Uri of the sync task */
    this.uri = uri;

    /**
     * Datetime as Data object since when delta files should be retrievd from the producer service
    */
    this.since = since;

    /**
     * Datetime as Data object when the task was created in the triplestore
    */
    this.created = created;

    /**
     * Current status of the sync task as stored in the triplestore
    */
    this.status = status;

    /**
     * Time in ms of the latest successfully ingested delta file.
     * Will be updated while delta files are being consumed.
    */
    this.latestDeltaMs = Date.parse(since.toISOString());

    /**
     * List of delta files to be ingested for this task
     * I.e. delta files generated since timestamp {since}
     * retrieved from the producer server just before
     * the start of the task execution
     *
     * @type Array of DeltaFile
    */
    this.files = [];

    /**
     * Number of already successfull ingested delta files for this task
    */
    this.handledFiles = 0;

    /**
     * Progress status of the handling of delta files.
     * This status is only used during execution and never persisted in the store.
     * Possible values: [ 'notStarted', 'progressing', 'failed' ]
    */
    this.progressStatus = 'notStarted';
  }

  /**
   * Get datetime as Date object of the latest successfully ingested delta file
  */
  get latestDelta() {
    return new Date(this.latestDeltaMs);
  }

  /**
   * Get the total number of files to be ingested for this task
  */
  get totalFiles() {
    return this.files.length;
  }

  /**
   * Execute the sync task
   * I.e. consume the delta files one-by-one as long as there are delta files
   * or until ingestion of a file fails
   *
   * @public
  */
  async execute() {
    try {
      await this.persistStatus(TASK_ONGOING_STATUS);
      console.log(`Found ${this.totalFiles} new files to be consumed`);
      if (this.totalFiles) {
        await this.consumeNext();
      } else {
        console.log(`No files to consume. Finished sync task successfully.`);
        console.log(`Most recent delta file consumed is created at ${this.latestDelta.toISOString()}.`);
        await this.persistLatestDelta(this.latestDeltaMs);
        await this.persistStatus(TASK_SUCCESS_STATUS);
      }
    } catch (e) {
      console.log(`Something went wrong while consuming the files`);
      console.log(e);
    }
  }

  /**
   * Close the sync task with a failure status
   *
   * @public
  */
  async closeWithFailure() {
    await this.persistLatestDelta(this.latestDeltaMs);
    await this.persistStatus(TASK_FAILED_STATUS);
  }

  /**
   * Recursive function to consume the next delta file in the files array
   *
   * @private
  */
  async consumeNext() {
    const file = this.files[this.handledFiles];
    await file.consume(async (file, isSuccess) => {
      this.handledFiles++;
      console.log(`Consumed ${this.handledFiles}/${this.totalFiles} files`);
      await this.updateProgressStatus(file, isSuccess);

      if (this.progressStatus == 'progressing' && this.handledFiles < this.totalFiles) {
        await this.consumeNext();
      } else {
        if (this.progressStatus == 'failed') {
          await this.persistStatus(TASK_FAILED_STATUS);
          console.log(`Failed to finish sync task. Skipping the remaining files. Most recent delta file successfully consumed is created at ${this.latestDelta.toISOString()}.`);
        } else {
          await this.persistStatus(TASK_SUCCESS_STATUS);
          console.log(`Finished sync task successfully. Ingested ${this.totalFiles} files. Most recent delta file consumed is created at ${this.latestDelta.toISOString()}.`);
        }
      }
    });
  };

  /**
   * Update the progress status of the delta handling and write the latest ingested delta timestamp to the store.
   * I.e. update the progress status to 'progressing' and update the latest delta timestamp on success.
   * Update the progress status to 'failed' on failure
   *
   * @param file {DeltaFile} Ingested delta file
   * @param isSuccess {boolean} Flag to indicate success of ingestion of the given delta file
   * @private
  */
  async updateProgressStatus(file, isSuccess) {
    if (isSuccess && this.progressStatus != 'failed') {
      this.progressStatus = 'progressing';

      const deltaMs = Date.parse(file.created);
      if (deltaMs > this.latestDeltaMs) {
        await this.persistLatestDelta(deltaMs);
      }
    } else if (!isSuccess) {
      this.progressStatus = 'failed';
    }
  }

  /**
   * Perists the given timestamp as timestamp of the latest consumed delta file in the triple store.
   *
   * At any moment the latest ext:deltaUntil timestamp on a task, either in failed/ongoing/success state,
   * should reflect the timestamp of the latest delta file that has been completly and successfully consumed.
   * Therefore, the ext:deltaUntil needs to be updated immediately after every delta file consumption.
   *
   * @param deltaMs {int} Timestamp in milliseconds of the latest successfully consumed delta file
   * @private
  */
  async persistLatestDelta(deltaMs) {
    this.latestDeltaMs = deltaMs;

    await update(`
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

      DELETE WHERE {
        GRAPH ?g {
          <${this.uri}> ext:deltaUntil ?latestDelta .
        }
      }
    `);

    await update(`
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

      INSERT {
        GRAPH ?g {
          <${this.uri}> ext:deltaUntil ${sparqlEscapeDateTime(this.latestDelta)} .
        }
      } WHERE {
        GRAPH ?g {
          <${this.uri}> a ext:SyncTask .
        }
      }
    `);

  }

  /**
   * Persists the given status as task status in the triple store
   *
   * @param status {string} URI of the task status
   * @private
  */
  async persistStatus(status) {
    this.status = status;

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      PREFIX adms: <http://www.w3.org/ns/adms#>

      DELETE WHERE {
        GRAPH ?g {
          <${this.uri}> adms:status ?status .
        }
      }
    `);

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      PREFIX adms: <http://www.w3.org/ns/adms#>

      INSERT {
        GRAPH ?g {
          <${this.uri}> adms:status <${this.status}> .
        }
      } WHERE {
        GRAPH ?g {
          <${this.uri}> a ext:SyncTask .
        }
      }
    `);
  }
}

/**
 * Insert a new sync task in the store to consume delta's if there isn't one scheduled yet.
 * The timestamp from which delta's need to be consumed is determined at the start of the task execution.
 *
 * @public
*/
async function scheduleSyncTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    SELECT ?s WHERE {
      ?s a ext:SyncTask ;
         adms:status <${TASK_NOT_STARTED_STATUS}> .
    } LIMIT 1
  `);

  if (result.results.bindings.length) {
    console.log(`There is already a sync task scheduled to ingest delta files. No need to create a new task.`);
  } else {
    const uuid = mu.uuid();
    const uri = `http://lblod.data.gift/mandatendatabank-consumer-sync-tasks/${uuid}`;
    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      PREFIX adms: <http://www.w3.org/ns/adms#>

      INSERT DATA {
        GRAPH <${MU_APPLICATION_GRAPH}> {
          <${uri}> a ext:SyncTask ;
             mu:uuid "${uuid}" ;
             adms:status <${TASK_NOT_STARTED_STATUS}> ;
             dct:creator <http://lblod.data.gift/services/mandatendatabank-consumer> ;
             dct:created ${sparqlEscapeDateTime(new Date())} .
        }
      }
    `);
    console.log(`Scheduled new sync task <${uri}> to ingest delta files`);
  }
}

/**
 * Get the next sync task with the earliest creation date that has not started yet
 *
 * @public
*/
async function getNextSyncTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    SELECT ?s ?created WHERE {
      ?s a ext:SyncTask ;
         adms:status <${TASK_NOT_STARTED_STATUS}> ;
         dct:created ?created .
    } ORDER BY ?created LIMIT 1
  `);

  if (result.results.bindings.length) {
    const b = result.results.bindings[0];

    console.log('Getting the timestamp of the latest successfully ingested delta file. This will be used as starting point for consumption.');
    let latestDeltaTimestamp = await getLatestDeltaTimestamp();

    if (!latestDeltaTimestamp) {
      console.log(`It seems to be the first time we will consume delta's. No delta's have been consumed before.`);
      if (process.env.START_FROM_DELTA_TIMESTAMP) {
        console.log(`Service is configured to start consuming delta's since ${process.env.START_FROM_DELTA_TIMESTAMP}`);
        latestDeltaTimestamp = new Date(Date.parse(process.env.START_FROM_DELTA_TIMESTAMP));
      } else {
        console.log(`No configuration as of when delta's should be consumed. Starting consuming from sync task creation time ${b['created'].value}.`);
        latestDeltaTimestamp = new Date(Date.parse(b['created'].value));
      }
    }

    return new SyncTask({
      uri: b['s'].value,
      status: TASK_NOT_STARTED_STATUS,
      since: latestDeltaTimestamp,
      created: new Date(Date.parse(b['created'].value))
    });
  } else {
    return null;
  }
}

/**
 * Get the URI of the currently running sync task.
 * Null if no task is running.
 *
 * @public
*/
async function getRunningSyncTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    SELECT ?s WHERE {
      ?s a ext:SyncTask ;
         adms:status <${TASK_ONGOING_STATUS}> .
    } ORDER BY ?created LIMIT 1
  `);

  return result.results.bindings.length ? { uri: result.results.bindings[0]['s'] } : null;
}

/**
 * Get the latest timestamp of a successfully ingested delta file.
 * Even on failed tasks, we're sure ext:deltaUntil reflects the latest
 * successfully ingested delta file.
 *
 * @private
*/
async function getLatestDeltaTimestamp() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?s ?latestDelta WHERE {
      ?s a ext:SyncTask ;
         ext:deltaUntil ?latestDelta .
    } ORDER BY DESC(?latestDelta) LIMIT 1
  `);

  if (result.results.bindings.length) {
    const b = result.results.bindings[0];
    return new Date(Date.parse(b['latestDelta'].value));
  } else {
    return null;
  }
}

export default SyncTask;
export {
  scheduleSyncTask,
  getNextSyncTask,
  getRunningSyncTask
}
