import { updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeDateTime, sparqlEscapeUri, sparqlEscapeString, uuid } from 'mu';

import {
  MU_APPLICATION_GRAPH,
  BATCH_SIZE,
  TASK_URI_PREFIX,
  PREFIXES,
  TASK_TYPE,
  JOBS_GRAPH,
  STATUS_SCHEDULED,
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
  ERROR_URI_PREFIX,
  ERROR_TYPE,
  DELTA_ERROR_TYPE,
  INITIAL_SYNC_TASK_OPERATION
} from '../config';

class InitialSyncTask {
  constructor({ uri, created, status }) {
    /** Uri of the sync task */
    this.uri = uri;

    /**
     * Datetime as Data object when the task was created in the triplestore
    */
    this.created = created;

    /**
     * Current status of the sync task as stored in the triplestore
    */
    this.status = status;

    /**
     * The dump file to be ingested for this task
     *
     * @type DumpFile
    */
    this.dumpFile = null;
  }

  /**
   * Execute the initial sync task
   * I.e. consume the dump file by chuncks
   *
   * @public
  */
  async execute() {
    try {
      await this.updateStatus(STATUS_BUSY);
      if (this.dumpFile) {
        console.log(`Found dump file ${this.dumpFile.id} to be ingested`);
        await this.ingest();
      } else {
        console.log(`No dump file to consume. Is the producing stack ready?`);
        throw new Error('No dump file found.');
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
  async closeWithFailure(error) {
    await this.updateStatus(STATUS_FAILED);
    await this.storeError(error.message || error);

  }

  /**
   * Ingest the dump file in the triplestore.
   * We expect the receiving graph to be empty.
   *
   * @private
  */
  async ingest() {
    const file = this.dumpFile;
    await file.ingest(async (file, isSuccess, e = null) => {
      if (isSuccess) {
        console.log(`Finished initial sync task successfully.`);
        await this.updateStatus(STATUS_SUCCESS);
      } else {
        console.log(`Failed to ingest the dump file ${file.id}.`);
        throw e;
      }
    });
  }

  async storeError(errorMsg) {
    const id = uuid();
    const uri = ERROR_URI_PREFIX + id;

    const queryError = `
      ${PREFIXES}
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(JOBS_GRAPH)} {
          ${sparqlEscapeUri(uri)}
            a ${sparqlEscapeUri(ERROR_TYPE)}, ${sparqlEscapeUri(DELTA_ERROR_TYPE)} ;
            mu:uuid ${sparqlEscapeString(id)} ;
            oslc:message ${sparqlEscapeString(errorMsg)} .
          ${sparqlEscapeUri(this.uri)} task:error ${sparqlEscapeUri(uri)} .
        }
      }
    `;

    await update(queryError);
  }

  /**
  * Updates the status of the given resource
  */
  async updateStatus(status) {
    this.status = status;

    const q = `
      PREFIX adms: <http://www.w3.org/ns/adms#>

      DELETE {
        GRAPH ?g {
          ${sparqlEscapeUri(this.uri)} adms:status ?status .
        }
      }
      INSERT {
        GRAPH ?g {
          ${sparqlEscapeUri(this.uri)} adms:status ${sparqlEscapeUri(this.status)} .
        }
      }
      WHERE {
        GRAPH ?g {
          ${sparqlEscapeUri(this.uri)} adms:status ?status .
        }
      }
    `;
    await update(q);
  }
}

/**
 * Insert an initial sync job in the store to consume a dump file if no such task exists yet.
 *
 * @public
*/
async function scheduleInitialSyncTask(job) {
  const taskUri = await scheduleTask(job.uri, INITIAL_SYNC_TASK_OPERATION);
  console.log(`Scheduled initial sync task <${taskUri}> to ingest dump file`);
  return taskUri;
}

async function scheduleTask(jobUri, taskOperationUri, taskIndex = "0"){
  const taskId = uuid();
  const taskUri = TASK_URI_PREFIX + `${taskId}`;
  const created = new Date();
  const createTaskQuery = `
    ${PREFIXES}
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(JOBS_GRAPH)} {
        ${sparqlEscapeUri(taskUri)}
          a ${sparqlEscapeUri(TASK_TYPE)};
          mu:uuid ${sparqlEscapeString(taskId)};
          adms:status ${sparqlEscapeUri(STATUS_SCHEDULED)};
          dct:created ${sparqlEscapeDateTime(created)};
          dct:modified ${sparqlEscapeDateTime(created)};
          task:operation ${sparqlEscapeUri(taskOperationUri)};
          task:index ${sparqlEscapeString(taskIndex)};
          dct:isPartOf ${sparqlEscapeUri(jobUri)}.
      }
    }`;

  await update(createTaskQuery);

  return new InitialSyncTask({
    uri: taskUri,
    status: STATUS_SCHEDULED,
    created: created
  });
}

export default InitialSyncTask;
export {
  scheduleInitialSyncTask
};
