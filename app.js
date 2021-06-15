import requestPromise from 'request-promise';
import { app, errorHandler } from 'mu';
import { INGEST_INTERVAL,
         STATUS_SUCCESS,
         STATUS_FAILED,
         DISABLE_INITIAL_SYNC,
         DISABLE_DELTA_INGEST,
         INITIAL_SYNC_JOB_OPERATION
       } from './config';
import { getNextSyncTask,
         getRunningSyncTask,
         scheduleSyncTask,
         setTaskFailedStatus,
         createSyncTask,
         TASK_SUCCESS_STATUS
       } from './lib/sync-task';
import { scheduleInitialSyncTask } from './lib/initial-sync-task';
import { getLatestInitialSyncJob, scheduleInitialSyncJob } from './lib/initial-sync-job';
import { getUnconsumedFiles } from './lib/delta-file';
import { getLatestDumpFile } from './lib/dump-file';
import { waitForDatabase } from './lib/database-utils';
import { storeError, cleanupJobs, getJobs } from './lib/utils';

/**
 * Core assumption of the microservice that must be respected at all times:
 * 0. An initial sync has run
 * 1. At any moment we know that the latest ext:deltaUntil timestamp
 *    on a task, either in failed/ongoing/success state, reflects
 *    the timestamp of the latest delta file that has been
 *    completly and successfully consumed
 * 2. Maximum 1 sync task is running at any moment in time
*/

async function triggerIngest() {
  console.log(`Executing scheduled ingest at ${new Date().toISOString()}`);
  requestPromise.post('http://localhost/ingest/');
  setTimeout( triggerIngest, INGEST_INTERVAL );
}

async function runInitialSync(){
  let job;
  let task;

  try {
    //Note: they get status busy
    job = await scheduleInitialSyncJob();
    task = await scheduleInitialSyncTask(job);

    const dumpFile = await getLatestDumpFile();
    task.dumpFile = dumpFile;
    task.execute();

    //Some glue to coordinate the nex sync-task. It needs to know from where it needs to start syncing
    await createSyncTask(task.dumpFile.issued, TASK_SUCCESS_STATUS);
    await job.updateStatus(STATUS_SUCCESS);

    return job;
  }
  catch(e) {
    console.log(`Something went wrong while doing the initial sync. Closing task with failure state.`);
    console.trace(e);
    if(task)
      await task.closeWithFailure(e);
    if(job)
      await job.closeWithFailure();
  }
  return null;
}

waitForDatabase(async () => {
  try {

    console.log('Database is up, proceeding with setup.');
    console.info(`DISABLE_INITIAL_SYNC: ${DISABLE_INITIAL_SYNC}`);

    if(!DISABLE_INITIAL_SYNC) {

      const initialSyncJob = await getLatestInitialSyncJob();

      //In following case we can safely (re)schedule an initial sync
      if(!initialSyncJob || initialSyncJob.status == STATUS_FAILED){
        console.log(`No initial sync has run yet, or previous failed (see: ${initialSyncJob ? initialSyncJob.uri : 'N/A'})`);
        console.log(`(Re)starting initial sync`);

        const job = await runInitialSync();

        console.log(`${job.uri} has status ${job.status}, start ingesting deltas`);
      }
      else if(initialSyncJob.status !== STATUS_SUCCESS){
        throw `Unexpected status for ${initialSyncJob.uri}: ${initialSyncJob.status}. Check in the database what went wrong`;
      }

    }
    else {
      console.warn('Initial sync disabled');
    }

    console.info(`DISABLE_DELTA_INGEST: ${DISABLE_DELTA_INGEST}`);
    if(!DISABLE_DELTA_INGEST) {

      //normal operation mode: ingest deltas
      const runningTask = await getRunningSyncTask();
      if (runningTask) {
        console.log(`Task <${runningTask.uri.value}> is still ongoing at startup. Updating its status to failed.`);
        await setTaskFailedStatus(runningTask.uri.value);
      }

      triggerIngest(); //don't wait because periodic/recursive job. Note: it won't catch errors!
    }
    else {
      console.warn('Automated delta ingest disabled');
    }
  }

  catch(e) {
    console.log(e);
    await storeError(`Unexpected error while booting the service: ${e}`);
  }
});

/*
 * ENDPOINTS CURRENTLY MEANT FOR DEBUGGING
 */
app.post('/ingest', async function( _, res, next ) {
  //TODO: it feels cleaner to move the logic to a separate function
  //TODO: proper error logging so we get a mail!
  await scheduleSyncTask();

  const isRunning = await getRunningSyncTask();

  if (!isRunning) {
    const task = await getNextSyncTask();
    if (task) {
      console.log(`Start ingesting new delta files since ${task.since.toISOString()}`);
      try {
        const files = await getUnconsumedFiles(task.since);
        task.files = files;
        task.execute();
        return res.status(202).end();
      } catch(e) {
        console.log(`Something went wrong while ingesting. Closing sync task with failure state.`);
        console.trace(e);
        await task.closeWithFailure();
        return next(new Error(e));
      }
    } else {
      console.log(`No scheduled sync task found. Did the insertion of a new task just fail?`);
      return res.status(200).end();
    }
  } else {
    console.log('A sync task is already running. A new task is scheduled and will start when the previous task finishes');
    return res.status(409).end();
  }
});

app.post('/initial-sync-jobs', async function( _, res ){
  runInitialSync();
  res.send({ msg: 'Started initial sync job' });
});

app.delete('/initial-sync-jobs', async function( _, res ){
  const jobs = await getJobs(INITIAL_SYNC_JOB_OPERATION);
  await cleanupJobs(jobs);
  res.send({ msg: 'Initial sync jobs cleaned' });
});

app.use(errorHandler);
