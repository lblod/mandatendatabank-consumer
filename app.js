import requestPromise from 'request-promise';
import { app, errorHandler } from 'mu';
import { INGEST_INTERVAL, SYNC_BASE_URL, STATUS_SUCCESS, STATUS_BUSY } from './config';
import { getNextSyncTask, getRunningSyncTask, scheduleSyncTask, setTaskFailedStatus } from './lib/sync-task';
import { scheduleInitialSyncTask } from './lib/initial-sync-task';
import { getLatestInitialSyncJob, scheduleInitialSyncJob } from './lib/initial-sync-job';
import { getUnconsumedFiles } from './lib/delta-file';
import { getLatestDumpFile } from './lib/dump-file';
import { waitForDatabase } from './lib/database-utils';

/**
 * Core assumption of the microservice that must be respected at all times:
 *
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

async function triggerInitialSync() {
  console.log(`Executing scheduled initial sync at ${new Date().toISOString()}`);
  requestPromise.post('http://localhost/initial-sync/');
}

waitForDatabase(async () => {
  const runningTask = await getRunningSyncTask();
  if (runningTask) {
    console.log(`Task <${runningTask.uri.value}> is still ongoing at startup. Updating its status to failed.`);
    await setTaskFailedStatus(runningTask.uri.value);
  }
  triggerInitialSync();
});

app.post('/ingest', async function( req, res, next ) {
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

app.post('/initial-sync', async function( req, res ) {
  const previousJob = getLatestInitialSyncJob();
  let job = null;

  if (previousJob) {
    if (previousJob.status == STATUS_SUCCESS) {
      console.log(`Previous initial sync job <${previousJob.uri}> was a success, starting the ingestion of deltas.`);
      triggerIngest();
    } else {
      if (previousJob.status == STATUS_BUSY) {
        console.log(`Previous job ${previousJob.uri} seems to be stuck on busy, failing it.`);
        previousJob.closeWithFailure();
      }
      console.log(`Latest previous job didn't end up as expected. Schedueling a new one.`);
      job = await scheduleInitialSyncJob();
    }
  } else {
    console.log(`No previous job found, schedueling one.`);
    job = await scheduleInitialSyncJob();
  }

  if (job) {
    const task = await scheduleInitialSyncTask(job);
    console.log(`Start initial sync with ${SYNC_BASE_URL}`);
    try {
      const dumpFile = await getLatestDumpFile();
      task.dumpFile = dumpFile;
      task.execute();
      job.updateStatus(STATUS_SUCCESS);
      console.log(`Initial sync went smoothly, starting ingesting delta files.`);
      triggerIngest();
      return res.status(202).end();
    } catch(e) {
      console.log(`Something went wrong while doing the initial sync. Closing task with failure state.`);
      console.trace(e);
      await task.closeWithFailure(e);
      await job.closeWithFailure();
      console.log(`Error stored, stopping the service as the initial sync is mandatory.`);
      process.exit();
    }
  } else {
    console.log(`No scheduled initial sync task found.`);
    return res.status(200).end();
  }
});

app.use(errorHandler);
