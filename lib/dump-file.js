import requestPromise from 'request-promise';
import fs from 'fs-extra';
import request from 'request';
import { updateSudo as update } from '@lblod/mu-auth-sudo';

import {
  MU_APPLICATION_GRAPH,
  SYNC_BASE_URL,
  SYNC_DATASET_ENDPOINT,
  DOWNLOAD_FILE_ENDPOINT,
  BATCH_SIZE
} from '../config';

class DumpFile {
  constructor(data) {
    this.id = data.id;
  }

  get downloadUrl() {
    return DOWNLOAD_FILE_ENDPOINT.replace(':id', this.id);
  }

  get tmpFilepath() {
    return `/tmp/${this.id}.ttl`;
  }

  async ingest(onFinishCallback) {
    const writeStream = fs.createWriteStream(this.tmpFilepath);
    writeStream.on('finish', () => this.ingestByChunks(onFinishCallback));

    try {
      request(this.downloadUrl)
        .on('error', function(e) {
          console.log(`Something went wrong while downloading file from ${this.downloadUrl}`);
          console.log(e);
          onFinishCallback(this, false, e);
        })
        .pipe(writeStream);
    } catch (e) {
      console.log(`Something went wrong while consuming the file ${this.id}`);
      await onFinishCallback(this, false, e);
    }
  }

  async ingestByChunks(onFinishCallback) {
    console.log(`Start ingesting file ${this.id} stored at ${this.tmpFilepath}`);
    try {
      const triples = fs.readFileSync(this.tmpFilepath, 'utf8').split('\n');
      await insertTriples(triples);
      console.log(`Successfully finished ingesting file ${this.id} stored at ${this.tmpFilepath}`);
      await onFinishCallback(this, true);
      await fs.unlink(this.tmpFilepath);
    } catch (e) {
      console.log(`Something went wrong while ingesting file ${this.id} stored at ${this.tmpFilepath}`);
      console.log(e);
      await onFinishCallback(this, false, e);
    }
  }
}

async function getLatestDumpFile() {
  try {
    console.log(`Retrieving latest dataset from ${SYNC_DATASET_ENDPOINT}`);
    const resultDataset = await requestPromise({
      uri: `${SYNC_DATASET_ENDPOINT}`,
      qs: {
        'filter[:has-no:next-version]': 'yes',
      },
      headers: {
        'Accept': 'application/vnd.api+json'
      },
      json: true // Automatically parses the JSON string in the response
    });

    if (resultDataset.data.length) {
      const distributionRelatedLink = resultDataset.data[0].relationships.distributions.links.related;
      const distributionUri = `${SYNC_BASE_URL}/${distributionRelatedLink}`;

      console.log(`Retrieving distribution from ${distributionUri}`);
      const resultDistribution = await requestPromise({
        uri: distributionUri,
        qs: {
          include: 'subject'
        },
        headers: {
          'Accept': 'application/vnd.api+json'
        },
        json: true // Automatically parses the JSON string in the response
      });

      return new DumpFile(resultDistribution.data[0].relationships.subject.data);
    } else {
      throw 'No dataset was found at the producing endpoint.';
    }
  } catch (e) {
    console.log(`Unable to retrieve dataset from ${SYNC_DATASET_ENDPOINT}`);
    throw e;
  }
}

async function insertTriples(triples) {
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    console.log(`Inserting triples in batch: ${i}-${i + BATCH_SIZE}`);

    const batch = triples.slice(i, i + BATCH_SIZE).join('\n');

    await update(`
      INSERT DATA {
        GRAPH <${MU_APPLICATION_GRAPH}> {
          ${batch}
        }
      }
    `);
  }
}

export {
  getLatestDumpFile
};
