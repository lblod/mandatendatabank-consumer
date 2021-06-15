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
  DUMPFILE_FOLDER,
  SYNC_FILES_PATH,
} from '../config';

//TODO: use other place to store these downloaded files
const BASEPATH = path.join(SYNC_FILES_PATH, DUMPFILE_FOLDER);
fs.ensureDirSync(BASEPATH);
class DumpFile {
  constructor(data) {
    this.id = data.id;
  }

  get downloadUrl() {
    return DOWNLOAD_FILE_ENDPOINT.replace(':id', this.id);
  }

  get filePath() {
    return path.join(BASEPATH,`${this.id}.ttl`);
  }

  async download(){
    return new Promise((resolve, reject)=> {
      //TODO: it can be modernized with node-fetch
      const writeStream = fs.createWriteStream(this.filePath);
      writeStream
        .on('finish', resolve)
        .on('error', error => {
          console.log(`Something went wrong while saving file from ${this.downloadUrl}`);
          console.log(error);
          reject(error);
        });

      request(this.downloadUrl)
        .pipe(writeStream)
        .on('error', error => {
          console.log(`Something went wrong while downloading file from ${this.downloadUrl}`);
          console.log(error);
          reject(error);
        });
    });
  }

  async load() {
    try {
      await this.download();
      const triples = fs.readFileSync(this.filePath, 'utf8').split('\n');
      console.log(`Start ingesting file ${this.id} stored at ${this.filePath}`);
      console.log(`Successfully finished ingesting file ${this.id} stored at ${this.filePath}`);
      return triples;
    }
    catch(error){
      console.log(`Something went wrong while ingesting file ${this.id} stored at ${this.filePath}`);
      console.log(error);
      throw error;
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

export {
  getLatestDumpFile
};
