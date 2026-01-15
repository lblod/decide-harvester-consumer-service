import fs from "fs-extra";
import path from "path";
import zlib from "zlib";
import { json } from "stream/consumers";
import fetcher from "node-fetch";
import {
  DELTA_FILE_FOLDER,
  DOWNLOAD_FILE_ENDPOINT,
  SYNC_FILES_ENDPOINT,
  PREFIXES,
  TASK_TYPE,
  STATUS_SUCCESS,
  GET_FILE_ENDPOINT,
  TASK_CONSUME,
  START_FROM_DELTA_TIMESTAMP,
  TYPE_DELTA_FILES,
} from "./../constant";
import { toTermObjectArray, downloadFile, parseResult } from "./super-utils";
import mu from "mu";

import { querySudo as query } from "@lblod/mu-auth-sudo";

fs.ensureDirSync(DELTA_FILE_FOLDER);

export default class DeltaFile {
  constructor(data) {
    /** Id of the delta file */
    this.id = data.id;
    /** Creation datetime of the delta file */
    this.created = data.attributes.created;
    /** Name of the delta file */
    this.name = data.attributes.name || mu.uuid() + ".json";
    this.format = data.format || "application/json";
    /** Folder in which the delta files are stored in case they are kept */
    this.folderByDay = new Intl.DateTimeFormat("en-CA", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date(this.created));
  }

  /**
   * Public endpoint to download the delta file from based on its id
   */
  get downloadUrl() {
    return DOWNLOAD_FILE_ENDPOINT.replace(":id", this.id);
  }

  /**
   * Location to store the delta file during processing
   */
  get filePath() {
    return path.join(DELTA_FILE_FOLDER, this.fileName);
  }

  get fileName() {
    return this.name;
  }

  async download() {
    try {
      await downloadFile(this.downloadUrl, this.filePath);
    } catch (e) {
      console.log(`Something went wrong while downloading file from ${this.downloadUrl}`);
      console.log(e);
      throw e;
    }
  }

  async load(
    processDeltaFileCallback = async (deltaFilePath, deltaFileName) =>
      console.log("do not store or delete delta file by default")
  ) {
    try {
      await this.download();
      let fileStream = fs.createReadStream(this.filePath);
      if (this.format === "application/gzip") {
        console.log(`${this.filePath} is a gzipped file. piping gunzip to process ttl...`);
        const gunzip = zlib.createGunzip();
        fileStream = fileStream.pipe(gunzip);
      }
      const changeSets = await json(fileStream);

      const termObjectChangeSets = [];
      for (let { inserts, deletes } of changeSets) {
        const changeSet = {};
        changeSet.deletes = toTermObjectArray(deletes);
        changeSet.inserts = toTermObjectArray(inserts);

        termObjectChangeSets.push(changeSet);
      }
      console.log(`Successfully loaded file ${this.id} stored at ${this.filePath}`);

      await processDeltaFileCallback(this.filePath, this.fileName);

      return { termObjectChangeSets };
    } catch (error) {
      console.log(
        `Something went wrong while ingesting file ${this.id} stored at ${this.filePath}`
      );
      console.log(error);
      throw error;
    }
  }
}

async function loadTimestampFromJob() {
  const queryStr = `
    ${PREFIXES}
    SELECT DISTINCT ?deltaTimestamp WHERE {
      ?task a ${sparqlEscapeUri(TASK_TYPE)};
              adms:status ${sparqlEscapeUri(STATUS_SUCCESS)};
              task:operation ${sparqlEscapeUri(TASK_CONSUME)};
              ext:metadata ${sparqlEscapeUri(TYPE_DELTA_FILES)};
              dct:modified ?deltaTimestamp.

       }
    }
    ORDER BY DESC(?deltaTimestamp)
    LIMIT 1
  `;
  const res = parseResult(await query(queryStr));
  return res?.length ? res[0] : undefined;
}

export async function calculateLatestDeltaTimestamp() {
  const timeStampFromConfig = loadTimestampFromConfig();
  const deltaTimestamp = (await loadTimestampFromJob())?.deltaTimestamp;

  if (deltaTimestamp && timeStampFromConfig && timeStampFromConfig > deltaTimestamp) {
    console.log(`
      The timestamp provided by the config (${timeStampFromConfig})
        is more recent than the one found in the DB (${deltaTimestamp}).
      We start from the provided timestamp in the config.`);
    return timeStampFromConfig;
  } else if (deltaTimestamp) {
    return deltaTimestamp;
  } else if (timeStampFromConfig) {
    console.log(`Using provided timestamp from config ${timeStampFromConfig}`);
    return timeStampFromConfig;
  } else {
    const now = new Date();
    console.log(`No previous timestamp found, starting from ${now}`);
    return now;
  }
}
function loadTimestampFromConfig() {
  console.log(`Trying loading START_FROM_DELTA_TIMESTAMP from the environment.`);
  if (START_FROM_DELTA_TIMESTAMP) {
    console.log(
      `Service is configured to start consuming delta's since ${START_FROM_DELTA_TIMESTAMP}`
    );
    return new Date(Date.parse(START_FROM_DELTA_TIMESTAMP));
  } else return null;
}
export async function getSortedUnconsumedFiles(since) {
  try {
    const urlToCall = `${SYNC_FILES_ENDPOINT}?since=${since.toISOString()}`;
    console.log(`Fetching delta files with url: ${urlToCall}`);
    const response = await fetcher(urlToCall, {
      headers: {
        Accept: "application/vnd.api+json",
        "Accept-encoding": "deflate,gzip",
      },
    });
    const json = await response.json();

    const deltaFiles = await Promise.all(
      json.data.map(async (deltaFileMetadata) => {
        let format = "application/json";
        try {
          const fileResponse = await fetcher(
            `${GET_FILE_ENDPOINT.replace(":id", deltaFileMetadata.id)}`,
            {
              headers: {
                Accept: "application/vnd.api+json",
              },
            }
          );
          const fileMetadata = await fileResponse.json();
          const file = { ...fileMetadata.data.attributes };
          format = file.format || format;
        } catch (e) {
          console.log("file endpoint not available, rollback to distribution.");
        }

        return new new DeltaFile({
          ...deltaFileMetadata,
          format,
        })();
      })
    );
    return deltaFiles.sort((f) => f.created);
  } catch (e) {
    console.log(`Unable to retrieve unconsumed files from ${SYNC_FILES_ENDPOINT}`);
    throw e;
  }
}
