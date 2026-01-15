import { uuid } from "mu";
import {
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
  LANDING_GRAPH,
  TYPE_INITIAL_SYNC,
  TYPE_DELTA_FILES,
  HIGH_LOAD_DATABASE_ENDPOINT,
} from "../constant";
import {
  loadTask,
  updateTaskStatus,
  appendTaskError,
  getHarvestCollectionForTask,
  getRemoteDataObjects,
  appendTaskResultFile,
  appendTaskResultGraph,
} from "./task";
import { getLatestDumpFile } from "./dump-file";
import { calculateLatestDeltaTimestamp, getSortedUnconsumedFiles } from "./delta-file";
import { writeFile } from "./file-helper";
import { deleteFromGraph, insertIntoGraph } from "./super-utils";

export async function run(deltaEntry) {
  const task = await loadTask(deltaEntry);
  if (!task) return;
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    const graphContainer = { id: uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;

    const fileContainer = { id: uuid() };
    fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${fileContainer.id}`;

    const collection = await getHarvestCollectionForTask(task);
    const rdo = await getRemoteDataObjects(task, collection);
    if (rdo?.length !== 1) {
      throw Error("length of rdo should be one! " + rdo?.length);
    }

    let { taskType } = rdo[0];
    let resultGraphUri = LANDING_GRAPH;
    switch (taskType) {
      case TYPE_INITIAL_SYNC:
        const dumpFile = await getLatestDumpFile();
        await dumpFile.loadAndDispatch(LANDING_GRAPH);
        break;

      case TYPE_DELTA_FILES:
        const tempGraphUri = `http://redpencil.data.gift/id/graphs/${uuid()}`;
        resultGraphUri = tempGraphUri;
        const latestDeltaTimestamp = await calculateLatestDeltaTimestamp();
        const sortedDeltafiles = await getSortedUnconsumedFiles(latestDeltaTimestamp);

        for (const deltaFile of sortedDeltafiles) {
          const { termObjectChangeSets } = await deltaFile.load(async (filePath, fileName) => {
            const fileResult = await writeFile(
              task.graph,
              filePath,
              fileName,
              task.id,
              "delta",
              path.extname(fileName),
              deltaFile.format
            );
            await appendTaskResultFile(task, fileContainer, fileResult);
          });

          for (const { deletes, inserts } of termObjectChangeSets) {
            await deleteFromGraph(deletes, HIGH_LOAD_DATABASE_ENDPOINT, LANDING_GRAPH, {});
            await insertIntoGraph(inserts, HIGH_LOAD_DATABASE_ENDPOINT, LANDING_GRAPH, {});
            await insertIntoGraph(inserts, HIGH_LOAD_DATABASE_ENDPOINT, tempGraphUri, {});
          }
        }
        break;
    }

    await appendTaskResultGraph(task, graphContainer, resultGraphUri);
    await updateTaskStatus(task, STATUS_SUCCESS, taskType);
  } catch (e) {
    console.error(e);
    if (task) {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}
