import { uuid } from "mu";
import path from "path";
import {
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
  TARGET_GRAPH,
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
    switch (taskType) {
      case TYPE_INITIAL_SYNC:
        const dumpFile = await getLatestDumpFile();
        await dumpFile.loadAndDispatch(TARGET_GRAPH);
        break;

      case TYPE_DELTA_FILES:
        const latestDeltaTimestamp = await calculateLatestDeltaTimestamp();
        const sortedDeltafiles = await getSortedUnconsumedFiles(latestDeltaTimestamp);

        for (const deltaFile of sortedDeltafiles) {
          const { deletes, inserts } = deltaFile.load(async (filePath, fileName) => {
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

          await deleteFromGraph(deletes, HIGH_LOAD_DATABASE_ENDPOINT, TARGET_GRAPH, {});
          await insertIntoGraph(inserts, HIGH_LOAD_DATABASE_ENDPOINT, TARGET_GRAPH, {});
        }
        break;
    }

    await appendTaskResultGraph(task, graphContainer, TARGET_GRAPH);
    await updateTaskStatus(task, STATUS_SUCCESS, taskType);
  } catch (e) {
    console.error(e);
    if (task) {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}
