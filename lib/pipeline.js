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
  getInputContainersForTask,
  appendTaskResultContainer,
  graphHasData,
} from "./task";
import { getLatestDumpFile } from "./dump-file";
import { calculateLatestDeltaTimestamp, getSortedUnconsumedFiles } from "./delta-file";
import { deleteFromGraph, insertIntoGraph } from "./super-utils";

export async function run(deltaEntry) {
  const task = await loadTask(deltaEntry);
  if (!task) return;
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    const inputContainers = await getInputContainersForTask(task);
    if (!inputContainers?.length) {
      throw Error("task has no input containers");
    }

    const containersByInputContainer = new Map();
    for (const container of inputContainers) {
      const group = containersByInputContainer.get(container.inputContainer) || [];
      group.push(container);
      containersByInputContainer.set(container.inputContainer, group);
    }
    for (const [inputContainer, group] of containersByInputContainer) {
      if (group.length > 1) {
        throw Error(`input container ${inputContainer} resolves to more than one remote data object`);
      }
    }

    const initialSyncContainers = inputContainers.filter((c) => c.taskType === TYPE_INITIAL_SYNC);
    const deltaContainers = inputContainers.filter((c) => c.taskType === TYPE_DELTA_FILES);

    if (initialSyncContainers.length) {
      if (await graphHasData(LANDING_GRAPH)) {
        console.log(
          `Landing graph <${LANDING_GRAPH}> already contains data, skipping initial sync ingestion.`
        );
      } else {
        const dumpFile = await getLatestDumpFile();
        await dumpFile.loadAndDispatch(LANDING_GRAPH);
      }

      for (const { resource } of initialSyncContainers) {
        const resultContainer = { id: uuid() };
        resultContainer.uri = `http://redpencil.data.gift/id/dataContainers/${resultContainer.id}`;
        await appendTaskResultContainer(task, resultContainer, resource);
      }
    }

    if (deltaContainers.length) {
      const tempGraphUri = `http://redpencil.data.gift/id/graphs/${uuid()}`;
      const latestDeltaTimestamp = await calculateLatestDeltaTimestamp();
      const sortedDeltafiles = await getSortedUnconsumedFiles(latestDeltaTimestamp);

      for (const deltaFile of sortedDeltafiles) {
        const { termObjectChangeSets } = await deltaFile.load();

        for (const { deletes, inserts } of termObjectChangeSets) {
          await deleteFromGraph(deletes, HIGH_LOAD_DATABASE_ENDPOINT, LANDING_GRAPH, {});
          await insertIntoGraph(inserts, HIGH_LOAD_DATABASE_ENDPOINT, LANDING_GRAPH, {});
          await insertIntoGraph(inserts, HIGH_LOAD_DATABASE_ENDPOINT, tempGraphUri, {});
        }
      }

      for (const { resource } of deltaContainers) {
        const resultContainer = { id: uuid() };
        resultContainer.uri = `http://redpencil.data.gift/id/dataContainers/${resultContainer.id}`;
        await appendTaskResultContainer(task, resultContainer, resource, tempGraphUri);
      }
    }

    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    if (task) {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}
