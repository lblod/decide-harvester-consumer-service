import { sparqlEscapeUri, sparqlEscapeString, uuid } from "mu";
import { updateSudo as update } from '@lblod/mu-auth-sudo';

import {
    STATUS_BUSY,
    STATUS_SUCCESS,
    STATUS_FAILED,
    TARGET_GRAPH,
    connectionOptions,
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
} from "./task";
import { getLatestDumpFile } from "./dump-file";
import { calculateLatestDeltaTimestamp, getSortedUnconsumedFiles } from "./delta-file";
import { writeTtlFile } from "./file-helper";



export async function run(deltaEntry) {
    const task = await loadTask(deltaEntry);
    if (!task) return;
    try {
        await updateTaskStatus(task, STATUS_BUSY);
        const graphContainer = { id: uuid() };
        const resultContainer = `http://redpencil.data.gift/id/result-containers/initial-sync/${uuid()}`;
        graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
        const fileContainer = { id: uuid() };
        fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${fileContainer.id}`;
        const collection = await getHarvestCollectionForTask(task);
        const rdo = await getRemoteDataObjects(task, collection);
        if (rdo?.length !== 1) {
            throw Error('length of rdo should be one! ' + rdo?.length);
        }
        let { taskType } = rdo[0];
        switch (taskType) {
            case TYPE_INITIAL_SYNC:
                const dumpFile = await getLatestDumpFile();
                await dumpFile.loadAndDispatch(resultContainer);

                break;
            case TYPE_DELTA_FILES:
                const latestDeltaTimestamp = await calculateLatestDeltaTimestamp();
                const sortedDeltafiles = await getSortedUnconsumedFiles(
                    latestDeltaTimestamp
                );
                for (const deltaFile of sortedDeltafiles) {
                    const { deletes, inserts } = deltaFile.load(async (filePath, fileName) => {
                        const fileResult = await writeTtlFile(task.graph, filePath, fileName, task.id);
                        await appendTaskResultFile(task, fileContainer, fileResult);
                    });
                    //  fixme you probably mapped data differently so this could be too simplistic for your use case
                    await deleteFromGraph(deletes, HIGH_LOAD_DATABASE_ENDPOINT, TARGET_GRAPH, {}); // deletion is processed directly.
                    await insertIntoGraph(inserts, HIGH_LOAD_DATABASE_ENDPOINT, resultContainer, {}); // insertions will be processed in the next step
                }
                break;
        }

        await appendTaskResultGraph(task, graphContainer, resultContainer);
        await updateTaskStatus(task, STATUS_SUCCESS, taskType);
    } catch (e) {
        console.error(e);
        if (task) {
            await appendTaskError(task, e.message);
            await updateTaskStatus(task, STATUS_FAILED);
        }
    }
}
async function appendTaskResultGraph(task, container, graphUri) {
    // prettier-ignore
    const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(container.id)}.
        ${sparqlEscapeUri(container.uri)} task:hasGraph ${sparqlEscapeUri(graphUri)}.

        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(container.uri)}.
    }
  `;

    await update(queryStr, connectionOptions);
}
