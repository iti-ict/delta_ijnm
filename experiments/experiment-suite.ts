/**
 * DELTA: DLT - Database synchronization
 * Suite of evaluation experiments
 */

import { IDeltaClient, SmartContract, ContractEvent } from '../delta/Delta';
import { IDeltaDatabaseConnector } from '../database/DeltaDatabase';
import { BlockExplorer } from '../delta/components/BlockManager';
import { EventSynchronizer } from '../delta/components/EventSynchronizer';
import { AssetKV, AssetType, assetKeyValueGenerator } from './asset-generation';
import { logMessage, logGreenMessage, logRedMessage, logObject } from '../utils/Log';
import { sleep } from '../utils/Util';

import { performance as perf } from 'perf_hooks';
import * as fs from 'fs';

const NUM_ASSETS = 'numAssets';
const INSERTION = 'insert';
const KEY_VALUE_QUERY = 'key-value';
const COMPLEX_QUERY = 'complex';
const COMPLEX_RICH_QUERY = 'rich';
const SYNCHRONIZATION = 'sync';

interface ResultEntry {
    [INSERTION]: number,
    [KEY_VALUE_QUERY]?: number | number[],
    [COMPLEX_QUERY]?: number | number[],
    [COMPLEX_RICH_QUERY]?: number | number[]
}

interface JsonResult {
    [assets: number]: ResultEntry,
    [SYNCHRONIZATION]?: number
}

abstract class ExperimentSuite<DLT extends IDeltaClient> {

    protected abstract setupDltClients(numNodes: number): Array<Promise<DLT>>;
    protected abstract instantiateSmartContract(dltClient: DLT, chaincode: string): Promise<SmartContract>;
    protected abstract decodeAssetObject(dltClient: DLT, rawAsset: any): object;
    
    protected dltClients: DLT[];
    protected smartContract: SmartContract;
    protected assetType: AssetType;

    protected async setupDltEvaluation(numOrgs: number, chaincode: string, assetType: AssetType): Promise<void> {
        this.dltClients = await Promise.all(this.setupDltClients(numOrgs));
        this.smartContract = await this.instantiateSmartContract(this.dltClients[0], chaincode);
        this.assetType = assetType;
    }
    
    // Entry point for launching evaluation to the DLT network
    public async dltEvaluation(numOrgs: number, chaincode: string, assetType: AssetType,
            totalAssets: number, batchSize: number, checkpoint: number, numQueries: number,
            outputPath?: string): Promise<object> {
        
        await this.setupDltEvaluation(numOrgs, chaincode, assetType);
        
        const assetGenerator = assetKeyValueGenerator(assetType, totalAssets);
        const resultObj: JsonResult = {};
        const numRounds = totalAssets / numOrgs / batchSize;
        let numAssets = 0;
        let insertionTotalTime = 0;
        let insertionInit;

        const dumpResultsObject = async (): Promise<void> => {
            if (!!outputPath) {
                writeResultsFile(resultObj, outputPath);
            } else {
                logObject(resultObj);
            }
            process.exit(1);
        }
        process.on('SIGINT', dumpResultsObject);
        process.on('SIGTERM', dumpResultsObject);

        try {
            for (let i = 0; i < numRounds; i++) {
                for (let j = 0; j < numOrgs; j++) {
                    for (let k = 0; k < batchSize; k++) {

                        // Transaction dispatch
                        if (numAssets % checkpoint == 0) {
                            insertionInit = perf.now();
                        }
                        const asset = assetGenerator.next().value;
                        const dlt = this.dltClients[j];
                        await this.insertAssetDlt(dlt, asset, k == batchSize - 1);

                        // Measure time and perform synchronization and queries when reaching a checkpoint
                        if (++numAssets % checkpoint == 0) {
                            insertionTotalTime += perf.now() - insertionInit;
                            logTime('insertion', insertionTotalTime, numAssets, assetType);
                            const measuredTime: ResultEntry = { insert: insertionTotalTime };
                            resultObj[numAssets] = measuredTime;
                            // Dump results in case an error raises in next insertion/query operation batch
                            writeResultsFile(resultObj, outputPath);
                            await sleep(numAssets / 4);

                            // Queries
                            measuredTime[KEY_VALUE_QUERY] = median(await this.assetQuery(
                                dlt, numAssets, numQueries));
                            measuredTime[COMPLEX_QUERY] = await this.complexQuery(
                                dlt, numAssets, numQueries / 10);
                            measuredTime[COMPLEX_RICH_QUERY] = await this.complexRichQuery(
                                dlt, numAssets, numQueries / 10);

                            // Dump results in case an error raises in next insertion batch
                            writeResultsFile(resultObj, outputPath);
                        }
                    }
                }
            }
        } catch (ex) {
            console.error('DLT EVALUATION PROCESS FAILED!!!!');
            console.error(ex);
        } finally {
            if (!!outputPath) {
                writeResultsFile(resultObj, outputPath);
            }

            this.dltClients.forEach(async (dlt) => await dlt.disconnectEventHub());
        }

        return resultObj;
    }

    // Key-Value query
    protected async assetQuery(client: DLT, numAssets: number, numQueries: number): Promise<number[]> {
        const queryTime: number[] = [];
        for (let n = 0; n < numQueries; n++) {
            const key = `id-${this.assetType}-${Math.floor(Math.random() * numAssets) + 1}`;
            const initTime = perf.now();
            await this.retrieveAssetDlt(client, key);
            queryTime.push(perf.now() - initTime);
        }
        logTime(`${numQueries} key-value queries`, queryTime.reduce((a, b) => a + b), numAssets, this.assetType);
        await sleep(numAssets / 4);
        return queryTime;
    }

    // Complex query, performed by iterating and filtering on the ledger data assets
    protected async complexQuery(client: DLT, numAssets: number, numQueries: number): Promise<number[]> {
        const queryTime: number[] = [];
        for (let n = 0; n < numQueries; n++) {
            const initTime = perf.now();
            await this.executeComplexQueryDlt(client, false);
            queryTime.push(perf.now() - initTime);
        }
        logTime(`${numQueries} complex queries`, queryTime.reduce((a, b) => a + b), numAssets, this.assetType);
        await sleep(numAssets / 4);
        return queryTime;
    }

    // Complex query, performed using sophisticate mechanisms native to the DLT
    protected async complexRichQuery(client: DLT,  numAssets: number, numQueries: number): Promise<number[]> {
        const richQueryTime: number[] = [];
        for (let n = 0; n < numQueries; n++) {
            const initTime = perf.now();

            // This method is implemented on a different fashion depending on the specific DLT used
            await this.executeComplexQueryDlt(client, true);
            richQueryTime.push(perf.now() - initTime);
        }
        logTime(`${numQueries} rich complex queries`, richQueryTime.reduce((a, b) => a + b), numAssets, this.assetType);
        await sleep(numAssets / 4);
        return richQueryTime;
    }

    protected async insertAssetDlt(dltClient: DLT, asset: AssetKV,
            sync: boolean, log=false): Promise<void> {
        try {
            const invokation = sync ? dltClient.invokeSync : dltClient.invokeAsync;
            await invokation.call(dltClient, this.smartContract, 'upsertAsset',
                asset.id, asset.value);
            if (log) {
                logMessage(`Inserted asset ${asset.id}`);
            }
        } catch (ex) {
            logRedMessage(`${asset.id}: ${ex.message}`);
        }
    }

    protected async retrieveAssetDlt(dltClient: DLT, key: string, log=false): Promise<void> {
        try {
            const queryResult = await dltClient.query(this.smartContract, 'readAsset', key);
            if (log && !!queryResult) {
                const queryResultAsset = this.decodeAssetObject(dltClient, queryResult);
                logMessage(`Retrieved asset ${key}:\t${JSON.stringify(queryResultAsset)}`);
            }
        } catch (ex) {
            logRedMessage(`${key}: ${ex.message}`);
        }
    }

    protected abstract executeComplexQueryDlt(client: DLT, richQuery: boolean,
        param1?: number, param2?: number, log?: boolean): Promise<void>;
}

abstract class SynchronizationExperimentSuite
        <DLT extends IDeltaClient, DB extends IDeltaDatabaseConnector> extends ExperimentSuite<DLT> {
    
    protected abstract setupDatabaseConnector(): Promise<DB>;
    
    protected dbConnector: DB;
    private lastBlockNumberSynchronized = 1;

    protected async setupDltEvaluation(numOrgs: number, chaincode: string, assetType: AssetType): Promise<void> {
        await super.setupDltEvaluation(numOrgs, chaincode, assetType);
        this.dbConnector = await this.setupDatabaseConnector();
    }

    // Entry point for launching evaluation to the DLT network and the synchronization process
    public async dltEvaluation(numOrgs: number, chaincode: string, assetType: AssetType,
            totalAssets: number, batchSize: number, checkpoint: number, numQueries: number,
            outputPath?: string): Promise<object> {
        
        // Setup DLT and synchronization evaluation
        await this.setupDltEvaluation(numOrgs, chaincode, assetType);
        
        const assetGenerator = assetKeyValueGenerator(assetType, totalAssets);
        const resultObj: JsonResult = {};
        const numRounds = totalAssets / numOrgs / batchSize;
        let numAssets = 0;
        let insertionTotalTime = 0;
        let insertionInit;

        const dumpResultsObject = async (): Promise<void> => {
            if (!!outputPath) {
                writeResultsFile(resultObj, outputPath);
            } else {
                logObject(resultObj);
            }
            process.exit(1);
        };
        process.on('SIGINT', dumpResultsObject);
        process.on('SIGTERM', dumpResultsObject);

        try {
            for (let i = 0; i < numRounds; i++) {
                for (let j = 0; j < numOrgs; j++) {
                    for (let k = 0; k < batchSize; k++) {

                        // Transaction dispatch
                        if (numAssets % checkpoint == 0) {
                            insertionInit = perf.now();
                        }
                        const asset = assetGenerator.next().value;
                        const dlt = this.dltClients[j];
                        await this.insertAssetDlt(dlt, asset, k == batchSize - 1);

                        // Measure time and perform synchronization and queries when reaching a checkpoint
                        if (++numAssets % checkpoint == 0) {
                            insertionTotalTime += perf.now() - insertionInit;
                            logTime('insertion', insertionTotalTime, numAssets, assetType);
                            const measuredTime: ResultEntry = { insert: insertionTotalTime };
                            resultObj[numAssets] = measuredTime;
                            // Dump results in case an error raises in next insertion/query operation batch
                            writeResultsFile(resultObj, outputPath);
                            await sleep(numAssets / 4);

                            // Queries
                            measuredTime[KEY_VALUE_QUERY] = median(await this.assetQuery(
                                dlt, numAssets, numQueries));
                            if (assetType === AssetType.Simple) {
                                measuredTime[COMPLEX_QUERY] = await this.complexQuery(
                                    dlt, numAssets, numQueries / 10);
                                measuredTime[COMPLEX_RICH_QUERY] = await this.complexRichQuery(
                                    dlt, numAssets, numQueries / 10);
                            }

                            // Dump results in case an error raises in next insertion batch
                            writeResultsFile(resultObj, outputPath);
                        }
                    }
                }
            }

            // Synchronization
            const syncTime = await this.synchronize(this.dltClients[0], this.dbConnector,
                this.smartContract, totalAssets);
            resultObj[SYNCHRONIZATION] = syncTime.reduce((a, b) => a + b);
        } catch (ex) {
            console.error('DLT EVALUATION AND SYNCHRONIZATION PROCESS FAILED!!!!');
            console.error(ex);
        } finally {
            if (!!outputPath) {
                writeResultsFile(resultObj, outputPath);
            }
            await sleep(5000);
    
            this.dltClients.forEach(async (dlt) => await dlt.disconnectEventHub());
            await this.dbConnector.disconnect();
        }

        return resultObj;
    }

    protected abstract synchronize(dltClient: DLT, dbConnector: DB, smartContract: SmartContract,
        totalAssets: number, log?: boolean): Promise<number[]>;

    protected async launchBlockExplorer(dltClient: DLT, dbConnector: DB): Promise<BlockExplorer<DLT, DB>> {
        const blockExplorer = new BlockExplorer<DLT, DB>(dltClient, dbConnector);
        await blockExplorer.subscribeToBlockEvents();
        return blockExplorer;
    }

    /**
     * Custom event synchronizer for the evaluation process.
     * It optionally logs the code events received. In addition, it keeps its own control
     * of the index of the last block having been synchronized, instead of checking this
     * value in the database, which, in turn, receives it from the block explorer component.
     */
    protected async launchEventSynchronizer(dltClient: DLT, dbConnector: DB,
            smartContract: SmartContract, numAssets: number, log=false): Promise<EventSynchronizer<DLT, DB>> {
        const getLastBlock = (): number => { return this.lastBlockNumberSynchronized; };
        const updateLastBlock = (blockNumber: number): number => {
            this.lastBlockNumberSynchronized = Math.max(blockNumber, this.lastBlockNumberSynchronized);
            return getLastBlock();
        };

        return new Promise<EventSynchronizer<DLT, DB>>(async (resolve) => {
            const assetLimit = numAssets.toString();
            const synchronizer = new class extends EventSynchronizer<DLT, DB> {
                public async subscribeToCodeEvents(smartContract: SmartContract): Promise<void> {
                    logGreenMessage(`--> Start code event stream to smart-contract ${smartContract.name()} from block ${getLastBlock()}`);
                    await this.dltClient.subscribeToCodeEvents(
                        smartContract, this.handleEvent, getLastBlock());
                }

                protected readonly handleEvent = async (event: ContractEvent): Promise<void> => {
                    updateLastBlock(event.blockNumber);
                    await this.storeAssetFromDeltaEvent(event.smartContractId, event.payload);
                    if (log) {
                        logMessage(`Received event from ${event.smartContractId}:\tasset ${event.payload.data.key}`);
                    }
                    if (event.payload.data.key.endsWith(assetLimit)) {
                        logGreenMessage(`Synchronization of ${numAssets} assets completed.`);
                        resolve(this);
                    }
                }
            }(dltClient, dbConnector);
            await synchronizer.subscribeToCodeEvents(smartContract);
        });
    }
}

function logTime(operation: string, sumTime: number, numAssets: number, assetType: AssetType): void {
    logGreenMessage(`Asset ${operation} for ${numAssets} ${assetType} documents took ${Math.round(sumTime)} milliseconds.`);
    logGreenMessage(`${Math.round(sumTime / numAssets * 1e4) / 1e4} milliseconds per asset`);
}

function median(numbers: number[]): number {
    const sorted = numbers.slice().sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);

    return sorted.length % 2 === 1 ? sorted[middle]
        : (sorted[middle-1] + sorted[middle]) / 2;
}

interface CsvResultEntry extends ResultEntry {
    [NUM_ASSETS]: number
}

type CsvResult = Array<CsvResultEntry>;

function writeResultsFile(results: JsonResult, path: string): void {
    // JSON
    fs.writeFileSync(path + '.json', JSON.stringify(results, null, 4), { mode: 0o664 });

    // CSV
    const csv = resultsObjectToCsv(results);
    fs.writeFileSync(path + '.csv', csv, { mode: 0o664 });
}

function restructureResultsObject(inputObject: JsonResult): CsvResult {
    const outputObject: CsvResult = [];

    for (const numAssets of Object.getOwnPropertyNames(inputObject)) {
        const assets = Number(numAssets);
        if (!isNaN(assets)) {
            outputObject.push({
                [NUM_ASSETS]: assets,
                ...inputObject[assets]
            });
        }
    }
    return outputObject;
}

function resultsObjectToCsv(obj: JsonResult): Buffer {
    const csvObjectArray = restructureResultsObject(obj);
    const csvBuf = Buffer.alloc(JSON.stringify(csvObjectArray).length);
    
    const CSV_SEP = ',';

    let offset = csvBuf.write([NUM_ASSETS, INSERTION,
        KEY_VALUE_QUERY, COMPLEX_QUERY, COMPLEX_RICH_QUERY].join(CSV_SEP) + '\n');
    
    for (const profile of csvObjectArray) {
        offset += csvBuf.write(Object.values(profile).map(Math.round).join(CSV_SEP) + '\n', offset);
    }
    return csvBuf.slice(0, csvBuf.indexOf(0x00));
}

export { ExperimentSuite, SynchronizationExperimentSuite };
