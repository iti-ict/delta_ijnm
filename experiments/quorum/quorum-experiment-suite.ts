/**
 * DELTA: DLT - Database synchronization
 * Suite of evaluation experiments on GoQuorum
 */

import { QuorumClient, QuorumSmartContract, NetworkConfig } from '../../delta/quorum/QuorumClient';
import { MongoConnector, ConnectionOptions } from '../../database/mongodb/MongoConnector';
import { DeltaAsset, ContractListener } from '../../delta/Delta';
import { AssetKV, AssetType, SimpleAsset } from '../asset-generation';
import { logMessage, logObject, logRedMessage } from '../../utils/Log';
import { parseObjectFromJsonFile as loadConfigFile } from '../../utils/Util';
import { ExperimentSuite, SynchronizationExperimentSuite } from '../experiment-suite';

import { EventData, EventOptions } from 'web3-eth-contract';
import { performance as perf } from 'perf_hooks';
import { resolve as resolvePath } from 'path';

class QuorumExperimentSuite extends ExperimentSuite<QuorumClient> {

    protected smartContract: QEvalSmartContract;

    protected setupDltClients(numNodes: number): Array<Promise<QuorumClient>> {
        return setupDeltaDltClients(numNodes);
    }

    protected instantiateSmartContract(dltClient: QuorumSyncClient, address: string): Promise<QEvalSmartContract> {
        return instantiateQEvalSmartContract(dltClient, address);
    }

    protected decodeAssetObject(dltClient: QuorumSyncClient, rawAsset: any): object {
        return decodeDeltaAssetObject(dltClient, this.smartContract, rawAsset);
    }

    protected insertAssetDlt(dltClient: QuorumSyncClient, asset: AssetKV,
            sync: boolean, log=false): Promise<void> {
        return insertDeltaAssetDlt(dltClient, this.smartContract, asset, sync, log);
    }

    protected executeComplexQueryDlt(dltClient: QuorumSyncClient, _noDelta=false,
            param1=5, param2=1000, log=false): Promise<void> {
        return executeComplexQueryDlt(dltClient, this.smartContract, false, param1, param2, log);
    }
}

class QuorumNoDeltaExperimentSuite extends QuorumExperimentSuite {

    protected setupDltClients(numNodes: number): Array<Promise<QuorumNoDeltaClient>> {
        const nodes: Array<Promise<QuorumNoDeltaClient>> = [];
        for (let n = 1; n <= numNodes; n++) {
            const networkConfig = resolvePath(
                __dirname, '..', '..', '..', 'rsc', `connection-quorum${n}.json`);
            const walletKeyPath = resolvePath(__dirname, '..', '..', '..', 'rsc', `key${n}.json`);
            nodes.push(new Promise<QuorumNoDeltaClient>((resolve) => resolve(
                QuorumNoDeltaClient.setupClient(networkConfig, walletKeyPath, n))));
        }
        return nodes;
    }

    protected instantiateSmartContract(dltClient: QuorumSyncClient, address: string): Promise<QEvalSmartContract> {
        return instantiateQEvalSmartContract(dltClient, address, 'quorum-no-delta-abi');
    }

    protected decodeAssetObject(_dltClient: QuorumNoDeltaClient, rawAsset: any): SimpleAsset {
        return rawAsset;
    }

    protected async insertAssetDlt(dltClient: QuorumSyncClient, asset: AssetKV,
            sync: boolean, log=false): Promise<void> {
        try {
            const invokation = sync ? dltClient.invokeSync : dltClient.invokeAsync;
            const assetValue = asset.value as SimpleAsset;
            await invokation.call(dltClient, this.smartContract, 'createSimpleAsset', asset.id,
                assetValue.value1, assetValue.value2);
            if (log) {
                logMessage(`Inserted asset ${asset.id}`);
            }
        } catch (ex) {
            logRedMessage(`${asset.id}: ${ex.message}`);
        }
    }

    protected async executeComplexQueryDlt(dltClient: QuorumNoDeltaClient, _noDelta=true,
            param1=5, param2=1000, log=false): Promise<void> {
        return executeComplexQueryDlt(dltClient, this.smartContract, true, param1, param2, log);
    }
}

class QuorumSyncExperimentSuite extends SynchronizationExperimentSuite<QuorumSyncClient, MongoConnector> {

    protected smartContract: QEvalSmartContract;

    protected setupDltClients(numNodes: number): Array<Promise<QuorumSyncClient>> {
        return setupDeltaDltClients(numNodes);
    }

    protected setupDatabaseConnector(): Promise<MongoConnector> {
        const connectionOptions = <ConnectionOptions> loadConfigFile(resolvePath(
            __dirname, '..', '..', '..', 'rsc', 'db-conn-opts.json'));
        const mongo = new MongoConnector(connectionOptions);
        return new Promise<MongoConnector>((resolve) => mongo.connect().then(() => resolve(mongo)));
    }

    protected instantiateSmartContract(dltClient: QuorumSyncClient, address: string): Promise<QEvalSmartContract> {
        return instantiateQEvalSmartContract(dltClient, address);
    }

    protected async synchronize(dltClient: QuorumSyncClient, dbConnector: MongoConnector,
            smartContract: QEvalSmartContract, totalAssets: number, log?: boolean): Promise<number[]> {
        await this.launchBlockExplorer(dltClient, dbConnector);
        await this.launchEventSynchronizer(dltClient, dbConnector, smartContract, totalAssets, log);
        return dltClient.transformTime;
    }

    protected decodeAssetObject(dltClient: QuorumSyncClient, rawAsset: any): object {
        return decodeDeltaAssetObject(dltClient, this.smartContract, rawAsset);
    }

    protected insertAssetDlt(dltClient: QuorumSyncClient, asset: AssetKV,
            sync: boolean, log=false): Promise<void> {
        return insertDeltaAssetDlt(dltClient, this.smartContract, asset, sync, log);
    }

    protected executeComplexQueryDlt(dltClient: QuorumSyncClient, _noDelta=false,
            param1=5, param2=1000, log=false): Promise<void> {
        return executeComplexQueryDlt(dltClient, this.smartContract, false, param1, param2, log);
    }
}

class QEvalSmartContract extends QuorumSmartContract {
    constructor(address: string, abi: string, public assetSignature: string) {
        super(address, abi);
    }

    public name(): string {
        return this.address;
    }
}

class QuorumSyncClient extends QuorumClient {

    public transformTime: number[] = [];

    /** Create and setup a DELTA Quorum client. */
    public static setupClient(netConfPath: string, walletKeyPath: string, nodeNumber=1): QuorumSyncClient {
        const networkConfig = loadConfigFile(netConfPath) as NetworkConfig;
        //networkConfig.port += nodeNumber - 1;
        const key = '0x' + loadConfigFile(walletKeyPath).address;
        return new QuorumSyncClient(networkConfig, key);
    }

    public async subscribeToCodeEvents(smartContract: QEvalSmartContract,
            callback: ContractListener, startBlock?: number): Promise<void> {
        const address = smartContract.address;
        if (address in this.codeEventListeners) {
            await this.codeEventListeners[address].sub.unsubscribe();
        }

        const quorumCodeEventListener = async (error: Error, event: EventData): Promise<void> => {
            if (!error) {
                try {
                    const initTime = perf.now();
                    const contractEvent = this.transformContractEvent(event, smartContract.assetSignature);
                    this.transformTime.push(perf.now() - initTime);
                    await callback(contractEvent);
                } catch (ex) {
                    logRedMessage(ex.message);
                }
            }
        };

        const subscriptionOptions: EventOptions = {
            address,
            topics: [null],
            fromBlock: startBlock || 1
        };

        const contract = this.buildContract(address, smartContract.abi);
        const subscription = contract.events.Delta(subscriptionOptions, quorumCodeEventListener);
        this.codeEventListeners[address] = { sub: subscription, listener: quorumCodeEventListener };
    }
}

class QuorumNoDeltaClient extends QuorumClient {

    /** Create and setup a NO-DELTA sync Quorum client. */
    public static setupClient(netConfPath: string, walletKeyPath: string, nodeNumber=1): QuorumNoDeltaClient {
        const networkConfig = loadConfigFile(netConfPath) as NetworkConfig;
        //networkConfig.port += nodeNumber - 1;
        const key = '0x' + loadConfigFile(walletKeyPath).address;
        return new QuorumNoDeltaClient(networkConfig, key);
    }
}

type QueryResult = Array<{ key: string, asset: DeltaAsset }>;

function setupDeltaDltClients(numNodes: number): Array<Promise<QuorumSyncClient>> {
    const nodes: Array<Promise<QuorumSyncClient>> = [];
    for (let n = 1; n <= numNodes; n++) {
        const networkConfig = resolvePath(
            __dirname, '..', '..', '..', 'rsc', `connection-quorum${n}.json`);
        const walletKeyPath = resolvePath(__dirname, '..', '..', '..', 'rsc', `key${n}.json`);
        nodes.push(new Promise<QuorumSyncClient>((resolve) => resolve(
            QuorumSyncClient.setupClient(networkConfig, walletKeyPath, n))));
    }
    return nodes;
}

async function instantiateQEvalSmartContract(dltClient: QuorumClient, address: string, abiFile?: string): Promise<QEvalSmartContract> {
    const abiPath = resolvePath(__dirname, '..', '..', '..', 'rsc',
        (abiFile || 'quorum-delta-default-abi') + '.json');

    const abi = JSON.stringify(loadConfigFile(abiPath));
    const assetSignature = await retrieveAssetSignature(dltClient, new QuorumSmartContract(address, abi));
    return new QEvalSmartContract(address, abi, assetSignature);
}

async function retrieveAssetSignature(dltClient: QuorumClient, smartContract: QuorumSmartContract): Promise<string> {
    return dltClient.query(smartContract, 'getAssetSignature');
}

function decodeDeltaAssetObject(dltClient: QuorumClient, smartContract: QEvalSmartContract,
        rawAsset: any): object {
    return dltClient.decodeAssetArray(rawAsset, smartContract.assetSignature);
}

async function insertDeltaAssetDlt(dltClient: QuorumClient, smartContract: QEvalSmartContract,
        asset: AssetKV, sync: boolean, log: boolean): Promise<void> {
    try {
        const invokation = sync ? dltClient.invokeSync : dltClient.invokeAsync;
        await invokation.call(dltClient, smartContract, 'upsertAsset', asset.id,
            dltClient.encodeAsset(smartContract.assetSignature, asset.value));
        if (log) {
            logMessage(`Inserted asset ${asset.id}`);
        }
    } catch (ex) {
        logRedMessage(`${asset.id}: ${ex.message}`);
    }
}

async function executeComplexQueryDlt(dltClient: QuorumClient, smartContract: QEvalSmartContract,
        noDelta: boolean, param1: number, param2: number, log: boolean): Promise<void> {
    try {
        const queryResult: any[] = await dltClient.query(smartContract, 'complexQuery', param1, param2);
        if (log) {
            const queryResultAssets = <QueryResult>
                queryResult.filter((record) => record['key'].length > 0)
                .map((record) => { return { key: record['key'], asset: (noDelta ?
                    { value1: record['asset']['value1'], value2: record['asset']['value2'] } :
                    dltClient.decodeAsset(smartContract.assetSignature, record['asset'].value)) }; });
            logMessage(`Retrieved ${queryResultAssets.length} assets from complex DELTA query`);
            logObject(queryResultAssets);
        }
    } catch (ex) {
        logRedMessage(`Complex DELTA query: ${ex.message}`);
    }
}

// Evaluation constant parameters
const numOrgs = 7;
const assetType = AssetType.Simple;
const synchronize = false;
const noDelta = false;
const totalAssets = 39900;
const checkpoint = 2100;
const batchSize = 300;
const numQueries = 200;

// Dilucidation of the smart-contract address on the test network. If no supplementary CLI
// arguments are provided, it is inferred from the asset type and the "NO DELTA" option.
let smartContractAddress: string;
if (process.argv.length > 2) {
    smartContractAddress = process.argv[2];
} else {
    switch (assetType) {
        case (AssetType.Simple as AssetType):
            smartContractAddress = noDelta ?
                '0x29DDA0837d6d6d51d58FE5CE0b943Ef896E340C1' : '0x03bDD25ef210c85871aA3bbc0Ed68167FD3E69da';
            break;
        case (AssetType.Intermediate as AssetType):
            smartContractAddress = '0x6c192CBD98c54BB723e87EcfBBd13f379062d464';
            break;
        case (AssetType.Complex as AssetType):
            smartContractAddress = '0x357bb5946e53422b1EA99cBdBe492c4256A36962';
            break;
    }
}

// Initialization of the proper evaluation object
let evaluation: ExperimentSuite<QuorumClient>;
if (noDelta) {
    evaluation = new QuorumNoDeltaExperimentSuite();
} else {
    if (synchronize) {
        evaluation = new QuorumSyncExperimentSuite();
    } else {
        evaluation = new QuorumExperimentSuite();
    }
}

async function main() {
    await evaluation.dltEvaluation(
        numOrgs, smartContractAddress, assetType, totalAssets, batchSize, checkpoint, numQueries,
        `delta-quorum${noDelta ? '-NoDelta-' : '-'}${assetType}-${totalAssets}`);
}

main();
