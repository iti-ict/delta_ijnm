/**
 * DELTA: DLT - Database synchronization
 * Suite of evaluation experiments on Hyperledger Fabric
 */

import { ContractEvent as FabricContractEvent, Wallet } from 'fabric-network';
import { CAUtil, ClientUtil } from 'fabric-chaincode-client';
import { FabricClient, FabricSmartContract, setupClient,
    NetworkConnectionProfile, ConfigOptions, NetworkConfig } from '../../delta/fabric/FabricClient';
import { MongoConnector, ConnectionOptions } from '../../database/mongodb/MongoConnector';
import { DeltaAsset, ContractListener } from '../../delta/Delta';
import { AssetType, SimpleAsset } from '../asset-generation';
import { logMessage, logObject, logRedMessage } from '../../utils/Log';
import { parseObjectFromJsonFile as loadConfigFile } from '../../utils/Util';
import { ExperimentSuite, SynchronizationExperimentSuite } from '../experiment-suite';

import { performance as perf } from 'perf_hooks';
import { resolve as resolvePath } from 'path';
import * as fs from 'fs';

class HlfExperimentSuite extends ExperimentSuite<FabricClient> {

    protected smartContract: FabricSmartContract;

    protected setupDltClients(numNodes: number): Array<Promise<FabricClient>> {
        return setupDltClients(numNodes);
    }

    protected instantiateSmartContract(dltClient: FabricClient, name: string): Promise<FabricSmartContract> {
        return instantiateSmartContract(dltClient, name);
    }

    protected decodeAssetObject(dltClient: FabricClient, rawAsset: string): object {
        return dltClient.decodeAssetObject(rawAsset);
    }

    protected executeComplexQueryDlt(client: FabricClient, richQuery: boolean,
            param1=5, param2=1000, log=false): Promise<void> {
        return executeComplexQueryDlt(client, this.smartContract, richQuery, param1, param2, log);
    }
}

class HlfNoDeltaExperimentSuite extends HlfExperimentSuite {

    protected setupDltClients(numNodes: number): Array<Promise<FabricClient>> {
            const nodes: Array<Promise<FabricClient>> = [];
        for (let n = 1; n <= numNodes; n++) {
            const connectionProfile = resolvePath(
                __dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', `org${n}.delta.net`, `connection-org${n}.json`);
            const configOptions = resolvePath(
                __dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', `org${n}.delta.net`, 'config-options.json');
            const walletPath = resolvePath(__dirname, '..', '..', '..', `wallet-org${n}`);
            nodes.push(setupClient(connectionProfile, configOptions, walletPath));
        }
        return nodes;
    }

    protected async instantiateSmartContract(dltClient: FabricClient, name: string): Promise<FabricSmartContract> {
        return dltClient.instantiateSmartContract(name);
    }

    protected decodeAssetObject(_dltClient: FabricClient, rawAsset: string): SimpleAsset {
        return JSON.parse(rawAsset) as SimpleAsset;
    }

    protected async insertAssetDlt(dltClient: FabricClient, asset: SimpleAssetKV,
            sync: boolean, log=false): Promise<void> {
        try {
            const invokation = sync ? dltClient.invokeSync : dltClient.invokeAsync;
            await invokation.call(dltClient, this.smartContract, 'insertAsset',
                asset.id, JSON.stringify(asset.value));
            if (log) {
                logMessage(`Inserted asset ${asset.id}`);
            }
        } catch (ex) {
            logRedMessage(`${asset.id}: ${ex.message}`);
        }
    }

    protected async executeComplexQueryDlt(dltClient: FabricClient, richQuery: boolean,
            param1=5, param2=1000, log=false): Promise<void> {
        try {
            const queryResultString = await dltClient.query(this.smartContract,
                richQuery ? 'complexRichQuery' : 'complexQuery', assetType, param1, param2);
            if (log) {
                const queryResultAssets = (JSON.parse(queryResultString) as QueryResult)
                    .map((record) => { return { key: record.key, asset: record.asset }; });
                logMessage(`Retrieved ${queryResultAssets.length} assets from complex ${richQuery ? 'rich ' : ''}query`);
                logObject(queryResultAssets);
            }
        } catch (ex) {
            logRedMessage(`Complex ${richQuery ? 'rich ' : ''} query: ${ex.message}`);
        }
    }
}

class HlfSyncExperimentSuite extends SynchronizationExperimentSuite<FabricSyncClient, MongoConnector> {

    protected smartContract: FabricSmartContract;

    protected setupDltClients(numNodes: number): Array<Promise<FabricSyncClient>> {
        return setupDltClients(numNodes);
    }

    protected setupDatabaseConnector(): Promise<MongoConnector> {
        const connectionOptions = <ConnectionOptions> loadConfigFile(resolvePath(
            __dirname, '..', '..', '..', 'rsc', 'db-conn-opts.json'));
        connectionOptions.database = this.dltClients[0].getNetworkChannel();
        const mongo = new MongoConnector(connectionOptions);
        return new Promise<MongoConnector>((resolve) => mongo.connect().then(() => resolve(mongo)));
    }

    protected instantiateSmartContract(dltClient: FabricSyncClient, name: string): Promise<FabricSmartContract> {
        return instantiateSmartContract(dltClient, name);
    }

    protected async synchronize(dltClient: FabricSyncClient, dbConnector: MongoConnector,
            smartContract: FabricSmartContract, totalAssets: number, log?: boolean): Promise<number[]> {
        await this.launchBlockExplorer(dltClient, dbConnector);
        await this.launchEventSynchronizer(dltClient, dbConnector, smartContract, totalAssets, log);
        return dltClient.transformTime;
    }

    protected decodeAssetObject(dltClient: FabricSyncClient, rawAsset: string): object {
        return decodeAssetObject(dltClient, rawAsset);
    }

    protected executeComplexQueryDlt(dltClient: FabricSyncClient, richQuery: boolean,
            param1=5, param2=1000, log=false): Promise<void> {
        return executeComplexQueryDlt(dltClient, this.smartContract, richQuery, param1, param2, log);
    }
}

class FabricSyncClient extends FabricClient {

    public transformTime: number[] = [];
    
    /** Setup the user's wallet and build a client object,
     *  which will connect to the network when actually needed. */
    public static async setupClient(connProf: string, confOpts: string,
            walletPath: string): Promise<FabricSyncClient> {
        const connectionProfile = loadConfigFile(connProf) as NetworkConnectionProfile;
        const configOptions = loadConfigFile(confOpts) as ConfigOptions;
        const network: NetworkConfig = { connectionProfile, configOptions };

        // Setup wallet, which may imply enrolling user and possibly also admin if needed
        await FabricSyncClient.initWallet(network, walletPath);

        return new FabricSyncClient(network, walletPath);
    }

    /** Create and setup a wallet. */
    private static async initWallet(network: NetworkConfig, walletPath: string): Promise<Wallet> {
        let wallet: Wallet;

        let walletExists = fs.existsSync(walletPath);
        if (walletExists && !fs.statSync(walletPath).isDirectory()) {
            fs.unlinkSync(walletPath);
            walletExists = false;
        }

        wallet = await ClientUtil.createWallet(walletPath);

        if (!walletExists) {
            const caClient = CAUtil.buildCAClient(
                network.connectionProfile, network.configOptions.ca.host);
            await CAUtil.enrollAdmin(caClient, wallet,
                network.configOptions, network.configOptions.adminPassword);
            await CAUtil.registerAndEnrollUser(caClient, wallet, network.configOptions);
        }

        return wallet;
    }

    public async subscribeToCodeEvents(smartContract: FabricSmartContract,
            callback: ContractListener, startBlock?: number): Promise<void> {
        const fabricCodeEventListener = async (event: FabricContractEvent): Promise<void> => {
            if (event.eventName == "Delta") {
                try {
                    const initTime = perf.now();
                    const contractEvent = this.transformContractEvent(event);
                    this.transformTime.push(perf.now() - initTime);
                    await callback(contractEvent);
                } catch (ex) {
                    logRedMessage('Skipping non DELTA event: ' + ex.message);
                }
            }
        };
        await this.client.subscribeToChaincodeEvents(
            this.getNetworkChannel(), smartContract.name(), fabricCodeEventListener, startBlock);
    }
}

type SimpleAssetKV = { id: string, value: SimpleAsset };
type QueryResult = Array<{ key: string, asset: DeltaAsset }>;

function setupDltClients(numNodes: number): Array<Promise<FabricSyncClient>> {
    const nodes: Array<Promise<FabricSyncClient>> = [];
    for (let n = 1; n <= numNodes; n++) {
        const connectionProfile = resolvePath(
            __dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', `org${n}.delta.net`, `connection-org${n}.json`);
        const configOptions = resolvePath(
            __dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', `org${n}.delta.net`, 'config-options.json');
        const walletPath = resolvePath(__dirname, '..', '..', '..', `wallet-org${n}`);
        nodes.push(FabricSyncClient.setupClient(connectionProfile, configOptions, walletPath));
    }
    return nodes;
}

async function instantiateSmartContract(dltClient: FabricClient, name: string): Promise<FabricSmartContract> {
    return dltClient.instantiateSmartContract(name);
}

function decodeAssetObject(dltClient: FabricClient, rawAsset: string): object {
    return dltClient.decodeAssetObject(rawAsset);
}

async function executeComplexQueryDlt(client: FabricClient, smartContract: FabricSmartContract,
        richQuery: boolean, param1: number, param2: number, log: boolean): Promise<void> {
    try {
        const queryResultString = await client.query(smartContract,
            richQuery ? 'complexRichQuery' : 'complexQuery', param1, param2);
        if (log) {
            const queryResultAssets = (JSON.parse(queryResultString) as QueryResult)
                .map((record) => { return { key: record.key, asset: record.asset.value }; });
            logMessage(`Retrieved ${queryResultAssets.length} assets from complex ${richQuery ? 'rich ' : ''}query`);
            logObject(queryResultAssets);
        }
    } catch (ex) {
        logRedMessage(`Complex ${richQuery ? 'rich' : ''}query: ${ex.message}`);
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

// Initialization of the proper evaluation object
let evaluation: ExperimentSuite<FabricClient>;
let smartContractName: string;
if (process.argv.length > 2) {
    smartContractName = process.argv[2];
} else {
    smartContractName = noDelta ? 'nodelta' : 'delta';
}

if (noDelta) {
    evaluation = new HlfNoDeltaExperimentSuite();
} else {
    evaluation = synchronize ? new HlfSyncExperimentSuite() : new HlfExperimentSuite();
}

async function main() {
    await evaluation.dltEvaluation(
        numOrgs, smartContractName, assetType, totalAssets, batchSize, checkpoint, numQueries,
        `delta-fabric${noDelta ? '-NoDelta-' : '-'}${assetType}-${totalAssets}`);
}

main();
