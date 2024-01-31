/**
 * DELTA: DLT - Database synchronization
 * Experimento Quorum 3: Consultas y recuperación de assets
 */

import { QuorumMongoApplication } from '../../delta/quorum/QuorumMongoApplication';
import { QuorumClient, QuorumSmartContract } from '../../delta/quorum/QuorumClient';
import { MongoConnector } from '../../database/mongodb/MongoConnector';
import { retrieveAssetSignature } from '../../delta/quorum/QuorumAssetSignature';
import { DeltaAsset } from '../../delta/Delta';
import { logBlueMessage, logGreenMessage, logRedMessage } from '../../utils/Log';

import { resolve as resolvePath } from 'path';
import { readFileSync as readFile } from 'fs';
import { performance as perf } from 'perf_hooks';

class ExperimentQuorum3 extends QuorumMongoApplication {

    private smartContract: QuorumSmartContract;

    public constructor(netConnProfPath: string, walletKeyPath: string, dbConnOptsPath: string,
            address: string, abi: string) {
        super(netConnProfPath, walletKeyPath, dbConnOptsPath);
        this.smartContract = new QuorumSmartContract(address, abi);
    }

    protected async deltaApp(dltClient: QuorumClient, dbClient: MongoConnector): Promise<void> {
        const assetKeyIndex = 20201;
        const assetSize = 5;
        const assetRandThreshold = 1000;
        const assetSignature = await retrieveAssetSignature(dltClient, this.smartContract);
        logBlueMessage(assetSignature);

        await this.dltKeyQuery(dltClient, assetKeyIndex, assetSignature);
        await this.dbKeyQuery(dbClient, assetKeyIndex);
        //await this.dltComplexQuery(dltClient, assetSize, assetRandThreshold, assetSignature);
        await this.dbComplexQuery(dbClient, assetSize, assetRandThreshold);
    }

    private async dltKeyQuery(dlt: QuorumClient, keyIndex: number, assetSignature: string): Promise<Asset|null> {
        // Warming query for relieving first execution overload
        // (different key to prevent caching)
        dlt.decodeAssetArray(
            await dlt.query(this.smartContract, 'readAsset', buildAssetKey(keyIndex - 1)),
            assetSignature);

        const key = buildAssetKey(keyIndex);
        const initTime = perf.now();
        const queryResult = await dlt.query(this.smartContract, 'readAsset', key);
        const midTime = perf.now();
        const queryResultAsset = !!queryResult ? dlt.decodeAssetArray(queryResult, assetSignature) as Asset : null;
        const endTime = perf.now();

        logBlueMessage('\n' + JSON.stringify(queryResultAsset));
        logGreenMessage(`DLT asset retrieval: ${midTime - initTime} ms`
            + `\nResult processing: ${endTime - midTime} ms`
            + `\nFull DLT key query: ${endTime - initTime} ms`);

        return queryResultAsset;
    }

    private async dbKeyQuery(db: MongoConnector, keyIndex: number): Promise<Asset|null> {
        // Warming query for relieving first execution overload
        // (different key to prevent caching)
        await db.retrieveAsset(this.smartContract.address, buildAssetKey(keyIndex - 1));

        const key = buildAssetKey(keyIndex);
        const initTime = perf.now();
        const queryResult = await db.retrieveAsset(this.smartContract.address, key);
        const midTime = perf.now();
        const queryResultAsset = !!queryResult ? queryResult.value : null;
        const endTime = perf.now();

        logBlueMessage('\n' + JSON.stringify(queryResultAsset));
        logGreenMessage(`DB asset retrieval: ${midTime - initTime} ms`
            + `\nResult processing: ${endTime - midTime} ms`
            + `\nFull DB key query: ${endTime - initTime} ms`);
        
        return queryResultAsset;
    }

    private async dltComplexQuery(dlt: QuorumClient, size: number, randThreshold: number, assetSignature: string): Promise<Asset[]> {
        const initTime = perf.now();
        const queryResult = await dlt.query(this.smartContract, 'complexQuery', size, randThreshold);
        const midTime = perf.now();
        const queryResultAssets: Asset[] = queryResult.map((register: any[]) => dlt.decodeAsset(assetSignature, register[1].value));
        const endTime = perf.now();

        logBlueMessage('\n' + JSON.stringify(queryResultAssets));
        logGreenMessage(`DLT asset complex query: ${midTime - initTime} ms`
            + `\nResult processing: ${endTime - midTime} ms`
            + `\nFull DB complex query: ${endTime - initTime} ms`);
        
        return queryResultAssets;
    }

    private async dbComplexQuery(db: MongoConnector, size: number, randThreshold: number): Promise<Asset[]> {
        const initTime = perf.now();
        const assetDocuments = await db.executeQuery(this.smartContract.address, {
            'size': size,
            'randValues.1': { '$lt': randThreshold.toString() }
        });
        const midTime = perf.now();
        const assets = (assetDocuments as any[] as DeltaAsset[]).map(doc => doc.value);
        const endTime = perf.now();

        logBlueMessage('\n' + JSON.stringify(assets));
        logGreenMessage(`DB asset complex query: ${midTime - initTime} ms`
            + `\nResult processing: ${endTime - midTime} ms`
            + `\nFull DB complex query: ${endTime - initTime} ms`);

        return assets;
    }
}

interface Asset {
    id: string,
    color: string,
    size: number,
    owner: string,
    appraisedValue: number,
    randValues: number[]
};

function buildAssetKey(sequenceIndex: number) {
    return `item-${sequenceIndex}`;
}

if (process.argv.length < 3) {
    logRedMessage("This contract object doesn't have address set yet, please set an address first.");
    process.exit(1);
}

const netConf = resolvePath(__dirname, '..', '..', '..', 'rsc', 'connection-quorum.json');
const dbConnOpts = resolvePath(__dirname, '..', '..', '..', 'rsc', 'db-conn-opts.json');
const walletKey = resolvePath(__dirname, '..', '..', '..', 'rsc', 'key1.json');
const abi = readFile(resolvePath(__dirname, '..', '..', '..', 'rsc', 'quorum-complex-asset-abi.json'), 'utf8');
const smartContractAddress = process.argv[2];

async function main() {
    const deltaApp = new ExperimentQuorum3(netConf, walletKey, dbConnOpts, smartContractAddress, abi);
    await deltaApp.run();
}

main();
