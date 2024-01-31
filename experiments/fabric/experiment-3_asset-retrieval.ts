/**
 * DELTA: DLT - Database synchronization
 * Experimento Hyperledger Fabric 3: Consultas y recuperación de assets
 */

import { FabricMongoApplication } from '../../delta/fabric/FabricMongoApplication';
import { FabricClient, FabricSmartContract } from '../../delta/fabric/FabricClient';
import { MongoConnector } from '../../database/mongodb/MongoConnector';
import { DeltaAsset } from '../../delta/Delta';
import { logBlueMessage, logGreenMessage, logRedMessage } from '../../utils/Log';

import { resolve as resolvePath } from 'path';
import { performance as perf } from 'perf_hooks';

class ExperimentFabric3 extends FabricMongoApplication {

    private smartContract: FabricSmartContract;

    public constructor(netConnProfPath: string, confOptsPath: string, walletPath: string,
            dbConnOptsPath: string, chaincode: string) {
        super(netConnProfPath, confOptsPath, walletPath, dbConnOptsPath);
        this.smartContract = new FabricSmartContract(chaincode);
    }

    protected async deltaApp(dltClient: FabricClient, dbClient: MongoConnector): Promise<void> {
        const assetKeyIndex = 20201;
        const assetSize = 5;
        const assetRandThreshold = 1000;

        await this.dltKeyQuery(dltClient, assetKeyIndex);
        await this.dbKeyQuery(dbClient, assetKeyIndex);
        await this.dltComplexQuery(dltClient, false, assetSize, assetRandThreshold);    // complex query
        await this.dltComplexQuery(dltClient, true, assetSize, assetRandThreshold);     // complex rich query
        await this.dbComplexQuery(dbClient, assetSize, assetRandThreshold);
    }

    private async dltKeyQuery(dlt: FabricClient, keyIndex: number): Promise<Asset|null> {
        // Warming query for relieving first execution overload
        // (different key to prevent caching)
        await dlt.query(this.smartContract, 'readAsset', buildAssetKey(keyIndex - 1));

        const key = buildAssetKey(keyIndex);
        const initTime = perf.now();
        const queryResult = await dlt.query(this.smartContract, 'readAsset', key);
        const midTime = perf.now();
        const queryResultAsset = !!queryResult ? dlt.decodeAssetObject(queryResult) as Asset : null;
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
        await db.retrieveAsset(this.smartContract.chaincode, buildAssetKey(keyIndex - 1));

        const key = buildAssetKey(keyIndex);
        const initTime = perf.now();
        const queryResult = await db.retrieveAsset(this.smartContract.chaincode, key);
        const midTime = perf.now();
        const queryResultAsset = !!queryResult ? queryResult.value : null;
        const endTime = perf.now();

        logBlueMessage('\n' + JSON.stringify(queryResultAsset));
        logGreenMessage(`DB asset retrieval: ${midTime - initTime} ms`
            + `\nResult processing: ${endTime - midTime} ms`
            + `\nFull DB key query: ${endTime - initTime} ms`);
        
        return queryResultAsset;
    }

    private async dltComplexQuery(dlt: FabricClient, richQuery: boolean, size: number, randThreshold: number): Promise<Asset[]> {
        const initTime = perf.now();
        const queryResultString = await dlt.query(this.smartContract, richQuery ? 'complexRichQuery' : 'complexQuery',
            size, randThreshold);
        const midTime = perf.now();
        const queryResult: Array<{ key: string, asset: DeltaAsset }> = JSON.parse(queryResultString);
        const queryResultAssets: Asset[] = queryResult.map(register => register.asset.value);
        const endTime = perf.now();

        logBlueMessage('\n' + JSON.stringify(queryResultAssets));
        logGreenMessage(`DLT asset complex${richQuery ? ' rich' : '' } query: ${midTime - initTime} ms`
            + `\nResult processing: ${endTime - midTime} ms`
            + `\nFull DLT complex${richQuery ? ' rich' : ''} query: ${endTime - initTime} ms`);
        
        return queryResultAssets;
    }

    private async dbComplexQuery(db: MongoConnector, size: number, randThreshold: number): Promise<Asset[]> {
        const initTime = perf.now();
        const assetDocuments = await db.executeQuery(this.smartContract.chaincode, {
            'size': size,
            'randValues.1': { '$lt': randThreshold }
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

const netConnProf = resolvePath(__dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', 'org1.delta.net', 'connection-org1.json');
const confOpts = resolvePath(__dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', 'org1.delta.net', 'config-options.json');
const dbConnOpts = resolvePath(__dirname, '..', '..', '..', 'rsc', 'db-conn-opts.json');
const wallet = resolvePath(__dirname, '..', '..', '..', 'wallet');
const smartContract = process.argv.length > 2 ? process.argv[2] : 'delta';

async function main() {
    const deltaApp = new ExperimentFabric3(netConnProf, confOpts, wallet, dbConnOpts, smartContract);
    await deltaApp.run();
}

main();
