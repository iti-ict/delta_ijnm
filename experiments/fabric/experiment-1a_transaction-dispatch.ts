/**
 * DELTA: DLT - Database Synchronization
 * Experimento Hyperledger Fabric 1.1: Envío de transacciones
 */

import { FabricApplication } from '../../delta/fabric/FabricApplication';
import { FabricClient, FabricSmartContract } from '../../delta/fabric/FabricClient';
import { logMessage, logGreenMessage, logRedMessage } from '../../utils/Log';

import { resolve as resolvePath } from 'path';
import { performance as perf } from 'perf_hooks';

class ExperimentFabric1_1 extends FabricApplication {

    private smartContract: FabricSmartContract;

    public constructor(netConnProfPath: string, confOptsPath: string, walletPath: string,
            chaincode: string) {
        super(netConnProfPath, confOptsPath, walletPath);
        this.smartContract = new FabricSmartContract(chaincode);
    }

    protected async deltaApp(dltClient: FabricClient): Promise<void> {
        
        // Asset creation
        const numSamples = 200;
        const assetBatch = generateAssetBatch(numSamples, 20120);

        // Asset insertion
        const initTime = perf.now();
        for (let n = 0; n < numSamples - 1; n++) {
            await this.insertAssetDlt(dltClient, assetBatch[n], false);
        }
        await this.insertAssetDlt(dltClient, assetBatch[numSamples - 1], true, true);
        const endTime = perf.now() - initTime;

        logGreenMessage(`Asset insertion for ${numSamples} samples took ${endTime} milliseconds.`);
        logGreenMessage(`${endTime / numSamples} milliseconds per asset`);
    }

    private async insertAssetDlt(client: FabricClient, asset: Asset,
            sync: boolean, log: boolean = false): Promise<void> {
        try {
            const invokation = sync ? client.invokeSync : client.invokeAsync;
            await invokation.call(client, this.smartContract, 'upsertAsset', asset.id, asset);
            if (log) {
                logMessage(`Inserted asset ${asset.id}`);
            }
        } catch (ex) {
            logRedMessage(`${asset.id}: ${ex.message}`);
        }
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

function generateAssetBatch(sequenceLength: number, baseIndex: number = 0): Asset[] {
    const color = ['blue', 'red', 'green', 'yellow', 'black', 'white'];
    const size = [5, 10, 15];
    const owner = ['Tomoko', 'Brad', 'Jin Soo', 'Max', 'Adriana', 'Michel'];
    const appraisedValue = [300, 400, 500, 600, 700, 800];
    const rand = [1000, 2000];

    var assetBatch: Asset[] = [];
    const endIndex = baseIndex + sequenceLength;
    for (let n = baseIndex; n < endIndex; n++) {
        assetBatch.push({
            id: `item-${n}`,
            color: color[n % color.length],
            size: size[n % size.length],
            owner: owner[n % owner.length],
            appraisedValue: appraisedValue[n % appraisedValue.length],
            randValues: [ Math.floor(Math.random() * (rand[0] + 1)), Math.floor(Math.random() * (rand[1] + 1)) ]
        });
    }
    return assetBatch;
}

const netConnProf = resolvePath(__dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', 'org1.delta.net', 'connection-org1.json');
const confOpts = resolvePath(__dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', 'org1.delta.net', 'config-options.json');
const wallet = resolvePath(__dirname, '..', '..', '..', 'wallet');
const smartContract = process.argv.length > 2 ? process.argv[2] : 'delta';

async function main() {
    const deltaApp = new ExperimentFabric1_1(netConnProf, confOpts, wallet, smartContract);
    await deltaApp.run();
}

main();
