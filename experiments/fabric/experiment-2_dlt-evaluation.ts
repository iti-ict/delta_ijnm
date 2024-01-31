/**
 * DELTA: DLT - Database synchronization
 * Experimento Hyperledger Fabric 2: Evaluación de rendimiento por componentes
 */

import { FabricApplication } from '../../delta/fabric/FabricApplication';
import { FabricClient, FabricSmartContract } from '../../delta/fabric/FabricClient';
import { logMessage, logGreenMessage, logRedMessage } from '../../utils/Log';

import { resolve as resolvePath } from 'path';
import { performance as perf } from 'perf_hooks';

class ExperimentFabric2_1 extends FabricApplication {

    private smartContract: FabricSmartContract;

    public constructor(netConnProfPath: string, confOptsPath: string, walletPath: string,
            chaincode: string) {
        super(netConnProfPath, confOptsPath, walletPath);
        this.smartContract = new FabricSmartContract(chaincode);
    }

    protected async deltaApp(dltClient: FabricClient): Promise<void> {
        const assetKeyIndex = 2020;

        // Warming insertion (with different key) for relieving first execution overload
        await this.insertAssetDlt(dltClient, generateAsset(assetKeyIndex + 1), true);

        // Asset creation and insertion
        const asset = generateAsset(assetKeyIndex);
        const initTime = perf.now();
        await this.insertAssetDlt(dltClient, asset, true);
        const endTime = perf.now() - initTime;

        logMessage(`Inserted asset ${asset.id}`);
        logGreenMessage(`Asset insertion took ${endTime} ms`);
    }

    private async insertAssetDlt(client: FabricClient, asset: Asset,
            sync: boolean): Promise<void> {
        try {
            const invokation = sync ? client.invokeSync : client.invokeAsync;
            await invokation.call(client, this.smartContract, 'upsertAsset',
                asset.id, asset);
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

function generateAsset(sequenceIndex: number): Asset {
    const color = ['blue', 'red', 'green', 'yellow', 'black', 'white'];
    const size = [5, 10, 15];
    const owner = ['Tomoko', 'Brad', 'Jin Soo', 'Max', 'Adriana', 'Michel'];
    const appraisedValue = [300, 400, 500, 600, 700, 800];
    const rand = [1000, 2000];

    return {
        id: `item-${sequenceIndex}`,
        color: color[sequenceIndex % color.length],
        size: size[sequenceIndex % size.length],
        owner: owner[sequenceIndex % owner.length],
        appraisedValue: appraisedValue[sequenceIndex % appraisedValue.length],
        randValues: [ Math.floor(Math.random() * (rand[0] + 1)),
                        Math.floor(Math.random() * (rand[1] + 1)) ]
    };
}

const netConnProf = resolvePath(__dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', 'org1.delta.net', 'connection-org1.json');
const confOpts = resolvePath(__dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', 'org1.delta.net', 'config-options.json');
const wallet = resolvePath(__dirname, '..', '..', '..', 'wallet');
const smartContract = process.argv.length > 2 ? process.argv[2] : 'delta';

async function main() {
    const deltaApp = new ExperimentFabric2_1(netConnProf, confOpts, wallet, smartContract);
    await deltaApp.run();
}

main();
