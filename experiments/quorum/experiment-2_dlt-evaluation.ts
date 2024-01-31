/**
 * DELTA: DLT - Database synchronization
 * Experimento Quorum 2: Evaluación de rendimiento por componentes
 */

import { QuorumApplication } from '../../delta/quorum/QuorumApplication';
import { QuorumClient, QuorumSmartContract } from '../../delta/quorum/QuorumClient';
import { retrieveAssetSignature } from '../../delta/quorum/QuorumAssetSignature';
import { logMessage, logBlueMessage, logGreenMessage, logRedMessage } from '../../utils/Log';

import { resolve as resolvePath } from 'path';
import { performance } from 'perf_hooks';

class ExperimentQuorum2_1 extends QuorumApplication {

    private smartContract: QuorumSmartContract;

    public constructor(netConnProfPath: string, walletKeyPath: string,
            address: string) {
        super(netConnProfPath, walletKeyPath);
        this.smartContract = QuorumClient.instantiateQuorumSmartContract(address);
    }

    protected async deltaApp(dltClient: QuorumClient): Promise<void> {
        const assetKeyIndex = 2020;
        const assetSignature = await retrieveAssetSignature(dltClient, this.smartContract);
        logBlueMessage(assetSignature);

        // Warming insertion (with different key) for relieving first execution overload
        await this.insertAssetDlt(dltClient, generateAsset(assetKeyIndex + 1), assetSignature, true);

        // Asset creation and insertion
        const asset = generateAsset(assetKeyIndex);
        const initTime = performance.now();
        await this.insertAssetDlt(dltClient, asset, assetSignature, true);
        const endTime = performance.now() - initTime;

        logMessage(`Inserted asset ${asset.id}`);
        logGreenMessage(`Asset insertion took ${endTime} ms`);
    }

    private async insertAssetDlt(client: QuorumClient, asset: Asset, signature: string,
            sync: boolean): Promise<void> {
        try {
            const invokation = sync ? client.invokeSync : client.invokeAsync;
            await invokation.call(client, this.smartContract, 'createAsset',
                asset.id, client.encodeAsset(signature, asset));
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

if (process.argv.length < 3) {
    logRedMessage("This contract object doesn't have address set yet, please set an address first.");
    process.exit(1);
}

const netConf = resolvePath(__dirname, '..', '..', '..', 'rsc', 'connection-quorum.json');
const walletKey = resolvePath(__dirname, '..', '..', '..', 'rsc', 'key1.json');
const smartContractAddress = process.argv[2];

async function main() {
    const deltaApp = new ExperimentQuorum2_1(netConf, walletKey, smartContractAddress);
    await deltaApp.run();
}

main();
