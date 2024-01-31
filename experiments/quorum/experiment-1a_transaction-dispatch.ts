/**
 * DELTA: DLT - Database synchronization
 * Experimento Quorum 1.1: Envío de transacciones
 */

import { QuorumApplication } from '../../delta/quorum/QuorumApplication';
import { QuorumClient, QuorumSmartContract } from '../../delta/quorum/QuorumClient';
import { retrieveAssetSignature } from '../../delta/quorum/QuorumAssetSignature';
import { logMessage, logBlueMessage, logGreenMessage, logRedMessage } from '../../utils/Log';

import { resolve as resolvePath } from 'path';
import { performance } from 'perf_hooks';

class ExperimentQuorum1_1 extends QuorumApplication {

    private smartContract: QuorumSmartContract;

    public constructor(netConnProfPath: string, walletKeyPath: string,
            address: string) {
        super(netConnProfPath, walletKeyPath);
        this.smartContract = QuorumClient.instantiateQuorumSmartContract(address);
    }

    protected async deltaApp(dltClient: QuorumClient): Promise<void> {
        const assetSignature = await retrieveAssetSignature(dltClient, this.smartContract);
        logBlueMessage(assetSignature);

        // Asset creation
        const numSamples = 2000;
        const assetGenerator = generateAssetBatch(numSamples, 4752);
        var assetBatch: Asset[] = [];
        for (let auxAsset = assetGenerator.next(); !auxAsset.done; auxAsset = assetGenerator.next()) {
            assetBatch.push(auxAsset.value);
        }
        
        // Asset insertion
        const initTime = performance.now();
        for (let n = 0; n < numSamples - 1; n++) {
            await this.insertAssetDlt(dltClient, assetBatch[n], assetSignature, false, false);
        }
        await this.insertAssetDlt(dltClient, assetBatch[numSamples - 1], assetSignature, true, true);
        const endTime = performance.now() - initTime;

        logGreenMessage(`Asset insertion for ${numSamples} samples took ${endTime} milliseconds.`);
        logGreenMessage(`${endTime / numSamples} milliseconds per asset`);
    }

    private async insertAssetDlt(client: QuorumClient, asset: Asset, signature: string,
            sync: boolean, log: boolean = false): Promise<void> {
        try {
            const invokation = sync ? client.invokeSync : client.invokeAsync;
            await invokation.call(client, this.smartContract, 'createAsset',
                asset.id, client.encodeAsset(signature, asset));
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
    randValue1: number,
    randValue2: number
};

function* generateAssetBatch(sequenceLength: number, baseIndex: number = 0): Generator<Asset> {
    const color = ['blue', 'red', 'green', 'yellow', 'black', 'white'];
    const size = [5, 10, 15];
    const owner = ['Tomoko', 'Brad', 'Jin Soo', 'Max', 'Adriana', 'Michel'];
    const appraisedValue = [300, 400, 500, 600, 700, 800];
    const rand = [1000, 2000];

    const endIndex = baseIndex + sequenceLength;
    for (let n = baseIndex; n < endIndex; n++) {
        yield {
            id: `item-${n}`,
            color: color[n % color.length],
            size: size[n % size.length],
            owner: owner[n % owner.length],
            appraisedValue: appraisedValue[n % appraisedValue.length],
            randValue1: Math.floor(Math.random() * (rand[0] + 1)),
            randValue2: Math.floor(Math.random() * (rand[1] + 1))
        };
    }
}

if (process.argv.length < 3) {
    logRedMessage("This contract object doesn't have address set yet, please set an address first.");
    process.exit(1);
}

const netConf = resolvePath(__dirname, '..', '..', '..', 'rsc', 'connection-quorum.json');
const walletKey = resolvePath(__dirname, '..', '..', '..', 'rsc', 'key1.json');
const smartContractAddress = process.argv[2];

async function main() {
    const deltaApp = new ExperimentQuorum1_1(netConf, walletKey, smartContractAddress);
    await deltaApp.run();
}

main();
