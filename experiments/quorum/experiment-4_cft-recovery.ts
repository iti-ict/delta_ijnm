/**
 * DELTA: DLT - Database synchronization
 * Experimento Quorum 4: Recuperación ante pérdida de datos
 */

import { DeltaSync as DeltaSyncExp1B } from '../../delta/components/DeltaSynchronization';
import { QuorumMongoApplication } from '../../delta/quorum/QuorumMongoApplication';
import { QuorumClient, QuorumSmartContract } from '../../delta/quorum/QuorumClient';
import { MongoConnector } from '../../database/mongodb/MongoConnector';
import { logGreenMessage, logRedMessage } from '../../utils/Log';

import { resolve as resolvePath } from 'path';
import { performance } from 'perf_hooks';

class ExperimentQuorum4_2 extends QuorumMongoApplication {

    private smartContract: QuorumSmartContract;

    public constructor(netConnProfPath: string, walletKeyPath: string, dbConnOptsPath: string,
            address: string) {
        super(netConnProfPath, walletKeyPath, dbConnOptsPath);
        this.smartContract = QuorumClient.instantiateQuorumSmartContract(address);
    }

    protected async deltaApp(dltClient: QuorumClient, dbClient: MongoConnector): Promise<void> {
        const numSamples = 200;

        const synchronizer = new DeltaSyncExp1B<QuorumClient, MongoConnector>(dltClient, dbClient);
        const initTime = performance.now();
        await synchronizer.startEventListeners(this.smartContract/*, numSamples*/);
        const endTime = performance.now() - initTime;

        logGreenMessage(`Asset synchronization took ${endTime} milliseconds for ${numSamples} samples.`);
        logGreenMessage(`${endTime / numSamples} milliseconds per asset`);
    }
}

if (process.argv.length < 3) {
    logRedMessage("This contract object doesn't have address set yet, please set an address first.");
    process.exit(1);
}

const netConf = resolvePath(__dirname, '..', '..', '..', 'rsc', 'connection-quorum.json');
const dbConnOpts = resolvePath(__dirname, '..', '..', '..', 'rsc', 'db-conn-opts.json');
const walletKey = resolvePath(__dirname, '..', '..', '..', 'rsc', 'key1.json');
const smartContractAddress = process.argv[2];

async function main() {
    const deltaApp = new ExperimentQuorum4_2(netConf, walletKey, dbConnOpts, smartContractAddress);
    await deltaApp.run();
}

main();
