/**
 * DELTA: DLT - Database synchronization
 * Experimento Quorum 2: Evaluación de rendimiento por componentes
 */

import { DeltaSync as DeltaSyncExp2 } from '../../delta/components/DeltaSynchronization';
import { QuorumMongoApplication } from '../../delta/quorum/QuorumMongoApplication';
import { QuorumClient, QuorumSmartContract } from '../../delta/quorum/QuorumClient';
import { MongoConnector } from '../../database/mongodb/MongoConnector';
import { logRedMessage } from '../../utils/Log';
import { awaitInterrupt } from '../../utils/Util';

import { resolve as resolvePath } from 'path';

class ExperimentQuorum2_2 extends QuorumMongoApplication {

    private smartContract: QuorumSmartContract;

    public constructor(netConnProfPath: string, walletKeyPath: string, dbConnOptsPath: string,
            address: string) {
        super(netConnProfPath, walletKeyPath, dbConnOptsPath);
        this.smartContract = QuorumClient.instantiateQuorumSmartContract(address);
    }

    protected async deltaApp(dltClient: QuorumClient, dbClient: MongoConnector): Promise<void> {
        const synchronizer = new DeltaSyncExp2<QuorumClient, MongoConnector>(dltClient, dbClient);
        await synchronizer.startEventListeners(this.smartContract);
        await awaitInterrupt();
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
    const deltaApp = new ExperimentQuorum2_2(netConf, walletKey, dbConnOpts, smartContractAddress);
    await deltaApp.run();
}

main();
