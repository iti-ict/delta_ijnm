/**
 * DELTA: DLT - Database synchronization
 * Experimento Hyperledger Fabric 4: Recuperación ante pérdida de datos
 */

import { DeltaSync as DeltaSyncExp1B } from '../../delta/components/DeltaSynchronization';
import { FabricMongoApplication } from '../../delta/fabric/FabricMongoApplication';
import { FabricClient, FabricSmartContract } from '../../delta/fabric/FabricClient';
import { MongoConnector } from '../../database/mongodb/MongoConnector';
import { logGreenMessage, } from '../../utils/Log';
import { sleep } from '../../utils/Util';

import { resolve as resolvePath } from 'path';
import { performance as perf } from 'perf_hooks';

class ExperimentFabric4 extends FabricMongoApplication {

    public constructor(netConnProfPath: string, confOptsPath: string, walletPath: string,
            dbConnOptsPath: string, private chaincode: string) {
        super(netConnProfPath, confOptsPath, walletPath, dbConnOptsPath);
    }

    protected async deltaApp(dltClient: FabricClient, dbClient: MongoConnector): Promise<void> {
        const numSamples = 200;

        const synchronizer = new DeltaSyncExp1B<FabricClient, MongoConnector>(dltClient, dbClient);
        const initTime = perf.now();
        await synchronizer.startEventListeners(new FabricSmartContract(this.chaincode)/*, numSamples*/);
        const endTime = perf.now() - initTime;

        logGreenMessage(`Asset synchronization took ${endTime} milliseconds for ${numSamples} samples.`);
        logGreenMessage(`${endTime / numSamples} milliseconds per asset`);
        await sleep(numSamples / 4);
    }
}

const netConnProf = resolvePath(__dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', 'org1.delta.net', 'connection-org1.json');
const confOpts = resolvePath(__dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', 'org1.delta.net', 'config-options.json');
const dbConnOpts = resolvePath(__dirname, '..', '..', '..', 'rsc', 'db-conn-opts.json');
const wallet = resolvePath(__dirname, '..', '..', '..', 'wallet');
const smartContract = process.argv.length > 2 ? process.argv[2] : 'delta';

async function main() {
    const deltaApp = new ExperimentFabric4(netConnProf, confOpts, wallet, dbConnOpts, smartContract);
    await deltaApp.run();
}

main();
