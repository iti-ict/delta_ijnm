/**
 * DELTA: DLT - Database synchronization
 * Experimento Hyperledger Fabric 2: Evaluación de rendimiento por componentes
 */

import { DeltaSync as DeltaSyncExp2 } from '../../delta/components/DeltaSynchronization';
import { FabricMongoApplication } from '../../delta/fabric/FabricMongoApplication';
import { FabricClient, FabricSmartContract } from '../../delta/fabric/FabricClient';
import { MongoConnector } from '../../database/mongodb/MongoConnector';
import { awaitInterrupt } from '../../utils/Util';

import { resolve as resolvePath } from 'path';

class ExperimentFabric2_2 extends FabricMongoApplication {

    public constructor(netConnProfPath: string, confOptsPath: string, walletPath: string,
            dbConnOptsPath: string, private chaincode: string) {
        super(netConnProfPath, confOptsPath, walletPath, dbConnOptsPath);
    }

    protected async deltaApp(dltClient: FabricClient, dbClient: MongoConnector): Promise<void> {
        const synchronizer = new DeltaSyncExp2<FabricClient, MongoConnector>(dltClient, dbClient);
        await synchronizer.startEventListeners(new FabricSmartContract(this.chaincode));
        await awaitInterrupt();
    }
}

const netConnProf = resolvePath(__dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', 'org1.delta.net', 'connection-org1.json');
const confOpts = resolvePath(__dirname, '..', '..', '..', 'rsc', 'organizations', 'peerOrganizations', 'org1.delta.net', 'config-options.json');
const dbConnOpts = resolvePath(__dirname, '..', '..', '..', 'rsc', 'db-conn-opts.json');
const wallet = resolvePath(__dirname, '..', '..', '..', 'wallet');
const smartContract = process.argv.length > 2 ? process.argv[2] : 'delta';

async function main() {
    const deltaApp = new ExperimentFabric2_2(netConnProf, confOpts, wallet, dbConnOpts, smartContract);
    await deltaApp.run();
}

main();
