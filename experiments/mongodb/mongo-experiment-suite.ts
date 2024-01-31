/**
 * DELTA: DLT - Database synchronization
 * Suite of evaluation experiments on MongoDB queries
 * (both key-value and filtering with custom search criteria)
 */

import { DeltaAsset } from '../../delta/Delta';
import { IDeltaDatabaseConnector } from '../../database/DeltaDatabase';
import { MongoConnector, ConnectionOptions } from '../../database/mongodb/MongoConnector';
import { AssetType, generateSimpleAsset } from '../asset-generation';
import { logMessage, logGreenMessage, logRedMessage, logObject } from '../../utils/Log';
import { parseObjectFromJsonFile as loadConfigFile, sleep } from '../../utils/Util';

import { performance as perf } from 'perf_hooks';
import { resolve as resolvePath } from 'path';
import * as fs from 'fs';

interface Result {
    [documents: number]: {
        key_value_query: number | number[],
        complex_query?: number | number[]
    }
}

abstract class DatabaseExperimentSuite<DB extends IDeltaDatabaseConnector> {

    protected abstract setupDatabaseConnector(database: string): Promise<DB>;
    protected abstract countDbDocuments(db: DB, collection: string): Promise<number>;
    protected abstract keyValueDbQuery(db: DB, collection: string,
        key: string, log?: boolean): Promise<object>;
    protected abstract customDbQuery(db: DB, collection: string, assetType: AssetType,
        param1: number, param2: number, log?: boolean): Promise<object[]>;

    // Entry point for launching evaluation to the database
    public readonly dbEvaluation = async(database: string, collection: string,
            assetType: AssetType, numQueries: number, outputPath?: string): Promise<Result> => {
        
        const dbConnector = await this.setupDatabaseConnector(database);
        
        const dumpResult = async(): Promise<void> => {
            if (!!outputPath) {
                writeResultFile(result, outputPath);
            } else {
                logObject(result);
            }
            process.exit(1)
        }
        process.on('SIGINT', dumpResult);
        process.on('SIGTERM', dumpResult);
        
        const result: Result = {};
        try {
            // Measure time when executing queries
            result[await this.countDbDocuments(dbConnector, collection)] = {
                'key_value_query': await this.keyValueQuery(dbConnector, collection, assetType, numQueries),
                'complex_query': await this.customQuery(dbConnector, collection, assetType, numQueries / 10)
            };
        } catch (ex) {
            logRedMessage('DATABASE EVALUATION PROCESS FAILED!!!!');
            console.error(ex);
        } finally {
            if (!!outputPath) {
                writeResultFile(result, outputPath);
            }

            await dbConnector.disconnect();
        }

        return result;
    }

    private async keyValueQuery(db: DB, collection: string, assetType: AssetType, numQueries: number):
            Promise<number[]> {
        const documentCount = await this.countDbDocuments(db, collection);
        
        const queryTime: number[] = [];
        for (let n = 0; n < numQueries; n++) {
            const key = `id-${assetType}-${Math.floor(Math.random() * documentCount) + 1}`;
            const initTime = perf.now();
            await this.keyValueDbQuery(db, collection, key);
            queryTime.push(perf.now() - initTime);
        }

        logTime('key-value', assetType, documentCount, queryTime);
        await sleep(documentCount / 1000 + numQueries);
        return queryTime;
    }

    private async customQuery(db: DB, collection: string, assetType: AssetType, numQueries: number):
            Promise<number[]> {
        const documentCount = await this.countDbDocuments(db, collection);

        const queryTime: number[] = [];
        for (let n = 0; n < numQueries; n++) {
            const { value1, value2 } = generateSimpleAsset();
            const initTime = perf.now();
            await this.customDbQuery(db, collection, assetType, value1, value2);
            queryTime.push(perf.now() - initTime);
        }

        logTime('custom', assetType, documentCount, queryTime);
        await sleep(documentCount / 1000 + numQueries);
        return queryTime;
    }
}

class MongoDbExperimentSuite extends DatabaseExperimentSuite<MongoConnector> {

    protected setupDatabaseConnector(database: string): Promise<MongoConnector> {
        const connectionOptions = <ConnectionOptions> loadConfigFile(resolvePath(
            'rsc', 'db-conn-opts.json'));
        connectionOptions.database = database;
        const mongo = new MongoConnector(connectionOptions);
        return new Promise<MongoConnector>((resolve) => mongo.connect().then(() => resolve(mongo)));
    }

    protected async countDbDocuments(db: MongoConnector, collection: string): Promise<number> {
        return (await db.retrieveAllAssets(collection)).length;
    }

    protected async keyValueDbQuery(db: MongoConnector, collection: string,
            key: string, log=false): Promise<object> {
        try {
            const documentValue = await db.retrieveAssetValue(collection, key);
            if (log && !!documentValue) {
                logMessage(`Retrieved document ${key}:\t${JSON.stringify(documentValue)}`);
            }
            return documentValue;
        } catch (ex) {
            logRedMessage(`${key}: ${ex.message}`);
        }
    }

    protected async customDbQuery(db: MongoConnector, collection: string, assetType: AssetType,
            param1: number, param2: number, log=false): Promise<object[]> {
        try {
            let querySelector: object;
            switch (assetType) {
                case AssetType.Simple:
                    querySelector = {
                        value1: { '$eq': param1 },
                        value2: { '$lt': param2 }
                    };
                    break;
                case AssetType.Intermediate:
                    querySelector = {
                        integerArrays: {
                            randValues1: { '$elemMatch': { '$eq': param1 } },
                            randValues2: { '$elemMatch': { '$lt': param2 } }
                        }
                    };
                    break;
                case AssetType.Complex:
                    querySelector = {
                        empresa: {
                            [`declaracionesConformidad.${0}`]: { leyes: { '$size': param1 } },
                            productos: { '$lt': { '$size': param2 } }
                        }
                    };
                    break;
            }
            const queryResult = await db.executeQuery(collection, querySelector);

            if (log && queryResult && queryResult.length) {
                const queryResultDocuments = queryResult.map((asset) =>
                    { return { key: asset._id, document: (asset as unknown as DeltaAsset).value } });
                logMessage(`Retrieved ${queryResult.length} documents from complex query`);
                logObject(queryResultDocuments);
            }
            return queryResult;
        } catch (ex) {
            logRedMessage(`Complex query: ${ex.message}`);
        }
    }
}

function writeResultFile(result: Result, path: string): void {
    fs.writeFileSync(resolvePath(process.cwd(), path + '.json'),
        JSON.stringify(result, null, 4), { mode: 0o644 });
}

function logTime(operation: string, assetType: AssetType,
        numAssets: number, queryTime: number[]): void {
    const numQueries = queryTime.length;
    const sumTime = Math.round(queryTime.reduce((a, b) => a + b));
    const queryMedianTime = Math.round(median(queryTime) * 1e4) / 1e4;

    logGreenMessage(`${numQueries} ${operation} queries on ${numAssets} ${assetType} documents`
        + ` took ${sumTime} milliseconds.`);
    logGreenMessage(`${queryMedianTime} milliseconds per query`);
}

function median(numbers: number[]): number {
    const sorted = numbers.slice().sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);

    return sorted.length % 2 ? sorted[middle]
        : (sorted[middle - 1] + sorted[middle]) / 2;
}

// Evaluation constant parameters
const assetType = AssetType.Simple;
const database = 'delta-fabric-channel';
const numQueries = 20000;

// Dilucidation of the collection in the database. If no supplementary CLI
// arguments are provided, it is inferred from the asset type.
const dbCollection = process.argv.length > 2 ? process.argv[2] : `delta_${assetType}`;

// Initialization of the proper evaluation object
const evaluation = new MongoDbExperimentSuite();

async function main() {
    await evaluation.dbEvaluation(database, dbCollection, assetType, numQueries,
        `delta-mongo-${assetType}-${numQueries}`);
}

main();
