// Copyright (c) 2015 The SQLECTRON Team
import { MongodbChangeBuilder } from '@shared/lib/sql/change_builder/MongodbChangeBuilder';
import rawLog from 'electron-log';
import { readFileSync } from 'fs';
import _ from 'lodash';
import mysql from 'mysql2';
import mongodb, { MongoClient } from 'mongodb';
import { identify } from 'sql-query-identifier';
import globals from '../../../common/globals';
import { createCancelablePromise } from '../../../common/utils';
import { errors } from '../../errors';
import { MongodbCursor } from './mongodb/MongodbCursor';
import { buildDeleteQueries, buildInsertQueries, buildSelectTopQuery, escapeString, joinQueries } from './utils';

const log = rawLog.scope('mongodb')
const logger = () => log

const mongodbErrors = {
  EMPTY_QUERY: 'ER_EMPTY_QUERY',
  CONNECTION_LOST: 'PROTOCOL_CONNECTION_LOST',
};

export default async function (server, database) {
  const dbConfig = configDatabase(server, database);
  logger().debug('create driver client for mongodb with config %j', dbConfig);

  const conn = {
    connection: new MongoClient(dbConfig.url),
    config: dbConfig,
  };

  const versionInfo = await getVersion(conn)

  return {
    supportedFeatures: () => ({ customRoutines: true, comments: true, properties: true }),
    wrapIdentifier,
    disconnect: () => disconnect(conn),
    listTables: () => listTables(conn),
    listViews: () => listViews(conn),
    listMaterializedViews: () => Promise.resolve([]),
    listRoutines: () => listRoutines(conn),
    listTableColumns: (db, table) => listTableColumns(conn, db, table),
    listTableTriggers: (table) => listTableTriggers(conn, table),
    listTableIndexes: (db, table) => listTableIndexes(conn, db, table),
    listSchemas: () => listSchemas(conn),
    getTableReferences: (table) => getTableReferences(conn, table),
    getPrimaryKey: (db, table) => getPrimaryKey(conn, db, table),
    getPrimaryKeys: (db, table) => getPrimaryKeys(conn, db, table),
    getTableKeys: (db, table) => getTableKeys(conn, db, table),
    query: (queryText) => query(conn, queryText),
    applyChanges: (changes) => applyChanges(conn, changes),
    executeQuery: (queryText) => executeQuery(conn, queryText),
    listDatabases: (filter) => listDatabases(conn, filter),
    // tabletable
    getTableLength: (table) => getTableLength(conn, table),
    selectTop: (table, offset, limit, orderBy, filters) => selectTop(conn, table, offset, limit, orderBy, filters),
    selectTopStream: (db, table, orderBy, filters, chunkSize, schema) => selectTopStream(conn, db, table, orderBy, filters, chunkSize, schema),
    getQuerySelectTop: (table, limit) => getQuerySelectTop(conn, table, limit),
    getTableCreateScript: (table) => getTableCreateScript(conn, table),
    getViewCreateScript: (view) => getViewCreateScript(conn, view),
    getRoutineCreateScript: (routine, type) => getRoutineCreateScript(conn, routine, type),
    truncateAllTables: () => truncateAllTables(conn),
    getTableProperties: (table) => getTableProperties(conn, table),
    setTableDescription: (table, description) => setTableDescription(conn, table, description),

    // schema
    alterTableSql: (change) => alterTableSql(conn, change),
    alterTable: (change) => alterTable(conn, change),

    // indexes
    alterIndexSql: (adds, drops) => alterIndexSql(adds, drops),
    alterIndex: (adds, drops) => alterIndex(conn, adds, drops),

    // relations
    alterRelationSql: (payload) => alterRelationSql(payload),
    alterRelation: (payload) => alterRelation(conn, payload)

  };
}


export function disconnect(conn) {
  conn.connection.close();
}

async function getVersion(conn) {
  const version = await driverExecuteQuery(conn, {
    getVersion: 1
  })

  if (!version) {
    return {
      versionString: '',
      version: 4.4
    }
  }

  const stuff = version.split(".")

  return {
    versionString: version,
    version: Number(stuff[0] || 0)
  }
}


export async function listTables(conn) {
  if (! conn.config.database.database) {
    return [];
  }

  const tables = await driverExecuteQuery(conn, {
    listTables: 1,
  });

  return tables;
}

export async function listViews(conn) {
  return [];
}

export async function listRoutines(conn) {
  return [];
}

export async function listTableColumns(conn, database, table) {
  return [];
}

async function getTableLength(conn, table) {
  if (! conn.config.database.database) {
    return Number(0)
  }

  const tables = await driverExecuteQuery(conn, {
    tableCount: 1,
  });
}



export async function selectTop(conn, table, offset, limit, orderBy, filters) {
  return [];
}

export async function selectTopStream(conn, db, table, orderBy, filters, chunkSize) {
  return [];
}

export async function listTableTriggers(conn, table) {
  return [];
}

export async function listTableIndexes(conn, database, table) {
  return [];
}

export function listSchemas() {
  return [];
}

export async function getTableReferences(conn, table) {
  return [];
}

export async function getPrimaryKeys(conn, database, table) {
  return [];
}

export async function getPrimaryKey(conn, database, table) {
  return [];
}

export async function getTableKeys(conn, database, table) {
  return [];
}


export function query(conn, queryText) {
  return [];
}

export async function applyChanges(conn, changes) {
  return false;
}

export async function insertRows(cli, inserts) {
  return false
}

export async function updateValues(cli, updates) {
  return true;
}

export async function deleteRows(cli, deletes) {
  return true
}

export async function executeQuery(conn, queryText, rowsAsArray = false) {
  return [];
}


export async function listDatabases(conn, filter) {
  const databases = await driverExecuteQuery(conn, {
    listDatabases: 1
  });

  return databases;
}


export function getQuerySelectTop(conn, table, limit) {
  return [];
}

export function getTableCreateScript(conn, table, limit) {
  return [];
}

export async function getViewCreateScript(conn, view) {
  return [];
}

export async function getRoutineCreateScript(conn, routine, type) {
  return [];
}

export function wrapIdentifier(value) {
  return '';
}

async function getSchema(conn) {
  return [];
}

export async function truncateAllTables(conn) {
  return new Promise.resolve([]);
}

export async function getTableProperties(conn, table) {
  return new Promise.resolve([]);
}

async function setTableDescription(conn, table, description) {
  return [];
}
async function alterTableSql(conn, change) {
  return [];
}

async function alterTable(_conn, change) {
  return [];
}

export function alterIndexSql(payload) {
  return [];
}

export async function alterIndex(conn, payload) {
  return [];
}


export function alterRelationSql(payload) {
  return [];
}

export async function alterRelation(conn, payload) {
  return [];
}



function configDatabase(server, database) {
  const url = `${server.config.client}://${server.config.host}:${server.config.port}`;

  let config = {
    url: url,
    database: database,
    server: server,
  };

  return config;
}


function getRealError(conn, err) {
  /* eslint no-underscore-dangle:0 */
  if (conn && conn._protocol && conn._protocol._fatalError) {
    logger().warn("Query error", err, conn._protocol._fatalError)
    return conn._protocol._fatalError;
  }
  return err;
}

function parseFields(fields, rowsAsArray) {
  return [];
}


function parseRowQueryResult(data, rawFields, command, rowsAsArray = false) {
  return [];
}


function isMultipleQuery(fields) {
  if (!fields) { return false; }
  if (!fields.length) { return false; }
  return (Array.isArray(fields[0]) || fields[0] === undefined);
}


function identifyCommands(queryText) {
  try {
    return identify(queryText);
  } catch (err) {
    return [];
  }
}

async function executeWithTransaction(conn, queryArgs) {
  return new Promise.resolve([]);
}

function driverExecuteQuery(conn, queryArgs) {
  const runQuery = (conn) => new Promise((resolve, reject) => {
    const config = conn.config;
    const connection = conn.connection;

    if (queryArgs.getVersion) {
      const defaultDb = connection.db().databaseName;
      const dbAdmin = connection.db(defaultDb).admin();
      dbAdmin.serverStatus({ serverStatus: 1 })
        .then(result => resolve(result.version));
    }

    if (queryArgs.listDatabases) {
      const defaultDb = connection.db().databaseName;
      const dbAdmin = connection.db(defaultDb).admin();
      dbAdmin.listDatabases((error, result) => {
        resolve(result.databases.map(database => database.name));
      });
    }

    if (queryArgs.listTables) {
      if (config.database.database) {
        const dbQuery = connection.db(config.database.database);
        let tables = dbQuery
          .listCollections()
          .toArray()
          .then(tables => {
            tables.forEach(table => {
              table.entityType = 'table';
            });

            resolve(tables);
          });
      }
    }

    if (queryArgs.tableCount) {
      if (config.database.database) {
        const dbQuery = connection.db(config.database.database);
        let tables = dbQuery
          .count({}, (err, count) => {
            resolve(count)
          });
      }
    }

    logger().info(`Running Query Finished`);
  });

  return runWithConnection(conn, runQuery);
}

async function runWithConnection(conn, run) {
  let rejected = false;
  return new Promise((resolve, reject) => {
    const rejectErr = (err) => {
      if (!rejected) {
        rejected = true;
        reject(err);
      }
    };

    try {
      conn.connection
        .connect()
        .then(() => {
          run(conn).then(result => resolve(result))
        })
    } catch (error) {
      rejectErr(`Error: ${error}`);
    }
  });
}

async function runWithTransaction(conn, func) {
  return new Promise.resolve([]);
}

export function filterDatabase(item, { database } = {}, databaseField) {
  if (!database) { return true; }

  const value = item[databaseField];
  if (typeof database === 'string') {
    return database === value;
  }

  const { only, ignore } = database;

  if (only && only.length && !~only.indexOf(value)) {
    return false;
  }

  if (ignore && ignore.length && ~ignore.indexOf(value)) {
    return false;
  }

  return true;
}


export const testOnly = {
  parseFields
}
