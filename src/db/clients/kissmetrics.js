import { Client } from 'cassandra-driver';
import { identify } from 'sql-query-identifier';
import ogRequest from 'request';
import request from 'request-promise';
import fs from 'fs';
import zlib from 'zlib';
import Baby from 'papaparse';
import createLogger from '../../logger';

const logger = createLogger('db:clients:kissmetrics');

/**
 * To keep compatibility with the other clients we treat keyspaces as database.
 */

export default function (server, database) {
  return new Promise(async (resolve, reject) => {
    logger().debug(server.config);

    let qapiToken;
    if (server.config.user && server.config.password) {
      qapiToken = new Buffer(`${server.config.user}:${server.config.password}`).toString('base64');
    }

    resolve({
      wrapIdentifier,
      disconnect: () => disconnect(),
      listTables: (db) => listTables(),
      listViews: () => listViews(),
      listRoutines: () => listRoutines(),
      listTableColumns: (db, table) => listTableColumns(),
      listTableTriggers: (table) => listTableTriggers(),
      listTableIndexes: (db, table) => listTableIndexes(),
      listSchemas: () => listSchemas(),
      getTableReferences: (table) => getTableReferences(),
      getTableKeys: (db, table) => getTableKeys(),
      query: (queryText, productId, events, properties) => executeQuery(queryText, qapiToken, productId, events, properties),
      executeQuery: (queryText, productId, events, properties) => executeQuery(queryText, qapiToken, productId, events, properties),
      listDatabases: () => listDatabases(qapiToken),
      getQuerySelectTop: (table, limit) => getQuerySelectTop(),
      getTableCreateScript: (table) => getTableCreateScript(),
      getViewCreateScript: (view) => getViewCreateScript(),
      getRoutineCreateScript: (routine) => getRoutineCreateScript(),
      truncateAllTables: (db) => truncateAllTables(),
      listEvents: (productId) => listEvents(productId, qapiToken),
      listProperties: (productId) => listProperties(productId, qapiToken)
    });
  });
}

export async function listEvents(product, token) {
  if (!token) {
    return Promise.resolve([]);
  }
  const { data } = await request({
    method: 'GET',
    uri: `https://query.kissmetrics.com/v3/products/${product}/events?limit=10000`,
    json: true,
    headers: {
      Authorization: `Basic ${token}`,
    },
  });

  // Add the index to the names.
  return data
    .filter(d => d.visible)
    .map(d => ({ ...d, ogName: d.name, name: `${d.name} (${d.index})` }));
}

export async function listProperties(product, token) {
  if (!token) {
    return Promise.resolve([]);
  }
  const { data } = await request({
    method: 'GET',
    uri: `https://query.kissmetrics.com/v3/products/${product}/properties?limit=10000`,
    json: true,
    headers: {
      Authorization: `Basic ${token}`,
    },
  });

  // Add the index to the names.
  return data
    .filter(d => d.visible)
    .map(d => ({ ...d, ogName: d.name, name: `${d.name} (${d.index})` }))
}

export function disconnect() {
  return Promise.resolve();
}


export function listTables() {
  return Promise.resolve([{
    name: 'records',
  }]);
}

export function listViews() {
  return Promise.resolve([]);
}

export function listRoutines() {
  return Promise.resolve([
    {
      routineName: 'is_alias()',
      routineType: 'FUNCTION',
    },
    {
      routineName: 'is_set()',
      routineType: 'FUNCTION',
    },
    {
      routineName: 'is_event(nameOrID)',
      routineType: 'FUNCTION',
    },
    {
      routineName: 'property_value(nameOrID)',
      routineType: 'FUNCTION',
    },
    {
      routineName: 'numeric_property_value(nameOrID)',
      routineType: 'FUNCTION',
    },
    {
      routineName: 'has_property(nameOrID)',
      routineType: 'FUNCTION',
    },
  ]);
}

export function listTableColumns(client, database, table) {
  return Promise.resolve([
    {
      columnName: 'timestamp_ms',
      dataType: 'LONG',
    },
    {
      columnName: 'person',
      dataType: 'INT',
    },
    {
      columnName: 'year',
      dataType: 'INT',
    },
    {
      columnName: 'month',
      dataType: 'INT',
    },
    {
      columnName: 'orig_person',
      dataType: 'INT',
    },
    {
      columnName: 'dest_person',
      dataType: 'INT',
    },
    {
      columnName: 'event',
      dataType: 'INT',
    },
    {
      columnName: 'person_id',
      dataType: 'STRING',
    },
    {
      columnName: 'email',
      dataType: 'STRING',
    },
    {
      columnName: 'remote_ip',
      dataType: 'STRING',
    },
    {
      columnName: 'channel',
      dataType: 'STRING',
    },
    {
      columnName: 'channel_source',
      dataType: 'STRING',
    },
    {
      columnName: 'channel_with_source',
      dataType: 'STRING',
    },
    {
      columnName: 'previous_page',
      dataType: 'STRING',
    },
    {
      columnName: 'referrer',
      dataType: 'STRING',
    },
    {
      columnName: 'new_vs_returning',
      dataType: 'STRING',
    },
  ]);
}

export function listTableTriggers() {
  return Promise.resolve([]);
}
export function listTableIndexes() {
  return Promise.resolve([]);
}

export function listSchemas() {
  return Promise.resolve([]);
}

export function getTableReferences() {
  return Promise.resolve([]);
}

export function getTableKeys(client, database, table) {
  return Promise.resolve([]);
}

function query(conn, queryText) { // eslint-disable-line no-unused-vars
  throw new Error('"query" function is not implementd by kissmetrics client.');
}

export async function executeQuery(queryText, token, productId, events, properties) {
  const commands = identifyCommands(queryText).map((item) => item.type);
  if (!token) {
    return Promise.resolve([]);
  }

  // Start the query
  const { id } = await request({
    method: 'POST',
    uri: 'https://query.kissmetrics.com/v3/queries',
    json: true,
    body: {
      product_id: productId,
      query_type: 'sql',
      query_params: {
        statement: queryText,
      },
    },
    headers: {
      Authorization: `Basic ${token}`,
    },
  });

  logger().debug(`New query started: ${id}`);

  // Wait for it to finish
  let queryComplete = false;
  let data;
  while (!queryComplete) {
    await new Promise((resolve) => setTimeout(resolve, 2000));
    const response = await request({
      method: 'GET',
      uri: `https://query.kissmetrics.com/v3/queries/${id}?limit=10000`, // 10000 is the max.
      json: true,
      headers: {
        Authorization: `Basic ${token}`,
      },
    });
    // logger().debug(response);
    queryComplete = response.completed;
    data = response.data;
  }

  
  return [parseRowQueryResult(data, commands[0], events, properties)];
}


export async function listDatabases(token) {
  // Get the products from QAPI and label
  // them as database
  if (!token) {
    return Promise.resolve([]);
  }
  const { data } = await request({
    method: 'GET',
    uri: 'https://query.kissmetrics.com/v3/products/',
    json: true,
    headers: {
      Authorization: `Basic ${token}`,
    },
  });

  // Filter out products that return 403
  const featureFlagRequests = await Promise.all(data.map(product => request({
    method: 'POST',
    uri: 'https://query.kissmetrics.com/v3/queries',
    json: true,
    resolveWithFullResponse: true,
    simple: false,
    body: {
      product_id: product.id,
      query_type: 'sql',
      query_params: {
        statement: '',
      },
    },
    headers: {
      Authorization: `Basic ${token}`,
    },
    transform(body, response) {
      if (response.statusCode !== 403) {
        return product
      }

      return false
    }
  })))

  // Filter out falseys.
  return featureFlagRequests.filter(p => !!p);
}


export function getQuerySelectTop(client, table, limit) {
  return `SELECT * FROM ${wrapIdentifier(table)} LIMIT ${limit}`;
}

export function getTableCreateScript() {
  return Promise.resolve([]);
}

export function getViewCreateScript() {
  return Promise.resolve([]);
}

export function getRoutineCreateScript() {
  return Promise.resolve([]);
}

export function wrapIdentifier(value) {
  if (value === '*') return value;
  const matched = value.match(/(.*?)(\[[0-9]\])/); // eslint-disable-line no-useless-escape
  if (matched) return wrapIdentifier(matched[1]) + matched[2];
  return `"${value.replace(/"/g, '""')}"`;
}


export const truncateAllTables = async (connection, database) => {
  const sql = `
    SELECT table_name
    FROM system_schema.tables
    WHERE keyspace_name = '${database}'
  `;
  const [result] = await executeQuery(connection, sql);
  const tables = result.rows.map((row) => row.table_name);
  const promises = tables.map((t) => {
    const truncateSQL = `
      TRUNCATE TABLE ${wrapIdentifier(database)}.${wrapIdentifier(t)};
    `;
    return executeQuery(connection, truncateSQL);
  });

  await Promise.all(promises);
};

function configDatabase(server, database) {
  const config = {
    contactPoints: [server.config.host],
    protocolOptions: {
      port: server.config.port,
    },
    keyspace: database.database,
  };

  if (server.sshTunnel) {
    config.contactPoints = [server.config.localHost];
    config.protocolOptions.port = server.config.localPort;
  }

  if (server.config.ssl) {
    // TODO: sslOptions
  }

  return config;
}


function parseRowQueryResult(data, command, events, properties) {

  // Fallback in case the identifier could not reconize the command
  const isSelect = command ? command === 'SELECT' : Array.isArray(data);
  const dataLength = data.length;

  // This loops through each row and pulls fields unique to that row.
  // Any mismatch between columns and properties and the whole table doesn't
  // render.
  const fields = data.reduce((a, v, i) => {
    const keys = Object.keys(v);
    keys.forEach((k) => {
        if (k.indexOf('prop_mod') !== -1) {
          // Pluck the index from the value and set it as the property.
          const propMod = k
          const propModValue = JSON.parse(v[k])
          const propIndex = parseInt(Object.keys(propModValue)[0])
          let prop = properties.find(p => p.index === propIndex)
          console.log(prop)
          if (!prop) {
            // Weird edge case
            prop = {
              name: propMod,
              index: propIndex
            }
          }
          console.log(prop)

          // Set the internal index as the value.
          data[i][prop.name] = propModValue[propIndex]

          // Delete the "prop_mod" stringified value.
          if (prop.name !== propMod) {
            delete data[i][propMod]
          }
          
          // Push the internal index as a field.
          if (!a.includes(prop.name)) {
            a.push(prop.name)
          }
        } else {
          // Push the property name
          if (!a.includes(k)) {
            a.push(k);
          }
        }
    });
    return a;
  }, []);

  // Map the event indices to the display name.
  data = data.map(record => {
    if (record.event) {
      let event = events.find(e => e.index === record.event)
      if (!event) {
        event = {
          name: '',
          index: record.event
        }
      }
      return { ...record, event: event.name }
    }

    return record
  })

  return {
    fields: fields.map((f) => ({ name: f })),
    command: command || (isSelect && 'SELECT'),
    rows: data || [],
    rowCount: isSelect ? (dataLength || 0) : undefined,
    affectedRows: !isSelect && !isNaN(dataLength) ? dataLength : undefined,
  };
}


function identifyCommands(queryText) {
  try {
    return identify(queryText);
  } catch (err) {
    return [];
  }
}
