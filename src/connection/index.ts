import type { RConnectionOptions, RPoolConnectionOptions } from '../types';
import { RethinkDBError, RethinkDBErrorType } from '../error';
import { RethinkDBConnection } from './connection';
import { MasterConnectionPool } from './master-pool';

export async function createRethinkdbConnection(
  options: RConnectionOptions = {},
): Promise<RethinkDBConnection> {
  const { host, port, server = { host, port } } = options;
  if ((host || port) && options.server) {
    throw new RethinkDBError(
      'If `host` or `port` are defined `server` must not be.',
      { type: RethinkDBErrorType.API_FAIL },
    );
  }
  const c = new RethinkDBConnection(server, options);
  await c.reconnect();
  return c;
}

export async function createRethinkdbMasterPool(
  options: RPoolConnectionOptions = {},
): Promise<MasterConnectionPool> {
  const {
    host,
    port,
    server = { host, port },
    servers = [server],
    waitForHealthy = true,
  } = options;
  if (host || port) {
    if (options.server) {
      throw new RethinkDBError(
        'If `host` or `port` are defined `server` must not be.',
        { type: RethinkDBErrorType.API_FAIL },
      );
    } else if (options.servers) {
      throw new RethinkDBError(
        'If `host` or `port` are defined `servers` must not be.',
        { type: RethinkDBErrorType.API_FAIL },
      );
    }
  }
  if (options.server && options.servers) {
    throw new RethinkDBError('If `server` is defined `servers` must not be.', {
      type: RethinkDBErrorType.API_FAIL,
    });
  }
  if (!servers.length) {
    throw new RethinkDBError(
      'If `servers` is an array, it must contain at least one server.',
      { type: RethinkDBErrorType.API_FAIL },
    );
  }
  const connectionPool = new MasterConnectionPool({
    ...options,
    servers,
  });
  connectionPool.initServers().catch(console.error);
  if (waitForHealthy) {
    await connectionPool.waitForHealthy();
  }
  return connectionPool;
}
