import {
  RConnectionOptions,
  RethinkDBErrorType,
  RPoolConnectionOptions,
} from '../types';
import { RethinkDBError } from '../error/error';
import { RethinkDBConnection } from './connection';
import { MasterConnectionPool } from './master-pool';

async function createRethinkdbConnection(
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

async function createRethinkdbMasterPool(
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
    } else if ((options as any).servers) {
      throw new RethinkDBError(
        'If `host` or `port` are defined `servers` must not be.',
        { type: RethinkDBErrorType.API_FAIL },
      );
    }
  }
  if ((options as any).server && (options as any).servers) {
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
  const cpool = new MasterConnectionPool({
    ...options,
    servers,
  } as any);
  cpool.initServers().catch(() => undefined);
  if (waitForHealthy) {
    await cpool.waitForHealthy();
  }
  return cpool;
}

export { createRethinkdbConnection, createRethinkdbMasterPool };
