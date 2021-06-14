import { RethinkDBError, RethinkDBErrorType } from '../error';
import { RethinkDBConnection } from './connection';
import { MasterConnectionPool } from './master-pool';
import {
  RethinkDBConnectionOptions,
  RethinkDBServerConnectionOptions,
} from './types';

export * from './types';

export async function connect(
  server: RethinkDBServerConnectionOptions,
  options: RethinkDBConnectionOptions,
): Promise<RethinkDBConnection> {
  const c = new RethinkDBConnection(server, options);
  await c.reconnect();
  return c;
}

export async function connectPool(
  servers: RethinkDBServerConnectionOptions[],
  options: RethinkDBConnectionOptions & { waitForHealthy?: boolean },
): Promise<MasterConnectionPool> {
  const { waitForHealthy = true } = options;
  if (!servers.length) {
    throw new RethinkDBError(
      'If `servers` is an array, it must contain at least one server.',
      { type: RethinkDBErrorType.API_FAIL },
    );
  }
  const connectionPool = new MasterConnectionPool(servers, options);

  if (waitForHealthy) {
    await connectionPool.waitForHealthy();
  }

  return connectionPool;
}
