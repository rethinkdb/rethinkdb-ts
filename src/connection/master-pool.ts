import { EventEmitter } from 'events';
import { RethinkDBError, RethinkDBErrorType } from '../error';
import { r } from '../query-builder/r';
import { RQuery } from '../query-builder/query';
import { Cursor } from '../response/cursor';
import type {
  RethinkDBPoolConnectionOptions,
  RethinkDBServerConnectionOptions,
  RunOptions,
} from './types';
import type { Changes, RServer, TermJson } from '../types';
import { delay, isIPv6 } from '../util';
import { RethinkDBConnection } from './connection';
import { ServerConnectionPool } from './server-pool';
import { setConnectionDefaults } from './socket';
import { RFeed } from '../types';

function flat<T>(acc: T[], next: T[]) {
  return [...acc, ...next];
}

export interface MasterPoolOptions {
  discovery?: boolean;
  buffer?: number;
  max?: number;
  timeoutError?: number;
  timeoutGb?: number;
  maxExponent?: number;
  silent?: boolean;
  log?: (msg: string) => void;
}

// Try to extract the most global address
// https://github.com/neumino/rethinkdbdash/blob/f77d2ffb77a8c0fa41aabc511d74aa86ea1136d9/lib/helper.js
function getCanonicalAddress(addresses: RServer[]) {
  // We suppose that the addresses are all valid, and therefore use loose regex
  return addresses
    .map((address) => {
      if (
        /^127(\.\d{1,3}){3}$/.test(address.host) ||
        /0?:?0?:?0?:?0?:?0?:?0?:0?:1/.test(address.host)
      ) {
        return { address, value: 0 };
      }
      if (isIPv6(address.host) && /^[fF]|[eE]80:.*:.*:/.test(address.host)) {
        return { address, value: 1 };
      }
      if (/^169\.254\.\d{1,3}\.\d{1,3}$/.test(address.host)) {
        return { address, value: 2 };
      }
      if (/^192\.168\.\d{1,3}\.\d{1,3}$/.test(address.host)) {
        return { address, value: 3 };
      }
      if (/^172\.(1\d|2\d|30|31)\.\d{1,3}\.\d{1,3}$/.test(address.host)) {
        return { address, value: 4 };
      }
      if (/^10(\.\d{1,3}){3}$/.test(address.host)) {
        return { address, value: 5 };
      }
      if (isIPv6(address.host) && /^[fF]|[cCdD].*:.*:/.test('address.host')) {
        return { address, value: 6 };
      }
      return { address, value: 7 };
    })
    .reduce((acc, next) => (acc.value > next.value ? acc : next)).address.host;
}

interface ServerStatus {
  id: string;
  name: string;
  network: {
    // eslint-disable-next-line camelcase
    canonical_addresses: Array<{
      host: string;
      port: number;
    }>;
    // eslint-disable-next-line camelcase
    cluster_port: number;
    // eslint-disable-next-line camelcase
    connected_to: Record<string, unknown>;
    hostname: string;
    // eslint-disable-next-line camelcase
    http_admin_port: number;
    // eslint-disable-next-line camelcase
    reql_port: number;
    // eslint-disable-next-line camelcase
    time_connected: Date;
  };
  process: {
    argv: string[];
    // eslint-disable-next-line camelcase
    cache_size_mb: number;
    pid: number;
    // eslint-disable-next-line camelcase
    time_started: Date;
    version: string;
  };
}

export class MasterConnectionPool extends EventEmitter {
  public draining = false;

  private healthy: boolean | undefined = undefined;

  private discovery: boolean;

  private discoveryCursor?: Cursor<Changes<ServerStatus>>;

  private servers: RServer[];

  private readonly serverPools: ServerConnectionPool[];

  private connParam: RethinkDBPoolConnectionOptions;

  constructor(
    servers: RethinkDBServerConnectionOptions[],
    options: RethinkDBPoolConnectionOptions,
  ) {
    super();
    const {
      db = 'test',
      user = 'admin',
      password = '',
      discovery = false,
      buffer = servers.length,
      max = servers.length,
      timeout = 20,
      pingInterval = -1,
      timeoutError = 1000,
      timeoutGb = 60 * 60 * 1000,
      maxExponent = 6,
      silent = false,
      log = () => undefined,
    } = options;
    // min one per server but wont redistribute conn from failed servers
    this.discovery = discovery;
    this.connParam = {
      db,
      user,
      password,
      buffer: Math.max(buffer, 1),
      max: Math.max(max, buffer),
      timeout,
      pingInterval,
      timeoutError,
      timeoutGb,
      maxExponent,
      silent,
      log,
    };
    this.servers = servers.map(setConnectionDefaults);
    this.serverPools = [];
  }

  public setOptions({
    discovery = this.discovery,
    buffer = this.connParam.buffer,
    max = this.connParam.max,
    timeoutError = this.connParam.timeoutError,
    timeoutGb = this.connParam.timeoutGb,
    maxExponent = this.connParam.maxExponent,
    silent = this.connParam.silent,
    log = this.connParam.log,
  }: MasterPoolOptions): void {
    if (this.discovery !== discovery) {
      this.discovery = discovery;
      if (discovery) {
        this.discover();
      } else if (this.discoveryCursor) {
        this.discoveryCursor.close();
      }
    }
    this.connParam = {
      ...this.connParam,
      buffer,
      max,
      timeoutError,
      timeoutGb,
      maxExponent,
      silent,
      log,
    };
    this.setServerPoolsOptions(this.connParam);
  }

  public eventNames(): string[] {
    return [
      'draining',
      'queueing',
      'size',
      'available-size',
      'healthy',
      'error',
    ];
  }

  public async initServers(serverNum = 0): Promise<void> {
    if (serverNum < this.servers.length) {
      return this.createServerPool(this.servers[serverNum]).then((pool) => {
        if (!this.draining) {
          return this.initServers(serverNum + 1);
        }
        return pool.drain();
      });
    }
    if (!this.draining) {
      this.setServerPoolsOptions(this.connParam);
    }
    return undefined;
  }

  public get isHealthy(): boolean {
    return this.serverPools.some((pool) => pool.isHealthy);
  }

  public waitForHealthy(): Promise<this> {
    return new Promise<this>((resolve, reject) => {
      if (this.isHealthy) {
        resolve(this);
      } else {
        this.once('healthy', (healthy, error) => {
          if (healthy) {
            resolve(this);
          } else {
            reject(
              new RethinkDBError('Error initializing master pool', {
                type: RethinkDBErrorType.MASTER_POOL_FAIL,
                cause: error,
              }),
            );
          }
        });
      }
    });
  }

  public async drain(): Promise<void> {
    this.emit('draining');
    this.draining = true;
    this.discovery = false;
    if (this.discoveryCursor) {
      this.discoveryCursor.close();
    }
    this.setHealthy(false);
    await Promise.all(
      this.serverPools.map((pool) => this.closeServerPool(pool)),
    );
  }

  public getPools(): ServerConnectionPool[] {
    return this.serverPools;
  }

  public getConnections(): RethinkDBConnection[] {
    return this.serverPools
      .map((pool) => pool.getConnections())
      .reduce(flat, []);
  }

  public getLength(): number {
    return this.getOpenConnections().length;
  }

  public getAvailableLength(): number {
    return this.getIdleConnections().length;
  }

  public async queue(
    term: TermJson,
    globalArgs: RunOptions = {},
  ): Promise<Cursor | undefined> {
    if (!this.isHealthy) {
      throw new RethinkDBError(
        'None of the pools have an opened connection and failed to open a new one.',
        { type: RethinkDBErrorType.POOL_FAIL },
      );
    }
    this.emit('queueing');
    const pool = this.getPoolWithMinQueries();
    return pool.queue(term, globalArgs);
  }

  private async createServerPool(server: RServer) {
    const pool = new ServerConnectionPool(server, {
      ...this.connParam,
      buffer: 1,
      max: 1,
    });
    this.serverPools.push(pool);
    this.subscribeToPool(pool);
    pool.initConnections().catch(() => undefined);
    return pool.waitForHealthy();
  }

  private setServerPoolsOptions(params: RethinkDBPoolConnectionOptions) {
    const { buffer = 1, max = 1, ...otherParams } = params;
    const pools = this.getPools();
    const healthyLength = pools.filter((pool) => pool.isHealthy).length;
    for (let i = 0; i < pools.length; i += 1) {
      const pool = pools[i];
      pool
        .setOptions(
          pool.isHealthy
            ? {
                ...otherParams,
                buffer:
                  Math.floor(buffer / healthyLength) +
                  (i === (buffer % healthyLength) - 1 ? 1 : 0),
                max:
                  Math.floor(max / healthyLength) +
                  (i === (max % healthyLength) - 1 ? 1 : 0),
              }
            : otherParams,
        )
        .then(() => {
          if (this.draining) {
            pool.drain();
          }
        });
    }
    if (this.draining) {
      pools.forEach((pool) => pool.drain());
    }
  }

  private async discover(): Promise<void> {
    this.discoveryCursor = (await this.run(
      r
        .db('rethinkdb')
        .table<ServerStatus>('server_status')
        .changes({ includeInitial: true, includeStates: true }),
    )) as Cursor<Changes<ServerStatus>>;
    const newServers: RServer[] = [];
    let state: 'initializing' | 'ready' = 'initializing';
    return (
      this.discoveryCursor
        .eachAsync(async (row) => {
          if (row.state) {
            state = row.state;
            if (row.state === 'ready') {
              this.servers.forEach((server) => {
                if (!newServers.some((s) => s === server)) {
                  this.removeServer(server);
                }
              });
            }
          }
          if (row.new_val) {
            const server = this.getServerFromStatus(row.new_val);
            if (state === 'initializing') {
              newServers.push(server);
            }
            if (!this.servers.includes(server)) {
              this.servers.push(server);
              this.createServerPool(server).then(() =>
                this.setServerPoolsOptions(this.connParam),
              );
            }
          } else if (row.old_val) {
            this.removeServer(this.getServerFromStatus(row.old_val));
          }
        })
        // handle disconnections
        .catch(() => delay(20_000))
        .then(() => (this.discovery ? this.discover() : undefined))
    );
  }

  private getServerFromStatus(status: ServerStatus) {
    const oldServer = this.servers.find(
      (server) =>
        (server.host === status.network.hostname ||
          !!status.network.canonical_addresses.find(
            (addr) => addr.host === server.host,
          )) &&
        server.port === status.network.reql_port,
    );
    return (
      oldServer || {
        host: getCanonicalAddress(status.network.canonical_addresses),
        port: status.network.reql_port,
      }
    );
  }

  private async removeServer(server: RServer) {
    if (this.servers.includes(server)) {
      this.servers = this.servers.filter((s) => s !== server);
    }
    const pool = this.serverPools.find(
      (p) =>
        server.host === p.serverOptions.host &&
        server.port === p.serverOptions.port,
    );
    if (pool) {
      await this.closeServerPool(pool);
      this.setServerPoolsOptions(this.connParam);
    }
  }

  private subscribeToPool(pool: ServerConnectionPool) {
    const size = this.getOpenConnections().length;
    this.emit('size', size);
    if (size > 0) {
      this.setHealthy(true);
    }
    pool
      .on('size', () => this.emit('size', this.getOpenConnections().length))
      .on('available-size', () =>
        this.emit('available-size', this.getAvailableLength()),
      )
      .on('error', (error) => {
        if (this.listenerCount('error') > 0) {
          this.emit('error', error);
        }
      })
      .on('healthy', (healthy?: boolean, error?: Error) => {
        if (!healthy) {
          const { serverOptions } = pool;
          this.closeServerPool(pool)
            .then(
              () =>
                new Promise((resolve) =>
                  // fixme get rid of condition in number
                  setTimeout(
                    resolve,
                    (this.connParam && this.connParam.timeoutError) ||
                      1000 ||
                      1000,
                  ),
                ),
            )
            .then(() => {
              if (!this.draining) {
                this.createServerPool(serverOptions).catch(() => undefined);
              }
            });
        }
        this.setHealthy(!!this.getHealthyServerPools().length, error);
      });
  }

  private setHealthy(healthy: boolean | undefined, error?: Error) {
    if (healthy === undefined) {
      this.healthy = undefined;
    } else if (healthy !== this.healthy && healthy !== undefined) {
      this.healthy = healthy;
      this.emit('healthy', healthy, error);
    }
  }

  private async closeServerPool(pool: ServerConnectionPool) {
    if (pool) {
      pool.removeAllListeners();
      const index = this.serverPools.indexOf(pool);
      if (index >= 0) {
        this.serverPools.splice(index, 1);
      }
      await pool.drain();
    }
  }

  private getHealthyServerPools() {
    return this.serverPools.filter((pool) => pool.isHealthy);
  }

  private getPoolWithMinQueries() {
    return this.getHealthyServerPools().reduce((min, next) =>
      min.getNumOfRunningQueries() < next.getNumOfRunningQueries() ? min : next,
    );
  }

  private getOpenConnections() {
    return this.getConnections().filter((conn) => conn.open);
  }

  private getIdleConnections() {
    return this.getOpenConnections().filter((conn) => !conn.numOfQueries);
  }

  public async run(query: RFeed, options?: RunOptions): Promise<Cursor>;

  public async run(query: RQuery, options?: RunOptions): Promise<any>;

  async run(query: RQuery, options?: RunOptions): Promise<unknown> {
    const { term } = query;
    if (this.draining) {
      throw new RethinkDBError(
        '`run` was called without a connection and no pool has been created after:',
        { term, type: RethinkDBErrorType.API_FAIL },
      );
    }
    const cursor = await this.queue(term, options);
    if (!cursor) {
      return undefined;
    }
    const results = await cursor.resolve();
    switch (cursor.getType()) {
      case 'Atom':
        return cursor.profile
          ? { profile: cursor.profile, result: results[0] }
          : results[0];
      case 'Cursor':
        return cursor.profile
          ? { profile: cursor.profile, result: await cursor.toArray() }
          : cursor.toArray();
      default:
        return cursor;
    }
  }

  public async getCursor<T = unknown>(
    query: RQuery,
    options?: RunOptions,
  ): Promise<Cursor<T>> {
    const { term } = query;
    if (this.draining) {
      throw new RethinkDBError(
        '`run` was called without a connection and no pool has been created after:',
        { term, type: RethinkDBErrorType.API_FAIL },
      );
    }
    const cursor = await this.queue(term, options);
    if (!cursor) {
      throw new RethinkDBError(
        'cursor was not returned! maybe you provided "noreply" option?',
      );
    }
    cursor.init();
    return cursor;
  }
}
