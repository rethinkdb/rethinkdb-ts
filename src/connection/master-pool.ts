import { EventEmitter } from 'events';
import { isIPv6 } from 'net';
import { RethinkDBError } from '../error/error';
import { TermJson } from '../internal-types';
import { r } from '../query-builder/r';
import { Cursor } from '../response/cursor';
import {
  Changes,
  Connection,
  MasterPool,
  RCursor,
  RethinkDBErrorType,
  RPoolConnectionOptions,
  RServer,
  RunOptions,
} from '../types';
import { RethinkDBConnection } from './connection';
import { ServerConnectionPool } from './server-pool';
import { setConnectionDefaults } from './socket';

export class MasterConnectionPool extends EventEmitter implements MasterPool {
  public draining = false;

  private healthy: boolean | undefined = undefined;

  private discovery: boolean;

  private discoveryCursor?: RCursor<Changes<ServerStatus>>;

  private servers: RServer[];

  private serverPools: ServerConnectionPool[];

  private connParam: RPoolConnectionOptions;

  constructor({
    db = 'test',
    user = 'admin',
    password = '',
    discovery = false,
    servers = [{ host: 'localhost', port: 28015 }],
    buffer = servers.length,
    max = servers.length,
    timeout = 20,
    pingInterval = -1,
    timeoutError = 1000,
    timeoutGb = 60 * 60 * 1000,
    maxExponent = 6,
    silent = false,
    log = (message: string) => undefined,
  }: RPoolConnectionOptions = {}) {
    super();
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
  }) {
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

  public eventNames() {
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
  }

  public get isHealthy() {
    return this.serverPools.some((pool) => pool.isHealthy);
  }

  public waitForHealthy() {
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

  public async drain({ noreplyWait = false } = {}) {
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

  public getPools() {
    return this.serverPools;
  }

  public getConnections(): Connection[] {
    return this.serverPools
      .map((pool) => pool.getConnections())
      .reduce(flat, []);
  }

  public getLength() {
    return this.getOpenConnections().length;
  }

  public getAvailableLength() {
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

  private setServerPoolsOptions(params: RPoolConnectionOptions) {
    const { buffer = 1, max = 1, ...otherParams } = params;
    const pools = this.getPools();
    const healthyLength = pools.filter((pool) => pool.isHealthy).length;
    for (let i = 0; i < pools.length; i++) {
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
    this.discoveryCursor = await r
      .db('rethinkdb')
      .table<ServerStatus>('server_status')
      .changes({ includeInitial: true, includeStates: true })
      .run();
    const newServers: RServer[] = [];
    let state: 'initializing' | 'ready' = 'initializing';
    return (
      this.discoveryCursor
        .eachAsync(async (row) => {
          if (row.state) {
            state = row.state;
            if (row.state === 'ready') {
              this.servers
                .filter((server) => !newServers.some((s) => s === server))
                .forEach((server) => this.removeServer(server));
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
        .catch(() => new Promise((resolve) => setTimeout(resolve, 20 * 1000)))
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
      (p) => server.host === p.server.host && server.port === p.server.port,
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
          const { server } = pool;
          this.closeServerPool(pool)
            .then(
              () =>
                new Promise((resolve) =>
                  setTimeout(resolve, this.connParam.timeoutError),
                ),
            )
            .then(() => {
              if (!this.draining) {
                return this.createServerPool(server).catch(() => undefined);
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
      return pool.drain();
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
    return this.getOpenConnections().filter(
      (conn) => !(conn as RethinkDBConnection).numOfQueries,
    );
  }
}

function flat<T>(acc: T[], next: T[]) {
  return [...acc, ...next];
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
      if (isIPv6(address.host) && /^[fF]|[eE]80:.*\:.*\:/.test(address.host)) {
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
      if (isIPv6(address.host) && /^[fF]|[cCdD].*\:.*\:/.test('address.host')) {
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
    canonical_addresses: Array<{
      host: string;
      port: number;
    }>;
    cluster_port: number;
    connected_to: object;
    hostname: string;
    http_admin_port: number;
    reql_port: number;
    time_connected: Date;
  };
  process: {
    argv: string[];
    cache_size_mb: number;
    pid: number;
    time_started: Date;
    version: string;
  };
}
