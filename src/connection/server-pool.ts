import { EventEmitter } from 'events';
import { isRethinkDBError, RethinkDBError } from '../error/error';
import { TermJson } from '../internal-types';
import { Cursor } from '../response/cursor';
import {
  ConnectionPool,
  RConnectionOptions,
  RethinkDBErrorType,
  RServerConnectionOptions,
  RunOptions,
} from '../types';
import { RethinkDBConnection } from './connection';
import { RNConnOpts, setConnectionDefaults } from './socket';
import { delay } from '../util';

export class ServerConnectionPool
  extends EventEmitter
  implements ConnectionPool
{
  public readonly server: RNConnOpts;

  private draining = false;

  private healthy: boolean | undefined = undefined;

  private buffer: number;

  private max: number;

  private timeoutError: number;

  private timeoutGb: number;

  private maxExponent: number;

  private silent: boolean;

  private log: (message: string) => any;

  private connParam: any;

  private connections: RethinkDBConnection[] = [];

  private timers = new Map<RethinkDBConnection, NodeJS.Timer>();

  constructor(
    connectionOptions: RServerConnectionOptions,
    {
      db = 'test',
      user = 'admin',
      password = '',
      buffer = 1,
      max = 1,
      timeout = 20,
      pingInterval = -1,
      timeoutError = 1000,
      timeoutGb = 60 * 60 * 1000,
      maxExponent = 6,
      silent = false,
      log = (message: string) => undefined,
    }: RConnectionOptions = {},
  ) {
    super();
    this.buffer = Math.max(buffer, 1);
    this.max = Math.max(max, buffer);
    this.timeoutError = timeoutError;
    this.timeoutGb = timeoutGb;
    this.maxExponent = maxExponent;
    this.silent = silent;
    this.log = log;
    this.server = setConnectionDefaults(connectionOptions);
    this.connParam = { db, user, password, timeout, pingInterval, silent, log };
    this.connections = [];
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

  public async initConnections(): Promise<void> {
    if (this.connections.length < this.buffer && !this.draining) {
      await this.createConnection().then(() => this.initConnections());
    }
  }

  public get isHealthy() {
    return this.connections.some((conn) => conn.open);
  }

  public waitForHealthy(this: ServerConnectionPool) {
    return new Promise<ServerConnectionPool>((resolve, reject) => {
      if (this.isHealthy) {
        resolve(this);
      } else {
        this.once('healthy', (healthy, error) => {
          if (healthy) {
            resolve(this);
          } else {
            reject(
              new RethinkDBError('Error initializing pool', {
                type: RethinkDBErrorType.POOL_FAIL,
                cause: error,
              }),
            );
          }
        });
      }
    });
  }

  public async setOptions({
    buffer = this.buffer,
    max = this.max,
    silent = this.silent,
    log = this.log,
    timeoutError = this.timeoutError,
    timeoutGb = this.timeoutGb,
    maxExponent = this.maxExponent,
  }: RConnectionOptions) {
    this.silent = silent;
    this.log = log;
    this.timeoutError = timeoutError;
    this.timeoutGb = timeoutGb;
    this.maxExponent = maxExponent;
    if (this.buffer < buffer && this.connections.length < buffer) {
      this.buffer = buffer;
      await this.initConnections();
    } else {
      this.connections.forEach((conn) => this.checkIdle(conn));
    }
    if (this.max > max) {
      const connections = this.getIdleConnections();
      for (let i = 0; i < this.max - max; i++) {
        const conn = connections.pop();
        if (!conn) {
          break;
        }
        await this.closeConnection(conn);
      }
    }
    this.max = max;
  }

  public async drain(emit = true) {
    if (emit) {
      this.emit('draining');
      this.setHealthy(undefined);
    }
    this.draining = true;
    await Promise.all(
      this.connections.map((conn) => this.closeConnection(conn)),
    );
  }

  public getConnections() {
    return this.connections;
  }

  public getLength() {
    return this.getOpenConnections().length;
  }

  public getAvailableLength() {
    return this.getIdleConnections().length;
  }

  public getNumOfRunningQueries() {
    return this.getOpenConnections().reduce(
      (num, next) => next.numOfQueries + num,
      0,
    );
  }

  public async queue(
    term: TermJson,
    globalArgs: RunOptions = {},
  ): Promise<Cursor | undefined> {
    this.emit('queueing');
    const openConnections = this.getOpenConnections();
    if (!openConnections) {
      throw this.reportError(
        new RethinkDBError('No connections available', {
          type: RethinkDBErrorType.POOL_FAIL,
        }),
        true,
      );
    }
    const minQueriesRunningConnection = openConnections.reduce(
      (acc: RethinkDBConnection, next: RethinkDBConnection) =>
        acc.numOfQueries <= next.numOfQueries ? acc : next,
    );
    if (this.connections.length < this.max) {
      this.createConnection();
    }
    return minQueriesRunningConnection.query(term, globalArgs);
  }

  private setHealthy(healthy: boolean | undefined, error?: Error) {
    if (healthy === undefined) {
      this.healthy = undefined;
    } else if (healthy !== this.healthy && healthy !== undefined) {
      this.healthy = healthy;
      this.emit('healthy', healthy, error);
    }
  }

  private async createConnection() {
    const conn = new RethinkDBConnection(this.server, this.connParam);
    this.connections = [...this.connections, conn];
    await this.persistConnection(conn);
  }

  private subscribeToConnection(conn: RethinkDBConnection) {
    if (conn.open && !this.draining) {
      const size = this.getOpenConnections().length;
      this.emit('size', size);
      this.setHealthy(true);
      this.checkIdle(conn);
      conn
        .on('close', (error) => {
          const innerSize = this.getOpenConnections().length;
          this.emit('size', innerSize);
          if (innerSize === 0) {
            this.setHealthy(false, error);
            // if no connections are available need to remove all connections and start over
            // so it won't try to reconnect all connections at once
            // this.drain({}, false).then(() => this.initConnections());
          }
          conn.removeAllListeners();
          this.persistConnection(conn);
        })
        .on('data', () => this.checkIdle(conn))
        .on('query', () => this.checkIdle(conn));
    }
  }

  private async closeConnection(conn: RethinkDBConnection) {
    this.removeIdleTimer(conn);
    conn.removeAllListeners();
    this.connections = this.connections.filter((c) => c !== conn);
    await conn.close();
    this.emit('size', this.getOpenConnections().length);
  }

  private checkIdle(conn: RethinkDBConnection) {
    this.removeIdleTimer(conn);
    if (!conn.numOfQueries) {
      this.emit('available-size', this.getIdleConnections().length);
      this.timers.set(
        conn,
        setTimeout(() => {
          this.timers.delete(conn);
          if (this.connections.length > this.buffer) {
            this.closeConnection(conn).then(() =>
              this.emit('available-size', this.getIdleConnections().length),
            );
          }
        }, this.timeoutGb),
      );
    }
  }

  private removeIdleTimer(conn: RethinkDBConnection) {
    const timer = this.timers.get(conn);
    if (timer) {
      clearTimeout(timer);
    }
    this.timers.delete(conn);
  }

  private async persistConnection(conn: RethinkDBConnection): Promise<void> {
    let exp = 0;
    while (this.connections.includes(conn) && !conn.open && !this.draining) {
      try {
        await conn.reconnect();
      } catch (error: any) {
        this.reportError(error);
        if (this.connections.length > this.buffer) {
          // if trying to go above buffer and failing just use one of the open connections
          this.closeConnection(conn);
          break;
        }
        if (this.healthy === undefined) {
          this.setHealthy(false, error);
        }
        await delay(2 ** exp * this.timeoutError);
        exp = Math.min(exp + 1, this.maxExponent);
      }
    }
    if (!this.connections.includes(conn) || this.draining) {
      // draining/removing
      await this.closeConnection(conn);
      return;
    }
    this.subscribeToConnection(conn);
  }

  private reportError(err: Error, log = false) {
    if (this.listenerCount('error') > 0) {
      this.emit('error', err);
    }
    if (
      log &&
      (!isRethinkDBError(err) || err.type !== RethinkDBErrorType.CANCEL)
    ) {
      this.log(err.toString());
      if (!this.silent) {
        console.error(err.toString());
      }
    }
    return err;
  }

  private getOpenConnections() {
    return this.connections.filter((conn) => conn.open);
  }

  private getIdleConnections() {
    return this.getOpenConnections().filter((conn) => !conn.numOfQueries);
  }
}
