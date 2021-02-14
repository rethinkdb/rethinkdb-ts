import { EventEmitter } from 'events';
import { isRethinkDBError, RethinkDBError, RethinkDBErrorType } from '../error';
import { QueryJson, TermJson } from '../types';
import { ErrorType, QueryType, ResponseType, TermType } from '../proto/enums';
import { globals } from '../query-builder/globals';
import { parseOptarg, RQuery } from '../query-builder/query';
import { Cursor } from '../response/cursor';
import type {
  IConnectionLogger,
  RethinkdbConnectionParams,
  RethinkDBServerConnectionOptions,
  RunOptions,
  ServerInfo,
} from './types';
import { RethinkDBSocket, setConnectionDefaults } from './socket';
import { delay } from '../util';

const tableQueries = [
  TermType.TABLE_CREATE,
  TermType.TABLE_DROP,
  TermType.TABLE_LIST,
  TermType.TABLE,
];

function omitFormatOptions(
  options: RunOptions,
): Omit<RunOptions, 'timeFormat' | 'groupFormat' | 'binaryFormat'> {
  const newOptions = { ...options };

  delete newOptions.binaryFormat;
  delete newOptions.groupFormat;
  delete newOptions.timeFormat;

  return newOptions;
}

function findTableTermAndAddDb(term: TermJson, db: string): void {
  if (!Array.isArray(term)) {
    if (term !== null && typeof term === 'object') {
      // @ts-ignore
      Object.values(term).forEach((value) => findTableTermAndAddDb(value, db));
      return;
    }
    return;
  }
  const termParam = term[1];
  if (tableQueries.includes(term[0])) {
    if (!termParam) {
      // eslint-disable-next-line no-param-reassign
      term[1] = [[TermType.DB, [db]]];
      return;
    }
    const innerTerm = termParam[0];
    if (Array.isArray(innerTerm) && innerTerm[0] === TermType.DB) {
      return;
    }
    termParam.unshift([TermType.DB, [db]]);
    return;
  }
  if (termParam) {
    termParam.forEach((value) => findTableTermAndAddDb(value, db));
  }
}

export class RethinkDBConnection extends EventEmitter {
  public readonly socket: RethinkDBSocket;

  public options: RethinkDBServerConnectionOptions;

  private timeout: number;

  private pingInterval: number;

  private silent: boolean;

  private log?: IConnectionLogger;

  private pingTimer?: NodeJS.Timer;

  private db: string;

  constructor(
    private serverOptions: RethinkDBServerConnectionOptions,
    options: RethinkdbConnectionParams,
  ) {
    super();
    const {
      db,
      user = 'admin',
      password = '',
      timeout = 20,
      pingInterval = -1,
      silent = false,
      log,
    } = options;
    this.options = setConnectionDefaults(serverOptions);
    this.timeout = timeout;
    this.pingInterval = pingInterval;
    this.silent = silent;
    this.log = log;
    this.db = db;

    this.socket = new RethinkDBSocket(this.options, user, password);
  }

  public eventNames(): string[] {
    return ['release', 'close', 'timeout', 'error'];
  }

  public get open(): boolean {
    return this.socket.status === 'open';
  }

  public get numOfQueries(): number {
    return this.socket.runningQueries.size;
  }

  public async close({ noreplyWait = false } = {}): Promise<void> {
    try {
      this.stopPinging();
      if (noreplyWait) {
        await this.noreplyWait();
      }
      await this.socket.close();
    } catch (error) {
      await this.socket.close();
      throw error;
    }
  }

  public async reconnect(options?: {
    noreplyWait: boolean;
  }): Promise<RethinkDBConnection> {
    if (this.socket.status === 'open' || this.socket.status === 'handshake') {
      await this.close(options);
    }
    this.socket
      .on('connect', () => this.emit('connect'))
      .on('close', (error) => {
        this.close();
        this.emit('close', error);
      })
      .on('error', (error) => {
        this.reportError(error);
      })
      .on('data', (data, token) => this.emit(data, token))
      .on('release', (count) => {
        if (count === 0) {
          this.emit('release');
        }
      });
    try {
      await Promise.race([delay(this.timeout * 1000), this.socket.connect()]);
    } catch (connectionError) {
      const error = new RethinkDBError(
        'Unable to establish connection, see cause for more info.',
        {
          cause: connectionError,
          type: RethinkDBErrorType.CONNECTION,
        },
      );
      this.reportError(error);
      this.emit('close', error);
      this.close();
      throw error;
    }
    if (this.socket.status === 'errored') {
      if (this.socket.lastError) {
        this.reportError(this.socket.lastError);
      }
      this.emit('close', this.socket.lastError);
      this.close();
      throw this.socket.lastError;
    }
    if (this.socket.status !== 'open') {
      const error = new RethinkDBError(
        `Failed to connect to ${this.options.host}:${this.options.port} in less than ${this.timeout}s.`,
        { type: RethinkDBErrorType.TIMEOUT },
      );
      this.emit('timeout');
      this.emit('close', error);
      this.close().catch(() => undefined);
      throw error;
    }
    this.startPinging();
    return this;
  }

  public use(db: string): void {
    this.db = db;
  }

  public async noreplyWait(): Promise<void> {
    const token = this.socket.sendQuery([QueryType.NOREPLY_WAIT]);
    const result = await this.socket.readNext(token);
    if (result.t !== ResponseType.WAIT_COMPLETE) {
      if (this.socket.status === 'errored') {
        throw this.socket.lastError;
      }
      const error = new RethinkDBError('Unexpected return value');
      this.reportError(error);
      throw error;
    }
  }

  public async server(): Promise<ServerInfo> {
    const token = this.socket.sendQuery([QueryType.SERVER_INFO]);
    const result = await this.socket.readNext(token);
    if (result.t !== ResponseType.SERVER_INFO) {
      if (this.socket.status === 'errored') {
        throw this.socket.lastError;
      }
      const error = new RethinkDBError('Unexpected return value');
      this.reportError(error);
      throw error;
    }
    return result.r[0];
  }

  public async query(
    term: TermJson,
    options: RunOptions = {},
  ): Promise<Cursor | undefined> {
    const rest = omitFormatOptions(options);
    rest.db = rest.db || this.db;
    findTableTermAndAddDb(term, rest.db);
    if (globals.arrayLimit !== undefined && rest.arrayLimit === undefined) {
      rest.arrayLimit = globals.arrayLimit;
    }
    const jsonQuery: QueryJson = [QueryType.START, term];
    const optionalArgs = parseOptarg(omitFormatOptions(rest));
    if (optionalArgs) {
      jsonQuery.push(optionalArgs);
    }
    const token = this.socket.sendQuery(jsonQuery);
    if (options.noreply) {
      return undefined;
    }
    return new Cursor(this.socket, token, options, jsonQuery);
  }

  private startPinging() {
    if (this.pingInterval > 0) {
      this.pingTimer = setTimeout(async () => {
        try {
          if (this.socket.status === 'open') {
            const token = this.socket.sendQuery([
              QueryType.START,
              [TermType.ERROR, ['ping']],
            ]);
            const result = await this.socket.readNext(token);
            if (
              result.t !== ResponseType.RUNTIME_ERROR ||
              result.e !== ErrorType.USER ||
              result.r[0] !== 'ping'
            ) {
              this.reportError(
                new RethinkDBError('Ping error', { responseType: result.t }),
              );
            }
          }
        } catch (e) {
          this.reportError(e);
        }
        if (this.pingTimer) {
          this.startPinging();
        }
      }, this.pingInterval);
    }
  }

  private stopPinging() {
    if (this.pingTimer) {
      clearTimeout(this.pingTimer);
    }
    this.pingTimer = undefined;
  }

  private reportError(error: Error) {
    if (this.listenerCount('error') > 0) {
      this.emit('error', error);
    }
    if (!isRethinkDBError(error) || error.type !== RethinkDBErrorType.CANCEL) {
      if (this.log) {
        this.log(error.toString());
      }
      if (!this.silent) {
        console.error(error.toString());
      }
    }
  }

  public async run(query: RQuery, options?: RunOptions): Promise<any> {
    const { term } = query;
    const cursor = await this.query(term, options);
    if (cursor) {
      const results = await cursor.resolve();
      switch (cursor.getType()) {
        case 'Atom':
          if (cursor.profile) {
            return { profile: cursor.profile, result: results[0] };
          }
          return results[0];

        case 'Cursor':
          if (cursor.profile) {
            return {
              profile: cursor.profile,
              result: await cursor.toArray(),
            };
          }
          return cursor.toArray();
        default:
          return cursor;
      }
    }
    return undefined;
  }

  public async getCursor<T = unknown>(
    query: RQuery,
    options?: RunOptions,
  ): Promise<Cursor<T>> {
    const { term } = query;
    const cursor = await this.query(term, options);
    if (!cursor) {
      throw new RethinkDBError(
        'cursor was not returned! maybe you provided "noreply" option?',
      );
    }
    cursor.init();
    return cursor;
  }
}
