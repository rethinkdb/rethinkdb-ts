import { EventEmitter } from 'events';
import { connect as netConnect, Socket } from 'net';
import { connect as tlsConnect } from 'tls';
import { RethinkDBError, RethinkDBErrorType } from '../error';
import type { QueryJson, ResponseJson } from '../types';
import type { RethinkDBServerConnectionOptions } from './types';
import { QueryType, ResponseType } from '../proto/enums';
import { DataQueue } from './data-queue';
import {
  buildAuthBuffer,
  compareDigest,
  computeSaltedPassword,
  NULL_BUFFER,
  validateVersion,
} from './handshake-utils';
import { isNativeError } from '../util';

// FIXME reduce number of types, this is excess
export type RethinkDBServerConnectionParsedOptions =
  RethinkDBServerConnectionOptions & {
    host: string;
    port: number;
  };

export function setConnectionDefaults(
  connectionOptions: RethinkDBServerConnectionOptions,
): RethinkDBServerConnectionParsedOptions {
  return {
    ...connectionOptions,
    host: connectionOptions.host || 'localhost',
    port: connectionOptions.port || 28015,
  };
}
export type RethinkDBSocketStatuses =
  | 'errored'
  | 'closed'
  | 'handshake'
  | 'open';

export class RethinkDBSocket extends EventEmitter {
  public connectionOptions: RethinkDBServerConnectionParsedOptions;

  public readonly user: string;

  public readonly password: Buffer;

  public lastError?: Error;

  public get status(): RethinkDBSocketStatuses {
    if (this.lastError) {
      return 'errored';
    }
    if (!this.isOpen) {
      return 'closed';
    }
    if (this.mode === 'handshake') {
      return 'handshake';
    }
    return 'open';
  }

  public socket?: Socket;

  public runningQueries = new Map<
    number,
    {
      query: QueryJson;
      data: DataQueue<ResponseJson | Error>;
    }
  >();

  private isOpen = false;

  private nextToken = 0;

  private buffer = Buffer.alloc(0);

  private mode: 'handshake' | 'response' = 'handshake';

  constructor(
    connectionOptions: RethinkDBServerConnectionOptions,
    user = 'admin',
    password = '',
  ) {
    super();
    this.connectionOptions = setConnectionDefaults(connectionOptions);
    this.user = user;
    this.password = password ? Buffer.from(password) : NULL_BUFFER;
  }

  public async connect(): Promise<void> {
    if (this.socket) {
      throw new RethinkDBError('Socket already connected', {
        type: RethinkDBErrorType.CONNECTION,
      });
    }
    const { tls = false, ...options } = this.connectionOptions;
    try {
      const socket = await new Promise<Socket>((resolve, reject) => {
        const s = tls ? tlsConnect(options) : netConnect(options);
        s.once('connect', () => resolve(s)).once('error', reject);
      });
      socket.removeAllListeners();
      socket
        .on('close', () => this.close())
        .on('end', () => this.close())
        .on('error', (error) => this.handleError(error))
        .on('data', (data) => {
          try {
            this.buffer = Buffer.concat([this.buffer, data]);
            switch (this.mode) {
              case 'handshake':
                this.handleHandshakeData();
                break;
              case 'response':
                this.handleData();
                break;
              default:
                break;
            }
          } catch (error: any) {
            this.handleError(error);
          }
        });
      socket.setKeepAlive(true);
      this.socket = socket;
      await new Promise<void>((resolve, reject) => {
        socket.once('connect', resolve);
        socket.once('error', reject);
        if (socket.destroyed) {
          socket.removeListener('connect', resolve);
          socket.removeListener('error', reject);
          reject(this.lastError);
        } else if (!socket.connecting) {
          socket.removeListener('connect', resolve);
          socket.removeListener('error', reject);
          resolve();
        }
      });
      this.isOpen = true;
      this.lastError = undefined;
      await this.performHandshake();
      this.emit('connect');
    } catch (error: any) {
      this.handleError(error);
    }
  }

  // eslint-disable-next-line no-plusplus
  public sendQuery(newQuery: QueryJson, token?: number) {
    if (!this.socket || this.status !== 'open') {
      throw new RethinkDBError(
        '`run` was called with a closed connection after:',
        { term: newQuery[1], type: RethinkDBErrorType.CONNECTION },
      );
    }

    if (token === undefined) {
      token = this.nextToken++;
    }

    const encoded = JSON.stringify(newQuery);
    const querySize = Buffer.byteLength(encoded);
    const buffer = Buffer.alloc(8 + 4 + querySize);
    // eslint-disable-next-line no-bitwise
    buffer.writeUInt32LE(token & 0xffffffff, 0);
    buffer.writeUInt32LE(Math.floor(token / 0xffffffff), 4);
    buffer.writeUInt32LE(querySize, 8);
    buffer.write(encoded, 12);
    const { noreply = false } = newQuery[2] || {};
    if (noreply) {
      this.socket.write(buffer);
      this.emit('query', token);
      return token;
    }
    const [type] = newQuery;
    const { query = newQuery, data = null } =
      this.runningQueries.get(token) || {};
    if (type === QueryType.STOP) {
      this.socket.write(buffer);
      if (data) {
        // Resolving and not rejecting so there won't be "unhandled rejection" if nobody listens
        data.destroy(
          new RethinkDBError('Query cancelled', {
            term: query[1],
            type: RethinkDBErrorType.CANCEL,
          }),
        );
        this.runningQueries.delete(token);
        this.emit('release', this.runningQueries.size);
      }
      return token;
    }
    if (!data) {
      this.runningQueries.set(token, { data: new DataQueue(), query });
    }
    this.socket.write(buffer);
    this.emit('query', token);
    return token;
  }

  public stopQuery(token: number) {
    if (this.runningQueries.has(token)) {
      this.sendQuery([QueryType.STOP], token);
    }
  }

  public continueQuery(token: number) {
    if (this.runningQueries.has(token)) {
      this.sendQuery([QueryType.CONTINUE], token);
    }
  }

  public async readNext<T = any>(token: number): Promise<T | ResponseJson> {
    if (!this.isOpen) {
      throw (
        this.lastError ||
        new RethinkDBError(
          'The connection was closed before the query could be completed',
          {
            type: RethinkDBErrorType.CONNECTION,
          },
        )
      );
    }
    if (!this.runningQueries.has(token)) {
      throw new RethinkDBError('No more rows in the cursor.', {
        type: RethinkDBErrorType.CURSOR_END,
      });
    }
    const { data = null } = this.runningQueries.get(token) || {};
    if (!data) {
      throw new RethinkDBError('Query is not running.', {
        type: RethinkDBErrorType.CURSOR,
      });
    }
    const res = await data.dequeue();
    if (isNativeError(res)) {
      data.destroy(res);
      this.runningQueries.delete(token);
      throw res;
    } else if (this.status === 'handshake') {
      this.runningQueries.delete(token);
    } else if (res.t !== ResponseType.SUCCESS_PARTIAL) {
      this.runningQueries.delete(token);
      this.emit('release', this.runningQueries.size);
    }
    return res;
  }

  public close(error?: Error) {
    // eslint-disable-next-line no-restricted-syntax
    for (const { data, query } of this.runningQueries.values()) {
      data.destroy(
        new RethinkDBError(
          'The connection was closed before the query could be completed',
          {
            term: query[1],
            type: RethinkDBErrorType.CONNECTION,
          },
        ),
      );
    }
    this.runningQueries.clear();
    if (!this.socket) {
      return;
    }
    this.socket.removeAllListeners();
    this.socket.destroy();
    this.socket = undefined;
    this.isOpen = false;
    this.mode = 'handshake';
    this.emit('close', error);
    this.removeAllListeners();
    this.nextToken = 0;
  }

  private async performHandshake() {
    if (!this.socket || this.status !== 'handshake') {
      throw new RethinkDBError('Connection is not open', {
        type: RethinkDBErrorType.CONNECTION,
      });
    }
    const { randomString, authBuffer } = buildAuthBuffer(this.user);

    this.socket.write(authBuffer);

    const query: QueryJson = [QueryType.START];
    this.runningQueries.set(0, { data: new DataQueue(), query });
    this.runningQueries.set(1, { data: new DataQueue(), query });
    validateVersion(await this.readNext<any>(0));
    const { authentication } = await this.readNext(1);

    const { serverSignature, proof } = await computeSaltedPassword(
      authentication,
      randomString,
      this.user,
      this.password,
    );

    this.socket.write(proof);
    this.runningQueries.set(2, { data: new DataQueue(), query });
    const { authentication: returnedSignature } = await this.readNext(2);
    compareDigest(returnedSignature, serverSignature);
    this.mode = 'response';
  }

  private handleHandshakeData() {
    let index = -1;
    // eslint-disable-next-line no-cond-assign
    while ((index = this.buffer.indexOf(0)) >= 0) {
      const strMsg = this.buffer.subarray(0, index).toString('utf8');
      // eslint-disable-next-line no-plusplus
      const { data = null } = this.runningQueries.get(this.nextToken++) || {};
      let error: RethinkDBError | undefined;
      try {
        const jsonMsg = JSON.parse(strMsg);
        if (jsonMsg.success) {
          if (data) {
            data.enqueue(jsonMsg);
          }
        } else {
          error = new RethinkDBError(jsonMsg.error, {
            errorCode: jsonMsg.error_code,
          });
        }
      } catch (cause: any) {
        error = new RethinkDBError(strMsg, {
          cause,
          type: RethinkDBErrorType.AUTH,
        });
      }
      if (error) {
        if (data) {
          data.destroy(error);
        }
        this.handleError(error);
      }
      this.buffer = this.buffer.subarray(index + 1);
      index = this.buffer.indexOf(0);
    }
  }

  private handleData() {
    while (this.buffer.length >= 12) {
      const token =
        this.buffer.readUInt32LE(0) + 0x100000000 * this.buffer.readUInt32LE(4);
      const responseLength = this.buffer.readUInt32LE(8);

      if (this.buffer.length < 12 + responseLength) {
        break;
      }

      const responseBuffer = this.buffer.subarray(12, 12 + responseLength);
      const response: ResponseJson = JSON.parse(
        responseBuffer.toString('utf8'),
      );
      this.buffer = this.buffer.subarray(12 + responseLength);
      const { data = null } = this.runningQueries.get(token) || {};
      if (data) {
        data.enqueue(response);
      }
    }
  }

  private handleError(error: Error) {
    this.close(error);
    this.lastError = error;
    if (this.listenerCount('error') > 0) {
      this.emit('error', error);
    }
  }
}
