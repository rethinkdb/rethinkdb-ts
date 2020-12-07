import { Readable } from 'stream';
import { RethinkDBSocket } from '../connection/socket';
import { isRethinkDBError, RethinkDBError } from '../error/error';
import { QueryJson, ResponseJson } from '../internal-types';
import { ResponseNote, ResponseType } from '../proto/enums';
import { RCursor, RCursorType, RethinkDBErrorType, RunOptions } from '../types';
import { getNativeTypes } from './response-parser';

function isPromise(obj: any): obj is Promise<any> {
  return (
    obj !== null && typeof obj === 'object' && typeof obj.then === 'function'
  );
}

export class Cursor extends Readable implements RCursor {
  public get profile() {
    return this._profile;
  }

  // tslint:disable-next-line:variable-name
  private _profile: any;

  private position = 0;

  private type: RCursorType = 'Cursor';

  private includeStates = false;

  private closed = false;

  private emitting = false;

  private resolving: Promise<any> | undefined;

  private lastError: Error | undefined;

  constructor(
    private conn: RethinkDBSocket,
    private token: number,
    private runOptions: Pick<
      RunOptions,
      'binaryFormat' | 'groupFormat' | 'timeFormat'
    >,
    private query: QueryJson,
    private results?: any[],
    private hasNextBatch?: boolean,
  ) {
    super({ objectMode: true });
  }

  public init() {
    this.resolving = this.resolve().catch((err) => (this.lastError = err));
  }

  public _read() {
    this.emitting = true;
    const push = (row: any): any => {
      if (row === null) {
        this._next().then(push);
      } else {
        this.push(row);
      }
    };
    this._next()
      .then(push)
      .catch((err) => {
        if (
          (!isRethinkDBError(err) ||
            ![
              RethinkDBErrorType.CURSOR_END,
              RethinkDBErrorType.CANCEL,
            ].includes(err.type)) &&
          this.listenerCount('error') > 0
        ) {
          this.emit('error', err);
        }
        this.push(null);
      });
  }

  public pause() {
    this.emitting = false;
    return super.pause();
  }

  public resume() {
    this._read();
    return super.resume();
  }

  public destroy() {
    super.destroy();
  }

  public _destroy() {
    this.close();
  }

  public toString() {
    return `[object ${this.type}]`;
  }

  public getType() {
    return this.type;
  }

  public async close() {
    if (!this.closed) {
      if (this.conn.status === 'open') {
        this.conn.stopQuery(this.token);
      }
      this.emitting = false;
      this.closed = true;
    }
  }

  public async next() {
    if (this.emitting) {
      throw new RethinkDBError(
        'You cannot call `next` once you have bound listeners on the Feed.',
        { type: RethinkDBErrorType.CURSOR },
      );
    }
    if (this.closed) {
      throw new RethinkDBError(
        `You cannot call \`next\` on a closed ${this.type}`,
        { type: RethinkDBErrorType.CURSOR },
      );
    }
    return this._next();
  }

  public async toArray() {
    if (this.emitting) {
      throw new RethinkDBError(
        'You cannot call `toArray` once you have bound listeners on the Feed.',
        { type: RethinkDBErrorType.CURSOR },
      );
    }
    const all: any[] = [];
    return this.eachAsync(async (row) => {
      if (this.type.endsWith('Feed')) {
        throw new RethinkDBError(
          'You cannot call `toArray` on a change Feed.',
          {
            type: RethinkDBErrorType.CURSOR,
          },
        );
      }
      all.push(row);
    }).then(() => all);
  }

  public async each(
    callback: (err: RethinkDBError | undefined, row?: any) => boolean,
    onFinishedCallback?: () => any,
  ) {
    if (this.emitting) {
      throw new RethinkDBError(
        'You cannot call `each` once you have bound listeners on the Feed.',
        { type: RethinkDBErrorType.CURSOR },
      );
    }
    if (this.closed) {
      callback(
        new RethinkDBError(
          'You cannot retrieve data from a cursor that is closed',
          { type: RethinkDBErrorType.CURSOR },
        ),
      );
      if (onFinishedCallback) {
        onFinishedCallback();
      }
      return;
    }
    let resume = true;
    let err: RethinkDBError | undefined;
    let next: any;
    while (resume !== false && !this.closed) {
      err = undefined;
      try {
        next = await this.next();
      } catch (error) {
        err = error;
      }
      if (err && err.type === RethinkDBErrorType.CURSOR_END) {
        break;
      }
      resume = callback(err, next);
    }
    if (onFinishedCallback) {
      onFinishedCallback();
    }
  }

  public async eachAsync(
    rowHandler: (row: any, rowFinished?: (error?: string) => any) => any,
    final?: (error: any) => any,
  ) {
    if (this.emitting) {
      throw new RethinkDBError(
        'You cannot call `eachAsync` once you have bound listeners on the Feed.',
        { type: RethinkDBErrorType.CURSOR },
      );
    }
    if (this.closed) {
      throw new RethinkDBError(
        'You cannot retrieve data from a cursor that is closed',
        { type: RethinkDBErrorType.CURSOR },
      );
    }
    let nextRow: any;
    try {
      while (!this.closed) {
        nextRow = await this.next();
        if (rowHandler.length > 1) {
          await new Promise((resolve, reject) => {
            rowHandler(nextRow, (err) =>
              err
                ? reject(
                    new RethinkDBError(err, { type: RethinkDBErrorType.USER }),
                  )
                : resolve(),
            );
          });
        } else {
          const result = rowHandler(nextRow);
          if (result !== undefined && !isPromise(result)) {
            throw result;
          }
          await result;
        }
      }
    } catch (error) {
      if (final) {
        try {
          await final(error);
          return;
        } catch (err) {
          error = err;
        }
      }
      if (
        !isRethinkDBError(error) ||
        ![RethinkDBErrorType.CURSOR_END, RethinkDBErrorType.CANCEL].includes(
          error.type,
        )
      ) {
        throw error;
      }
    }
  }

  public async resolve() {
    try {
      const response = await this.conn.readNext(this.token);
      const { n: notes, t: type, r: results, p: profile } = response;
      this._profile = profile;
      this.position = 0;
      this.results = getNativeTypes(results, this.runOptions);
      this.handleResponseNotes(type, notes);
      this.handleErrors(response);
      this.hasNextBatch = type === ResponseType.SUCCESS_PARTIAL;
      return this.results;
    } catch (error) {
      this.emitting = false;
      this.closed = true;
      this.results = undefined;
      this.hasNextBatch = false;
      throw error;
    }
  }

  public [Symbol.asyncIterator](): AsyncIterableIterator<any> {
    return {
      next: async () => {
        if (this.closed) {
          return { done: true, value: undefined };
        }
        try {
          const value = await this.next();
          return { value, done: false };
        } catch (error) {
          if (
            isRethinkDBError(error) &&
            error.type === RethinkDBErrorType.CANCEL
          ) {
            return { done: true, value: undefined };
          }
          throw error;
        }
      },
    } as AsyncIterableIterator<any>;
  }

  private async _next() {
    if (this.lastError) {
      this.emitting = false;
      this.closed = true;
      this.results = undefined;
      this.hasNextBatch = false;
      throw this.lastError;
    }
    try {
      if (this.resolving) {
        await this.resolving;
        this.resolving = undefined;
      }
      let results = this.getResults();
      let next = results && results[this.position];
      while (next === undefined && this.hasNextBatch) {
        if (!this.resolving) {
          this.resolving = this.resolve();
          this.conn.continueQuery(this.token);
        }
        await this.resolving;
        this.resolving = undefined;
        results = this.getResults();
        next = results && results[this.position];
      }
      if (!this.hasNextBatch && next === undefined) {
        throw new RethinkDBError('No more rows in the cursor.', {
          type: RethinkDBErrorType.CURSOR_END,
        });
      }
      this.position += 1;
      return next;
    } catch (error) {
      this.closed = true;
      throw error;
    }
  }

  private getResults() {
    return this.results &&
      this.type === 'Atom' &&
      Array.isArray(this.results[0])
      ? this.results[0]
      : this.results;
  }

  private handleErrors(response: ResponseJson) {
    const { t: type, b: backtrace, r: results, e: error } = response;
    switch (type) {
      case ResponseType.CLIENT_ERROR:
      case ResponseType.COMPILE_ERROR:
      case ResponseType.RUNTIME_ERROR:
        throw new RethinkDBError(results[0], {
          responseErrorType: error,
          responseType: type,
          query: this.query,
          backtrace,
        });
      case ResponseType.SUCCESS_ATOM:
      case ResponseType.SUCCESS_PARTIAL:
      case ResponseType.SUCCESS_SEQUENCE:
        break;
      default:
        throw new RethinkDBError('Unexpected return value');
    }
  }

  private handleResponseNotes(rType: ResponseType, notes: ResponseNote[] = []) {
    if (rType === ResponseType.SUCCESS_ATOM) {
      this.includeStates = false;
      this.type = 'Atom';
      return;
    }
    const { type, includeStates } = notes.reduce(
      (acc, next) => {
        switch (next) {
          case ResponseNote.SEQUENCE_FEED:
            acc.type = 'Feed';
            break;
          case ResponseNote.ATOM_FEED:
            acc.type = 'AtomFeed';
            break;
          case ResponseNote.ORDER_BY_LIMIT_FEED:
            acc.type = 'OrderByLimitFeed';
            break;
          case ResponseNote.UNIONED_FEED:
            acc.type = 'UnionedFeed';
            break;
          case ResponseNote.INCLUDES_STATES:
            acc.includeStates = true;
        }
        return acc;
      },
      { type: 'Cursor' as RCursorType, includeStates: true },
    );
    this.type = type;
    this.includeStates = includeStates;
  }
}

export function isCursor<T = any>(cursor: any): cursor is RCursor<T> {
  return cursor instanceof Cursor;
}
