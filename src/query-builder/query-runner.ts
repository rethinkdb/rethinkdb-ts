import { bracket, funcall, termConfig } from './query-config';
import { TermJson } from '../internal-types';
import { RethinkDBConnection } from '../connection/connection';
import { RCursor, RethinkDBErrorType, RunOptions } from '../types';
import { r } from './r';
import { MasterConnectionPool } from '../connection/master-pool';
import { RethinkDBError } from '../error/error';
import { backtraceTerm } from '../error/term-backtrace';
import { querySymbol, RunnableRQuery, termBuilder } from './query';

const doTermFunc = (termQuery: any) => {
  return (...args: any[]) => {
    const last = args.pop();
    const tb = termBuilder(funcall, toQuery);
    return last ? tb(last, termQuery, ...args) : tb(termQuery);
  };
};

const runQueryFunc = (term: TermJson) => {
  return async (
    conn?: RethinkDBConnection | RunOptions,
    options?: RunOptions,
  ): Promise<any | void> => {
    const c = conn instanceof RethinkDBConnection ? conn : undefined;
    const cpool = r.getPoolMaster() as MasterConnectionPool;
    const opt = conn instanceof RethinkDBConnection ? options : conn;
    if (!c && (!cpool || cpool.draining)) {
      throw new RethinkDBError(
        '`run` was called without a connection and no pool has been created after:',
        { term, type: RethinkDBErrorType.API_FAIL },
      );
    }
    const cursor = c ? await c.query(term, opt) : await cpool.queue(term, opt);
    if (cursor) {
      const results = await cursor.resolve();
      if (results) {
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
    }
  };
};

const getCursorQueryFunc = (term: TermJson) => {
  return async (
    conn?: RethinkDBConnection | RunOptions,
    options?: RunOptions,
  ): Promise<RCursor | undefined> => {
    const c = conn instanceof RethinkDBConnection ? conn : undefined;
    const cpool = r.getPoolMaster() as MasterConnectionPool;
    const opt = conn instanceof RethinkDBConnection ? options : conn;
    if (!c && (!cpool || cpool.draining)) {
      throw new RethinkDBError(
        '`getCursor` was called without a connection and no pool has been created after:',
        { term, type: RethinkDBErrorType.API_FAIL },
      );
    }
    const cursor = c ? await c.query(term, opt) : await cpool.queue(term, opt);
    if (cursor) {
      cursor.init();
      // @ts-ignore
      return cursor;
    }
  };
};

export function toQuery(term: TermJson): RunnableRQuery {
  const query: any = termBuilder(bracket, toQuery, term);
  query.term = term;

  query[querySymbol] = true;

  query.toString = () => backtraceTerm(term)[0];
  query.run = runQueryFunc(term);
  query.getCursor = getCursorQueryFunc(term);
  query.do = doTermFunc(query);

  for (let i = 0; i < termConfig.length; i += 1) {
    const config = termConfig[i];
    query[config[1]] = termBuilder(config, toQuery, term);
  }
  return query;
}
