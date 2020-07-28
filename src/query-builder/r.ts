import { RethinkDBConnection } from '../connection/connection';
import { MasterConnectionPool } from '../connection/master-pool';
import { RethinkDBError } from '../error/error';
import { TermType } from '../proto/enums';
import {
  R,
  RConnectionOptions,
  RethinkDBErrorType,
  RPoolConnectionOptions,
  RQuery,
} from '../types';
import { globals } from './globals';
import { toQuery } from './query';
import { funcall, rConfig, rConsts, termConfig } from './query-config';
import { expr, termBuilder } from './term-builder';

export const r: R = expr as any;
r.connectPool = async (options: RPoolConnectionOptions = {}) => {
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
  if ((r as any).pool) {
    ((r as any).pool as MasterConnectionPool).removeAllListeners();
    ((r as any).pool as MasterConnectionPool).drain();
  }
  const cpool = new MasterConnectionPool({
    ...options,
    servers,
  } as any);
  (r as any).pool = cpool;
  cpool.initServers().catch(() => undefined);
  return waitForHealthy ? await cpool.waitForHealthy() : cpool;
};

r.connect = async (options: RConnectionOptions = {}) => {
  const { host, port, server = { host, port } } = options;
  if ((host || port) && options.server) {
    throw new RethinkDBError(
      'If `host` or `port` are defined `server` must not be.',
      { type: RethinkDBErrorType.API_FAIL },
    );
  }
  const c = new RethinkDBConnection(server, options as any);
  await c.reconnect();
  return c;
};
r.getPoolMaster = () => (r as any).pool;
r.waitForHealthy = () => {
  if ((r as any).pool) {
    return (r as any).pool.waitForHealthy();
  }
  throw new RethinkDBError('Pool not initialized', {
    type: RethinkDBErrorType.MASTER_POOL_FAIL,
  });
};
r.setNestingLevel = (level: number) => (globals.nestingLevel = level);
r.setArrayLimit = (limit?: number) => (globals.arrayLimit = limit);
r.serialize = (termStr: RQuery) => JSON.stringify((termStr as any).term);
r.deserialize = (termStr: string) => toQuery(validateTerm(JSON.parse(termStr)));
r.expr = expr;
r.do = (...args: any[]) => {
  const last = args.pop();
  return termBuilder(funcall)(last, ...args);
};
rConfig.forEach((config) => ((r as any)[config[1]] = termBuilder(config)));
rConsts.forEach(([type, name]) => ((r as any)[name] = toQuery([type])));
termConfig
  .filter(([_, name]) => !(name in r))
  .forEach(
    ([type, name, minArgs, maxArgs, optArgs]) =>
      ((r as any)[name] = termBuilder([
        type,
        name,
        minArgs + 1,
        maxArgs === -1 ? maxArgs : maxArgs + 1,
        optArgs,
      ])),
  );

function validateTerm(term: any): any {
  if (term === undefined) {
    throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
  }
  if (typeof term === 'function') {
    throw new RethinkDBError(`Invalid term:\n${term.toString()}\n`);
  }
  if (typeof term === 'object') {
    if (Array.isArray(term)) {
      if (term.length > 3) {
        throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
      }
      const [func, args, options] = term;
      if (typeof func !== 'number' || TermType[func] === undefined) {
        throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
      }
      if (args !== undefined) {
        if (!Array.isArray(args)) {
          throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
        }
        if (!args.every((arg) => validateTerm(arg))) {
          throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
        }
      }
      if (
          options !== undefined &&
        !Object.values(term).every((value) => validateTerm(value))
      ) {
        throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
      }
    } else if (!Object.values(term).every((value) => validateTerm(value))) {
      throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
    }
  }
  return term;
}
