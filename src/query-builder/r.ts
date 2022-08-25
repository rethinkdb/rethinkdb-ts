import { RethinkDBConnection } from '../connection/connection';
import { MasterConnectionPool } from '../connection/master-pool';
import { RethinkDBError, RethinkDBErrorType } from '../error/error';
import {
  R,
  RConnectionOptions,
  RPoolConnectionOptions,
  RQuery,
} from '../types';
import { globals } from './globals';
import { funcall, rConfig, rConsts, termConfig } from './query-config';
import { isQuery, termBuilder } from './query';
import { toQuery } from './query-runner';
import { validateTerm } from './validate-term';
import { parseParam } from './param-parser';

const expr = (arg: any, nestingLevel: number = globals.nestingLevel) => {
  if (isQuery(arg)) {
    return arg;
  }
  return toQuery(parseParam(arg, nestingLevel));
};

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
  return waitForHealthy ? cpool.waitForHealthy() : cpool;
};

r.connect = async (options: RConnectionOptions = {}) => {
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
r.setNestingLevel = (level: number) => {
  globals.nestingLevel = level;
};
r.setArrayLimit = (limit?: number) => {
  globals.arrayLimit = limit;
};
// @ts-ignore
r.serialize = (termStr: RQuery) => JSON.stringify(termStr.term);
// @ts-ignore
r.deserialize = (termStr: string) => toQuery(validateTerm(JSON.parse(termStr)));
// @ts-ignore
r.expr = expr;
// @ts-ignore
r.do = (...args: any[]) => {
  const last = args.pop();
  return termBuilder(funcall, toQuery)(last, ...args);
};
rConfig.forEach(
  (config) => ((r as any)[config[1]] = termBuilder(config, toQuery)),
);
rConsts.forEach(([type, name]) => ((r as any)[name] = toQuery([type])));
termConfig
  .filter(([_, name]) => !(name in r))
  .forEach(
    ([type, name, minArgs, maxArgs, optArgs]) =>
      ((r as any)[name] = termBuilder(
        [
          type,
          name,
          minArgs + 1,
          maxArgs === -1 ? maxArgs : maxArgs + 1,
          optArgs,
        ],
        toQuery,
      )),
  );
