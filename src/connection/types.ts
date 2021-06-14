import { TcpNetConnectOpts } from 'net';
import { ConnectionOptions } from 'tls';
import type { Durability, Format } from '../types';

export type RethinkDBServerConnectionOptions =
  | (Partial<ConnectionOptions> & { tls: true })
  | (Partial<TcpNetConnectOpts> & { tls?: false });

export interface IConnectionLogger {
  (message: string): void;
}

export interface RethinkdbConnectionParams {
  db: string; // default 'test'
  user?: string; // default 'admin'
  password?: string; // default ''
  timeout?: number; // default = 20
  pingInterval?: number; // default -1
  silent?: boolean; // default = false
  log?: IConnectionLogger; // default undefined;
}

export type RethinkDBConnectionOptions = RethinkdbConnectionParams & {
  discovery?: boolean; // default false
  pool?: boolean; // default true
  buffer?: number; // default = number of servers
  max?: number; // default = number of servers
  timeoutError?: number; // default = 1000
  timeoutGb?: number; // default = 60*60*1000
  maxExponent?: number; // default 6
};

export type RunOptions = {
  timeFormat?: Format | 'ISO8601'; // 'native' or 'raw', default 'native'
  groupFormat?: Format; // 'native' or 'raw', default 'native'
  binaryFormat?: Format; // 'native' or 'raw', default 'native'
  useOutdated?: boolean; // default false
  profile?: boolean; // default false
  durability?: Durability; // 'hard' or 'soft'
  noreply?: boolean; // default false
  db?: string;
  arrayLimit?: number; // default 100,000
  minBatchRows?: number; // default 8
  maxBatchRows?: number;
  maxBatchRow?: number; // default unlimited
  maxBatchBytes?: number; // default 1MB
  maxBatchSeconds?: number; // default 0.5
  firstBatchScaledownFactor?: number; // default 4
  readMode?: 'single' | 'majority' | 'outdated';
};

export interface ServerInfo {
  id: string;
  name: string;
  proxy: boolean;
}
