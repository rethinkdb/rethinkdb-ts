import type {
  RethinkDBServerConnectionOptions,
  RethinkDBPoolConnectionOptions,
} from '../src/connection/types';

const config: {
  server: RethinkDBServerConnectionOptions;
  options: RethinkDBPoolConnectionOptions;
  fakeServer: any;
} = {
  server: {
    host: process.env.WERCKER_RETHINKDB_HOST || 'localhost',
    port: parseInt(process.env.WERCKER_RETHINKDB_PORT || '', 10) || 28015,
  },
  options: {
    db: 'test',
    user: 'admin',
    password: '',
    buffer: 2,
    max: 50,
    discovery: false,
    silent: false,
    waitForHealthy: true,
  },
  fakeServer: {
    host: process.env.WERCKER_RETHINKDB_HOST || 'localhost',
    port: parseInt(process.env.WERCKER_RETHINKDB_PORT || '', 10) + 1 || 28016,
  },
};

export default config;
