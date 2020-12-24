import { RPoolConnectionOptions } from "../src";

const config: RPoolConnectionOptions = {
  host: process.env.WERCKER_RETHINKDB_HOST || 'localhost',
  port: parseInt(process.env.WERCKER_RETHINKDB_PORT || '', 10) || 28015,
  user: 'admin',
  password: '',
  buffer: 2,
  max: 50,
  fake_server: {
    host: process.env.WERCKER_RETHINKDB_HOST || 'localhost',
    port: parseInt(process.env.WERCKER_RETHINKDB_PORT || '', 10) + 1 || 28016
  },
  discovery: false,
  silent: false,
 waitForHealthy: true
};

export default config;
