export default {
  host: process.env.WERCKER_RETHINKDB_HOST || 'localhost',
  port: parseInt(process.env.WERCKER_RETHINKDB_PORT || '', 10) || 28015,
  user: 'admin',
  password: '',
  buffer: 2,
  max: 5,
  discovery: false,
  silent: true,
};
