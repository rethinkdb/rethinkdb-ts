import assert from 'assert';
import * as net from 'net';
import { Connection, r } from '../src';
import config from './config';
import { uuid } from './util/common';

describe('accessing-reql', () => {
  let connection: Connection; // global connection
  let dbName: string;
  let tableName: string;

  beforeEach(async () => {
    connection = await r.connect(config);
    assert(connection.open);
  });

  afterEach(async () => {
    if (!connection.open) {
      connection = await r.connect(config);
      assert(connection.open);
    }
    // remove any dbs created between each test case
    await r
      .dbList()
      .filter((db) => r.expr(['rethinkdb', 'test']).contains(db).not())
      .forEach((db) => r.dbDrop(db))
      .run(connection);
    await connection.close();
    assert(!connection.open);
  });

  it('`run` should throw an error when called without connection', async () => {
    try {
      await r.expr(1).run();
      assert.fail('shold throw an error');
    } catch (e) {
      assert.equal(
        e.message,
        '`run` was called without a connection and no pool has been created after:\nr.expr(1)\n',
      );
    }
  });

  it('`run` should throw an error when called with a closed connection', async () => {
    try {
      connection.close();
      assert(!connection.open);

      await r.expr(1).run(connection);
      assert.fail('should throw an error');
    } catch (e) {
      assert.equal(
        e.message,
        '`run` was called with a closed connection after:\nr.expr(1)\n',
      );
    }
  });

  // tslint:disable-next-line:max-line-length
  it('should be able to create a db, a table, insert array into table, delete array from table, drop table and drop db', async () => {
    dbName = uuid();
    tableName = uuid();

    assert(connection.open);
    const result1 = await r.dbCreate(dbName).run(connection);
    assert.equal(result1.config_changes.length, 1);
    assert.equal(result1.dbs_created, 1);

    const result2 = await r.db(dbName).tableCreate(tableName).run(connection);
    assert.equal(result2.tables_created, 1);

    const result3 = await r
      .db(dbName)
      .table(tableName)
      .insert(new Array(100).fill({}))
      .run(connection);
    assert.equal(result3.inserted, 100);

    const result4 = await r
      .db(dbName)
      .table(tableName)
      .delete()
      .run(connection);
    assert.equal(result4.deleted, 100);

    const result5 = await r.db(dbName).tableDrop(tableName).run(connection);
    assert.equal(result5.config_changes.length, 1);
    assert.equal(result5.tables_dropped, 1);

    const result6 = await r.dbDrop(dbName).run(connection);
    assert.equal(result6.config_changes.length, 1);
    assert.equal(result6.dbs_dropped, 1);
  });

  it('`run` should use the default database', async () => {
    dbName = uuid();
    tableName = uuid();

    const result1 = await r.dbCreate(dbName).run(connection);
    assert.equal(result1.dbs_created, 1);

    const result2 = await r.db(dbName).tableCreate(tableName).run(connection);
    assert.equal(result2.tables_created, 1);

    await connection.close();
    assert(!connection.open);

    connection = await r.connect({
      db: dbName,
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection);

    const result = await r.tableList().run(connection);
    assert.deepEqual(result, [tableName]);
  });

  it('`use` should work', async () => {
    dbName = uuid();
    tableName = uuid();

    const result1 = await r.dbCreate(dbName).run(connection);
    assert.equal(result1.dbs_created, 1);

    const result2 = await r.db(dbName).tableCreate(tableName).run(connection);
    assert.equal(result2.tables_created, 1);

    connection.use(dbName);

    const result3 = await r.tableList().run(connection);
    assert.deepEqual(result3, [tableName]);
  });

  it('`reconnect` should work', async () => {
    await connection.close();
    assert(!connection.open);

    connection = await connection.reconnect();
    assert(connection.open);
  });

  it('`reconnect` should work with options', async () => {
    assert(connection.open);
    connection = await connection.reconnect({ noreplyWait: true });
    assert(connection.open);

    const result1 = await r.expr(1).run(connection);
    assert.equal(result1, 1);

    connection = await connection.reconnect({ noreplyWait: false });
    assert(connection.open);

    const result2 = await r.expr(1).run(connection);
    assert.equal(result2, 1);

    connection = await connection.reconnect();
    assert(connection);

    const result3 = await r.expr(1).run(connection);
    assert.equal(result3, 1);
  });

  it('`noReplyWait` should throw', async () => {
    try {
      // @ts-ignore
      await connection.noReplyWait();
      assert.fail('should throw an error');
    } catch (e) {
      assert.equal(e.message, 'connection.noReplyWait is not a function');
    }
  });

  it('`noreplyWait` should work', async () => {
    dbName = uuid();
    tableName = uuid();
    const largeishObject = Array(10000)
      .fill(Math.random())
      .map((random) => r.expr({ random }));

    await r.dbCreate(dbName).run(connection);
    await r.db(dbName).tableCreate(tableName).run(connection);

    const result1 = await r
      .db(dbName)
      .table(tableName)
      .insert(largeishObject)
      .run(connection, { noreply: true });
    assert.equal(result1, undefined);

    const result2 = await r.db(dbName).table(tableName).count().run(connection);
    assert.equal(result2, 0);

    const result3 = await connection.noreplyWait();
    assert.equal(result3, undefined);

    const result4 = await r.db(dbName).table(tableName).count().run(connection);
    assert.equal(result4, 10000);
  });

  it('`run` should take an argument', async () => {
    // @ts-ignore
    const result1 = await r.expr(1).run(connection, { readMode: 'primary' });
    assert.equal(result1, 1);

    const result2 = await r.expr(1).run(connection, { readMode: 'majority' });
    assert.equal(result2, 1);

    const result3 = await r.expr(1).run(connection, { profile: false });
    assert.equal(result3, 1);

    const result4 = await r.expr(1).run(connection, { profile: true });
    assert(result4.profile);
    assert.equal(result4.result, 1);

    const result5 = await r.expr(1).run(connection, { durability: 'soft' });
    assert.equal(result5, 1);

    const result6 = await r.expr(1).run(connection, { durability: 'hard' });
    assert.equal(result6, 1);
  });

  it('`run` should throw on an unrecognized argument', async () => {
    try {
      // @ts-ignore
      await r.expr(1).run(connection, { foo: 'bar' });
      assert.fail('should throw an error');
    } catch (e) {
      assert.equal(
        e.message,
        'Unrecognized global optional argument `foo` in:\nr.expr(1)\n^^^^^^^^^\n',
      );
    }
  });

  it('`r()` should be a shortcut for r.expr()', async () => {
    const result = await r(1).run(connection);
    assert.deepEqual(result, 1);
  });

  it('`timeFormat` should work', async () => {
    const result1 = await r.now().run(connection);
    assert(result1 instanceof Date);

    const result2 = await r.now().run(connection, { timeFormat: 'native' });
    assert(result2 instanceof Date);

    const result3 = await r.now().run(connection, { timeFormat: 'raw' });
    // @ts-ignore
    assert.equal(result3.$reql_type$, 'TIME');

    const result4 = await r
      .expr({ date: r.now() })
      .run(connection, { timeFormat: 'raw' });
    // @ts-ignore
    assert.equal(result4.date.$reql_type$, 'TIME');
  });

  it('`binaryFormat` should work', async () => {
    const result1 = await r
      .binary(Buffer.from([1, 2, 3]))
      .run(connection, { binaryFormat: 'raw' });
    // @ts-ignore
    assert.equal(result1.$reql_type$, 'BINARY');

    const result2 = await r
      .expr({ binary: r.binary(Buffer.from([3, 2, 1])) })
      .run(connection, { binaryFormat: 'raw' });
    // @ts-ignore
    assert.equal(result2.binary.$reql_type$, 'BINARY');
  });

  it('`groupFormat` should work', async () => {
    const result = await r
      .expr([
        { name: 'Michel', grownUp: true },
        { name: 'Laurent', grownUp: true },
        { name: 'Sophie', grownUp: true },
        { name: 'Luke', grownUp: false },
        { name: 'Mino', grownUp: false },
      ])
      .group('grownUp')
      .run(connection, { groupFormat: 'raw' });

    assert.deepEqual(result, {
      $reql_type$: 'GROUPED_DATA',
      data: [
        [
          false,
          [
            { grownUp: false, name: 'Luke' },
            { grownUp: false, name: 'Mino' },
          ],
        ],
        [
          true,
          [
            { grownUp: true, name: 'Michel' },
            { grownUp: true, name: 'Laurent' },
            { grownUp: true, name: 'Sophie' },
          ],
        ],
      ],
    });
  });

  it('`profile` should work', async () => {
    const result1 = await r.expr(true).run(connection, { profile: false });
    assert(result1);

    const result2 = await r.expr(true).run(connection, { profile: true });
    assert(result2.profile);
    assert.equal(result2.result, true);

    const result3 = await r.expr(true).run(connection, { profile: false });
    assert.equal(result3, true);
  });

  it('`timeout` option should work', async () => {
    let server: net.Server;
    let port: number;
    try {
      port = Math.floor(Math.random() * (65535 - 1025) + 1025);

      server = net.createServer().listen(port);

      connection = await r.connect({
        port,
        timeout: 1,
      });
      assert.fail('should throw an error');
    } catch (err) {
      await server.close();

      assert.equal(
        err.message,
        `Failed to connect to localhost:${port} in less than 1s.`,
      );
    }
  });

  it('`server` should work', async () => {
    const response = await connection.server();
    assert(typeof response.name === 'string');
    assert(typeof response.id === 'string');
  });

  it('`grant` should work', async () => {
    const restrictedDbName = uuid();
    const restrictedTableName = uuid();

    const result1 = await r.dbCreate(restrictedDbName).run(connection);
    assert.equal(result1.config_changes.length, 1);
    assert.equal(result1.dbs_created, 1);

    const result2 = await r
      .db(restrictedDbName)
      .tableCreate(restrictedTableName)
      .run(connection);
    assert.equal(result2.tables_created, 1);

    const user = uuid();
    const password = uuid();
    const result3 = await r
      .db('rethinkdb')
      .table('users')
      .insert({
        id: user,
        password,
      })
      .run(connection);
    const result4 = await r
      .db(restrictedDbName)
      .table(restrictedTableName)
      .grant(user, {
        read: true,
        write: true,
        config: true,
      })
      .run(connection);
    assert.deepEqual(result4, {
      granted: 1,
      permissions_changes: [
        {
          new_val: {
            config: true,
            read: true,
            write: true,
          },
          old_val: null,
        },
      ],
    });
  });

  it('If `servers` is specified, it cannot be empty', async () => {
    try {
      await r.connectPool({
        servers: [],
      });
      assert.fail('should throw an error');
    } catch (e) {
      assert.equal(
        e.message,
        'If `servers` is an array, it must contain at least one server.',
      );
    }
  });

  // tslint:disable-next-line:max-line-length
  it('should not throw an error (since 1.13, the token is now stored outside the query): `connection` should extend events.Emitter and emit an error if the server failed to parse the protobuf message', async () => {
    connection.addListener('error', () => assert.fail('should not throw'));
    const result = await Array(687)
      .fill(1)
      .reduce((acc, curr) => acc.add(curr), r.expr(1))
      .run(connection);
    assert.equal(result, 688);
  });
});
