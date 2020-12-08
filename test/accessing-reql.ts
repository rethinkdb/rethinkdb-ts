import assert from 'assert';
import * as net from 'net';
import {
  createRethinkdbConnection,
  createRethinkdbMasterPool,
  r,
} from '../src';
import config from './config';
import { uuid } from './util/common';
import { RethinkDBConnection } from '../src/connection/connection';

describe('accessing-reql', () => {
  let connection: RethinkDBConnection; // global connection
  let dbName: string;
  let tableName: string;

  beforeEach(async () => {
    connection = await createRethinkdbConnection(config);
    assert(connection.open);
  });

  afterEach(async () => {
    if (!connection.open) {
      connection = await createRethinkdbConnection(config);
      assert(connection.open);
    }
    // remove any dbs created between each test case
    await connection.run(
      r
        .dbList()
        .filter((db) => r.expr(['rethinkdb', 'test']).contains(db).not())
        .forEach((db) => r.dbDrop(db)),
    );
    await connection.close();
    assert(!connection.open);
  });

  it('`run` should throw an error when called with a closed connection', async () => {
    try {
      connection.close();
      assert(!connection.open);

      await connection.run(1);
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
    const result1 = await connection.run(r.dbCreate(dbName));
    assert.equal(result1.config_changes.length, 1);
    assert.equal(result1.dbs_created, 1);

    const result2 = await connection.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result2.tables_created, 1);

    const result3 = await connection.run(
      r.db(dbName).table(tableName).insert(new Array(100).fill({})),
    );
    assert.equal(result3.inserted, 100);

    const result4 = await connection.run(
      r.db(dbName).table(tableName).delete(),
    );
    assert.equal(result4.deleted, 100);

    const result5 = await connection.run(r.db(dbName).tableDrop(tableName));
    assert.equal(result5.config_changes.length, 1);
    assert.equal(result5.tables_dropped, 1);

    const result6 = await connection.run(r.dbDrop(dbName));
    assert.equal(result6.config_changes.length, 1);
    assert.equal(result6.dbs_dropped, 1);
  });

  it('`run` should use the default database', async () => {
    dbName = uuid();
    tableName = uuid();

    const result1 = await connection.run(r.dbCreate(dbName));
    assert.equal(result1.dbs_created, 1);

    const result2 = await connection.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result2.tables_created, 1);

    await connection.close();
    assert(!connection.open);

    connection = await createRethinkdbConnection({
      db: dbName,
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection);

    const result = await connection.run(r.tableList());
    assert.deepEqual(result, [tableName]);
  });

  it('`use` should work', async () => {
    dbName = uuid();
    tableName = uuid();

    const result1 = await connection.run(r.dbCreate(dbName));
    assert.equal(result1.dbs_created, 1);

    const result2 = await connection.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result2.tables_created, 1);

    connection.use(dbName);

    const result3 = await connection.run(r.tableList());
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

    const result1 = await connection.run(r.expr(1));
    assert.equal(result1, 1);

    connection = await connection.reconnect({ noreplyWait: false });
    assert(connection.open);

    const result2 = await connection.run(r.expr(1));
    assert.equal(result2, 1);

    connection = await connection.reconnect();
    assert(connection);

    const result3 = await connection.run(r.expr(1));
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

    await connection.run(r.dbCreate(dbName));
    await connection.run(r.db(dbName).tableCreate(tableName));

    const result1 = await connection.run(
      r.db(dbName).table(tableName).insert(largeishObject),
    );
    assert.equal(result1, undefined);

    const result2 = await connection.run(r.db(dbName).table(tableName).count());
    assert.equal(result2, 0);

    const result3 = await connection.noreplyWait();
    assert.equal(result3, undefined);

    const result4 = await connection.run(r.db(dbName).table(tableName).count());
    assert.equal(result4, 10000);
  });

  it('`run` should take an argument', async () => {
    // @ts-ignore
    const result1 = await connection.run(r.expr(1), { readMode: 'primary' });
    assert.equal(result1, 1);

    const result2 = await connection.run(r.expr(1), { readMode: 'majority' });
    assert.equal(result2, 1);

    const result3 = await connection.run(r.expr(1), { profile: false });
    assert.equal(result3, 1);

    const result4 = await connection.run(r.expr(1), { profile: true });
    assert(result4.profile);
    assert.equal(result4.result, 1);

    const result5 = await connection.run(r.expr(1), { durability: 'soft' });
    assert.equal(result5, 1);

    const result6 = await connection.run(r.expr(1), { durability: 'hard' });
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
    const result = await connection.run(r(1));
    assert.deepEqual(result, 1);
  });

  it('`timeFormat` should work', async () => {
    const result1 = await connection.run(r.now());
    assert(result1 instanceof Date);

    const result2 = await connection.run(r.now(), { timeFormat: 'native' });
    assert(result2 instanceof Date);

    const result3 = await connection.run(r.now(), { timeFormat: 'raw' });
    // @ts-ignore
    assert.equal(result3.$reql_type$, 'TIME');
  });

  it('`binaryFormat` should work', async () => {
    const result = await connection.run(r.binary(Buffer.from([1, 2, 3])), {
      binaryFormat: 'raw',
    });
    // @ts-ignore
    assert.equal(result.$reql_type$, 'BINARY');
  });

  it('`groupFormat` should work', async () => {
    const result = await connection.run(
      r
        .expr([
          { name: 'Michel', grownUp: true },
          { name: 'Laurent', grownUp: true },
          { name: 'Sophie', grownUp: true },
          { name: 'Luke', grownUp: false },
          { name: 'Mino', grownUp: false },
        ])
        .group('grownUp'),
      { groupFormat: 'raw' },
    );

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
    const result1 = await connection.run(r.expr(true), { profile: false });
    assert(result1);

    const result2 = await connection.run(r.expr(true), { profile: true });
    assert(result2.profile);
    assert.equal(result2.result, true);

    const result3 = await connection.run(r.expr(true), { profile: false });
    assert.equal(result3, true);
  });

  it('`timeout` option should work', async () => {
    let server: net.Server;
    let port: number;
    try {
      port = Math.floor(Math.random() * (65535 - 1025) + 1025);

      server = net.createServer().listen(port);

      connection = await createRethinkdbConnection({
        port,
        timeout: 1,
      });
      assert.fail('should throw an error');
    } catch (err) {
      await server.close();

      assert.equal(
        err.message,
        'Failed to connect to localhost:' + port + ' in less than 1s.',
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

    const result1 = await connection.run(r.dbCreate(restrictedDbName));
    assert.equal(result1.config_changes.length, 1);
    assert.equal(result1.dbs_created, 1);

    const result2 = await connection.run(
      r.db(restrictedDbName).tableCreate(restrictedTableName),
    );
    assert.equal(result2.tables_created, 1);

    const user = uuid();
    const password = uuid();
    const result3 = await connection.run(
      r.db('rethinkdb').table('users').insert({
        id: user,
        password,
      }),
    );
    const result4 = await connection.run(
      r.db(restrictedDbName).table(restrictedTableName).grant(user, {
        read: true,
        write: true,
        config: true,
      }),
    );
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
      await createRethinkdbMasterPool({
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
