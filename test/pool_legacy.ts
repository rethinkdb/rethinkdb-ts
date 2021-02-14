import assert from 'assert';
import { createRethinkdbMasterPool, r, Cursor } from '../src';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('pool legacy', () => {
  let pool: MasterConnectionPool;

  after(async () => {
    await pool.drain();
  });

  const servers = [
    {
      host: config.server.host,
      port: config.server.port,
    },
  ];
  const options = {
    db: 'test',
    max: 10,
    buffer: 2,
    user: config.options.user,
    password: config.options.password,
    discovery: false,
    silent: true,
  };

  before(async () => {
    pool = await createRethinkdbMasterPool(servers, options);
  });

  it('`createPool` should create a PoolMaster and `getPoolMaster` should return it', async () => {
    assert.ok(pool, 'expected an instance of pool master');
    assert.equal(pool.getPools().length, 1, 'expected number of pools is 1');
  });

  it('The pool should create a buffer', async () => {
    const result = await new Promise((resolve, reject) => {
      setTimeout(() => {
        const numConnections = pool.getAvailableLength();
        numConnections >= options.buffer
          ? resolve(numConnections)
          : reject(
              new Error(
                'expected number of connections to equal option.buffer within 250 msecs',
              ),
            );
      }, 50);
    });
    assert.equal(
      options.buffer,
      result,
      'expected buffer option to result in number of created connections',
    );
  });

  it('`run` should work without a connection if a pool exists and the pool should keep a buffer', async () => {
    const numExpr = 5;

    const result1 = await Promise.all(
      Array(numExpr)
        .fill(r.expr(1))
        .map((expr) => pool.run(expr)),
    );
    assert.deepEqual(result1, Array(numExpr).fill(1));
    await new Promise((resolve) => setTimeout(resolve, 200));
    const numConnections = pool.getAvailableLength();
    assert.ok(
      numConnections >= options.buffer + numExpr,
      'expected number of connections to be at least buffer size plus number of run expressions',
    );
  });

  it('A noreply query should release the connection kek', async () => {
    pool = await createRethinkdbMasterPool(servers, options);
    const numConnections = pool.getLength();
    await pool.run(r.expr(1), { noreply: true });
    assert.equal(
      numConnections,
      pool.getLength(),
      'expected number of connections be equal before and after a noreply query',
    );
  });

  it('The pool should not have more than `options.max` connections', async () => {
    let result = [];
    for (let i = 0; i <= options.max; i++) {
      result.push(pool.run(r.expr(1)));
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    result = await Promise.all(result);
    assert.deepEqual(result, Array(options.max + 1).fill(1));
    assert.equal(pool.getLength(), options.max);
    assert.ok(
      pool.getAvailableLength() <= options.max,
      'available connections more than max',
    );
    assert.equal(
      pool.getAvailableLength(),
      pool.getLength(),
      'expected available connections to equal pool size',
    );
  });

  it('The pool should shrink if a connection is not used for some time', async () => {
    pool.setOptions({ timeoutGb: 100 });

    const result = await Promise.all(
      Array(9)
        .fill(r.expr(1))
        .map((expr) => pool.run(expr)),
    );
    assert.deepEqual(result, Array(9).fill(1));

    const { availableLength, length } = await new Promise<{
      availableLength: number;
      length: number;
    }>((resolve) => {
      setTimeout(
        () =>
          resolve({
            availableLength: pool.getAvailableLength(),
            length: pool.getLength(),
          }),
        1000,
      );
    });
    assert.equal(
      availableLength,
      options.buffer,
      'expected available connections to equal buffer size',
    );
    assert.equal(
      length,
      options.buffer,
      'expected pool size to equal buffer size',
    );
  });

  it('`poolMaster.drain` should eventually remove all the connections', async () => {
    await pool.drain();

    assert.equal(pool.getAvailableLength(), 0);
    assert.equal(pool.getLength(), 0);
  });

  it('If the pool cannot create a connection, it should reject queries', async () => {
    try {
      const notARealPool = await createRethinkdbMasterPool(
        [{ host: 'notarealhost' }],
        {
          db: 'test',
          buffer: 1,
          max: 2,
          silent: true,
        },
      );
      await notARealPool.run(r.expr(1));
      if (notARealPool) {
        await notARealPool.drain();
      }
      assert.fail('should throw');
    } catch (e) {
      assert.match(e.message, /Error initializing master pool/);
    }
  });

  it('If the pool is drained, it should reject queries', async () => {
    await createRethinkdbMasterPool(
      [
        {
          port: config.server.port,
          host: config.server.host,
        },
      ],
      {
        db: 'test',
        buffer: 1,
        max: 2,
      },
    ).catch(() => undefined);
    await pool.drain();
    try {
      await pool.run(r.expr(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.startsWith(
          '`run` was called without a connection and no pool has been created after:',
        ),
      );
    } finally {
      await pool.drain();
    }
  });

  it('If the pool is draining, it should reject queries', async () => {
    await createRethinkdbMasterPool(
      [
        {
          port: config.server.port,
          host: config.server.host,
        },
      ],
      {
        db: 'test',
        buffer: 1,
        max: 2,
        silent: true,
      },
    );
    pool.drain();
    try {
      await pool.run(r.expr(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.startsWith(
          '`run` was called without a connection and no pool has been created after:',
        ),
      );
    } finally {
      await pool.drain();
    }
  });

  // it('`drain` should work in case of failures', async function () {
  //   await createRethinkdbMasterPool({ buffer: 1, max: 2, silent: true });
  //   r.createPools({
  //     port: 80, // non valid port
  //     silent: true,
  //     timeoutError: 100
  //   });
  //   const pool = pool;
  //   await new Promise(function (resolve, reject) {
  //     setTimeout(resolve, 150);
  //   });
  //   pool.drain();

  //   // timeoutReconnect should have been canceled
  //   assert.equal(pool.timeoutReconnect, null);
  //   pool.options.silent = false;
  // });

  it('The pool should remove a connection if it errored', async () => {
    const localPool = await createRethinkdbMasterPool(
      [
        {
          port: config.server.port,
          host: config.server.host,
        },
      ],
      {
        db: 'test',
        buffer: 1,
        max: 2,
        silent: true,
      },
    );
    localPool.setOptions({ timeoutGb: 60 * 60 * 1000 });

    try {
      const result1 = await Promise.all(
        Array(options.max)
          .fill(r.expr(1))
          .map((expr) => localPool.run(expr)),
      );
      assert.deepEqual(result1, Array(options.max).fill(1));
    } catch (e) {
      assert.ifError(e); // This should not error anymore because since the JSON protocol was introduced.

      assert.equal(
        e.message,
        'Client is buggy (failed to deserialize protobuf)',
      );

      // We expect the connection that errored to get closed in the next second
      await new Promise<void>((resolve) => {
        setTimeout(() => {
          assert.equal(localPool.getAvailableLength(), options.max - 1);
          assert.equal(localPool.getLength(), options.max - 1);
          resolve();
        }, 1000);
      });
    } finally {
      await localPool.drain();
    }
  });

  describe('cursor', () => {
    let dbName: string;
    let tableName: string;
    // eslint-disable-next-line no-shadow
    let pool: MasterConnectionPool;

    before(async () => {
      pool = await createRethinkdbMasterPool(servers, options);
      dbName = uuid();
      tableName = uuid();

      const result1 = await pool.run(r.dbCreate(dbName));
      assert.equal(result1.dbs_created, 1);

      const result2 = await pool.run(r.db(dbName).tableCreate(tableName));
      assert.equal(result2.tables_created, 1);

      const result3 = await pool.run(
        r.db(dbName).table(tableName).insert(Array(10000).fill({})),
      );
      assert.equal(result3.inserted, 10000);

      // Making bigger documents to retrieve multiple batches
      const result4 = await pool.run(
        r.db(dbName).table(tableName).update({
          foo: uuid(),
          fooo: uuid(),
          foooo: uuid(),
          fooooo: uuid(),
          foooooo: uuid(),
          fooooooo: uuid(),
          foooooooo: uuid(),
          fooooooooo: uuid(),
          foooooooooo: uuid(),
          date: r.now(),
        }),
      );
      assert.equal(result4.replaced, 10000);
    });

    after(async () => {
      const result1 = await pool.run(r.dbDrop(dbName));
      assert.equal(result1.dbs_dropped, 1);

      await pool.drain();
    });

    it('The pool should release a connection only when the cursor has fetch everything or get closed', async () => {
      const result: Cursor[] = [];
      for (let i = 0; i < options.max; i += 1) {
        result.push(await pool.getCursor(r.db(dbName).table(tableName)));
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      assert.equal(
        result.length,
        options.max,
        'expected to get the same number of results as number of expressions',
      );
      assert.equal(
        pool.getAvailableLength(),
        0,
        'expected no available connections',
      );
      await result[0].toArray();
      assert.equal(
        pool.getAvailableLength(),
        1,
        'expected available connections',
      );
      await result[1].toArray();
      assert.equal(
        pool.getAvailableLength(),
        2,
        'expected available connections',
      );
      await result[2].close();
      assert.equal(
        pool.getAvailableLength(),
        3,
        'expected available connections',
      );
      // close the 7 next seven cursors
      await Promise.all(
        [...Array(7).keys()].map((key) => {
          return result[key + 3].close();
        }),
      );
      assert.equal(
        pool.getAvailableLength(),
        options.max,
        'expected available connections to equal option.max',
      );
    });
  });
});
