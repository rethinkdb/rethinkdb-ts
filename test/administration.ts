import assert from 'assert';
import { connectPool, r } from '../src';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('administration', () => {
  let dbName: string;
  let tableName: string;
  let result: any;
  let pool: MasterConnectionPool;

  before(async () => {
    pool = await connectPool([config.server], config.options);

    dbName = uuid();
    tableName = uuid();

    result = await pool.run(r.dbCreate(dbName));
    assert.equal(result.dbs_created, 1);

    result = await pool.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result.tables_created, 1);

    result = await pool.run(
      r.db(dbName).table(tableName).insert(Array(100).fill({})),
    );
    assert.equal(result.inserted, 100);
    assert.equal(result.generated_keys.length, 100);
  });

  after(async () => {
    await pool.drain();
  });

  it('`config` should work', async () => {
    result = await pool.run(r.db(dbName).config());
    assert.equal(result.name, dbName);

    result = await pool.run(r.db(dbName).table(tableName).config());
    assert.equal(result.name, tableName);
  });

  it('`config` should throw if called with an argument', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).config('hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`config` takes 0 arguments, 1 provided after:/));
    }
  });

  it('`status` should work', async () => {
    result = await pool.run(r.db(dbName).table(tableName).status());
    assert.equal(result.name, tableName);
    assert.notEqual(result.status, undefined);
  });

  it('`status` should throw if called with an argument', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).status('hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`status` takes 0 arguments, 1 provided after:/));
    }
  });

  it('`wait` should work', async () => {
    result = await pool.run(r.db(dbName).table(tableName).wait());
    assert.equal(result.ready, 1);
  });

  it('`wait` should work with options', async () => {
    result = await pool.run(
      r.db(dbName).table(tableName).wait({ waitFor: 'ready_for_writes' }),
    );
    assert.equal(result.ready, 1);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .wait({ waitFor: 'ready_for_writes', timeout: 2000 }),
    );
    assert.equal(result.ready, 1);
  });

  it('`r.wait` should throw', async () => {
    try {
      // @ts-ignore
      await pool.run(r.wait());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.wait` takes at least 1 argument, 0 provided.',
      );
    }
  });

  it('`wait` should throw if called with 2 arguments', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).wait('hello', 'world'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`wait` takes at most 1 argument, 2 provided after:/),
      );
    }
  });

  it('`reconfigure` should work - 1', async () => {
    result = await pool.run(
      r.db(dbName).table(tableName).reconfigure({ shards: 1, replicas: 1 }),
    );
    assert.equal(result.reconfigured, 1);
  });

  it('`reconfigure` should work - 2 - dryRun', async () => {
    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .reconfigure({ shards: 1, replicas: 1, dryRun: true }),
    );
    assert.equal(result.reconfigured, 0);
  });

  it('`r.reconfigure` should throw', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.reconfigure());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.reconfigure` takes 2 arguments, 0 provided.');
    }
  });

  it('`reconfigure` should throw on an unrecognized key', async () => {
    try {
      result = await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .reconfigure({ foo: 1 }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.startsWith('Unrecognized optional argument `foo` in:'));
    }
  });

  it('`reconfigure` should throw on a number', async () => {
    try {
      result = await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .reconfigure(1),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^First argument of `reconfigure` must be an object./),
      );
    }
  });

  it('`rebalanced` should work - 1', async () => {
    result = await pool.run(r.db(dbName).table(tableName).rebalance());
    assert.equal(result.rebalanced, 1);
  });

  it('`r.rebalance` should throw', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.rebalance());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.rebalance` takes 1 argument, 0 provided.');
    }
  });

  it('`rebalance` should throw if an argument is provided', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.db(dbName).table(tableName).rebalance(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`rebalance` takes 0 arguments, 1 provided after:/),
      );
    }
  });
});
