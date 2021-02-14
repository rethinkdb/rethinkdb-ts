import assert from 'assert';
import { createRethinkdbMasterPool, r } from '../src';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('manipulating databases', () => {
  let pool: MasterConnectionPool;

  before(async () => {
    pool = await createRethinkdbMasterPool([config.server], config.options);
  });

  after(async () => {
    await pool.drain();
  });

  it('`expr` should work', async () => {
    const result = await pool.run(r.expr(1));
    assert.equal(result, 1);
  });

  it('`dbList` should return a cursor', async () => {
    const result = await pool.run(r.dbList());
    assert(Array.isArray(result));
  });

  it('`dbCreate` should create a database', async () => {
    const dbName = uuid(); // export to the global scope

    const result = await pool.run(r.dbCreate(dbName));
    assert.equal(result.dbs_created, 1);
  });

  it('`dbCreate` should throw if no argument is given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.dbCreate());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.dbCreate` takes 1 argument, 0 provided.');
    }
  });

  it('`dbCreate` is not defined after a term', async () => {
    try {
      await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .dbCreate('foo'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.dbCreate is not a function'));
    }
  });

  it('`dbCreate` is not defined after a term', async () => {
    try {
      await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .db('foo'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.db is not a function'));
    }
  });

  it('`db` should throw is the name contains special char', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db('*_*'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(
          /Database name `\*_\*` invalid \(Use A-Z, a-z, 0-9, _ and - only\)/,
        ),
      );
    }
  });

  it('`dbList` should show the database we created', async () => {
    const dbName = uuid(); // export to the global scope

    const result1 = await pool.run(r.dbCreate(dbName));
    assert.equal(result1.dbs_created, 1);

    const result2 = await pool.run(r.dbList());
    assert(Array.isArray(result2));
    assert(result2.find((name) => name === dbName) !== undefined);
  });

  it('`dbDrop` should drop a table', async () => {
    const dbName = uuid(); // export to the global scope

    let result = await pool.run(r.dbCreate(dbName));
    assert.equal(result.dbs_created, 1);

    result = await pool.run(r.dbDrop(dbName));
    assert.deepEqual(result.dbs_dropped, 1);
  });

  it('`dbDrop` should throw if given too many arguments', async () => {
    try {
      // @ts-ignore
      await pool.run(r.dbDrop('foo', 'bar', 'ette'));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.dbDrop` takes 1 argument, 3 provided.');
    }
  });

  it('`dbDrop` should throw if no argument is given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.dbDrop());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.dbDrop` takes 1 argument, 0 provided.');
    }
  });

  it('`dbDrop` is not defined after a term', async () => {
    try {
      await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .dbDrop('foo'),
      );
    } catch (e) {
      assert(e.message.endsWith('.dbDrop is not a function'));
    }
  });

  it('`dbList` is not defined after a term', async () => {
    try {
      await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .dbList('foo'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.dbList is not a function'));
    }
  });

  it('`dbList` should contain dropped databases', async () => {
    const dbName = uuid(); // export to the global scope

    const result1 = await pool.run(r.dbCreate(dbName));
    assert.equal(result1.dbs_created, 1);

    const result2 = await pool.run(r.dbDrop(dbName));
    assert.deepEqual(result2.dbs_dropped, 1);
    await new Promise((resolve) => setTimeout(resolve, 200));

    const result3 = await pool.run(r.dbList());
    assert(Array.isArray(result3));
    assert(result3.find((name) => name === dbName) === undefined);
  });
});
