import assert from 'assert';
import { connectPool, r } from '../src';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('pool legacy', () => {
  let dbName: string;
  let tableName: string;
  let pks: string[];
  let pool: MasterConnectionPool;

  before(async () => {
    pool = await connectPool([config.server], config.options);

    dbName = uuid();
    tableName = uuid();

    const result1 = await pool.run(r.dbCreate(dbName));
    assert.equal(result1.dbs_created, 1);

    const result2 = await pool.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result2.tables_created, 1);

    const result3 = await pool.run(
      r.db(dbName).table(tableName).insert(Array(100).fill({})),
    );
    assert.equal(result3.inserted, 100);
    pks = result3.generated_keys;
  });

  after(async () => {
    await pool.drain();
  });

  it('`db` should work', async () => {
    const result = await pool.run(r.db(dbName).info());
    assert.equal(result.name, dbName);
    assert.equal(result.type, 'DB');
  });

  it('`table` should work', async () => {
    const result1 = await pool.run(r.db(dbName).table(tableName).info());
    assert.equal(result1.name, tableName);
    assert.equal(result1.type, 'TABLE');
    assert.equal(result1.primary_key, 'id');
    assert.equal(result1.db.name, dbName);

    const result2 = await pool.run(r.db(dbName).table(tableName));
    assert.equal(result2.length, 100);
  });

  it('`table` should work with readMode', async () => {
    const result1 = await pool.run(
      r.db(dbName).table(tableName, { readMode: 'majority' }),
    );
    assert.equal(result1.length, 100);

    const result2 = await pool.run(
      r.db(dbName).table(tableName, { readMode: 'majority' }),
    );
    assert.equal(result2.length, 100);
  });

  it('`table` should throw with non valid otpions', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          // @ts-ignore
          .table(tableName, { nonValidKey: false }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.startsWith(
          'Unrecognized optional argument `non_valid_key` in:',
        ),
      );
    }
  });

  it('`get` should work', async () => {
    const result = await pool.run(r.db(dbName).table(tableName).get(pks[0]));
    assert.deepEqual(result, { id: pks[0] });
  });

  it('`get` should throw if no argument is passed', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).get());
      assert.fail('should throw');
    } catch (e) {
      // assert(e instanceof r.Error.ReqlDriverError)
      assert(e instanceof Error);
      assert.equal(
        e.message,
        `\`get\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`getAll` should work with multiple values - primary key', async () => {
    let table = r.db(dbName).table(tableName);
    let query = table.getAll.apply(table, pks);
    let result = await pool.run(query);
    assert.equal(result.length, 100);

    table = r.db(dbName).table(tableName);
    query = table.getAll.apply(table, pks.slice(0, 50));
    result = await pool.run(query);
    assert.equal(result.length, 50);
  });

  it('`getAll` should work with no argument - primary key', async () => {
    // @ts-ignore
    const result = await pool.run(r.db(dbName).table(tableName).getAll());
    assert.equal(result.length, 0);
  });

  it('`getAll` should work with no argument - index', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).getAll({ index: 'id' }),
    );
    assert.equal(result.length, 0);
  });

  it('`getAll` should work with multiple values - secondary index 1', async () => {
    const result1 = await pool.run(
      r.db(dbName).table(tableName).update({ field: 0 }),
    );
    assert.equal(result1.replaced, 100);
    const result2 = await pool.run(
      r.db(dbName).table(tableName).sample(20).update({ field: 10 }),
    );
    assert.equal(result2.replaced, 20);

    const result3 = await pool.run(
      r.db(dbName).table(tableName).indexCreate('field'),
    );
    assert.deepEqual(result3, { created: 1 });

    const result4 = await pool.run(
      r.db(dbName).table(tableName).indexWait('field').pluck('index', 'ready'),
    );
    assert.deepEqual(result4, [{ index: 'field', ready: true }]);

    const result5 = await pool.run(
      r.db(dbName).table(tableName).getAll(10, { index: 'field' }),
    );
    assert(result5);
    assert.equal(result5.length, 20);
  });

  it('`getAll` should return native dates (and cursor should handle them)', async () => {
    await pool.run(
      r.db(dbName).table(tableName).insert({ field: -1, date: r.now() }),
    );
    const result1 = await pool.run(
      r.db(dbName).table(tableName).getAll(-1, { index: 'field' }),
    );
    assert(result1[0].date instanceof Date);
    // Clean for later
    await pool.run(
      r.db(dbName).table(tableName).getAll(-1, { index: 'field' }).delete(),
    );
  });

  it('`getAll` should work with multiple values - secondary index 2', async () => {
    const result1 = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .indexCreate('fieldAddOne', (doc) => doc('field').add(1)),
    );
    assert.deepEqual(result1, { created: 1 });

    const result2 = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .indexWait('fieldAddOne')
        .pluck('index', 'ready'),
    );
    assert.deepEqual(result2, [{ index: 'fieldAddOne', ready: true }]);

    const result3 = await pool.run(
      r.db(dbName).table(tableName).getAll(11, { index: 'fieldAddOne' }),
    );
    assert(result3);
    assert.equal(result3.length, 20);
  });

  it('`between` should wrok -- secondary index', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).between(5, 20, { index: 'fieldAddOne' }),
    );
    assert(result);
    assert.equal(result.length, 20);
  });

  it('`between` should wrok -- all args', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).between(5, 20, {
        index: 'fieldAddOne',
        leftBound: 'open',
        rightBound: 'closed',
      }),
    );
    assert(result);
    assert.equal(result.length, 20);
  });

  it('`between` should throw if no argument is passed', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).between());
      assert.fail('should throw');
    } catch (e) {
      // assert(e instanceof r.Error.ReqlDriverError)
      assert(e instanceof Error);
      assert.equal(
        e.message,
        `\`between\` takes at least 2 arguments, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`between` should throw if non valid arg', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .between(1, 2, { nonValidKey: true }),
      );
      assert.fail('should throw');
    } catch (e) {
      // assert(e instanceof r.Error.ReqlDriverError)
      assert(e instanceof Error);
      assert(
        e.message.startsWith(
          'Unrecognized optional argument `non_valid_key` in:',
        ),
      );
    }
  });

  it('`filter` should work -- with an object', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).filter({ field: 10 }),
    );
    assert(result);
    assert.equal(result.length, 20);
  });

  it('`filter` should work -- with an object -- looking for an undefined field', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).filter({ nonExistingField: 10 }),
    );
    assert(result);
    assert.equal(result.length, 0);
  });

  it('`filter` should work -- with an anonymous function', async () => {
    const result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .filter((doc) => doc('field').eq(10)),
    );
    assert(result);
    assert.equal(result.length, 20);
  });

  it('`filter` should work -- default true', async () => {
    const result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .filter({ nonExistingField: 10 }, { default: true }),
    );
    assert(result);
    assert.equal(result.length, 100);
  });

  it('`filter` should work -- default false', async () => {
    const result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .filter({ nonExistingField: 10 }, { default: false }),
    );
    assert(result);
    assert.equal(result.length, 0);
  });

  it('`filter` should work -- default false', async () => {
    try {
      await pool.run(
        r.expr([{ a: 1 }, {}]).filter(r.row('a'), { default: r.error() }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^No attribute `a` in object:/));
    }
  });

  it('`filter` should throw if no argument is passed', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).filter());
      assert.fail('should throw');
    } catch (e) {
      // assert(e instanceof r.Error.ReqlDriverError)
      assert(e instanceof Error);
      assert.equal(
        e.message,
        `\`filter\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`filter` should throw with a non valid option', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .filter(() => true, { nonValidKey: false }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.startsWith(
          'Unrecognized optional argument `non_valid_key` in:',
        ),
      );
    }
  });
});
