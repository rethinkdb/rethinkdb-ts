import assert from 'assert';
import { r } from '../src';
import config from './config';
import { uuid } from './util/common';

describe('pool legacy', () => {
  let dbName: string;
  let tableName: string;
  let pks: string[];

  before(async () => {
    await r.connectPool(config);

    dbName = uuid();
    tableName = uuid();

    const result1 = await r.dbCreate(dbName).run();
    assert.equal(result1.dbs_created, 1);

    const result2 = await r.db(dbName).tableCreate(tableName).run();
    assert.equal(result2.tables_created, 1);

    const result3 = await r
      .db(dbName)
      .table(tableName)
      .insert(Array(100).fill({}))
      .run();
    assert.equal(result3.inserted, 100);
    pks = result3.generated_keys;
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  it('`db` should work', async () => {
    const result = await r.db(dbName).info().run();
    assert.equal(result.name, dbName);
    assert.equal(result.type, 'DB');
  });

  it('`table` should work', async () => {
    const result1 = await r.db(dbName).table(tableName).info().run();
    assert.equal(result1.name, tableName);
    assert.equal(result1.type, 'TABLE');
    assert.equal(result1.primary_key, 'id');
    assert.equal(result1.db.name, dbName);

    const result2 = await r.db(dbName).table(tableName).run();
    assert.equal(result2.length, 100);
  });

  it('`table` should work with readMode', async () => {
    const result1 = await r
      .db(dbName)
      .table(tableName, { readMode: 'majority' })
      .run();
    assert.equal(result1.length, 100);

    const result2 = await r
      .db(dbName)
      .table(tableName, { readMode: 'majority' })
      .run();
    assert.equal(result2.length, 100);
  });

  it('`table` should throw with non valid otpions', async () => {
    try {
      await r
        .db(dbName)
        // @ts-ignore
        .table(tableName, { nonValidKey: false })
        .run();
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
    const result = await r.db(dbName).table(tableName).get(pks[0]).run();
    assert.deepEqual(result, { id: pks[0] });
  });

  it('`get` should throw if no argument is passed', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).get().run();
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
    let result = await query.run();
    assert.equal(result.length, 100);

    table = r.db(dbName).table(tableName);
    query = table.getAll.apply(table, pks.slice(0, 50));
    result = await query.run();
    assert.equal(result.length, 50);
  });

  it('`getAll` should work with no argument - primary key', async () => {
    // @ts-ignore
    const result = await r.db(dbName).table(tableName).getAll().run();
    assert.equal(result.length, 0);
  });

  it('`getAll` should work with no argument - index', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .getAll({ index: 'id' })
      .run();
    assert.equal(result.length, 0);
  });

  it('`getAll` should work with multiple values - secondary index 1', async () => {
    const result1 = await r
      .db(dbName)
      .table(tableName)
      .update({ field: 0 })
      .run();
    assert.equal(result1.replaced, 100);
    const result2 = await r
      .db(dbName)
      .table(tableName)
      .sample(20)
      .update({ field: 10 })
      .run();
    assert.equal(result2.replaced, 20);

    const result3 = await r
      .db(dbName)
      .table(tableName)
      .indexCreate('field')
      .run();
    assert.deepEqual(result3, { created: 1 });

    const result4 = await r
      .db(dbName)
      .table(tableName)
      .indexWait('field')
      .pluck('index', 'ready')
      .run();
    assert.deepEqual(result4, [{ index: 'field', ready: true }]);

    const result5 = await r
      .db(dbName)
      .table(tableName)
      .getAll(10, { index: 'field' })
      .run();
    assert(result5);
    assert.equal(result5.length, 20);
  });

  it('`getAll` should return native dates (and cursor should handle them)', async () => {
    await r
      .db(dbName)
      .table(tableName)
      .insert({ field: -1, date: r.now() })
      .run();
    const result1 = await r
      .db(dbName)
      .table(tableName)
      .getAll(-1, { index: 'field' })
      .run();
    assert(result1[0].date instanceof Date);
    // Clean for later
    await r
      .db(dbName)
      .table(tableName)
      .getAll(-1, { index: 'field' })
      .delete()
      .run();
  });

  it('`getAll` should work with multiple values - secondary index 2', async () => {
    const result1 = await r
      .db(dbName)
      .table(tableName)
      .indexCreate('fieldAddOne', (doc) => doc('field').add(1))
      .run();
    assert.deepEqual(result1, { created: 1 });

    const result2 = await r
      .db(dbName)
      .table(tableName)
      .indexWait('fieldAddOne')
      .pluck('index', 'ready')
      .run();
    assert.deepEqual(result2, [{ index: 'fieldAddOne', ready: true }]);

    const result3 = await r
      .db(dbName)
      .table(tableName)
      .getAll(11, { index: 'fieldAddOne' })
      .run();
    assert(result3);
    assert.equal(result3.length, 20);
  });

  it('`between` should wrok -- secondary index', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .between(5, 20, { index: 'fieldAddOne' })
      .run();
    assert(result);
    assert.equal(result.length, 20);
  });

  it('`between` should wrok -- all args', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .between(5, 20, {
        index: 'fieldAddOne',
        leftBound: 'open',
        rightBound: 'closed',
      })
      .run();
    assert(result);
    assert.equal(result.length, 20);
  });

  it('`between` should throw if no argument is passed', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).between().run();
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
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .between(1, 2, { nonValidKey: true })
        .run();
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
    const result = await r
      .db(dbName)
      .table(tableName)
      .filter({ field: 10 })
      .run();
    assert(result);
    assert.equal(result.length, 20);
  });

  it('`filter` should work -- with an object -- looking for an undefined field', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .filter({ nonExistingField: 10 })
      .run();
    assert(result);
    assert.equal(result.length, 0);
  });

  it('`filter` should work -- with an anonymous function', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .filter((doc) => doc('field').eq(10))
      .run();
    assert(result);
    assert.equal(result.length, 20);
  });

  it('`filter` should work -- default true', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .filter({ nonExistingField: 10 }, { default: true })
      .run();
    assert(result);
    assert.equal(result.length, 100);
  });

  it('`filter` should work -- default false', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .filter({ nonExistingField: 10 }, { default: false })
      .run();
    assert(result);
    assert.equal(result.length, 0);
  });

  it('`filter` should work -- default false', async () => {
    try {
      await r
        .expr([{ a: 1 }, {}])
        .filter(r.row('a'), { default: r.error() })
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^No attribute `a` in object:/));
    }
  });

  it('`filter` should throw if no argument is passed', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).filter().run();
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
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .filter(() => true, { nonValidKey: false })
        .run();
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
