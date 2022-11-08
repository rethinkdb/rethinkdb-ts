import assert from 'assert';
import { r } from '../src';
import config from './config';
import { uuid } from './util/common';

describe('joins', () => {
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
      .insert([{ val: 1 }, { val: 2 }, { val: 3 }])
      .run();
    pks = result3.generated_keys;
    assert.equal(result3.inserted, 3);

    await r.db(dbName).table(tableName).indexCreate('val').run();
    const result4 = await r.db(dbName).table(tableName).indexWait('val').run();
    assert(result4);
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  it('`innerJoin` should return -- array-array', async () => {
    const result = await r
      .expr([1, 2, 3])
      .innerJoin(r.expr([1, 2, 3]), (left, right) => left.eq(right))
      .run();
    assert.deepEqual(result, [
      { left: 1, right: 1 },
      { left: 2, right: 2 },
      { left: 3, right: 3 },
    ]);
  });

  it('`innerJoin` should return -- array-stream', async () => {
    const result = await r
      .expr([1, 2, 3])
      .innerJoin(r.db(dbName).table(tableName), (left, right) =>
        left.eq(right('val')),
      )
      .run();
    assert.equal(result.length, 3);
    assert(result[0].left);
    assert(result[0].right);
    assert(result[1].left);
    assert(result[1].right);
    assert(result[2].left);
    assert(result[2].right);
  });

  it('`innerJoin` should return -- stream-stream', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .innerJoin(r.db(dbName).table(tableName), (left, right) => left.eq(right))
      .run();
    assert.equal(result.length, 3);
    assert(result[0].left);
    assert(result[0].right);
    assert(result[1].left);
    assert(result[1].right);
    assert(result[2].left);
    assert(result[2].right);
  });

  it('`innerJoin` should throw if no sequence', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).innerJoin().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`innerJoin\` takes 2 arguments, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`innerJoin` should throw if no predicate', async () => {
    try {
      // @ts-ignore
      await r
        .db(dbName)
        .table(tableName)
        .innerJoin(r.expr([1, 2, 3]))
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`innerJoin\` takes 2 arguments, 1 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`outerJoin` should return -- array-array', async () => {
    const result = await r
      .expr([1, 2, 3])
      .outerJoin(r.expr([1, 2, 3]), (left, right) => left.eq(right))
      .run();
    assert.deepEqual(result, [
      { left: 1, right: 1 },
      { left: 2, right: 2 },
      { left: 3, right: 3 },
    ]);
  });

  it('`outerJoin` should return -- array-stream - 1', async () => {
    const result = await r
      .expr([1, 2, 3, 4])
      .outerJoin(r.db(dbName).table(tableName), (left, right) =>
        left.eq(right('val')),
      )
      .run();
    assert.equal(result.length, 4);
    assert(result[0].left);
    assert(result[0].right);
    assert(result[1].left);
    assert(result[1].right);
    assert(result[2].left);
    assert(result[2].right);
  });

  it('`outerJoin` should return -- array-stream - 2', async () => {
    const result = await r
      .expr([1, 2, 3, 4])
      .outerJoin(r.db(dbName).table(tableName), (left, right) =>
        left.eq(right('val')),
      )
      .run();
    assert.equal(result.length, 4);
    assert(result[0].left);
    assert(result[0].right);
    assert(result[1].left);
    assert(result[1].right);
    assert(result[2].left);
    assert(result[2].right);
    assert(result[3].left);
    assert.equal(result[3].right, undefined);
  });

  it('`outerJoin` should return -- stream-stream', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .outerJoin(r.db(dbName).table(tableName), (left, right) => left.eq(right))
      .run();
    assert.equal(result.length, 3);
    assert(result[0].left);
    assert(result[0].right);
    assert(result[1].left);
    assert(result[1].right);
    assert(result[2].left);
    assert(result[2].right);
  });

  it('`outerJoin` should throw if no sequence', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).outerJoin().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`outerJoin\` takes 2 arguments, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`outerJoin` should throw if no predicate', async () => {
    try {
      // @ts-ignore
      await r
        .db(dbName)
        .table(tableName)
        .outerJoin(r.expr([1, 2, 3]))
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`outerJoin\` takes 2 arguments, 1 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`eqJoin` should return -- pk -- array-stream - function', async () => {
    const result = await r
      .expr(pks)
      .eqJoin((doc) => doc, r.db(dbName).table(tableName))
      .run();
    assert.equal(result.length, 3);
    assert(result[0].left);
    assert(result[0].right);
    assert(result[1].left);
    assert(result[1].right);
    assert(result[2].left);
    assert(result[2].right);
  });

  it('`eqJoin` should return -- pk -- array-stream - row => row', async () => {
    const result = await r
      .expr(pks)
      .eqJoin((row) => row, r.db(dbName).table(tableName))
      .run();
    assert.equal(result.length, 3);
    assert(result[0].left);
    assert(result[0].right);
    assert(result[1].left);
    assert(result[1].right);
    assert(result[2].left);
    assert(result[2].right);
  });

  it('`eqJoin` should return -- secondary index -- array-stream - row => row', async () => {
    const result = await r
      .expr([1, 2, 3])
      .eqJoin((row) => row, r.db(dbName).table(tableName), { index: 'val' })
      .run();
    assert.equal(result.length, 3);
    assert(result[0].left);
    assert(result[0].right);
    assert(result[1].left);
    assert(result[1].right);
    assert(result[2].left);
    assert(result[2].right);
  });

  it('`eqJoin` should throw if no argument', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).eqJoin().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`eqJoin\` takes at least 2 arguments, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`eqJoin` should throw with a non valid key', async () => {
    try {
      await r
        .expr([1, 2, 3])
        .eqJoin((row) => row, r.db(dbName).table(tableName), {
          // @ts-ignore
          nonValidKey: 'val',
        })
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.startsWith(
          'Unrecognized optional argument `non_valid_key` in',
        ),
      );
    }
  });

  it('`eqJoin` should throw if no sequence', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).eqJoin('id').run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`eqJoin\` takes at least 2 arguments, 1 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`eqJoin` should throw if too many arguments', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).eqJoin(1, 1, 1, 1, 1).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`eqJoin\` takes at most 3 arguments, 5 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`zip` should zip stuff', async () => {
    const result = await r
      .expr(pks)
      .eqJoin((doc) => doc, r.db(dbName).table(tableName))
      .zip()
      .run();
    assert.equal(result.length, 3);
    assert.equal(result[0].left, undefined);
  });
});
