import assert from 'assert';
import { createRethinkdbMasterPool, r } from '../src';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('document manipulation', () => {
  let dbName: string;
  let tableName: string;

  let pool: MasterConnectionPool;
  before(async () => {
    pool = await createRethinkdbMasterPool([config.server], config.options);
    dbName = uuid();
    tableName = uuid();

    const result1 = await pool.run(r.dbCreate(dbName));
    assert.equal(result1.dbs_created, 1);

    const result2 = await pool.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result2.tables_created, 1);
  });

  after(async () => {
    await pool.drain();
  });

  it('`row => row` should work - 1', async () => {
    const result = await pool.run(r.expr([1, 2, 3]).map((row) => row));
    assert.deepEqual(result, [1, 2, 3]);
  });

  it('`row => row` should work - 2', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).insert({}));
    assert.equal(result.inserted, 1);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .update((row) => ({ idCopyUpdate: row('id') })),
    );
    assert.equal(result.replaced, 1);
  });

  it('`row => row` should work - 3', async () => {
    const result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .replace((row) => row),
    );
    assert.equal(result.replaced, 0);
  });

  it('`row => row` should work - 4', async () => {
    const result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .replace((doc) => doc.merge({ idCopyReplace: doc('id') })),
    );
    assert.equal(result.replaced, 1);
  });

  it('`row => row` should work - 5', async () => {
    const result = await pool.run(r.db(dbName).table(tableName).delete());
    assert.equal(result.deleted, 1);
  });

  it('`r.row` should work - 1', async () => {
    const result = await pool.run(r.expr([1, 2, 3]).map(r.row));
    assert.deepEqual(result, [1, 2, 3]);
  });

  it('`r.row` should work - 2', async () => {
    const result1 = await pool.run(r.db(dbName).table(tableName).insert({}));
    assert.equal(result1.inserted, 1);

    const result2 = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .update({ idCopyUpdate: r.row('id') }),
    );
    console.dir(result2);
    assert.equal(result2.replaced, 1);
  });

  it('`r.row` should work - 3', async () => {
    const result = await pool.run(r.db(dbName).table(tableName).replace(r.row));
    assert.equal(result.replaced, 0);
  });
  it('`r.row` should work - 4', async () => {
    const result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .replace((doc) => doc.merge({ idCopyReplace: doc('id') })),
    );
    assert.equal(result.replaced, 1);
  });

  it('`r.row` should work - 5', async () => {
    const result = await pool.run(r.db(dbName).table(tableName).delete());
    assert.equal(result.deleted, 1);
  });

  it('`pluck` should work', async () => {
    const result1 = await pool.run(
      r.expr({ a: 0, b: 1, c: 2 }).pluck('a', 'b'),
    );
    assert.deepEqual(result1, { a: 0, b: 1 });

    const result2 = await pool.run(
      r
        .expr([
          { a: 0, b: 1, c: 2 },
          { a: 0, b: 10, c: 20 },
        ])
        .pluck('a', 'b'),
    );
    assert.deepEqual(result2, [
      { a: 0, b: 1 },
      { a: 0, b: 10 },
    ]);
  });

  it('`pluck` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r.db(dbName).table(tableName).pluck());
      assert.fail('should trow');
    } catch (e) {
      assert.equal(
        e.message,
        `\`pluck\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`without` should work', async () => {
    const result1 = await pool.run(r.expr({ a: 0, b: 1, c: 2 }).without('c'));
    assert.deepEqual(result1, { a: 0, b: 1 });

    const result2 = await pool.run(
      r
        .expr([
          { a: 0, b: 1, c: 2 },
          { a: 0, b: 10, c: 20 },
        ])
        .without('a', 'c'),
    );
    assert.deepEqual(result2, [{ b: 1 }, { b: 10 }]);
  });

  it('`without` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r.db(dbName).table(tableName).without());
    } catch (e) {
      assert.equal(
        e.message,
        `\`without\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`merge` should work', async () => {
    let result = await pool.run(r.expr({ a: 0 }).merge({ b: 1 }));
    assert.deepEqual(result, { a: 0, b: 1 });

    result = await pool.run(
      r.expr([{ a: 0 }, { a: 1 }, { a: 2 }]).merge({ b: 1 }),
    );
    assert.deepEqual(result, [
      { a: 0, b: 1 },
      { a: 1, b: 1 },
      { a: 2, b: 1 },
    ]);

    result = await pool.run(
      r
        .expr({ a: 0, c: { l: 'tt' } })
        .merge({ b: { c: { d: { e: 'fff' } }, k: 'pp' } }),
    );
    assert.deepEqual(result, {
      a: 0,
      b: { c: { d: { e: 'fff' } }, k: 'pp' },
      c: { l: 'tt' },
    });

    result = await pool.run(r.expr({ a: 1 }).merge({ date: r.now() }));
    assert.equal(result.a, 1);
    assert(result.date instanceof Date);

    result = await pool.run(
      r.expr({ a: 1 }).merge((row) => ({ nested: row }), { b: 2 }),
    );
    assert.deepEqual(result, { a: 1, nested: { a: 1 }, b: 2 });
  });

  it('`merge` should take an anonymous function', async () => {
    let result = await pool.run(
      r.expr({ a: 0 }).merge((doc) => ({ b: doc('a').add(1) })),
    );
    assert.deepEqual(result, { a: 0, b: 1 });

    result = await pool.run(
      r.expr({ a: 0 }).merge((row) => ({
        b: row('a').add(1),
      })),
    );
    assert.deepEqual(result, { a: 0, b: 1 });
  });

  it('`literal` should work', async () => {
    const result = await pool.run(
      r.expr({ a: { b: 1 } }).merge({ a: r.literal({ c: 2 }) }),
    );
    assert.deepEqual(result, { a: { c: 2 } });
  });

  it('`literal` is not defined after a term', async () => {
    try {
      await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .literal('foo'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.literal is not a function'));
    }
  });

  it('`merge` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r.db(dbName).table(tableName).merge());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`merge\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`literal` should work with no argument', async () => {
    const result = await pool.run(
      r.expr({ foo: 'bar' }).merge({ foo: r.literal() }),
    );
    assert.deepEqual(result, {});
  });

  it('`append` should work', async () => {
    const result = await pool.run(r.expr([1, 2, 3]).append(4));
    assert.deepEqual(result, [1, 2, 3, 4]);
  });

  it('`append` should throw if no argument has been passed', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .append(),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`append\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`prepend` should work', async () => {
    const result = await pool.run(r.expr([1, 2, 3]).prepend(4));
    assert.deepEqual(result, [4, 1, 2, 3]);
  });

  it('`prepend` should throw if no argument has been passed', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .prepend(),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`prepend\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`difference` should work', async () => {
    const result = await pool.run(r.expr([1, 2, 3]).prepend(4));
    assert.deepEqual(result, [4, 1, 2, 3]);
  });

  it('`difference` should throw if no argument has been passed', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .difference(),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`difference\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`setInsert` should work', async () => {
    let result = await pool.run(r.expr([1, 2, 3]).setInsert(4));
    assert.deepEqual(result, [1, 2, 3, 4]);

    result = await pool.run(r.expr([1, 2, 3]).setInsert(2));
    assert.deepEqual(result, [1, 2, 3]);
  });

  it('`setInsert` should throw if no argument has been passed', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .setInsert(),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`setInsert\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`setUnion` should work', async () => {
    const result = await pool.run(r.expr([1, 2, 3]).setUnion([2, 4]));
    assert.deepEqual(result, [1, 2, 3, 4]);
  });

  it('`setUnion` should throw if no argument has been passed', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .setUnion(),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`setUnion\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`setIntersection` should work', async () => {
    const result = await pool.run(r.expr([1, 2, 3]).setIntersection([2, 4]));
    assert.deepEqual(result, [2]);
  });

  it('`setIntersection` should throw if no argument has been passed', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .setIntersection(),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`setIntersection\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`setDifference` should work', async () => {
    const result = await pool.run(r.expr([1, 2, 3]).setDifference([2, 4]));
    assert.deepEqual(result, [1, 3]);
  });

  it('`setDifference` should throw if no argument has been passed', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .setDifference(),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`setDifference\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`getField` should work', async () => {
    let result = await pool.run(r.expr({ a: 0, b: 1 })('a'));
    assert.equal(result, 0);

    result = await pool.run(r.expr({ a: 0, b: 1 }).getField('a'));
    assert.equal(result, 0);

    result = await pool.run(r.expr([{ a: 0, b: 1 }, { a: 1 }])('a'));
    assert.deepEqual(result, [0, 1]);
  });

  it('`(...)` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName)());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`(...)\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`getField` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).getField());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`getField\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`hasFields` should work', async () => {
    const result = await pool.run(
      r
        .expr([
          { a: 0, b: 1, c: 2 },
          { a: 0, b: 10, c: 20 },
          { b: 1, c: 3 },
        ])
        .hasFields('a', 'c'),
    );
    assert.deepEqual(result, [
      { a: 0, b: 1, c: 2 },
      { a: 0, b: 10, c: 20 },
    ]);
  });

  it('`hasFields` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r.db(dbName).table(tableName).hasFields());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`hasFields\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`insertAt` should work', async () => {
    let result = await pool.run(r.expr([1, 2, 3, 4]).insertAt(0, 2));
    assert.deepEqual(result, [2, 1, 2, 3, 4]);

    result = await pool.run(r.expr([1, 2, 3, 4]).insertAt(3, 2));
    assert.deepEqual(result, [1, 2, 3, 2, 4]);
  });

  it('`insertAt` should throw if no argument has been passed', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .insertAt(),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`insertAt\` takes 2 arguments, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`spliceAt` should work', async () => {
    const result = await pool.run(r.expr([1, 2, 3, 4]).spliceAt(1, [9, 9]));
    assert.deepEqual(result, [1, 9, 9, 2, 3, 4]);
  });

  it('`spliceAt` should throw if no argument has been passed', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .spliceAt(),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`spliceAt\` takes 2 arguments, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`deleteAt` should work', async () => {
    let result = await pool.run(r.expr([1, 2, 3, 4]).deleteAt(1));
    assert.deepEqual(result, [1, 3, 4]);

    result = await pool.run(r.expr([1, 2, 3, 4]).deleteAt(1, 3));
    assert.deepEqual(result, [1, 4]);
  });

  it('`deleteAt` should throw if no argument has been passed', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .deleteAt(),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`deleteAt\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`deleteAt` should throw if too many arguments', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .deleteAt(1, 1, 1, 1),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`deleteAt\` takes at most 2 arguments, 4 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`changeAt` should work', async () => {
    const result = await pool.run(r.expr([1, 2, 3, 4]).changeAt(1, 3));
    assert.deepEqual(result, [1, 3, 3, 4]);
  });

  it('`changeAt` should throw if no argument has been passed', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .changeAt(),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`changeAt\` takes 2 arguments, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`keys` should work', async () => {
    const result = await pool.run(
      r
        .expr({ a: 0, b: 1, c: 2 })
        .keys()
        .orderBy((row) => row),
    );
    assert.deepEqual(result, ['a', 'b', 'c']);
  });

  it('`keys` throw on a string', async () => {
    try {
      await pool.run(
        r
          .expr('hello')
          .keys()
          .orderBy((row) => row),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^Cannot call `keys` on objects of type `STRING` in/),
      );
    }
  });

  it('`values` should work', async () => {
    const result = await pool.run(
      r
        .expr({ a: 0, b: 1, c: 2 })
        .values()
        .orderBy((row) => row),
    );
    assert.deepEqual(result, [0, 1, 2]);
  });

  it('`object` should work', async () => {
    const result = await pool.run(r.object('a', 1, r.expr('2'), 'foo'));
    assert.deepEqual(result, { a: 1, '2': 'foo' });
  });
});
