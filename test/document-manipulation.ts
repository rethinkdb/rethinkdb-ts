import assert from 'assert';
import { r } from '../src';
import config from './config';
import { uuid } from './util/common';

describe('document manipulation', () => {
  let dbName: string;
  let tableName: string;

  before(async () => {
    await r.connectPool(config);
    dbName = uuid();
    tableName = uuid();

    const result1 = await r.dbCreate(dbName).run();
    assert.equal(result1.dbs_created, 1);

    const result2 = await r.db(dbName).tableCreate(tableName).run();
    assert.equal(result2.tables_created, 1);
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  it('`row => row` should work - 1', async () => {
    const result = await r
      .expr([1, 2, 3])
      .map((row) => row)
      .run();
    assert.deepEqual(result, [1, 2, 3]);
  });

  it('`row => row` should work - 2', async () => {
    let result = await r.db(dbName).table(tableName).insert({}).run();
    assert.equal(result.inserted, 1);

    result = await r
      .db(dbName)
      .table(tableName)
      .update((row) => ({ idCopyUpdate: row('id') }))
      .run();
    assert.equal(result.replaced, 1);
  });

  it('`row => row` should work - 3', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .replace((row) => row)
      .run();
    assert.equal(result.replaced, 0);
  });

  it('`row => row` should work - 4', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .replace((doc) => doc.merge({ idCopyReplace: doc('id') }))
      .run();
    assert.equal(result.replaced, 1);
  });

  it('`row => row` should work - 5', async () => {
    const result = await r.db(dbName).table(tableName).delete().run();
    assert.equal(result.deleted, 1);
  });

  it('`r.row` should work - 1', async () => {
    const result = await r.expr([1, 2, 3]).map(r.row).run();
    assert.deepEqual(result, [1, 2, 3]);
  });

  it('`r.row` should work - 2', async () => {
    const result1 = await r.db(dbName).table(tableName).insert({}).run();
    assert.equal(result1.inserted, 1);

    const result2 = await r
      .db(dbName)
      .table(tableName)
      .update({ idCopyUpdate: r.row('id') })
      .run();
    console.dir(result2);
    assert.equal(result2.replaced, 1);
  });

  it('`r.row` should work - 3', async () => {
    const result = await r.db(dbName).table(tableName).replace(r.row).run();
    assert.equal(result.replaced, 0);
  });
  it('`r.row` should work - 4', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .replace((doc) => doc.merge({ idCopyReplace: doc('id') }))
      .run();
    assert.equal(result.replaced, 1);
  });

  it('`r.row` should work - 5', async () => {
    const result = await r.db(dbName).table(tableName).delete().run();
    assert.equal(result.deleted, 1);
  });

  it('`pluck` should work', async () => {
    const result1 = await r.expr({ a: 0, b: 1, c: 2 }).pluck('a', 'b').run();
    assert.deepEqual(result1, { a: 0, b: 1 });

    const result2 = await r
      .expr([
        { a: 0, b: 1, c: 2 },
        { a: 0, b: 10, c: 20 },
      ])
      .pluck('a', 'b')
      .run();
    assert.deepEqual(result2, [
      { a: 0, b: 1 },
      { a: 0, b: 10 },
    ]);
  });

  it('`pluck` should throw if no argument has been passed', async () => {
    try {
      await r.db(dbName).table(tableName).pluck().run();
      assert.fail('should trow');
    } catch (e) {
      assert.equal(
        e.message,
        `\`pluck\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`without` should work', async () => {
    const result1 = await r.expr({ a: 0, b: 1, c: 2 }).without('c').run();
    assert.deepEqual(result1, { a: 0, b: 1 });

    const result2 = await r
      .expr([
        { a: 0, b: 1, c: 2 },
        { a: 0, b: 10, c: 20 },
      ])
      .without('a', 'c')
      .run();
    assert.deepEqual(result2, [{ b: 1 }, { b: 10 }]);
  });

  it('`without` should throw if no argument has been passed', async () => {
    try {
      await r.db(dbName).table(tableName).without().run();
    } catch (e) {
      assert.equal(
        e.message,
        `\`without\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`merge` should work', async () => {
    let result = await r.expr({ a: 0 }).merge({ b: 1 }).run();
    assert.deepEqual(result, { a: 0, b: 1 });

    result = await r
      .expr([{ a: 0 }, { a: 1 }, { a: 2 }])
      .merge({ b: 1 })
      .run();
    assert.deepEqual(result, [
      { a: 0, b: 1 },
      { a: 1, b: 1 },
      { a: 2, b: 1 },
    ]);

    result = await r
      .expr({ a: 0, c: { l: 'tt' } })
      .merge({ b: { c: { d: { e: 'fff' } }, k: 'pp' } })
      .run();
    assert.deepEqual(result, {
      a: 0,
      b: { c: { d: { e: 'fff' } }, k: 'pp' },
      c: { l: 'tt' },
    });

    result = await r.expr({ a: 1 }).merge({ date: r.now() }).run();
    assert.equal(result.a, 1);
    assert(result.date instanceof Date);

    result = await r
      .expr({ a: 1 })
      .merge((row) => ({ nested: row }), { b: 2 })
      .run();
    assert.deepEqual(result, { a: 1, nested: { a: 1 }, b: 2 });
  });

  it('`merge` should take an anonymous function', async () => {
    let result = await r
      .expr({ a: 0 })
      .merge((doc) => ({ b: doc('a').add(1) }))
      .run();
    assert.deepEqual(result, { a: 0, b: 1 });

    result = await r
      .expr({ a: 0 })
      .merge((row) => ({
        b: row('a').add(1),
      }))
      .run();
    assert.deepEqual(result, { a: 0, b: 1 });
  });

  it('`literal` should work', async () => {
    const result = await r
      .expr({ a: { b: 1 } })
      .merge({ a: r.literal({ c: 2 }) })
      .run();
    assert.deepEqual(result, { a: { c: 2 } });
  });

  it('`literal` is not defined after a term', async () => {
    try {
      await r
        .expr(1)
        // @ts-ignore
        .literal('foo')
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.literal is not a function'));
    }
  });

  it('`merge` should throw if no argument has been passed', async () => {
    try {
      await r.db(dbName).table(tableName).merge().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`merge\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`literal` should work with no argument', async () => {
    const result = await r
      .expr({ foo: 'bar' })
      .merge({ foo: r.literal() })
      .run();
    assert.deepEqual(result, {});
  });

  it('`append` should work', async () => {
    const result = await r.expr([1, 2, 3]).append(4).run();
    assert.deepEqual(result, [1, 2, 3, 4]);
  });

  it('`append` should throw if no argument has been passed', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .append()
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`append\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`prepend` should work', async () => {
    const result = await r.expr([1, 2, 3]).prepend(4).run();
    assert.deepEqual(result, [4, 1, 2, 3]);
  });

  it('`prepend` should throw if no argument has been passed', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .prepend()
        .run();
      assert.fail('sholud throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`prepend\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`difference` should work', async () => {
    const result = await r.expr([1, 2, 3]).prepend(4).run();
    assert.deepEqual(result, [4, 1, 2, 3]);
  });

  it('`difference` should throw if no argument has been passed', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .difference()
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`difference\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`setInsert` should work', async () => {
    let result = await r.expr([1, 2, 3]).setInsert(4).run();
    assert.deepEqual(result, [1, 2, 3, 4]);

    result = await r.expr([1, 2, 3]).setInsert(2).run();
    assert.deepEqual(result, [1, 2, 3]);
  });

  it('`setInsert` should throw if no argument has been passed', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .setInsert()
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`setInsert\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`setUnion` should work', async () => {
    const result = await r.expr([1, 2, 3]).setUnion([2, 4]).run();
    assert.deepEqual(result, [1, 2, 3, 4]);
  });

  it('`setUnion` should throw if no argument has been passed', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .setUnion()
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`setUnion\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`setIntersection` should work', async () => {
    const result = await r.expr([1, 2, 3]).setIntersection([2, 4]).run();
    assert.deepEqual(result, [2]);
  });

  it('`setIntersection` should throw if no argument has been passed', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .setIntersection()
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`setIntersection\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`setDifference` should work', async () => {
    const result = await r.expr([1, 2, 3]).setDifference([2, 4]).run();
    assert.deepEqual(result, [1, 3]);
  });

  it('`setDifference` should throw if no argument has been passed', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .setDifference()
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`setDifference\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`getField` should work', async () => {
    let result = await r.expr({ a: 0, b: 1 })('a').run();
    assert.equal(result, 0);

    result = await r.expr({ a: 0, b: 1 }).getField('a').run();
    assert.equal(result, 0);

    result = await r
      .expr([{ a: 0, b: 1 }, { a: 1 }])('a')
      .run();
    assert.deepEqual(result, [0, 1]);
  });

  it('`(...)` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName)().run();
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
      await r.db(dbName).table(tableName).getField().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`getField\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`hasFields` should work', async () => {
    const result = await r
      .expr([
        { a: 0, b: 1, c: 2 },
        { a: 0, b: 10, c: 20 },
        { b: 1, c: 3 },
      ])
      .hasFields('a', 'c')
      .run();
    assert.deepEqual(result, [
      { a: 0, b: 1, c: 2 },
      { a: 0, b: 10, c: 20 },
    ]);
  });

  it('`hasFields` should throw if no argument has been passed', async () => {
    try {
      await r.db(dbName).table(tableName).hasFields().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`hasFields\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`insertAt` should work', async () => {
    let result = await r.expr([1, 2, 3, 4]).insertAt(0, 2).run();
    assert.deepEqual(result, [2, 1, 2, 3, 4]);

    result = await r.expr([1, 2, 3, 4]).insertAt(3, 2).run();
    assert.deepEqual(result, [1, 2, 3, 2, 4]);
  });

  it('`insertAt` should throw if no argument has been passed', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .insertAt()
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`insertAt\` takes 2 arguments, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`spliceAt` should work', async () => {
    const result = await r.expr([1, 2, 3, 4]).spliceAt(1, [9, 9]).run();
    assert.deepEqual(result, [1, 9, 9, 2, 3, 4]);
  });

  it('`spliceAt` should throw if no argument has been passed', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .spliceAt()
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`spliceAt\` takes 2 arguments, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`deleteAt` should work', async () => {
    let result = await r.expr([1, 2, 3, 4]).deleteAt(1).run();
    assert.deepEqual(result, [1, 3, 4]);

    result = await r.expr([1, 2, 3, 4]).deleteAt(1, 3).run();
    assert.deepEqual(result, [1, 4]);
  });

  it('`deleteAt` should throw if no argument has been passed', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .deleteAt()
        .run();
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
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .deleteAt(1, 1, 1, 1)
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`deleteAt\` takes at most 2 arguments, 4 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`changeAt` should work', async () => {
    const result = await r.expr([1, 2, 3, 4]).changeAt(1, 3).run();
    assert.deepEqual(result, [1, 3, 3, 4]);
  });

  it('`changeAt` should throw if no argument has been passed', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .changeAt()
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`changeAt\` takes 2 arguments, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`keys` should work', async () => {
    const result = await r
      .expr({ a: 0, b: 1, c: 2 })
      .keys()
      .orderBy((row) => row)
      .run();
    assert.deepEqual(result, ['a', 'b', 'c']);
  });

  it('`keys` throw on a string', async () => {
    try {
      await r
        .expr('hello')
        .keys()
        .orderBy((row) => row)
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^Cannot call `keys` on objects of type `STRING` in/),
      );
    }
  });

  it('`values` should work', async () => {
    const result = await r
      .expr({ a: 0, b: 1, c: 2 })
      .values()
      .orderBy((row) => row)
      .run();
    assert.deepEqual(result, [0, 1, 2]);
  });

  it('`object` should work', async () => {
    const result = await r.object('a', 1, r.expr('2'), 'foo').run();
    assert.deepEqual(result, { a: 1, '2': 'foo' });
  });
});
