import assert from 'assert';
import { r } from '../src';
import config from './config';
import { uuid } from './util/common';

describe('transformations', () => {
  let dbName: string;
  let tableName: string;

  before(async () => {
    await r.connectPool(config);
    dbName = uuid();
    tableName = uuid();
    const numDocs = 100;

    const result1 = await r.dbCreate(dbName).run();
    assert.equal(result1.dbs_created, 1);

    const result2 = await r.db(dbName).tableCreate(tableName).run();
    assert.equal(result2.tables_created, 1);

    const result3 = await r
      .db(dbName)
      .table(tableName)
      .insert(Array(numDocs).fill({}))
      .run();
    assert.equal(result3.inserted, numDocs);

    await r
      .db(dbName)
      .table(tableName)
      .update({ val: r.js('Math.random()') }, { nonAtomic: true })
      .run();
    await r.db(dbName).table(tableName).indexCreate('val').run();
    await r.db(dbName).table(tableName).indexWait('val').run();
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  it('`map` should work on array -- row => row', async () => {
    let result = await r
      .expr([1, 2, 3])
      .map((row) => row)
      .run();
    assert.deepEqual(result, [1, 2, 3]);

    result = await r
      .expr([1, 2, 3])
      .map((row) => row.add(1))
      .run();
    assert.deepEqual(result, [2, 3, 4]);
  });

  it('`map` should work on array -- function', async () => {
    let result = await r
      .expr([1, 2, 3])
      .map((doc) => doc)
      .run();
    assert.deepEqual(result, [1, 2, 3]);

    result = await r
      .expr([1, 2, 3])
      .map((doc) => doc.add(2))
      .run();
    assert.deepEqual(result, [3, 4, 5]);
  });

  it('`map` should throw if no argument has been passed', async () => {
    try {
      await r.db(dbName).table(tableName).map().run();
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`map` takes at least 1 argument, 0 provided after/),
      );
    }
  });

  it('`withFields` should work on array -- single field', async () => {
    const result = await r
      .expr([
        { a: 0, b: 1, c: 2 },
        { a: 4, b: 4, c: 5 },
        { a: 9, b: 2, c: 0 },
      ])
      .withFields('a')
      .run();
    assert.deepEqual(result, [{ a: 0 }, { a: 4 }, { a: 9 }]);
  });

  it('`withFields` should work on array -- multiple field', async () => {
    const result = await r
      .expr([
        { a: 0, b: 1, c: 2 },
        { a: 4, b: 4, c: 5 },
        { a: 9, b: 2, c: 0 },
      ])
      .withFields('a', 'c')
      .run();
    assert.deepEqual(result, [
      { a: 0, c: 2 },
      { a: 4, c: 5 },
      { a: 9, c: 0 },
    ]);
  });

  it('`withFields` should throw if no argument has been passed', async () => {
    try {
      await r.db(dbName).table(tableName).withFields().run();
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(
          /^`withFields` takes at least 1 argument, 0 provided after/,
        ),
      );
    }
  });

  it('`concatMap` should work on array -- function', async () => {
    const result = await r
      .expr([[1, 2], [3], [4]])
      .concatMap((doc) => doc)
      .run();
    assert.deepEqual(result, [1, 2, 3, 4]);
  });

  it('`concatMap` should work on array -- row => row', async () => {
    const result = await r
      .expr([[1, 2], [3], [4]])
      .concatMap((row) => row)
      .run();
    assert.deepEqual(result, [1, 2, 3, 4]);
  });

  it('`concatMap` should throw if no argument has been passed', async () => {
    try {
      await r.db(dbName).table(tableName).concatMap().run();
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`concatMap` takes 1 argument, 0 provided after/),
      );
    }
  });

  it('`orderBy` should work on array -- string', async () => {
    const result = await r
      .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
      .orderBy('a')
      .run();
    assert.deepEqual(result, [{ a: 0 }, { a: 10 }, { a: 23 }, { a: 100 }]);
  });

  it('`orderBy` should work on array -- row => row', async () => {
    const result = await r
      .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
      .orderBy((row) => row('a'))
      .run();
    assert.deepEqual(result, [{ a: 0 }, { a: 10 }, { a: 23 }, { a: 100 }]);
  });

  it('`orderBy` should work on a table -- pk', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .orderBy({ index: 'id' })
      .run();
    for (let i = 0; i < result.length - 1; i++) {
      assert(result[i].id < result[i + 1].id);
    }
  });

  it('`orderBy` should work on a table -- secondary', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .orderBy({ index: 'val' })
      .run();
    for (let i = 0; i < result.length - 1; i++) {
      assert(result[i].val < result[i + 1].val);
    }
  });

  it('`orderBy` should work on a two fields', async () => {
    const dbName1 = uuid();
    const tableName1 = uuid();
    const numDocs = 98;

    const result1 = await r.dbCreate(dbName1).run();
    assert.deepEqual(result1.dbs_created, 1);

    const result2 = await r.db(dbName1).tableCreate(tableName1).run();
    assert.equal(result2.tables_created, 1);

    const result3 = await r
      .db(dbName1)
      .table(tableName1)
      .insert(
        Array(numDocs)
          .fill(0)
          .map(() => ({ a: r.js('Math.random()') })),
      )
      .run();
    assert.deepEqual(result3.inserted, numDocs);

    const result4 = await r
      .db(dbName1)
      .table(tableName1)
      .orderBy('id', 'a')
      .run();
    assert(Array.isArray(result4));
    assert(result4[0].id < result4[1].id);
  });

  it('`orderBy` should throw if no argument has been passed', async () => {
    try {
      await r.db(dbName).table(tableName).orderBy().run();
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(
          /^`orderBy` takes at least 1 argument, 0 provided after/,
        ),
      );
    }
  });

  it('`orderBy` should not wrap on r.asc', async () => {
    const result = await r
      .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
      .orderBy(r.asc((row) => row('a')))
      .run();
    assert.deepEqual(result, [{ a: 0 }, { a: 10 }, { a: 23 }, { a: 100 }]);
  });

  it('`orderBy` should not wrap on r.desc', async () => {
    const result = await r
      .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
      .orderBy(r.desc((row) => row('a')))
      .run();
    assert.deepEqual(result, [{ a: 100 }, { a: 23 }, { a: 10 }, { a: 0 }]);
  });
  it('r.desc should work', async () => {
    const result = await r
      .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
      .orderBy(r.desc('a'))
      .run();
    assert.deepEqual(result, [{ a: 100 }, { a: 23 }, { a: 10 }, { a: 0 }]);
  });

  it('r.asc should work', async () => {
    const result = await r
      .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
      .orderBy(r.asc('a'))
      .run();
    assert.deepEqual(result, [{ a: 0 }, { a: 10 }, { a: 23 }, { a: 100 }]);
  });

  it('`desc` is not defined after a term', async () => {
    try {
      await r
        .expr(1)
        // @ts-ignore
        .desc('foo')
        .run();
      assert.fail('sholud throw');
    } catch (e) {
      assert(e.message.endsWith('.desc is not a function'));
    }
  });

  it('`asc` is not defined after a term', async () => {
    try {
      await r
        .expr(1)
        // @ts-ignore
        .asc('foo')
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.asc is not a function'));
    }
  });

  it('`skip` should work', async () => {
    const result = await r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).skip(3).run();
    assert.deepEqual(result, [3, 4, 5, 6, 7, 8, 9]);
  });

  it('`skip` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).skip().run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`skip` takes 1 argument, 0 provided after/));
    }
  });

  it('`limit` should work', async () => {
    const result = await r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).limit(3).run();
    assert.deepEqual(result, [0, 1, 2]);
  });

  it('`limit` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).limit().run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`limit` takes 1 argument, 0 provided after/));
    }
  });

  it('`slice` should work', async () => {
    const result = await r
      .expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      .slice(3, 5)
      .run();
    assert.deepEqual(result, [3, 4]);
  });

  it('`slice` should handle options and optional end', async () => {
    let result = await r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).slice(3).run();
    assert.deepEqual(result, [3, 4, 5, 6, 7, 8, 9]);

    result = await r
      .expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      .slice(3, { leftBound: 'open' })
      .run();
    assert.deepEqual(result, [4, 5, 6, 7, 8, 9]);

    result = await r
      .expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      .slice(3, 5, { leftBound: 'open' })
      .run();
    assert.deepEqual(result, [4]);
  });

  it('`slice` should work -- with options', async () => {
    let result = await r
      .expr([
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23,
      ])
      .slice(5, 10, { rightBound: 'closed' })
      .run();
    assert.deepEqual(result, [5, 6, 7, 8, 9, 10]);

    result = await r
      .expr([
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23,
      ])
      .slice(5, 10, { rightBound: 'open' })
      .run();
    assert.deepEqual(result, [5, 6, 7, 8, 9]);

    result = await r
      .expr([
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23,
      ])
      .slice(5, 10, { leftBound: 'open' })
      .run();
    assert.deepEqual(result, [6, 7, 8, 9]);

    result = await r
      .expr([
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23,
      ])
      .slice(5, 10, { leftBound: 'closed' })
      .run();
    assert.deepEqual(result, [5, 6, 7, 8, 9]);

    result = await r
      .expr([
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23,
      ])
      .slice(5, 10, { leftBound: 'closed', rightBound: 'closed' })
      .run();
    assert.deepEqual(result, [5, 6, 7, 8, 9, 10]);
  });

  it('`slice` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).slice().run();
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`slice` takes at least 1 argument, 0 provided after/),
      );
    }
  });

  it('`nth` should work', async () => {
    const result = await r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).nth(3).run();
    assert.equal(result, 3);
  });

  it('`nth` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).nth().run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`nth` takes 1 argument, 0 provided after/));
    }
  });

  it('`offsetsOf` should work - datum', async () => {
    const result = await r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).nth(3).run();
    assert.equal(result, 3);
  });

  it('`offsetsOf` should work - row => row', async () => {
    const result = await r
      .expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      .offsetsOf((row) => row.eq(3))
      .run();
    assert.equal(result, 3);
  });

  it('`offsetsOf` should work - function', async () => {
    const result = await r
      .expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      .offsetsOf((doc) => doc.eq(3))
      .run();
    assert.equal(result, 3);
  });

  it('`offsetsOf` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).offsetsOf().run();
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`offsetsOf` takes 1 argument, 0 provided after/),
      );
    }
  });

  it('`isEmpty` should work', async () => {
    let result = await r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).isEmpty().run();
    assert.equal(result, false);

    result = await r.expr([]).isEmpty().run();
    assert.equal(result, true);
  });

  it('`union` should work - 1', async () => {
    const result = await r.expr([0, 1, 2]).union([3, 4, 5]).run();
    assert.deepEqual(result.length, 6);
    for (let i = 0; i < 6; i++) {
      assert(result.indexOf(i) >= 0);
    }
  });

  it('`union` should work - 2', async () => {
    const result = await r.union([0, 1, 2], [3, 4, 5], [6, 7]).run();
    assert.deepEqual(result.length, 8);
    for (let i = 0; i < 8; i++) {
      assert(result.indexOf(i) >= 0);
    }
  });

  // it('`union` should work - 3', async () => {
  //   const result = await r.union().run();
  //   assert.deepEqual(result, []);
  // });

  it('`union` should work with interleave - 1', async () => {
    const result = await r
      .expr([0, 1, 2])
      .union([3, 4, 5], { interleave: false })
      .run();
    assert.deepEqual(result, [0, 1, 2, 3, 4, 5]);
  });

  it('`union` should work with interleave - 1', async () => {
    const result = await r
      .expr([{ name: 'Michel' }, { name: 'Sophie' }, { name: 'Laurent' }])
      .orderBy('name')
      .union(r.expr([{ name: 'Moo' }, { name: 'Bar' }]).orderBy('name'), {
        interleave: 'name',
      })
      .run();
    assert.deepEqual(result, [
      { name: 'Bar' },
      { name: 'Laurent' },
      { name: 'Michel' },
      { name: 'Moo' },
      { name: 'Sophie' },
    ]);
  });

  it('`sample` should work', async () => {
    const result = await r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).sample(2).run();
    assert.equal(result.length, 2);
  });

  it('`sample` should throw if given -1', async () => {
    try {
      await r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).sample(-1).run();
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(
          'Number of items to sample must be non-negative, got `-1`',
        ),
      );
    }
  });

  it('`sample` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).sample().run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`sample` takes 1 argument, 0 provided after/));
    }
  });
});
