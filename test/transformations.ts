import assert from 'assert';
import { connectPool, r } from '../src';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('transformations', () => {
  let dbName: string;
  let tableName: string;
  let pool: MasterConnectionPool;

  before(async () => {
    pool = await connectPool([config.server], config.options);
    dbName = uuid();
    tableName = uuid();
    const numDocs = 100;

    const result1 = await pool.run(r.dbCreate(dbName));
    assert.equal(result1.dbs_created, 1);

    const result2 = await pool.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result2.tables_created, 1);

    const result3 = await pool.run(
      r.db(dbName).table(tableName).insert(Array(numDocs).fill({})),
    );
    assert.equal(result3.inserted, numDocs);

    await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .update({ val: r.js('Math.random()') }, { nonAtomic: true }),
    );
    await pool.run(r.db(dbName).table(tableName).indexCreate('val'));
    await pool.run(r.db(dbName).table(tableName).indexWait('val'));
  });

  after(async () => {
    await pool.drain();
  });

  it('`map` should work on array -- row => row', async () => {
    let result = await pool.run(r.expr([1, 2, 3]).map((row) => row));
    assert.deepEqual(result, [1, 2, 3]);

    result = await pool.run(r.expr([1, 2, 3]).map((row) => row.add(1)));
    assert.deepEqual(result, [2, 3, 4]);
  });

  it('`map` should work on array -- function', async () => {
    let result = await pool.run(r.expr([1, 2, 3]).map((doc) => doc));
    assert.deepEqual(result, [1, 2, 3]);

    result = await pool.run(r.expr([1, 2, 3]).map((doc) => doc.add(2)));
    assert.deepEqual(result, [3, 4, 5]);
  });

  it('`map` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r.db(dbName).table(tableName).map());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`map` takes at least 1 argument, 0 provided after/),
      );
    }
  });

  it('`withFields` should work on array -- single field', async () => {
    const result = await pool.run(
      r
        .expr([
          { a: 0, b: 1, c: 2 },
          { a: 4, b: 4, c: 5 },
          { a: 9, b: 2, c: 0 },
        ])
        .withFields('a'),
    );
    assert.deepEqual(result, [{ a: 0 }, { a: 4 }, { a: 9 }]);
  });

  it('`withFields` should work on array -- multiple field', async () => {
    const result = await pool.run(
      r
        .expr([
          { a: 0, b: 1, c: 2 },
          { a: 4, b: 4, c: 5 },
          { a: 9, b: 2, c: 0 },
        ])
        .withFields('a', 'c'),
    );
    assert.deepEqual(result, [
      { a: 0, c: 2 },
      { a: 4, c: 5 },
      { a: 9, c: 0 },
    ]);
  });

  it('`withFields` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r.db(dbName).table(tableName).withFields());
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
    const result = await pool.run(
      r.expr([[1, 2], [3], [4]]).concatMap((doc) => doc),
    );
    assert.deepEqual(result, [1, 2, 3, 4]);
  });

  it('`concatMap` should work on array -- row => row', async () => {
    const result = await pool.run(
      r.expr([[1, 2], [3], [4]]).concatMap((row) => row),
    );
    assert.deepEqual(result, [1, 2, 3, 4]);
  });

  it('`concatMap` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r.db(dbName).table(tableName).concatMap());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`concatMap` takes 1 argument, 0 provided after/),
      );
    }
  });

  it('`orderBy` should work on array -- string', async () => {
    const result = await pool.run(
      r.expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }]).orderBy('a'),
    );
    assert.deepEqual(result, [{ a: 0 }, { a: 10 }, { a: 23 }, { a: 100 }]);
  });

  it('`orderBy` should work on array -- row => row', async () => {
    const result = await pool.run(
      r
        .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
        .orderBy((row) => row('a')),
    );
    assert.deepEqual(result, [{ a: 0 }, { a: 10 }, { a: 23 }, { a: 100 }]);
  });

  it('`orderBy` should work on a table -- pk', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).orderBy({ index: 'id' }),
    );
    for (let i = 0; i < result.length - 1; i++) {
      assert(result[i].id < result[i + 1].id);
    }
  });

  it('`orderBy` should work on a table -- secondary', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).orderBy({ index: 'val' }),
    );
    for (let i = 0; i < result.length - 1; i++) {
      assert(result[i].val < result[i + 1].val);
    }
  });

  it('`orderBy` should work on a two fields', async () => {
    const dbName1 = uuid();
    const tableName1 = uuid();
    const numDocs = 98;

    const result1 = await pool.run(r.dbCreate(dbName1));
    assert.deepEqual(result1.dbs_created, 1);

    const result2 = await pool.run(r.db(dbName1).tableCreate(tableName1));
    assert.equal(result2.tables_created, 1);

    const result3 = await pool.run(
      r
        .db(dbName1)
        .table(tableName1)
        .insert(
          Array(numDocs)
            .fill(0)
            .map(() => ({ a: r.js('Math.random()') })),
        ),
    );
    assert.deepEqual(result3.inserted, numDocs);

    const result4 = await pool.run(
      r.db(dbName1).table(tableName1).orderBy('id', 'a'),
    );
    assert(Array.isArray(result4));
    assert(result4[0].id < result4[1].id);
  });

  it('`orderBy` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r.db(dbName).table(tableName).orderBy());
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
    const result = await pool.run(
      r
        .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
        .orderBy(r.asc((row) => row('a'))),
    );
    assert.deepEqual(result, [{ a: 0 }, { a: 10 }, { a: 23 }, { a: 100 }]);
  });

  it('`orderBy` should not wrap on r.desc', async () => {
    const result = await pool.run(
      r
        .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
        .orderBy(r.desc((row) => row('a'))),
    );
    assert.deepEqual(result, [{ a: 100 }, { a: 23 }, { a: 10 }, { a: 0 }]);
  });
  it('r.desc should work', async () => {
    const result = await pool.run(
      r.expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }]).orderBy(r.desc('a')),
    );
    assert.deepEqual(result, [{ a: 100 }, { a: 23 }, { a: 10 }, { a: 0 }]);
  });

  it('r.asc should work', async () => {
    const result = await pool.run(
      r.expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }]).orderBy(r.asc('a')),
    );
    assert.deepEqual(result, [{ a: 0 }, { a: 10 }, { a: 23 }, { a: 100 }]);
  });

  it('`desc` is not defined after a term', async () => {
    try {
      await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .desc('foo'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.desc is not a function'));
    }
  });

  it('`asc` is not defined after a term', async () => {
    try {
      await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .asc('foo'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.asc is not a function'));
    }
  });

  it('`skip` should work', async () => {
    const result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).skip(3),
    );
    assert.deepEqual(result, [3, 4, 5, 6, 7, 8, 9]);
  });

  it('`skip` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).skip());
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`skip` takes 1 argument, 0 provided after/));
    }
  });

  it('`limit` should work', async () => {
    const result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).limit(3),
    );
    assert.deepEqual(result, [0, 1, 2]);
  });

  it('`limit` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).limit());
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`limit` takes 1 argument, 0 provided after/));
    }
  });

  it('`slice` should work', async () => {
    const result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).slice(3, 5),
    );
    assert.deepEqual(result, [3, 4]);
  });

  it('`slice` should handle options and optional end', async () => {
    let result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).slice(3),
    );
    assert.deepEqual(result, [3, 4, 5, 6, 7, 8, 9]);

    result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).slice(3, { leftBound: 'open' }),
    );
    assert.deepEqual(result, [4, 5, 6, 7, 8, 9]);

    result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).slice(3, 5, { leftBound: 'open' }),
    );
    assert.deepEqual(result, [4]);
  });

  it('`slice` should work -- with options', async () => {
    let result = await pool.run(
      r
        .expr([
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          15,
          16,
          17,
          18,
          19,
          20,
          21,
          22,
          23,
        ])
        .slice(5, 10, { rightBound: 'closed' }),
    );
    assert.deepEqual(result, [5, 6, 7, 8, 9, 10]);

    result = await pool.run(
      r
        .expr([
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          15,
          16,
          17,
          18,
          19,
          20,
          21,
          22,
          23,
        ])
        .slice(5, 10, { rightBound: 'open' }),
    );
    assert.deepEqual(result, [5, 6, 7, 8, 9]);

    result = await pool.run(
      r
        .expr([
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          15,
          16,
          17,
          18,
          19,
          20,
          21,
          22,
          23,
        ])
        .slice(5, 10, { leftBound: 'open' }),
    );
    assert.deepEqual(result, [6, 7, 8, 9]);

    result = await pool.run(
      r
        .expr([
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          15,
          16,
          17,
          18,
          19,
          20,
          21,
          22,
          23,
        ])
        .slice(5, 10, { leftBound: 'closed' }),
    );
    assert.deepEqual(result, [5, 6, 7, 8, 9]);

    result = await pool.run(
      r
        .expr([
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          15,
          16,
          17,
          18,
          19,
          20,
          21,
          22,
          23,
        ])
        .slice(5, 10, { leftBound: 'closed', rightBound: 'closed' }),
    );
    assert.deepEqual(result, [5, 6, 7, 8, 9, 10]);
  });

  it('`slice` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).slice());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`slice` takes at least 1 argument, 0 provided after/),
      );
    }
  });

  it('`nth` should work', async () => {
    const result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).nth(3),
    );
    assert.equal(result, 3);
  });

  it('`nth` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).nth());
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`nth` takes 1 argument, 0 provided after/));
    }
  });

  it('`offsetsOf` should work - datum', async () => {
    const result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).nth(3),
    );
    assert.equal(result, 3);
  });

  it('`offsetsOf` should work - row => row', async () => {
    const result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).offsetsOf((row) => row.eq(3)),
    );
    assert.equal(result, 3);
  });

  it('`offsetsOf` should work - function', async () => {
    const result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).offsetsOf((doc) => doc.eq(3)),
    );
    assert.equal(result, 3);
  });

  it('`offsetsOf` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).offsetsOf());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`offsetsOf` takes 1 argument, 0 provided after/),
      );
    }
  });

  it('`isEmpty` should work', async () => {
    let result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).isEmpty(),
    );
    assert.equal(result, false);

    result = await pool.run(r.expr([]).isEmpty());
    assert.equal(result, true);
  });

  it('`union` should work - 1', async () => {
    const result = await pool.run(r.expr([0, 1, 2]).union([3, 4, 5]));
    assert.deepEqual(result.length, 6);
    for (let i = 0; i < 6; i++) {
      assert(result.indexOf(i) >= 0);
    }
  });

  it('`union` should work - 2', async () => {
    const result = await pool.run(r.union([0, 1, 2], [3, 4, 5], [6, 7]));
    assert.deepEqual(result.length, 8);
    for (let i = 0; i < 8; i++) {
      assert(result.indexOf(i) >= 0);
    }
  });

  // it('`union` should work - 3', async () => {
  //   const result = await pool.run(r.union());
  //   assert.deepEqual(result, []);
  // });

  it('`union` should work with interleave - 1', async () => {
    const result = await pool.run(
      r.expr([0, 1, 2]).union([3, 4, 5], { interleave: false }),
    );
    assert.deepEqual(result, [0, 1, 2, 3, 4, 5]);
  });

  it('`union` should work with interleave - 1', async () => {
    const result = await pool.run(
      r
        .expr([{ name: 'Michel' }, { name: 'Sophie' }, { name: 'Laurent' }])
        .orderBy('name')
        .union(r.expr([{ name: 'Moo' }, { name: 'Bar' }]).orderBy('name'), {
          interleave: 'name',
        }),
    );
    assert.deepEqual(result, [
      { name: 'Bar' },
      { name: 'Laurent' },
      { name: 'Michel' },
      { name: 'Moo' },
      { name: 'Sophie' },
    ]);
  });

  it('`sample` should work', async () => {
    const result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).sample(2),
    );
    assert.equal(result.length, 2);
  });

  it('`sample` should throw if given -1', async () => {
    try {
      await pool.run(r.expr([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).sample(-1));
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
      await pool.run(r.db(dbName).table(tableName).sample());
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`sample` takes 1 argument, 0 provided after/));
    }
  });
});
