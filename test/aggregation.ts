import assert from 'assert';
import { connectPool, r } from '../src';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('aggregation', () => {
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
  });

  after(async () => {
    await pool.drain();
  });

  it('`reduce` should work -- no base ', async () => {
    result = await pool.run(
      r.expr([1, 2, 3]).reduce((left, right) => {
        return left.add(right);
      }),
    );
    assert.equal(result, 6);
  });
  it('`reduce` should throw if no argument has been passed', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.db(dbName).table(tableName).reduce());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`reduce\` takes 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`fold` should work', async () => {
    result = await pool.run(
      r.expr([1, 2, 3]).fold(10, (left, right) => {
        return left.add(right);
      }),
    );
    assert.equal(result, 16);
  });

  it('`fold` should work -- with emit', async () => {
    result = await pool.run(
      r.expr(['foo', 'bar', 'buzz', 'hello', 'world']).fold(
        0,
        (acc, row) => {
          return acc.add(1);
        },
        {
          emit: (oldAcc, element, newAcc) => {
            return [oldAcc, element, newAcc];
          },
        },
      ),
    );
    assert.deepEqual(result, [
      0,
      'foo',
      1,
      1,
      'bar',
      2,
      2,
      'buzz',
      3,
      3,
      'hello',
      4,
      4,
      'world',
      5,
    ]);
  });

  it('`fold` should work -- with emit and finalEmit', async () => {
    result = await pool.run(
      r.expr(['foo', 'bar', 'buzz', 'hello', 'world']).fold(
        0,
        (acc, row) => {
          return acc.add(1);
        },
        {
          emit: (oldAcc, element, newAcc) => {
            return [oldAcc, element, newAcc];
          },
          finalEmit: (acc) => {
            return [acc];
          },
        },
      ),
    );
    assert.deepEqual(result, [
      0,
      'foo',
      1,
      1,
      'bar',
      2,
      2,
      'buzz',
      3,
      3,
      'hello',
      4,
      4,
      'world',
      5,
      5,
    ]);
  });

  it('`count` should work -- no arg ', async () => {
    result = await pool.run(r.expr([0, 1, 2, 3, 4, 5]).count());
    assert.equal(result, 6);
  });

  it('`count` should work -- filter ', async () => {
    result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5]).count((row) => row.eq(2)),
    );
    assert.equal(result, 1);

    result = await pool.run(
      r.expr([0, 1, 2, 3, 4, 5]).count((doc) => {
        return doc.eq(2);
      }),
    );
    assert.equal(result, 1);
  });

  it('`group` should work ', async () => {
    result = await pool.run(
      r
        .expr([
          { name: 'Michel', grownUp: true },
          { name: 'Laurent', grownUp: true },
          { name: 'Sophie', grownUp: true },
          { name: 'Luke', grownUp: false },
          { name: 'Mino', grownUp: false },
        ])
        .group('grownUp'),
    );
    result.sort();

    assert.deepEqual(result, [
      {
        group: false,
        reduction: [
          { grownUp: false, name: 'Luke' },
          { grownUp: false, name: 'Mino' },
        ],
      },
      {
        group: true,
        reduction: [
          { grownUp: true, name: 'Michel' },
          { grownUp: true, name: 'Laurent' },
          { grownUp: true, name: 'Sophie' },
        ],
      },
    ]);
  });

  it('`group` should work with row => row', async () => {
    result = await pool.run(
      r
        .expr([
          { name: 'Michel', grownUp: true },
          { name: 'Laurent', grownUp: true },
          { name: 'Sophie', grownUp: true },
          { name: 'Luke', grownUp: false },
          { name: 'Mino', grownUp: false },
        ])
        .group((row) => row('grownUp')),
    );
    result.sort();

    assert.deepEqual(result, [
      {
        group: false,
        reduction: [
          { grownUp: false, name: 'Luke' },
          { grownUp: false, name: 'Mino' },
        ],
      },
      {
        group: true,
        reduction: [
          { grownUp: true, name: 'Michel' },
          { grownUp: true, name: 'Laurent' },
          { grownUp: true, name: 'Sophie' },
        ],
      },
    ]);
  });

  it('`group` should work with an index ', async () => {
    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert([
          { id: 1, group: 1 },
          { id: 2, group: 1 },
          { id: 3, group: 1 },
          { id: 4, group: 4 },
        ]),
    );
    result = await pool.run(r.db(dbName).table(tableName).indexCreate('group'));
    result = await pool.run(r.db(dbName).table(tableName).indexWait('group'));
    result = await pool.run(
      r.db(dbName).table(tableName).group({ index: 'group' }),
    );

    assert.equal(result.length, 2);
    assert(
      result[0].reduction.length === 3 || result[0].reduction.length === 1,
    );
    assert(
      result[1].reduction.length === 3 || result[1].reduction.length === 1,
    );
  });

  it('`groupFormat` should work -- with raw', async () => {
    result = await pool.run(
      r
        .expr([
          { name: 'Michel', grownUp: true },
          { name: 'Laurent', grownUp: true },
          { name: 'Sophie', grownUp: true },
          { name: 'Luke', grownUp: false },
          { name: 'Mino', grownUp: false },
        ])
        .group('grownUp'),
      { groupFormat: 'raw' },
    );

    assert.deepEqual(result, {
      $reql_type$: 'GROUPED_DATA',
      data: [
        [
          false,
          [
            { grownUp: false, name: 'Luke' },
            { grownUp: false, name: 'Mino' },
          ],
        ],
        [
          true,
          [
            { grownUp: true, name: 'Michel' },
            { grownUp: true, name: 'Laurent' },
            { grownUp: true, name: 'Sophie' },
          ],
        ],
      ],
    });
  });

  it('`group` results should be properly parsed ', async () => {
    result = await pool.run(
      r
        .expr([
          { name: 'Michel', date: r.now() },
          { name: 'Laurent', date: r.now() },
          { name: 'Sophie', date: r.now().sub(1000) },
        ])
        .group('date'),
    );
    assert.equal(result.length, 2);
    assert(result[0].group instanceof Date);
    assert(result[0].reduction[0].date instanceof Date);
  });

  it('`ungroup` should work ', async () => {
    result = await pool.run(
      r
        .expr([
          { name: 'Michel', grownUp: true },
          { name: 'Laurent', grownUp: true },
          { name: 'Sophie', grownUp: true },
          { name: 'Luke', grownUp: false },
          { name: 'Mino', grownUp: false },
        ])
        .group('grownUp')
        .ungroup(),
    );
    result.sort();

    assert.deepEqual(result, [
      {
        group: false,
        reduction: [
          { grownUp: false, name: 'Luke' },
          { grownUp: false, name: 'Mino' },
        ],
      },
      {
        group: true,
        reduction: [
          { grownUp: true, name: 'Michel' },
          { grownUp: true, name: 'Laurent' },
          { grownUp: true, name: 'Sophie' },
        ],
      },
    ]);
  });

  it('`contains` should work ', async () => {
    result = await pool.run(r.expr([1, 2, 3]).contains(2));
    assert.equal(result, true);

    result = await pool.run(r.expr([1, 2, 3]).contains(1, 2));
    assert.equal(result, true);

    result = await pool.run(r.expr([1, 2, 3]).contains(1, 5));
    assert.equal(result, false);

    result = await pool.run(
      r.expr([1, 2, 3]).contains((doc) => {
        return doc.eq(1);
      }),
    );
    assert.equal(result, true);

    result = await pool.run(r.expr([1, 2, 3]).contains((row) => row.eq(1)));
    assert.equal(result, true);

    result = await pool.run(
      r.expr([1, 2, 3]).contains(
        (row) => row.eq(1),
        (row) => row.eq(2),
      ),
    );
    assert.equal(result, true);

    result = await pool.run(
      r.expr([1, 2, 3]).contains(
        (row) => row.eq(1),
        (row) => row.eq(5),
      ),
    );
    assert.equal(result, false);
  });

  it('`contains` should throw if called without arguments', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.db(dbName).table(tableName).contains());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`contains\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`sum` should work ', async () => {
    result = await pool.run(r.expr([1, 2, 3]).sum());
    assert.equal(result, 6);
  });

  it('`sum` should work with a field', async () => {
    result = await pool.run(r.expr([{ a: 2 }, { a: 10 }, { a: 9 }]).sum('a'));
    assert.deepEqual(result, 21);
  });

  it('`avg` should work ', async () => {
    result = await pool.run(r.expr([1, 2, 3]).avg());
    assert.equal(result, 2);
  });

  it('`r.avg` should work ', async () => {
    result = await pool.run(r.avg([1, 2, 3]));
    assert.equal(result, 2);
  });

  it('`avg` should work with a field', async () => {
    result = await pool.run(r.expr([{ a: 2 }, { a: 10 }, { a: 9 }]).avg('a'));
    assert.equal(result, 7);
  });

  it('`r.avg` should work with a field', async () => {
    result = await pool.run(r.avg([{ a: 2 }, { a: 10 }, { a: 9 }], 'a'));
    assert.equal(result, 7);
  });

  it('`min` should work ', async () => {
    result = await pool.run(r.expr([1, 2, 3]).min());
    assert.equal(result, 1);
  });

  it('`r.min` should work ', async () => {
    result = await pool.run(r.min([1, 2, 3]));
    assert.equal(result, 1);
  });

  it('`min` should work with a field', async () => {
    result = await pool.run(r.expr([{ a: 2 }, { a: 10 }, { a: 9 }]).min('a'));
    assert.deepEqual(result, { a: 2 });
  });

  it('`r.min` should work with a field', async () => {
    result = await pool.run(r.min([{ a: 2 }, { a: 10 }, { a: 9 }], 'a'));
    assert.deepEqual(result, { a: 2 });
  });

  it('`max` should work ', async () => {
    result = await pool.run(r.expr([1, 2, 3]).max());
    assert.equal(result, 3);
  });

  it('`r.max` should work ', async () => {
    result = await pool.run(r.max([1, 2, 3]));
    assert.equal(result, 3);
  });

  it('`distinct` should work', async () => {
    result = await pool.run(
      r
        .expr([1, 2, 3, 1, 2, 1, 3, 2, 2, 1, 4])
        .distinct()
        .orderBy((row) => row),
    );
    assert.deepEqual(result, [1, 2, 3, 4]);
  });

  it('`r.distinct` should work', async () => {
    result = await pool.run(
      r.distinct([1, 2, 3, 1, 2, 1, 3, 2, 2, 1, 4]).orderBy((row) => row),
    );
    assert.deepEqual(result, [1, 2, 3, 4]);
  });

  it('`distinct` should work with an index', async () => {
    result = await pool.run(
      r.db(dbName).table(tableName).distinct({ index: 'id' }).count(),
    );
    const result2 = await pool.run(r.db(dbName).table(tableName).count());
    assert.equal(result, result2);
  });
});
