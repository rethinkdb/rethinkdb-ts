import assert from 'assert';
import { connectPool, r } from '../src';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('manipulating tables', () => {
  let dbName: string;
  let pool: MasterConnectionPool;

  before(async () => {
    pool = await connectPool([config.server], config.options);
    // delete all but the system dbs
    await pool.run(
      r
        .dbList()
        .filter((db) => r.expr(['rethinkdb', 'test']).contains(db).not())
        .forEach((db) => r.dbDrop(db)),
    );
    dbName = uuid(); // export to the global scope
    const result = await pool.run(r.dbCreate(dbName));
    assert.equal(result.dbs_created, 1);
  });

  after(async () => {
    // delete all but the system dbs
    await pool.run(
      r
        .dbList()
        .filter((db) => r.expr(['rethinkdb', 'test']).contains(db).not())
        .forEach((db) => r.dbDrop(db)),
    );
    await pool.drain();
  });

  it('`tableList` should return a cursor', async () => {
    const result = await pool.run(r.db(dbName).tableList());
    assert(Array.isArray(result));
  });

  it('`tableList` should show the table we created', async () => {
    const tableName = uuid(); // export to the global scope

    const result1 = await pool.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result1.tables_created, 1);

    const result2 = await pool.run(r.db(dbName).tableList());
    assert(Array.isArray(result2));
    assert.equal(
      tableName,
      result2.find((name) => name === tableName),
    );
  });

  it('`tableCreate` should create a table', async () => {
    const tableName = uuid(); // export to the global scope

    const result = await pool.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result.tables_created, 1);
  });

  it('`tableCreate` should create a table -- primaryKey', async () => {
    const tableName = uuid();

    const result1 = await pool.run(
      r.db(dbName).tableCreate(tableName, { primaryKey: 'foo' }),
    );
    assert.equal(result1.tables_created, 1);

    const result2 = await pool.run(r.db(dbName).table(tableName).info());
    assert.equal(result2.primary_key, 'foo');
  });

  it('`tableCreate` should create a table -- all args', async () => {
    const tableName = uuid();

    const result1 = await pool.run(
      r
        .db(dbName)
        .tableCreate(tableName, { durability: 'soft', primaryKey: 'foo' }),
    );
    assert.equal(result1.tables_created, 1); // We can't really check other parameters...

    const result2 = await pool.run(r.db(dbName).table(tableName).info());
    assert.equal(result2.primary_key, 'foo');
  });

  it('`tableCreate` should throw -- non valid args', async () => {
    try {
      const tableName = uuid();

      await pool.run(
        r
          .db(dbName)
          // @ts-ignore
          .tableCreate(tableName, { nonValidArg: true }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.startsWith(
          'Unrecognized optional argument `non_valid_arg` in',
        ),
      );
    }
  });

  it('`tableCreate` should throw if no argument is given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).tableCreate());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`tableCreate\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}")\n`,
      );
    }
  });

  it('`tableCreate` should throw is the name contains special char', async () => {
    try {
      await pool.run(r.db(dbName).tableCreate('-_-'));
    } catch (e) {
      assert(
        e.message.match(/Table name `-_-` invalid \(Use A-Za-z0-9_ only\)/),
      );
    }
  });

  it('`tableDrop` should drop a table', async () => {
    const tableName = uuid();

    const result1 = await pool.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result1.tables_created, 1);

    const result2 = await pool.run(r.db(dbName).tableList());
    assert(Array.isArray(result2));
    assert.equal(
      result2.find((name) => name === tableName),
      tableName,
    );

    const result3 = await pool.run(r.db(dbName).tableDrop(tableName));
    assert.equal(result3.tables_dropped, 1);

    const result4 = await pool.run(r.db(dbName).tableList());
    assert(result4.find((name) => name === tableName) === undefined);
  });

  it('`tableDrop` should throw if no argument is given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).tableDrop());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`tableDrop\` takes 1 argument, 0 provided after:\nr.db("${dbName}")\n`,
      );
    }
  });

  describe('indices', () => {
    const dbName1 = uuid();
    const tableName1 = uuid();

    before(async () => {
      const result1 = await pool.run(r.dbCreate(dbName1));
      assert.equal(result1.dbs_created, 1);

      const result2 = await pool.run(r.db(dbName1).tableCreate(tableName1));
      assert.equal(result2.tables_created, 1);
    });

    it('index operations', async () => {
      const result1 = await pool.run(
        r.db(dbName1).table(tableName1).indexCreate('newField'),
      );
      assert.deepEqual(result1, { created: 1 });

      const result2 = await pool.run(
        r.db(dbName1).table(tableName1).indexList(),
      );
      assert.deepEqual(result2, ['newField']);

      const result3 = await pool.run(
        r.db(dbName1).table(tableName1).indexWait().pluck('index', 'ready'),
      );
      assert.deepEqual(result3, [{ index: 'newField', ready: true }]);
      const result4 = await pool.run(
        r.db(dbName1).table(tableName1).indexStatus().pluck('index', 'ready'),
      );
      assert.deepEqual(result4, [{ index: 'newField', ready: true }]);

      const result5 = await pool.run(
        r.db(dbName1).table(tableName1).indexDrop('newField'),
      );
      assert.deepEqual(result5, { dropped: 1 });

      const result6 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .indexCreate('field1', (doc) => doc('field1')),
      );
      assert.deepEqual(result6, { created: 1 });

      const result7 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .indexWait('field1')
          .pluck('index', 'ready'),
      );
      assert.deepEqual(result7, [{ index: 'field1', ready: true }]);
      const result8 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .indexStatus('field1')
          .pluck('index', 'ready'),
      );
      assert.deepEqual(result8, [{ index: 'field1', ready: true }]);

      const result9 = await pool.run(
        r.db(dbName1).table(tableName1).indexDrop('field1'),
      );
      assert.deepEqual(result9, { dropped: 1 });
    });

    it('`indexCreate` should work with options', async () => {
      let result = await pool.run(
        r.db(dbName1).table(tableName1).indexCreate('foo', { multi: true }),
      );
      assert.deepEqual(result, { created: 1 });

      result = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .indexCreate('foo1', (row) => row('foo'), { multi: true }),
      );
      assert.deepEqual(result, { created: 1 });

      result = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .indexCreate('foo2', (doc) => doc('foo'), { multi: true }),
      );
      assert.deepEqual(result, { created: 1 });

      await pool.run(r.db(dbName1).table(tableName1).indexWait());

      const result1 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .insert({ foo: ['bar1', 'bar2'], buzz: 1 }),
      );
      assert.equal(result1.inserted, 1);

      const result2 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .insert({ foo: ['bar1', 'bar3'], buzz: 2 }),
      );
      assert.equal(result2.inserted, 1);

      const result3 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .getAll('bar1', { index: 'foo' })
          .count(),
      );
      assert.equal(result3, 2);

      const result4 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .getAll('bar1', { index: 'foo1' })
          .count(),
      );
      assert.equal(result4, 2);
      const result5 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .getAll('bar1', { index: 'foo2' })
          .count(),
      );
      assert.equal(result5, 2);

      const result6 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .getAll('bar2', { index: 'foo' })
          .count(),
      );
      assert.equal(result6, 1);
      const result7 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .getAll('bar2', { index: 'foo1' })
          .count(),
      );
      assert.equal(result7, 1);
      const result8 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .getAll('bar2', { index: 'foo2' })
          .count(),
      );
      assert.equal(result8, 1);

      const result9 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .getAll('bar3', { index: 'foo' })
          .count(),
      );
      assert.equal(result9, 1);
      const result10 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .getAll('bar3', { index: 'foo1' })
          .count(),
      );
      assert.equal(result10, 1);
      const result11 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .getAll('bar3', { index: 'foo2' })
          .count(),
      );
      assert.equal(result11, 1);

      // Test when the function is wrapped in an array
      const result12 = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .indexCreate('buzz', (row) => [row('buzz')]),
      );
      assert.deepEqual(result12, { created: 1 });

      await pool.run(r.db(dbName1).table(tableName1).indexWait());

      const result13 = await pool.run(
        r.db(dbName1).table(tableName1).getAll([1], { index: 'buzz' }).count(),
      );
      assert.equal(result13, 1);
    });

    it('`indexCreate` should throw if no argument is passed', async () => {
      try {
        // @ts-ignore
        await pool.run(r.db(dbName1).table(tableName1).indexCreate());
        assert.fail('should throw');
      } catch (e) {
        assert.equal(
          e.message,
          `\`indexCreate\` takes at least 1 argument, 0 provided after:\nr.db("${dbName1}").table("${tableName1}")\n`,
        );
      }
    });

    it('`indexDrop` should throw if no argument is passed', async () => {
      try {
        // @ts-ignore
        await pool.run(r.db(dbName1).table(tableName1).indexDrop());
        assert.fail('should throw');
      } catch (e) {
        assert.equal(
          e.message,
          `\`indexDrop\` takes 1 argument, 0 provided after:\nr.db("${dbName1}").table("${tableName1}")\n`,
        );
      }
    });

    it('`indexRename` should work', async () => {
      const toRename = uuid();
      const renamed = uuid();
      const existing = uuid();

      let result = await pool.run(
        r.db(dbName1).table(tableName1).indexCreate(toRename),
      );
      assert.deepEqual(result, { created: 1 });

      result = await pool.run(
        r.db(dbName1).table(tableName1).indexRename(toRename, renamed),
      );
      assert.deepEqual(result, { renamed: 1 });

      result = await pool.run(
        r.db(dbName1).table(tableName1).indexCreate(existing),
      );
      assert.deepEqual(result, { created: 1 });

      result = await pool.run(
        r
          .db(dbName1)
          .table(tableName1)
          .indexRename(renamed, existing, { overwrite: true }),
      );
      assert.deepEqual(result, { renamed: 1 });
    });

    it('`indexRename` should not overwrite an index if not specified', async () => {
      try {
        const name = uuid();
        const otherName = uuid();

        let result = await pool.run(
          r.db(dbName1).table(tableName1).indexCreate(name),
        );
        assert.deepEqual(result, { created: 1 });

        result = await pool.run(
          r.db(dbName1).table(tableName1).indexCreate(otherName),
        );
        assert.deepEqual(result, { created: 1 });

        await pool.run(
          r.db(dbName1).table(tableName1).indexRename(otherName, name),
        );
        assert.fail('should throw');
      } catch (e) {
        assert(e.message.match(/^Index `.*` already exists on table/));
      }
    });

    it('`indexRename` should throw -- non valid args', async () => {
      try {
        await pool.run(
          r
            .db(dbName1)
            .table(tableName1)
            // @ts-ignore
            .indexRename('foo', 'bar', { nonValidArg: true }),
        );
        assert.fail('should throw');
      } catch (e) {
        assert(
          e.message.startsWith(
            'Unrecognized optional argument `non_valid_arg` in',
          ),
        );
      }
    });
  });
});
