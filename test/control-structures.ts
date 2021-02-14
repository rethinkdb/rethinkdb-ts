import assert from 'assert';
import { createRethinkdbMasterPool, r } from '../src';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('control structures', () => {
  let result: any;
  let pool: MasterConnectionPool;

  before(async () => {
    pool = await createRethinkdbMasterPool([config.server], config.options);
  });

  after(async () => {
    await pool.drain();
  });

  it('`do` should work', async () => {
    result = await pool.run(
      r.expr({ a: 1 }).do((doc) => {
        return doc('a');
      }),
    );
    assert.equal(result, 1);
  });

  it('`r.do` should work', async () => {
    result = await pool.run(
      r.do(1, 2, (a, b) => {
        return a;
      }),
    );
    assert.equal(result, 1);

    result = await pool.run(
      r.do(1, 2, (a, b) => {
        return b;
      }),
    );
    assert.equal(result, 2);

    result = await pool.run(r.do(3));
    assert.equal(result, 3);

    result = await pool.run(r.expr(4).do());
    assert.equal(result, 4);

    result = await pool.run(r.do(1, 2));
    assert.deepEqual(result, 2);

    result = await pool.run(r.do(r.args([r.expr(3), r.expr(4)])));
    assert.deepEqual(result, 3);
  });

  it('`branch` should work', async () => {
    result = await pool.run(r.branch(true, 1, 2));
    assert.equal(result, 1);

    result = await pool.run(r.branch(false, 1, 2));
    assert.equal(result, 2);

    result = await pool.run(r.expr(false).branch('foo', false, 'bar', 'lol'));
    assert.equal(result, 'lol');

    result = await pool.run(r.expr(true).branch('foo', false, 'bar', 'lol'));
    assert.equal(result, 'foo');

    result = await pool.run(r.expr(false).branch('foo', true, 'bar', 'lol'));
    assert.equal(result, 'bar');
  });

  it('`branch` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.branch());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`r.branch` takes at least 3 arguments, 0 provided/),
      );
    }
  });

  it('`branch` should throw if just one argument has been given', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.branch(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`r.branch` takes at least 3 arguments, 1 provided/),
      );
    }
  });

  it('`branch` should throw if just two arguments have been given', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.branch(true, true));
    } catch (e) {
      assert(
        e.message.match(/^`r.branch` takes at least 3 arguments, 2 provided/),
      );
    }
  });

  it('`branch` is defined after a term', async () => {
    result = await pool.run(r.expr(true).branch(2, 3));
    assert.equal(result, 2);
    result = await pool.run(r.expr(false).branch(2, 3));
    assert.equal(result, 3);
  });

  it('`forEach` should work', async () => {
    debugger;
    const dbName = uuid();
    const tableName = uuid();

    result = await pool.run(r.dbCreate(dbName));
    assert.equal(result.dbs_created, 1);

    result = await pool.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result.tables_created, 1);

    result = await pool.run(
      r.expr([{ foo: 'bar' }, { foo: 'foo' }]).forEach((doc) => {
        return r.db(dbName).table(tableName).insert(doc);
      }),
    );
    assert.equal(result.inserted, 2);
  });

  it('`forEach` should throw if not given a function', async () => {
    try {
      // @ts-ignore
      result = await pool.run(
        r.expr([{ foo: 'bar' }, { foo: 'foo' }]).forEach(),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`forEach` takes 1 argument, 0 provided after/));
    }
  });

  it('`r.range(x)` should work', async () => {
    result = await pool.run(r.range(10));
    assert.deepEqual(result, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it('`r.range(x, y)` should work', async () => {
    result = await pool.run(r.range(3, 10));
    assert.deepEqual(result, [3, 4, 5, 6, 7, 8, 9]);
  });

  it('`r.range(1,2,3)` should throw - arity', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.range(1, 2, 3));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`r.range` takes at most 2 arguments, 3 provided/) !==
          null,
      );
    }
  });

  it('`r.range()` should throw - arity', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.range());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`r.range` takes at least 1 argument, 0 provided/) !==
          null,
      );
    }
  });

  it('`default` should work', async () => {
    result = await pool.run(r.expr({ a: 1 })('b').default('Hello'));
    assert.equal(result, 'Hello');
  });
  it('`default` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.expr({})('').default());
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`default` takes 1 argument, 0 provided after/));
    }
  });

  it('`r.js` should work', async () => {
    result = await pool.run(r.js('1'));
    assert.equal(result, 1);
  });

  it('`js` is not defined after a term', async () => {
    try {
      result = await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .js('foo'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.js is not a function'));
    }
  });

  it('`js` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.js());
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`r.js` takes at least 1 argument, 0 provided/));
    }
  });

  it('`coerceTo` should work', async () => {
    result = await pool.run(r.expr(1).coerceTo('STRING'));
    assert.equal(result, '1');
  });

  it('`coerceTo` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.expr(1).coerceTo());
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`coerceTo` takes 1 argument, 0 provided/));
    }
  });

  it('`typeOf` should work', async () => {
    result = await pool.run(r.expr(1).typeOf());
    assert.equal(result, 'NUMBER');
  });

  it('`r.typeOf` should work', async () => {
    result = await pool.run(r.typeOf(1));
    assert.equal(result, 'NUMBER');
  });

  it('`json` should work', async () => {
    result = await pool.run(r.json(JSON.stringify({ a: 1 })));
    assert.deepEqual(result, { a: 1 });

    result = await pool.run(r.json('{}'));
    assert.deepEqual(result, {});
  });

  it('`json` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.json());
      assert.fail('throw');
    } catch (e) {
      assert(e.message === '`r.json` takes 1 argument, 0 provided.');
    }
  });

  it('`json` is not defined after a term', async () => {
    try {
      result = await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .json('1'),
      );
    } catch (e) {
      assert(e.message.endsWith('.json is not a function'));
    }
  });

  it('`toJSON` and `toJsonString` should work', async () => {
    result = await pool.run(r.expr({ a: 1 }).toJSON());
    assert.equal(result, '{"a":1}');

    result = await pool.run(r.expr({ a: 1 }).toJsonString());
    assert.equal(result, '{"a":1}');
  });

  it('`toJSON` should throw if an argument is provided', async () => {
    try {
      // @ts-ignore
      result = await pool.run(r.expr({ a: 1 }).toJSON('foo'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`toJSON` takes 0 arguments, 1 provided/) !== null,
      );
    }
  });

  it('`args` should work', async () => {
    result = await pool.run(r.args([10, 20, 30]));
    assert.deepEqual(result, [10, 20, 30]);

    result = await pool.run(
      r.expr({ foo: 1, bar: 2, buzz: 3 }).pluck(r.args(['foo', 'buzz'])),
    );
    assert.deepEqual(result, { foo: 1, buzz: 3 });
  });

  it('`args` should throw if an implicit var is passed inside', async () => {
    try {
      // @ts-ignore
      await pool.run(r.table('foo').eqJoin(r.args([r.row, r.table('bar')])));
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.startsWith('Cannot use r.row in nested queries.'));
    }
  });

  it('`http` should work', async () => {
    const result = await pool.run(r.http('http://google.com'));
    assert.equal(typeof result, 'string');
  });

  it('`http` should work with options', async () => {
    const result = await pool.run(r.http('http://google.com', { timeout: 60 }));
    assert.equal(typeof result, 'string');
  });

  it('`http` should throw with an unrecognized option', async () => {
    try {
      // @ts-ignore
      await pool.run(r.http('http://google.com', { foo: 60 }));
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.startsWith('Unrecognized optional argument `foo` in:'));
    }
  });

  it('`r.uuid` should work', async () => {
    const result = await pool.run(r.uuid());
    assert.equal(typeof result, 'string');
  });

  it('`r.uuid("foo")` should work', async () => {
    const result = await pool.run(r.uuid('rethinkdbdash'));
    assert.equal(result, '291a8039-bc4b-5472-9b2a-f133254e3283');
  });
});
