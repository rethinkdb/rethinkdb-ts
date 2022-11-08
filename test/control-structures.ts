import assert from 'assert';
import { r } from '../src';
import config from './config';
import { uuid } from './util/common';

describe('control structures', () => {
  let result: any;

  before(async () => {
    await r.connectPool(config);
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  it('`do` should work', async () => {
    result = await r
      .expr({ a: 1 })
      .do((doc) => {
        return doc('a');
      })
      .run();
    assert.equal(result, 1);
  });

  it('`r.do` should work', async () => {
    result = await r
      .do(1, 2, (a, b) => {
        return a;
      })
      .run();
    assert.equal(result, 1);

    result = await r
      .do(1, 2, (a, b) => {
        return b;
      })
      .run();
    assert.equal(result, 2);

    result = await r.do(3).run();
    assert.equal(result, 3);

    result = await r.expr(4).do().run();
    assert.equal(result, 4);

    result = await r.do(1, 2).run();
    assert.deepEqual(result, 2);

    result = await r.do(r.args([r.expr(3), r.expr(4)])).run();
    assert.deepEqual(result, 3);
  });

  it('`branch` should work', async () => {
    result = await r.branch(true, 1, 2).run();
    assert.equal(result, 1);

    result = await r.branch(false, 1, 2).run();
    assert.equal(result, 2);

    result = await r.expr(false).branch('foo', false, 'bar', 'lol').run();
    assert.equal(result, 'lol');

    result = await r.expr(true).branch('foo', false, 'bar', 'lol').run();
    assert.equal(result, 'foo');

    result = await r.expr(false).branch('foo', true, 'bar', 'lol').run();
    assert.equal(result, 'bar');
  });

  it('`branch` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      result = await r.branch().run();
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
      result = await r.branch(true).run();
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
      result = await r.branch(true, true).run();
    } catch (e) {
      assert(
        e.message.match(/^`r.branch` takes at least 3 arguments, 2 provided/),
      );
    }
  });

  it('`branch` is defined after a term', async () => {
    result = await r.expr(true).branch(2, 3).run();
    assert.equal(result, 2);
    result = await r.expr(false).branch(2, 3).run();
    assert.equal(result, 3);
  });

  it('`forEach` should work', async () => {
    const dbName = uuid();
    const tableName = uuid();

    result = await r.dbCreate(dbName).run();
    assert.equal(result.dbs_created, 1);

    result = await r.db(dbName).tableCreate(tableName).run();
    assert.equal(result.tables_created, 1);

    result = await r
      .expr([{ foo: 'bar' }, { foo: 'foo' }])
      .forEach((doc) => {
        return r.db(dbName).table(tableName).insert(doc);
      })
      .run();
    assert.equal(result.inserted, 2);
  });

  it('`forEach` should throw if not given a function', async () => {
    try {
      // @ts-ignore
      result = await r
        .expr([{ foo: 'bar' }, { foo: 'foo' }])
        .forEach()
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`forEach` takes 1 argument, 0 provided after/));
    }
  });

  it('`r.range(x)` should work', async () => {
    result = await r.range(10).run();
    assert.deepEqual(result, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it('`r.range(x, y)` should work', async () => {
    result = await r.range(3, 10).run();
    assert.deepEqual(result, [3, 4, 5, 6, 7, 8, 9]);
  });

  it('`r.range(1,2,3)` should throw - arity', async () => {
    try {
      // @ts-ignore
      result = await r.range(1, 2, 3).run();
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
      result = await r.range().run();
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`r.range` takes at least 1 argument, 0 provided/) !==
          null,
      );
    }
  });

  it('`default` should work', async () => {
    result = await r.expr({ a: 1 })('b').default('Hello').run();
    assert.equal(result, 'Hello');
  });
  it('`default` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      result = await r.expr({})('').default().run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`default` takes 1 argument, 0 provided after/));
    }
  });

  it('`r.js` should work', async () => {
    result = await r.js('1').run();
    assert.equal(result, 1);
  });

  it('`js` is not defined after a term', async () => {
    try {
      result = await r
        .expr(1)
        // @ts-ignore
        .js('foo')
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.js is not a function'));
    }
  });

  it('`js` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      result = await r.js().run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`r.js` takes at least 1 argument, 0 provided/));
    }
  });

  it('`coerceTo` should work', async () => {
    result = await r.expr(1).coerceTo('STRING').run();
    assert.equal(result, '1');
  });

  it('`coerceTo` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      result = await r.expr(1).coerceTo().run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^`coerceTo` takes 1 argument, 0 provided/));
    }
  });

  it('`typeOf` should work', async () => {
    result = await r.expr(1).typeOf().run();
    assert.equal(result, 'NUMBER');
  });

  it('`r.typeOf` should work', async () => {
    result = await r.typeOf(1).run();
    assert.equal(result, 'NUMBER');
  });

  it('`json` should work', async () => {
    result = await r.json(JSON.stringify({ a: 1 })).run();
    assert.deepEqual(result, { a: 1 });

    result = await r.json('{}').run();
    assert.deepEqual(result, {});
  });

  it('`json` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      result = await r.json().run();
      assert.fail('throw');
    } catch (e) {
      assert(e.message === '`r.json` takes 1 argument, 0 provided.');
    }
  });

  it('`json` is not defined after a term', async () => {
    try {
      result = await r
        .expr(1)
        // @ts-ignore
        .json('1')
        .run();
    } catch (e) {
      assert(e.message.endsWith('.json is not a function'));
    }
  });

  it('`toJSON` and `toJsonString` should work', async () => {
    result = await r.expr({ a: 1 }).toJSON().run();
    assert.equal(result, '{"a":1}');

    result = await r.expr({ a: 1 }).toJsonString().run();
    assert.equal(result, '{"a":1}');
  });

  it('`toJSON` should throw if an argument is provided', async () => {
    try {
      // @ts-ignore
      result = await r.expr({ a: 1 }).toJSON('foo').run();
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.match(/^`toJSON` takes 0 arguments, 1 provided/) !== null,
      );
    }
  });

  it('`args` should work', async () => {
    result = await r.args([10, 20, 30]).run();
    assert.deepEqual(result, [10, 20, 30]);

    result = await r
      .expr({ foo: 1, bar: 2, buzz: 3 })
      .pluck(r.args(['foo', 'buzz']))
      .run();
    assert.deepEqual(result, { foo: 1, buzz: 3 });
  });

  it('`args` should throw if an implicit var is passed inside', async () => {
    try {
      // @ts-ignore
      await r
        .table('foo')
        .eqJoin(r.args([r.row, r.table('bar')]))
        .run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.startsWith('Cannot use r.row in nested queries.'));
    }
  });

  it('`http` should work', async () => {
    const result = await r.http('http://google.com').run();
    assert.equal(typeof result, 'string');
  });

  it('`http` should work with options', async () => {
    const result = await r.http('http://google.com', { timeout: 60 }).run();
    assert.equal(typeof result, 'string');
  });

  it('`http` should throw with an unrecognized option', async () => {
    try {
      // @ts-ignore
      await r.http('http://google.com', { foo: 60 }).run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.startsWith('Unrecognized optional argument `foo` in:'));
    }
  });

  it('`r.uuid` should work', async () => {
    const result = await r.uuid().run();
    assert.equal(typeof result, 'string');
  });

  it('`r.uuid("foo")` should work', async () => {
    const result = await r.uuid('rethinkdbdash').run();
    assert.equal(result, '291a8039-bc4b-5472-9b2a-f133254e3283');
  });
});
