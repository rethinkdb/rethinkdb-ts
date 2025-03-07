import assert from 'assert';
import { r } from '../src';
import { globals } from '../src/query-builder/globals';
import config from './config';
import { uuid } from './util/common';

describe('extra', () => {
  let dbName: string;
  let tableName: string;

  before(async () => {
    globals.backtraceType = 'function';
    await r.connectPool(config);
    dbName = uuid();
    tableName = uuid(); // Big table to test partial sequence

    const result1 = await r.dbCreate(dbName).run();
    assert.equal(result1.dbs_created, 1);

    const result2 = await r
      .db(dbName)
      .tableCreate(tableName)('tables_created')
      .run();
    assert.deepEqual(result2, 1);
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  it('Change the default database on the fly in run', async () => {
    const result = await r.tableList().run({ db: dbName });
    assert.deepEqual(result, [tableName]);
  });

  it('Anonymous function should throw if they return undefined', async () => {
    try {
      // tslint:disable-next-line
      r.expr(1).do(function () {});
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Anonymous function returned `undefined`. Did you forget a `return`? in:\nfunction () { }',
      );
    }
  });

  it('toString should work', () => {
    let result = r.expr(1).add(2).toString();
    assert.equal(result, 'r.expr(1).add(2)');

    result = r.expr(1).toString();
    assert.equal(result, 'r.expr(1)');
  });

  it('serialize and derialize should work', async () => {
    const result = r.serialize(r.expr(1).add(2));
    assert.equal(typeof result, 'string');
    const three = await r.deserialize(result).run();
    assert.equal(three, 3);
  });

  // it('await a query should work - 1', async () => {
  //   let result = await r.expr(1);
  //   assert.equal(result, 1);

  //   result = await r.expr(1).add(3);
  //   assert.equal(result, 4);
  // });

  // it('await a query should work - 2', async () => {
  //   try {
  //     await r.expr(1).add('foo');
  //     assert.fail('should throw');
  //   } catch (e) {
  //     assert(e.message.match(/Expected type NUMBER but found STRING/));
  //   }
  // });
});
