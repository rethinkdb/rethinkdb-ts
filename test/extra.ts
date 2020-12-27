import assert from 'assert';
import { createRethinkdbMasterPool, r, deserialize, serialize } from '../src';
import { globals } from '../src/query-builder/globals';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('extra', () => {
  let dbName: string;
  let tableName: string;
  let pool: MasterConnectionPool;

  before(async () => {
    globals.backtraceType = 'function';
    pool = await createRethinkdbMasterPool(config);
    dbName = uuid();
    tableName = uuid(); // Big table to test partial sequence

    const result1 = await pool.run(r.dbCreate(dbName));
    assert.equal(result1.dbs_created, 1);

    const result2 = await pool.run(
      r.db(dbName).tableCreate(tableName)('tables_created'),
    );
    assert.deepEqual(result2, 1);
  });

  after(async () => {
    await pool.drain();
  });

  it('Change the default database on the fly in run', async () => {
    const result = await pool.run(r.tableList(), { db: dbName });
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

  it('serialize and deserialize should work', async () => {
    const result = serialize(r.expr(1).add(2));
    assert.equal(typeof result, 'string');
    const three = await pool.run(deserialize(result));
    assert.equal(three, 3);
  });
});
