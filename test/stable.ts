import assert from 'assert';
import { connectPool, r } from '../src';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('stable', () => {
  let dbName: string;
  let tableName: string;
  let docs: any;
  let pool: MasterConnectionPool;

  before(async () => {
    pool = await connectPool([config.server], config.options);
    dbName = uuid();
    tableName = uuid();
  });

  after(async () => {
    await pool.drain();
  });

  // Tests for callbacks
  it('Create db', async () => {
    const result = await pool.run(r.dbCreate(dbName));
    assert.equal(result.dbs_created, 1);
  });

  it('Create table', async () => {
    const result = await pool.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result.tables_created, 1);
  });

  it('Insert', async () => {
    const result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert([
          { name: 'Michel', age: 27 },
          { name: 'Sophie', age: 23 },
        ]),
    );
    assert.deepEqual(result.inserted, 2);
  });

  it('Table', async () => {
    const result = (docs = await pool.run(r.db(dbName).table(tableName)));
    assert.equal(result.length, 2);
  });

  it('get', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).get(docs[0].id),
    );
    assert.deepEqual(result, docs[0]);
  });

  it('datum', async () => {
    const result = await pool.run(r.expr({ foo: 'bar' }));
    assert.deepEqual(result, { foo: 'bar' });
  });

  it('date', async () => {
    const result = await pool.run(r.now());
    assert(result instanceof Date);
  });
});
