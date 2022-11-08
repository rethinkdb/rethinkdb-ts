import assert from 'assert';
import { r } from '../src';
import config from './config';
import { uuid } from './util/common';

describe('stable', () => {
  let dbName: string;
  let tableName: string;
  let docs: any;

  before(async () => {
    await r.connectPool(config);
    dbName = uuid();
    tableName = uuid();
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  // Tests for callbacks
  it('Create db', async () => {
    const result = await r.dbCreate(dbName).run();
    assert.equal(result.dbs_created, 1);
  });

  it('Create table', async () => {
    const result = await r.db(dbName).tableCreate(tableName).run();
    assert.equal(result.tables_created, 1);
  });

  it('Insert', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .insert([
        { name: 'Michel', age: 27 },
        { name: 'Sophie', age: 23 },
      ])
      .run();
    assert.deepEqual(result.inserted, 2);
  });

  it('Table', async () => {
    const result = (docs = await r.db(dbName).table(tableName).run());
    assert.equal(result.length, 2);
  });

  it('get', async () => {
    const result = await r.db(dbName).table(tableName).get(docs[0].id).run();
    assert.deepEqual(result, docs[0]);
  });

  it('datum', async () => {
    const result = await r.expr({ foo: 'bar' }).run();
    assert.deepEqual(result, { foo: 'bar' });
  });

  it('date', async () => {
    const result = await r.now().run();
    assert(result instanceof Date);
  });
});
