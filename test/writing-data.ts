import assert from 'assert';
import { connectPool, r } from '../src';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('writing data', () => {
  let dbName: string;
  let tableName: string;
  let pool: MasterConnectionPool;

  before(async () => {
    pool = await connectPool([config.server], config.options);
    dbName = uuid();
    tableName = uuid();

    const result1 = await pool.run(r.dbCreate(dbName));
    assert.equal(result1.dbs_created, 1);

    const result2 = await pool.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result2.tables_created, 1);
  });

  after(async () => {
    await pool.drain();
  });

  it('`insert` should work - single insert`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).insert({}));
    assert.equal(result.inserted, 1);

    result = await pool.run(
      r.db(dbName).table(tableName).insert(Array(100).fill({})),
    );
    assert.equal(result.inserted, 100);
  });

  it('`insert` should work - batch insert 1`', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).insert([{}, {}]),
    );
    assert.equal(result.inserted, 2);
  });

  it('`insert` should work - batch insert 2`', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).insert(Array(100).fill({})),
    );
    assert.equal(result.inserted, 100);
  });

  it('`insert` should work - with returnChanges true`', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).insert({}, { returnChanges: true }),
    );
    assert.equal(result.inserted, 1);
    assert(result.changes[0].new_val);
    assert.equal(result.changes[0].old_val, null);
  });

  it('`insert` should work - with returnChanges false`', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).insert({}, { returnChanges: false }),
    );
    assert.equal(result.inserted, 1);
    assert.equal(result.changes, undefined);
  });

  it('`insert` should work - with durability soft`', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).insert({}, { durability: 'soft' }),
    );
    assert.equal(result.inserted, 1);
  });

  it('`insert` should work - with durability hard`', async () => {
    const result = await pool.run(
      r.db(dbName).table(tableName).insert({}, { durability: 'hard' }),
    );
    assert.equal(result.inserted, 1);
  });

  it('`insert` should work - testing conflict`', async () => {
    let result = await pool.run(
      r.db(dbName).table(tableName).insert({}, { conflict: 'update' }),
    );
    assert.equal(result.inserted, 1);

    const pk = result.generated_keys[0];

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert({ id: pk, val: 1 }, { conflict: 'update' }),
    );
    assert.equal(result.replaced, 1);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert({ id: pk, val: 2 }, { conflict: 'replace' }),
    );
    assert.equal(result.replaced, 1);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert({ id: pk, val: 3 }, { conflict: 'error' }),
    );
    assert.equal(result.errors, 1);
  });

  it('`insert` should throw if no argument is given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).insert());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`insert\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`insert` work with dates - 1', async () => {
    const result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert({ name: 'Michel', age: 27, birthdate: new Date() }),
    );
    assert.deepEqual(result.inserted, 1);
  });

  it('`insert` work with dates - 2', async () => {
    const result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert([
          {
            name: 'Michel',
            age: 27,
            birthdate: new Date(),
          },
          { name: 'Sophie', age: 23 },
        ]),
    );
    assert.deepEqual(result.inserted, 2);
  });

  it('`insert` work with dates - 3', async () => {
    const result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert({
          field: 'test',
          field2: { nested: 'test' },
          date: new Date(),
        }),
    );
    assert.deepEqual(result.inserted, 1);
  });

  it('`insert` work with dates - 4', async () => {
    const result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert({
          field: 'test',
          field2: { nested: 'test' },
          date: r.now(),
        }),
    );
    assert.deepEqual(result.inserted, 1);
  });

  it('`insert` should throw if non valid option', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .insert({}, { nonValidKey: true }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.startsWith(
          'Unrecognized optional argument `non_valid_key` in:',
        ),
      );
    }
  });

  it('`insert` with a conflict method', async () => {
    let result = await pool.run(
      r.db(dbName).table(tableName).insert({
        count: 7,
      }),
    );
    const savedId = result.generated_keys[0];
    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert(
          {
            id: savedId,
            count: 10,
          },
          {
            conflict: (id, oldDoc, newDoc) =>
              newDoc.merge({
                count: newDoc('count').add(oldDoc('count')),
              }),
          },
        ),
    );
    assert.equal(result.replaced, 1);
    result = await pool.run(r.db(dbName).table(tableName).get(savedId));
    assert.deepEqual(result, {
      id: savedId,
      count: 17,
    });
  });

  it('`replace` should throw if no argument is given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).replace());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`replace\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`replace` should throw if non valid option', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .replace({}, { nonValidKey: true }),
      );
    } catch (e) {
      assert(
        e.message.startsWith(
          'Unrecognized optional argument `non_valid_key` in:',
        ),
      );
    }
  });

  it('`delete` should work`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result.deleted > 0);

    result = await pool.run(r.db(dbName).table(tableName).delete());
    assert.equal(result.deleted, 0);
  });

  it('`delete` should work -- soft durability`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(r.db(dbName).table(tableName).insert({}));
    assert(result);

    result = await pool.run(
      r.db(dbName).table(tableName).delete({ durability: 'soft' }),
    );
    assert.equal(result.deleted, 1);

    result = await pool.run(r.db(dbName).table(tableName).insert({}));
    assert(result);

    result = await pool.run(r.db(dbName).table(tableName).delete());
    assert.equal(result.deleted, 1);
  });

  it('`delete` should work -- hard durability`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(r.db(dbName).table(tableName).insert({}));
    assert(result);

    result = await pool.run(
      r.db(dbName).table(tableName).delete({ durability: 'hard' }),
    );
    assert.equal(result.deleted, 1);

    result = await pool.run(r.db(dbName).table(tableName).insert({}));
    assert(result);

    result = await pool.run(r.db(dbName).table(tableName).delete());
    assert.equal(result.deleted, 1);
  });

  it('`delete` should throw if non valid option', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .delete({ nonValidKey: true }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.startsWith(
          'Unrecognized optional argument `non_valid_key` in:',
        ),
      );
    }
  });

  it('`update` should work - point update`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(r.db(dbName).table(tableName).insert({ id: 1 }));
    assert(result);

    result = await pool.run(
      r.db(dbName).table(tableName).get(1).update({ foo: 'bar' }),
    );
    assert.equal(result.replaced, 1);

    result = await pool.run(r.db(dbName).table(tableName).get(1));
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`update` should work - range update`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert([{ id: 1 }, { id: 2 }]),
    );
    assert(result);

    result = await pool.run(
      r.db(dbName).table(tableName).update({ foo: 'bar' }),
    );
    assert.equal(result.replaced, 2);

    result = await pool.run(r.db(dbName).table(tableName).get(1));
    assert.deepEqual(result, { id: 1, foo: 'bar' });
    result = await pool.run(r.db(dbName).table(tableName).get(2));
    assert.deepEqual(result, { id: 2, foo: 'bar' });
  });

  it('`update` should work - soft durability`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(r.db(dbName).table(tableName).insert({ id: 1 }));
    assert(result);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .get(1)
        .update({ foo: 'bar' }, { durability: 'soft' }),
    );
    assert.equal(result.replaced, 1);

    result = await pool.run(r.db(dbName).table(tableName).get(1));
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`update` should work - hard durability`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(r.db(dbName).table(tableName).insert({ id: 1 }));
    assert(result);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .get(1)
        .update({ foo: 'bar' }, { durability: 'hard' }),
    );
    assert.equal(result.replaced, 1);

    result = await pool.run(r.db(dbName).table(tableName).get(1));
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`update` should work - returnChanges true', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(r.db(dbName).table(tableName).insert({ id: 1 }));
    assert(result);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .get(1)
        .update({ foo: 'bar' }, { returnChanges: true }),
    );
    assert.equal(result.replaced, 1);
    assert.deepEqual(result.changes[0].new_val, { id: 1, foo: 'bar' });
    assert.deepEqual(result.changes[0].old_val, { id: 1 });

    result = await pool.run(r.db(dbName).table(tableName).get(1));
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`update` should work - returnChanges false`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(r.db(dbName).table(tableName).insert({ id: 1 }));
    assert(result);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .get(1)
        .update({ foo: 'bar' }, { returnChanges: false }),
    );
    assert.equal(result.replaced, 1);
    assert.equal(result.changes, undefined);
    assert.equal(result.changes, undefined);

    result = await pool.run(r.db(dbName).table(tableName).get(1));
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`update` should throw if no argument is given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.db(dbName).table(tableName).update());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`update\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`update` should throw if non valid option', async () => {
    try {
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          // @ts-ignore
          .update({}, { nonValidKey: true }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.startsWith(
          'Unrecognized optional argument `non_valid_key` in:',
        ),
      );
    }
  });

  it('`replace` should work - point replace`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(r.db(dbName).table(tableName).insert({ id: 1 }));
    assert(result);

    result = await pool.run(
      r.db(dbName).table(tableName).get(1).replace({ id: 1, foo: 'bar' }),
    );
    assert.equal(result.replaced, 1);

    result = await pool.run(r.db(dbName).table(tableName).get(1));
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`replace` should work - range replace`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert([{ id: 1 }, { id: 2 }]),
    );
    assert(result);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .replace((row) => row.merge({ foo: 'bar' })),
    );
    assert.equal(result.replaced, 2);

    result = await pool.run(r.db(dbName).table(tableName).get(1));
    assert.deepEqual(result, { id: 1, foo: 'bar' });

    result = await pool.run(r.db(dbName).table(tableName).get(2));
    assert.deepEqual(result, { id: 2, foo: 'bar' });
  });

  it('`replace` should work - soft durability`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(r.db(dbName).table(tableName).insert({ id: 1 }));
    assert(result);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .get(1)
        .replace({ id: 1, foo: 'bar' }, { durability: 'soft' }),
    );
    assert.equal(result.replaced, 1);

    result = await pool.run(r.db(dbName).table(tableName).get(1));
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`replace` should work - hard durability`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(r.db(dbName).table(tableName).insert({ id: 1 }));
    assert(result);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .get(1)
        .replace({ id: 1, foo: 'bar' }, { durability: 'hard' }),
    );
    assert.equal(result.replaced, 1);

    result = await pool.run(r.db(dbName).table(tableName).get(1));
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`replace` should work - returnChanges true', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(r.db(dbName).table(tableName).insert({ id: 1 }));
    assert(result);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .get(1)
        .replace({ id: 1, foo: 'bar' }, { returnChanges: true }),
    );
    assert.equal(result.replaced, 1);
    assert.deepEqual(result.changes[0].new_val, { id: 1, foo: 'bar' });
    assert.deepEqual(result.changes[0].old_val, { id: 1 });

    result = await pool.run(r.db(dbName).table(tableName).get(1));
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`replace` should work - returnChanges false`', async () => {
    let result = await pool.run(r.db(dbName).table(tableName).delete());
    assert(result);
    result = await pool.run(r.db(dbName).table(tableName).insert({ id: 1 }));
    assert(result);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .get(1)
        .replace({ id: 1, foo: 'bar' }, { returnChanges: false }),
    );
    assert.equal(result.replaced, 1);
    assert.equal(result.changes, undefined);
    assert.equal(result.changes, undefined);

    result = await pool.run(r.db(dbName).table(tableName).get(1));
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });
});
