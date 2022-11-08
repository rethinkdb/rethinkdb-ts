import assert from 'assert';
import { r } from '../src';
import config from './config';
import { uuid } from './util/common';

describe('writing data', () => {
  let dbName: string;
  let tableName: string;

  before(async () => {
    await r.connectPool(config);
    dbName = uuid();
    tableName = uuid();

    const result1 = await r.dbCreate(dbName).run();
    assert.equal(result1.dbs_created, 1);

    const result2 = await r.db(dbName).tableCreate(tableName).run();
    assert.equal(result2.tables_created, 1);
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  it('`insert` should work - single insert`', async () => {
    let result = await r.db(dbName).table(tableName).insert({}).run();
    assert.equal(result.inserted, 1);

    result = await r
      .db(dbName)
      .table(tableName)
      .insert(Array(100).fill({}))
      .run();
    assert.equal(result.inserted, 100);
  });

  it('`insert` should work - batch insert 1`', async () => {
    const result = await r.db(dbName).table(tableName).insert([{}, {}]).run();
    assert.equal(result.inserted, 2);
  });

  it('`insert` should work - batch insert 2`', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .insert(Array(100).fill({}))
      .run();
    assert.equal(result.inserted, 100);
  });

  it('`insert` should work - with returnChanges true`', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .insert({}, { returnChanges: true })
      .run();
    assert.equal(result.inserted, 1);
    assert(result.changes[0].new_val);
    assert.equal(result.changes[0].old_val, null);
  });

  it('`insert` should work - with returnChanges false`', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .insert({}, { returnChanges: false })
      .run();
    assert.equal(result.inserted, 1);
    assert.equal(result.changes, undefined);
  });

  it('`insert` should work - with durability soft`', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .insert({}, { durability: 'soft' })
      .run();
    assert.equal(result.inserted, 1);
  });

  it('`insert` should work - with durability hard`', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .insert({}, { durability: 'hard' })
      .run();
    assert.equal(result.inserted, 1);
  });

  it('`insert` should work - testing conflict`', async () => {
    let result = await r
      .db(dbName)
      .table(tableName)
      .insert({}, { conflict: 'update' })
      .run();
    assert.equal(result.inserted, 1);

    const pk = result.generated_keys[0];

    result = await r
      .db(dbName)
      .table(tableName)
      .insert({ id: pk, val: 1 }, { conflict: 'update' })
      .run();
    assert.equal(result.replaced, 1);

    result = await r
      .db(dbName)
      .table(tableName)
      .insert({ id: pk, val: 2 }, { conflict: 'replace' })
      .run();
    assert.equal(result.replaced, 1);

    result = await r
      .db(dbName)
      .table(tableName)
      .insert({ id: pk, val: 3 }, { conflict: 'error' })
      .run();
    assert.equal(result.errors, 1);
  });

  it('`insert` should throw if no argument is given', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).insert().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `\`insert\` takes at least 1 argument, 0 provided after:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  it('`insert` work with dates - 1', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .insert({ name: 'Michel', age: 27, birthdate: new Date() })
      .run();
    assert.deepEqual(result.inserted, 1);
  });

  it('`insert` work with dates - 2', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .insert([
        {
          name: 'Michel',
          age: 27,
          birthdate: new Date(),
        },
        { name: 'Sophie', age: 23 },
      ])
      .run();
    assert.deepEqual(result.inserted, 2);
  });

  it('`insert` work with dates - 3', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .insert({
        field: 'test',
        field2: { nested: 'test' },
        date: new Date(),
      })
      .run();
    assert.deepEqual(result.inserted, 1);
  });

  it('`insert` work with dates - 4', async () => {
    const result = await r
      .db(dbName)
      .table(tableName)
      .insert({
        field: 'test',
        field2: { nested: 'test' },
        date: r.now(),
      })
      .run();
    assert.deepEqual(result.inserted, 1);
  });

  it('`insert` should throw if non valid option', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .insert({}, { nonValidKey: true })
        .run();
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
    let result = await r
      .db(dbName)
      .table(tableName)
      .insert({
        count: 7,
      })
      .run();
    const savedId = result.generated_keys[0];
    result = await r
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
      )
      .run();
    assert.equal(result.replaced, 1);
    result = await r.db(dbName).table(tableName).get(savedId).run();
    assert.deepEqual(result, {
      id: savedId,
      count: 17,
    });
  });

  it('`replace` should throw if no argument is given', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).replace().run();
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
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .replace({}, { nonValidKey: true })
        .run();
    } catch (e) {
      assert(
        e.message.startsWith(
          'Unrecognized optional argument `non_valid_key` in:',
        ),
      );
    }
  });

  it('`delete` should work`', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result.deleted > 0);

    result = await r.db(dbName).table(tableName).delete().run();
    assert.equal(result.deleted, 0);
  });

  it('`delete` should work -- soft durability`', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r.db(dbName).table(tableName).insert({}).run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .delete({ durability: 'soft' })
      .run();
    assert.equal(result.deleted, 1);

    result = await r.db(dbName).table(tableName).insert({}).run();
    assert(result);

    result = await r.db(dbName).table(tableName).delete().run();
    assert.equal(result.deleted, 1);
  });

  it('`delete` should work -- hard durability`', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r.db(dbName).table(tableName).insert({}).run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .delete({ durability: 'hard' })
      .run();
    assert.equal(result.deleted, 1);

    result = await r.db(dbName).table(tableName).insert({}).run();
    assert(result);

    result = await r.db(dbName).table(tableName).delete().run();
    assert.equal(result.deleted, 1);
  });

  it('`delete` should throw if non valid option', async () => {
    try {
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .delete({ nonValidKey: true })
        .run();
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
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r.db(dbName).table(tableName).insert({ id: 1 }).run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .get(1)
      .update({ foo: 'bar' })
      .run();
    assert.equal(result.replaced, 1);

    result = await r.db(dbName).table(tableName).get(1).run();
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`update` should work - range update`', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r
      .db(dbName)
      .table(tableName)
      .insert([{ id: 1 }, { id: 2 }])
      .run();
    assert(result);

    result = await r.db(dbName).table(tableName).update({ foo: 'bar' }).run();
    assert.equal(result.replaced, 2);

    result = await r.db(dbName).table(tableName).get(1).run();
    assert.deepEqual(result, { id: 1, foo: 'bar' });
    result = await r.db(dbName).table(tableName).get(2).run();
    assert.deepEqual(result, { id: 2, foo: 'bar' });
  });

  it('`update` should work - soft durability`', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r.db(dbName).table(tableName).insert({ id: 1 }).run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .get(1)
      .update({ foo: 'bar' }, { durability: 'soft' })
      .run();
    assert.equal(result.replaced, 1);

    result = await r.db(dbName).table(tableName).get(1).run();
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`update` should work - hard durability`', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r.db(dbName).table(tableName).insert({ id: 1 }).run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .get(1)
      .update({ foo: 'bar' }, { durability: 'hard' })
      .run();
    assert.equal(result.replaced, 1);

    result = await r.db(dbName).table(tableName).get(1).run();
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`update` should work - returnChanges true', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r.db(dbName).table(tableName).insert({ id: 1 }).run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .get(1)
      .update({ foo: 'bar' }, { returnChanges: true })
      .run();
    assert.equal(result.replaced, 1);
    assert.deepEqual(result.changes[0].new_val, { id: 1, foo: 'bar' });
    assert.deepEqual(result.changes[0].old_val, { id: 1 });

    result = await r.db(dbName).table(tableName).get(1).run();
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`update` should work - returnChanges false`', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r.db(dbName).table(tableName).insert({ id: 1 }).run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .get(1)
      .update({ foo: 'bar' }, { returnChanges: false })
      .run();
    assert.equal(result.replaced, 1);
    assert.equal(result.changes, undefined);
    assert.equal(result.changes, undefined);

    result = await r.db(dbName).table(tableName).get(1).run();
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`update` should throw if no argument is given', async () => {
    try {
      // @ts-ignore
      await r.db(dbName).table(tableName).update().run();
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
      await r
        .db(dbName)
        .table(tableName)
        // @ts-ignore
        .update({}, { nonValidKey: true })
        .run();
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
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r.db(dbName).table(tableName).insert({ id: 1 }).run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .get(1)
      .replace({ id: 1, foo: 'bar' })
      .run();
    assert.equal(result.replaced, 1);

    result = await r.db(dbName).table(tableName).get(1).run();
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`replace` should work - range replace`', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r
      .db(dbName)
      .table(tableName)
      .insert([{ id: 1 }, { id: 2 }])
      .run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .replace((row) => row.merge({ foo: 'bar' }))
      .run();
    assert.equal(result.replaced, 2);

    result = await r.db(dbName).table(tableName).get(1).run();
    assert.deepEqual(result, { id: 1, foo: 'bar' });

    result = await r.db(dbName).table(tableName).get(2).run();
    assert.deepEqual(result, { id: 2, foo: 'bar' });
  });

  it('`replace` should work - soft durability`', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r.db(dbName).table(tableName).insert({ id: 1 }).run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .get(1)
      .replace({ id: 1, foo: 'bar' }, { durability: 'soft' })
      .run();
    assert.equal(result.replaced, 1);

    result = await r.db(dbName).table(tableName).get(1).run();
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`replace` should work - hard durability`', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r.db(dbName).table(tableName).insert({ id: 1 }).run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .get(1)
      .replace({ id: 1, foo: 'bar' }, { durability: 'hard' })
      .run();
    assert.equal(result.replaced, 1);

    result = await r.db(dbName).table(tableName).get(1).run();
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`replace` should work - returnChanges true', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r.db(dbName).table(tableName).insert({ id: 1 }).run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .get(1)
      .replace({ id: 1, foo: 'bar' }, { returnChanges: true })
      .run();
    assert.equal(result.replaced, 1);
    assert.deepEqual(result.changes[0].new_val, { id: 1, foo: 'bar' });
    assert.deepEqual(result.changes[0].old_val, { id: 1 });

    result = await r.db(dbName).table(tableName).get(1).run();
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });

  it('`replace` should work - returnChanges false`', async () => {
    let result = await r.db(dbName).table(tableName).delete().run();
    assert(result);
    result = await r.db(dbName).table(tableName).insert({ id: 1 }).run();
    assert(result);

    result = await r
      .db(dbName)
      .table(tableName)
      .get(1)
      .replace({ id: 1, foo: 'bar' }, { returnChanges: false })
      .run();
    assert.equal(result.replaced, 1);
    assert.equal(result.changes, undefined);
    assert.equal(result.changes, undefined);

    result = await r.db(dbName).table(tableName).get(1).run();
    assert.deepEqual(result, { id: 1, foo: 'bar' });
  });
});
