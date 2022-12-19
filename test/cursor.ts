import assert from 'assert';
import Stream from 'stream';
import {
  Changes,
  Connection,
  isRethinkDBError,
  r,
  RCursor,
  RDatum,
  RethinkDBErrorType,
} from '../src';
import { RethinkDBConnection } from '../src/connection/connection';
import config from './config';
import { uuid } from './util/common';

function isAsyncIterable(val: any): boolean {
  if (val === null || val === undefined) {
    return false;
  }
  const isIterable = typeof val[Symbol.iterator] === 'function';
  const isAsync = typeof val[Symbol.asyncIterator] === 'function';

  return isAsync || isIterable;
}

describe('cursor', () => {
  let connection: Connection;
  let dbName: string;
  let tableName: string;
  let tableName2: string;
  let cursor: RCursor;
  let result: any;
  let feed: RCursor;

  const numDocs = 100; // Number of documents in the "big table" used to test the SUCCESS_PARTIAL
  const smallNumDocs = 5; // Number of documents in the "small table"

  before(async () => {
    await r.connectPool(config);

    dbName = uuid();
    tableName = uuid(); // Big table to test partial sequence
    tableName2 = uuid(); // small table to test success sequence

    // delete all but the system dbs
    await r
      .dbList()
      .filter((db) => r.expr(['rethinkdb', 'test']).contains(db).not())
      .forEach((db) => r.dbDrop(db))
      .run(connection);

    result = await r.dbCreate(dbName).run();
    assert.equal(result.dbs_created, 1);

    result = await r.db(dbName).tableCreate(tableName).run();
    assert.equal(result.tables_created, 1);

    result = await r.db(dbName).tableCreate(tableName2).run();
    assert.equal(result.tables_created, 1);
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  it('Inserting batch - table 1', async () => {
    result = await r
      .db(dbName)
      .table(tableName)
      .insert(r.expr(Array(numDocs).fill({})))
      .run();
    assert.equal(result.inserted, numDocs);
  });

  it('Inserting batch - table 2', async () => {
    result = await r
      .db(dbName)
      .table(tableName2)
      .insert(r.expr(Array(smallNumDocs).fill({})))
      .run();
    assert.equal(result.inserted, smallNumDocs);
  });

  it('Updating batch', async () => {
    result = await r
      .db(dbName)
      .table(tableName)
      .update(
        {
          date: r.now().sub(r.random().mul(1000000)),
          value: r.random(),
        },
        { nonAtomic: true },
      )
      .run();
    assert.equal(result.replaced, 100);
  });

  it('`table` should return a cursor', async () => {
    cursor = await r.db(dbName).table(tableName).getCursor();
    assert(cursor);
    assert.equal(cursor.toString(), '[object Cursor]');
  });

  it('`next` should return a document', async () => {
    result = await cursor.next();
    assert(result);
    assert(result.id);
  });

  it('`each` should work', async () => {
    cursor = await r.db(dbName).table(tableName).getCursor();
    assert(cursor);

    await new Promise<void>((resolve, reject) => {
      let count = 0;
      cursor.each((err) => {
        if (err) {
          reject(err);
        }
        count += 1;
        if (count === numDocs) {
          resolve();
        }
      });
    });
  });

  it('`each` should work - onFinish - reach end', async () => {
    cursor = await r.db(dbName).table(tableName).getCursor();
    assert(cursor);

    await new Promise<void>((resolve, reject) => {
      let count = 0;
      cursor.each(
        (err) => {
          if (err) {
            reject(err);
          }
          count++;
        },
        () => {
          if (count !== numDocs) {
            reject(
              new Error(
                `expected count (${count}) to equal numDocs (${numDocs})`,
              ),
            );
          }
          assert.equal(count, numDocs);
          resolve();
        },
      );
    });
  });

  it('`each` should work - onFinish - return false', async () => {
    cursor = await r.db(dbName).table(tableName).getCursor();
    assert(cursor);

    await new Promise<void>((resolve, reject) => {
      let count = 0;
      cursor.each(
        (err) => {
          if (err) {
            reject(err);
          }
          count++;
          return false;
        },
        () => {
          count === 1
            ? resolve()
            : reject(new Error('expected count to not equal 1'));
        },
      );
    });
  });

  it('`eachAsync` should work', async () => {
    cursor = await r.db(dbName).table(tableName).getCursor();
    assert(cursor);

    const history: number[] = [];
    let count = 0;
    let promisesWait = 0;

    // TODO cleanup
    await cursor.eachAsync(async () => {
      history.push(count);
      count += 1;
      await new Promise<void>((resolve, reject) => {
        setTimeout(() => {
          history.push(promisesWait);
          promisesWait -= 1;

          if (count === numDocs) {
            const expected: number[] = [];
            for (let i = 0; i < numDocs; i += 1) {
              expected.push(i);
              expected.push(-1 * i);
            }
            assert.deepEqual(history, expected);
          }
          if (count > numDocs) {
            reject(new Error(`eachAsync exceeded ${numDocs} iterations`));
          } else {
            resolve();
          }
        }, 1);
      });
    });
  });

  it('`eachAsync` should work - callback style', async () => {
    cursor = await r.db(dbName).table(tableName).getCursor();
    assert(cursor);

    let count = 0;
    const now = Date.now();
    const timeout = 10;

    await cursor.eachAsync((_, onRowFinished) => {
      count++;
      setTimeout(onRowFinished, timeout);
    });
    assert.equal(count, 100);
    const elapsed = Date.now() - now;
    assert(elapsed >= timeout * count);
  });

  it('`toArray` should work', async () => {
    cursor = await r.db(dbName).table(tableName).getCursor();
    result = await cursor.toArray();
    assert.equal(result.length, numDocs);
  });

  it('`toArray` should work - 2', async () => {
    cursor = await r.db(dbName).table(tableName2).getCursor();
    result = await cursor.toArray();
    assert.equal(result.length, smallNumDocs);
  });

  it('`toArray` should work -- with a profile', async () => {
    cursor = await r.db(dbName).table(tableName).getCursor({ profile: true });
    result = await cursor.toArray();
    assert(Array.isArray(result));
    assert.equal(result.length, numDocs);
  });

  it('`toArray` should work with a datum', async () => {
    cursor = await r.expr([1, 2, 3]).getCursor();
    result = await cursor.toArray();
    assert(Array.isArray(result));
    assert.deepEqual(result, [1, 2, 3]);
  });

  it('`table` should return a cursor - 2', async () => {
    cursor = await r.db(dbName).table(tableName2).getCursor();
    assert(cursor);
  });

  it('`next` should return a document - 2', async () => {
    result = await cursor.next();
    assert(result);
    assert(result.id);
  });

  it('`next` should work -- testing common pattern', async () => {
    cursor = await r.db(dbName).table(tableName2).getCursor();
    assert(cursor);

    let i = 0;
    try {
      while (true) {
        result = await cursor.next();
        assert(result);
        i += 1;
      }
    } catch (e) {
      assert.equal(e.message, 'No more rows in the cursor.');
      assert.equal(smallNumDocs, i);
    }
  });

  it('`cursor.close` should return a promise', async () => {
    const cursor1 = await r.db(dbName).table(tableName2).getCursor();
    await cursor1.close();
  });

  it('`cursor.close` should still return a promise if the cursor was closed', async () => {
    cursor = await r.db(dbName).table(tableName2).changes().run();
    await cursor.close();
    result = cursor.close();
    try {
      result.then(() => undefined); // Promise's contract is to have a `then` method
    } catch (e: unknown) {
      assert.fail(e);
    }
  });

  it('cursor should throw if the user try to serialize it in JSON', async () => {
    cursor = await r.db(dbName).table(tableName).getCursor();

    try {
      // @ts-ignore
      cursor.toJSON();
    } catch (err) {
      assert.equal(err.message, 'cursor.toJSON is not a function');
    }
  });

  it('Remove the field `val` in some docs - 1', async () => {
    result = await r.db(dbName).table(tableName).update({ val: 1 }).run();
    assert.equal(result.replaced, numDocs);

    result = await r
      .db(dbName)
      .table(tableName)
      .sample(5)
      .replace((row: RDatum) => row.without('val'))
      .run();
    assert.equal(result.replaced, 5);
  });

  it('Remove the field `val` in some docs - 2', async () => {
    result = await r.db(dbName).table(tableName).update({ val: 1 }).run();

    result = await r
      .db(dbName)
      .table(tableName)
      .orderBy({ index: r.desc('id') })
      .limit(5)
      .replace((row) => row.without('val'))
      .run();
    assert.equal(result.replaced, 5);
  });

  it('`toArray` with multiple batches - testing empty SUCCESS_COMPLETE', async () => {
    connection = await r.connect({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection.open);

    cursor = await r
      .db(dbName)
      .table(tableName)
      .getCursor(connection, { maxBatchRows: 1 });
    assert(cursor);

    result = await cursor.toArray();
    assert(Array.isArray(result));
    assert.equal(result.length, 100);

    await connection.close();
    assert(!connection.open);
  });

  it('Automatic coercion from cursor to table with multiple batches', async () => {
    connection = await r.connect({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection.open);

    result = await r
      .db(dbName)
      .table(tableName)
      .run(connection, { maxBatchRows: 1 });
    assert(result.length > 0);

    await connection.close();
    assert(!connection.open);
  });

  it('`next` with multiple batches', async () => {
    connection = await r.connect({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection.open);

    cursor = await r
      .db(dbName)
      .table(tableName)
      .getCursor(connection, { maxBatchRows: 1 });
    assert(cursor);

    let i = 0;
    try {
      while (true) {
        result = await cursor.next();
        i += 1;
      }
    } catch (e) {
      if (i > 0 && e.message === 'No more rows in the cursor.') {
        await connection.close();
        assert(!connection.open);
      } else {
        assert.fail(e);
      }
    }
  });

  it('`next` should error when hitting an error -- not on the first batch', async () => {
    connection = await r.connect({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection);

    cursor = await r
      .db(dbName)
      .table(tableName)
      .orderBy({ index: 'id' })
      .map((row) => row('val').add(1))
      .getCursor(connection, { maxBatchRows: 10 });
    assert(cursor);

    let i = 0;

    try {
      while (true) {
        result = await cursor.next();
        i++;
      }
    } catch (e) {
      if (i > 0 && e.message.match(/^No attribute `val` in object/)) {
        await connection.close();
        assert(!connection.open);
      } else {
        assert.fail(e);
      }
    }
  });

  it('`changes` should return a feed', async () => {
    feed = await r.db(dbName).table(tableName).changes().run();
    assert(feed);
    assert.equal(feed.toString(), '[object Feed]');
    await feed.close();
  });

  it('`changes` should work with squash: true', async () => {
    feed = await r.db(dbName).table(tableName).changes({ squash: true }).run();
    assert(feed);
    assert.equal(feed.toString(), '[object Feed]');
    await feed.close();
  });

  it('`get.changes` should return a feed', async () => {
    feed = await r.db(dbName).table(tableName).get(1).changes().run();
    assert(feed);
    assert.equal(feed.toString(), '[object AtomFeed]');
    await feed.close();
  });

  it('`orderBy.limit.changes` should return a feed', async () => {
    feed = await r
      .db(dbName)
      .table(tableName)
      .orderBy({ index: 'id' })
      .limit(2)
      .changes()
      .run();
    assert(feed);
    assert.equal(feed.toString(), '[object OrderByLimitFeed]');
    await feed.close();
  });

  it('`changes` with `includeOffsets` should work', async () => {
    feed = await r
      .db(dbName)
      .table(tableName)
      .orderBy({ index: 'id' })
      .limit(2)
      .changes({
        includeOffsets: true,
        includeInitial: true,
      })
      .run();

    let counter = 0;

    const promise = new Promise<void>((resolve, reject) => {
      feed.each((error, change) => {
        if (error) {
          reject(error);
        }
        assert(typeof change.new_offset === 'number');
        if (counter >= 2) {
          assert(typeof change.old_offset === 'number');

          feed.close().then(resolve).catch(reject);
        }
        counter++;
      });
    });

    await r.db(dbName).table(tableName).insert({ id: 0 }).run();
    await promise;
  });

  it('`changes` with `includeTypes` should work', async () => {
    feed = await r
      .db(dbName)
      .table(tableName)
      .orderBy({ index: 'id' })
      .limit(2)
      .changes({
        includeTypes: true,
        includeInitial: true,
      })
      .run();

    let counter = 0;

    const promise = new Promise<void>((resolve, reject) => {
      feed.each((error, change) => {
        if (error) {
          reject(error);
        }
        assert(typeof change.type === 'string');
        if (counter > 0) {
          feed.close().then(resolve).catch(reject);
        }
        counter++;
      });
    });

    result = await r.db(dbName).table(tableName).insert({ id: 0 }).run();
    assert.equal(result.errors, 1); // Duplicate primary key (depends on previous test case)
    await promise;
  });

  it('`next` should work on a feed', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run();
    assert(feed);

    const promise = new Promise<void>((resolve, reject) => {
      let i = 0;
      while (true) {
        feed
          .next()
          .then(assert)
          .catch((err) => {
            if (
              isRethinkDBError(err) &&
              err.type === RethinkDBErrorType.CANCEL
            ) {
              resolve();
            }
          });
        i++;
        if (i === smallNumDocs) {
          return feed.close().catch(reject);
        }
      }
    });

    await r.db(dbName).table(tableName2).update({ foo: r.now() }).run();
    await promise;
  });

  it('`next` should work on an atom feed', async () => {
    const idValue = uuid();
    feed = await r
      .db(dbName)
      .table(tableName2)
      .get(idValue)
      .changes({ includeInitial: true })
      .run();
    assert(feed);

    const promise = new Promise<void>((resolve, reject) => {
      feed
        .next()
        .then((res) => assert.deepEqual(res, { new_val: null }))
        .then(() => feed.next())
        .then((res) =>
          assert.deepEqual(res, { new_val: { id: idValue }, old_val: null }),
        )
        .then(resolve)
        .catch(reject);
    });

    await r.db(dbName).table(tableName2).insert({ id: idValue }).run();
    await promise;
    await feed.close();
  });

  it('`close` should work on feed', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run();
    assert(feed);

    await feed.close();
  });

  it('`close` should work on feed with events', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run();

    const promise = new Promise((resolve, reject) => {
      feed.on('error', reject);
      feed.on('data', () => null).on('end', resolve);
    });

    await feed.close();
    await promise;
  });

  it('`on` should work on feed', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run();
    assert(feed);

    const promise = new Promise<void>((resolve, reject) => {
      let i = 0;
      feed.on('data', () => {
        i++;
        if (i === smallNumDocs) {
          feed.close().then(resolve).catch(reject);
        }
      });
      feed.on('error', reject);
    });

    await r.db(dbName).table(tableName2).update({ foo: r.now() }).run();
    await promise;
  });

  it('`on` should work on cursor - a `end` event should be eventually emitted on a cursor', async () => {
    cursor = await r.db(dbName).table(tableName2).getCursor();
    assert(cursor);

    const promise = new Promise((resolve, reject) => {
      cursor.on('data', () => null).on('end', resolve);
      cursor.on('error', reject);
    });

    await r.db(dbName).table(tableName2).update({ foo: r.now() }).run();
    await promise;
  });

  it('`next`, `each`, `toArray` should be deactivated if the EventEmitter interface is used', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run();

    feed.on('data', () => undefined);
    feed.on('error', assert.fail);

    try {
      await feed.next();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'You cannot call `next` once you have bound listeners on the Feed.',
      );
      await feed.close();
    }
  });

  it('`each` should not return an error if the feed is closed - 1', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run();
    assert(feed);

    const promise = new Promise<void>((resolve, reject) => {
      let count = 0;
      feed.each((err, res) => {
        if (err) {
          reject(err);
        }
        if (res.new_val.foo instanceof Date) {
          count += 1;
        }
        if (count === 1) {
          setTimeout(() => {
            feed.close().then(resolve).catch(reject);
          }, 100);
        }
      });
    });

    await r
      .db(dbName)
      .table(tableName2)
      .limit(2)
      .update({ foo: r.now() })
      .run();
    await promise;
  });

  it('`each` should not return an error if the feed is closed - 2', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run();
    assert(feed);

    const promise = new Promise<void>((resolve, reject) => {
      let count = 0;
      feed.each((err, res) => {
        if (err) {
          reject(err);
        }
        if (res.new_val.foo instanceof Date) {
          count += 1;
        }
        if (count === 2) {
          setTimeout(() => {
            feed.close().then(resolve).catch(reject);
          }, 100);
        }
      });
    });
    await r
      .db(dbName)
      .table(tableName2)
      .limit(2)
      .update({ foo: r.now() })
      .run();
    await promise;
  });

  it('events should not return an error if the feed is closed - 1', async () => {
    feed = await r.db(dbName).table(tableName2).get(1).changes().run();
    assert(feed);

    const promise = new Promise<void>((resolve, reject) => {
      feed.each((err, res) => {
        if (err) {
          reject(err);
        }
        if (res.new_val != null && res.new_val.id === 1) {
          feed.close().then(resolve).catch(reject);
        }
      });
    });
    await r.db(dbName).table(tableName2).insert({ id: 1 }).run();
    await promise;
  });

  it('events should not return an error if the feed is closed - 2', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run();
    assert(feed);

    const promise = new Promise<void>((resolve, reject) => {
      let count = 0;
      feed.on('data', (res) => {
        if (res.new_val.foo instanceof Date) {
          count += 1;
        }
        if (count === 1) {
          setTimeout(() => {
            feed.close().then(resolve).catch(reject);
          }, 100);
        }
      });
    });
    await r
      .db(dbName)
      .table(tableName2)
      .limit(2)
      .update({ foo: r.now() })
      .run();
    await promise;
  });

  it('`includeStates` should work', async () => {
    feed = await r
      .db(dbName)
      .table(tableName)
      .orderBy({ index: 'id' })
      .limit(10)
      .changes({ includeStates: true, includeInitial: true })
      .run();
    let i = 0;

    await new Promise<void>((resolve, reject) => {
      feed.each((err) => {
        if (err) {
          reject(err);
        }
        i++;
        if (i === 10) {
          feed.close().then(resolve).catch(reject);
        }
      });
    });
  });

  it('`each` should return an error if the connection dies', async () => {
    connection = await r.connect({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection);

    const feed1 = await r.db(dbName).table(tableName).changes().run(connection);

    const promise = feed1.each((err) => {
      assert(
        err.message.startsWith(
          'The connection was closed before the query could be completed',
        ),
      );
    });
    // Kill the TCP connection
    const { socket } = (connection as RethinkDBConnection).socket;
    if (socket) {
      socket.destroy();
    }
    return promise;
  });

  it('`eachAsync` should return an error if the connection dies', async () => {
    connection = await r.connect({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection);

    const feed1 = await r.db(dbName).table(tableName).changes().run(connection);
    const promise = feed1
      .eachAsync(() => undefined)
      .catch((err) => {
        assert(
          err.message.startsWith(
            'The connection was closed before the query could be completed',
          ),
        );
      });
    // Kill the TCP connection
    const { socket } = (connection as RethinkDBConnection).socket;
    if (socket) {
      socket.destroy();
    }
    return promise;
  });

  it('cursor should be an async iterator', async () => {
    connection = await r.connect({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection.open);

    const feed1 = await r.db(dbName).table(tableName).changes().run(connection);
    assert(feed1);

    const iterator = feed1;
    assert(isAsyncIterable(iterator));
    // Kill the TCP connection
    const { socket } = (connection as RethinkDBConnection).socket;
    if (socket) {
      socket.destroy();
    }
  });

  it('`asyncIterator` should work', async () => {
    const feed1 = await r.db(dbName).table(tableName2).changes().run();
    assert(feed1);

    const value = 1;

    const promise = (async () => {
      let res: any;
      for await (const row of feed1 as AsyncIterableIterator<Changes<any>>) {
        res = row;
        feed1.close();
      }
      return res;
    })();

    await r.db(dbName).table(tableName2).insert({ foo: value }).run();
    result = await promise;
    assert(result.new_val.foo === value);
  });

  it('pipes all objects', async () => {
    // create fresh db/table for testing
    dbName = uuid();
    tableName = uuid();

    result = await r.dbCreate(dbName).run();
    assert.equal(result.dbs_created, 1);

    result = await r.db(dbName).tableCreate(tableName).run();
    assert.equal(result.tables_created, 1);

    // fill it up with some records
    result = await r
      .db(dbName)
      .table(tableName)
      .insert(r.expr(Array(numDocs).fill({})))
      .run();

    // try to pipe() it into a "Writable stream"
    cursor = await r.db(dbName).table(tableName).getCursor();

    const retrieved = [];

    const writeStream = new Stream.Writable({
      objectMode: true,
      write(obj, encoding, next) {
        retrieved.push(obj);
        setTimeout(next, 0);
      },
    });

    cursor.pipe(writeStream);

    return new Promise((resolve, reject) => {
      writeStream
        .on('finish', () => {
          assert.equal(retrieved.length, numDocs);
          resolve();
        })
        .on('error', reject);
    });
  });
});
