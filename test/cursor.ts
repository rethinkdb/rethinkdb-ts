import assert from 'assert';
import {
  Changes,
  connect,
  connectPool,
  isRethinkDBError,
  r,
  RethinkDBError,
  RValue,
} from '../src';
import { RethinkDBConnection } from '../src/connection/connection';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';
import { Cursor } from '../src/response/cursor';
import { RethinkDBErrorType } from '../src/error';

function isAsyncIterable(val: any): boolean {
  if (val === null || val === undefined) {
    return false;
  }
  const isIterable = typeof val[Symbol.iterator] === 'function';
  const isAsync = typeof val[Symbol.asyncIterator] === 'function';

  return isAsync || isIterable;
}

const servers = [
  {
    host: config.server.host,
    port: config.server.port,
  },
];
const options = {
  db: 'test',
  max: 10,
  buffer: 2,
  user: config.options.user,
  password: config.options.password,
  discovery: false,
  silent: true,
};

describe('cursor', () => {
  let connection: RethinkDBConnection;
  let dbName: string;
  let tableName: string;
  let tableName2: string;
  let cursor: Cursor;
  let result: any;
  let feed: Cursor;

  const numDocs = 50; // Number of documents in the "big table" used to test the SUCCESS_PARTIAL
  const smallNumDocs = 5; // Number of documents in the "small table"

  let pool: MasterConnectionPool;
  before(async () => {
    pool = await connectPool(servers, options);

    dbName = uuid();
    tableName = uuid(); // Big table to test partial sequence
    tableName2 = uuid(); // small table to test success sequence

    // delete all but the system dbs
    await pool.run(
      r
        .dbList()
        .filter((db) => r.expr(['rethinkdb', 'test']).contains(db).not())
        .forEach((db) => r.dbDrop(db)),
    );

    result = await pool.run(r.dbCreate(dbName));
    assert.equal(result.dbs_created, 1);

    result = await pool.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result.tables_created, 1);

    result = await pool.run(r.db(dbName).tableCreate(tableName2));
    assert.equal(result.tables_created, 1);
  });

  after(async () => {
    await pool.drain();
  });

  it('Inserting batch - table 1', async () => {
    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .insert(r.expr(Array(numDocs).fill({}))),
    );
    assert.equal(result.inserted, numDocs);
  });

  it('Inserting batch - table 2', async () => {
    result = await pool.run(
      r
        .db(dbName)
        .table(tableName2)
        .insert(r.expr(Array(smallNumDocs).fill({}))),
    );
    assert.equal(result.inserted, smallNumDocs);
  });

  it('Updating batch', async () => {
    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .update(
          {
            date: r.now().sub(r.random().mul(1000000)),
            value: r.random(),
          },
          { nonAtomic: true },
        ),
    );
    assert.equal(result.replaced, numDocs);
  });

  it('`table` should return a cursor', async () => {
    cursor = await pool.getCursor(r.db(dbName).table(tableName));
    assert(cursor);
    assert.equal(cursor.toString(), '[object Cursor]');
  });

  it('`next` should return a document', async () => {
    result = await cursor.next();
    assert(result);
    assert(result.id);
  });

  it('`each` should work', async () => {
    cursor = await pool.getCursor(r.db(dbName).table(tableName));
    assert(cursor);

    await new Promise<void>((resolve, reject) => {
      let count = 0;
      cursor.each((error) => {
        if (error) {
          reject(error);
        }
        count += 1;
        if (count === numDocs) {
          resolve();
        }
      });
    });
  });

  it('`each` should work - onFinish - reach end', async () => {
    cursor = await pool.getCursor(r.db(dbName).table(tableName));
    assert(cursor);

    await new Promise<void>((resolve, reject) => {
      let count = 0;
      cursor.each(
        (err) => {
          if (err) {
            reject(err);
          }
          count += 1;
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
    cursor = await pool.getCursor(r.db(dbName).table(tableName));
    assert(cursor);

    await new Promise<void>((resolve, reject) => {
      let count = 0;
      cursor.each(
        (err) => {
          if (err) {
            reject(err);
          }
          count += 1;
          return false;
        },
        () => {
          if (count === 1) {
            resolve();
          } else {
            reject(new Error('expected count to not equal 1'));
          }
        },
      );
    });
  });

  it('`eachAsync` should work', async () => {
    cursor = await pool.getCursor(r.db(dbName).table(tableName));
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
            const expected = [];
            for (let i = 0; i < numDocs; i += 1) {
              expected.push(i, -1 * i);
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
    cursor = await pool.getCursor(r.db(dbName).table(tableName));
    assert(cursor);

    let count = 0;
    const now = Date.now();
    const timeout = 10;

    await cursor.eachAsync((_, onRowFinished) => {
      count += 1;
      setTimeout(onRowFinished, timeout);
    });
    assert.equal(count, numDocs);
    const elapsed = Date.now() - now;
    assert(elapsed >= timeout * count);
  });

  it('`toArray` should work', async () => {
    cursor = await pool.getCursor(r.db(dbName).table(tableName));
    result = await cursor.toArray();
    assert.equal(result.length, numDocs);
  });

  it('`toArray` should work - 2', async () => {
    cursor = await pool.getCursor(r.db(dbName).table(tableName2));
    result = await cursor.toArray();
    assert.equal(result.length, smallNumDocs);
  });

  it('`toArray` should work -- with a profile', async () => {
    cursor = await pool.getCursor(r.db(dbName).table(tableName), {
      profile: true,
    });
    result = await cursor.toArray();
    assert(Array.isArray(result));
    assert.equal(result.length, numDocs);
  });

  it('`toArray` should work with a datum', async () => {
    cursor = await pool.getCursor(r.expr([1, 2, 3]));
    result = await cursor.toArray();
    assert(Array.isArray(result));
    assert.deepEqual(result, [1, 2, 3]);
  });

  it('`table` should return a cursor - 2', async () => {
    cursor = await pool.getCursor(r.db(dbName).table(tableName2));
    assert(cursor);
  });

  it('`next` should return a document - 2', async () => {
    result = await cursor.next();
    assert(result);
    assert(result.id);
  });

  it('`next` should work -- testing common pattern', async () => {
    cursor = await pool.getCursor(r.db(dbName).table(tableName2));
    assert(cursor);

    let i = 0;
    try {
      while (true) {
        // eslint-disable-next-line no-await-in-loop
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
    const cursor1 = await pool.getCursor(r.db(dbName).table(tableName2));
    await cursor1.close();
  });

  it('`cursor.close` should still return a promise if the cursor was closed', async () => {
    cursor = await pool.run(r.db(dbName).table(tableName2).changes());
    await cursor.close();
    result = cursor.close();
    try {
      result.then(() => undefined); // Promise's contract is to have a `then` method
    } catch (e) {
      assert.fail(e);
    }
  });

  it('cursor should throw if the user try to serialize it in JSON', async () => {
    cursor = await pool.getCursor(r.db(dbName).table(tableName));

    try {
      // @ts-ignore
      cursor.toJSON();
    } catch (err) {
      assert.equal(err.message, 'cursor.toJSON is not a function');
    }
  });

  it('Remove the field `val` in some docs - 1', async () => {
    result = await pool.run(r.db(dbName).table(tableName).update({ val: 1 }));
    assert.equal(result.replaced, numDocs);

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .sample(5)
        .replace((row: RValue) => row.without('val')),
    );
    assert.equal(result.replaced, 5);
  });

  it('Remove the field `val` in some docs - 2', async () => {
    result = await pool.run(r.db(dbName).table(tableName).update({ val: 1 }));

    result = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .orderBy({ index: r.desc('id') })
        .limit(5)
        .replace((row: RValue) => row.without('val')),
    );
    assert.equal(result.replaced, 5);
  });

  it('`toArray` with multiple batches - testing empty SUCCESS_COMPLETE', async () => {
    connection = await connect(servers[0], options);
    assert(connection.open);

    cursor = await connection.getCursor(r.db(dbName).table(tableName), {
      maxBatchRows: 1,
    });
    assert(cursor);

    result = await cursor.toArray();
    assert(Array.isArray(result));
    assert.equal(result.length, numDocs);

    await connection.close();
    assert(!connection.open);
  });

  it('Automatic coercion from cursor to table with multiple batches', async () => {
    connection = await connect(servers[0], options);
    assert(connection.open);

    result = await connection.run(r.db(dbName).table(tableName), {
      maxBatchRows: 1,
    });
    assert(result.length > 0);

    await connection.close();
    assert(!connection.open);
  });

  it('`next` with multiple batches', async () => {
    connection = await connect(servers[0], options);

    assert(connection.open);

    cursor = await pool.getCursor(r.db(dbName).table(tableName), {
      maxBatchRows: 1,
    });
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

  // TODO sometimes it fails fill smaller numDocs
  it('`next` should error when hitting an error -- not on the first batch', async () => {
    connection = await connect(servers[0], options);

    assert(connection);

    cursor = await connection.getCursor(
      r
        .db(dbName)
        .table(tableName)
        .orderBy({ index: 'id' })
        .map((row: RValue) => row('val').add(1)),
      { maxBatchRows: 10 },
    );
    assert(cursor);

    let i = 0;

    try {
      while (true) {
        result = await cursor.next();
        i += 1;
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
    feed = await pool.run(r.db(dbName).table(tableName).changes());
    assert(feed);
    assert.equal(feed.toString(), '[object Feed]');
    await feed.close();
  });

  it('`changes` should work with squash: true', async () => {
    feed = await pool.run(
      r.db(dbName).table(tableName).changes({ squash: true }),
    );
    assert(feed);
    assert.equal(feed.toString(), '[object Feed]');
    await feed.close();
  });

  it('`get.changes` should return a feed', async () => {
    feed = await pool.run(r.db(dbName).table(tableName).get(1).changes());
    assert(feed);
    assert.equal(feed.toString(), '[object AtomFeed]');
    await feed.close();
  });

  it('`orderBy.limit.changes` should return a feed', async () => {
    feed = await pool.run(
      r.db(dbName).table(tableName).orderBy({ index: 'id' }).limit(2).changes(),
    );
    assert(feed);
    assert.equal(feed.toString(), '[object OrderByLimitFeed]');
    await feed.close();
  });

  it('`changes` with `includeOffsets` should work', async () => {
    feed = await pool.run(
      r.db(dbName).table(tableName).orderBy({ index: 'id' }).limit(2).changes({
        includeOffsets: true,
        includeInitial: true,
      }),
    );

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
        counter += 1;
      });
    });

    await pool.run(r.db(dbName).table(tableName).insert({ id: 0 }));
    await promise;
  });

  it('`changes` with `includeTypes` should work', async () => {
    feed = await pool.run(
      r.db(dbName).table(tableName).orderBy({ index: 'id' }).limit(2).changes({
        includeTypes: true,
        includeInitial: true,
      }),
    );

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
        counter += 1;
      });
    });

    result = await pool.run(r.db(dbName).table(tableName).insert({ id: 0 }));
    assert.equal(result.errors, 1); // Duplicate primary key (depends on previous test case)
    await promise;
  });

  it('`next` should work on a feed', async () => {
    feed = await pool.run(r.db(dbName).table(tableName2).changes());
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
        i += 1;
        if (i === smallNumDocs) {
          return feed.close().catch(reject);
        }
      }
    });

    await pool.run(r.db(dbName).table(tableName2).update({ foo: r.now() }));
    await promise;
  });

  it('`next` should work on an atom feed', async () => {
    const idValue = uuid();
    feed = await pool.run(
      r
        .db(dbName)
        .table(tableName2)
        .get(idValue)
        .changes({ includeInitial: true }),
    );
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

    await pool.run(r.db(dbName).table(tableName2).insert({ id: idValue }));
    await promise;
    await feed.close();
  });

  it('`close` should work on feed', async () => {
    feed = await pool.run(r.db(dbName).table(tableName2).changes());
    assert(feed);

    await feed.close();
  });

  it('`close` should work on feed with events', async () => {
    feed = await pool.run(r.db(dbName).table(tableName2).changes());

    const promise = new Promise((resolve, reject) => {
      feed.on('error', reject);
      feed.on('data', () => null).on('end', resolve);
    });

    await feed.close();
    await promise;
  });

  it('`on` should work on feed', async () => {
    feed = await pool.run(r.db(dbName).table(tableName2).changes());
    assert(feed);

    const promise = new Promise<void>((resolve, reject) => {
      let i = 0;
      feed.on('data', () => {
        i += 1;
        if (i === smallNumDocs) {
          feed.close().then(resolve).catch(reject);
        }
      });
      feed.on('error', reject);
    });

    await pool.run(r.db(dbName).table(tableName2).update({ foo: r.now() }));
    await promise;
  });

  it('`on` should work on cursor - a `end` event shoul be eventually emitted on a cursor', async () => {
    cursor = await pool.getCursor(r.db(dbName).table(tableName2));
    assert(cursor);

    const promise = new Promise((resolve, reject) => {
      cursor.on('data', () => null).on('end', resolve);
      cursor.on('error', reject);
    });

    await pool.run(r.db(dbName).table(tableName2).update({ foo: r.now() }));
    await promise;
  });

  it('`next`, `each`, `toArray` should be deactivated if the EventEmitter interface is used', async () => {
    feed = await pool.run(r.db(dbName).table(tableName2).changes());

    feed.on('data', () => undefined);
    feed.on('error', assert.fail);

    try {
      await feed.next();
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'You cannot call `next` once you have bound listeners on the Feed.',
      );
      await feed.close();
    }
  });

  it('`each` should not return an error if the feed is closed - 1', async () => {
    feed = await pool.run(r.db(dbName).table(tableName2).changes());
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

    await pool.run(
      r.db(dbName).table(tableName2).limit(2).update({ foo: r.now() }),
    );
    await promise;
  });

  it('`each` should not return an error if the feed is closed - 2', async () => {
    feed = await pool.run(r.db(dbName).table(tableName2).changes());
    assert(feed);

    const promise = new Promise<void>((resolve, reject) => {
      let count = 0;
      feed.each((error, res) => {
        if (error) {
          reject(error);
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
    await pool.run(
      r.db(dbName).table(tableName2).limit(2).update({ foo: r.now() }),
    );
    await promise;
  });

  it('events should not return an error if the feed is closed - 1', async () => {
    feed = await pool.run(r.db(dbName).table(tableName2).get(1).changes());
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
    await pool.run(r.db(dbName).table(tableName2).insert({ id: 1 }));
    await promise;
  });

  it('events should not return an error if the feed is closed - 2', async () => {
    feed = await pool.run(r.db(dbName).table(tableName2).changes());
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
    await pool.run(
      r.db(dbName).table(tableName2).limit(2).update({ foo: r.now() }),
    );
    await promise;
  });

  it('`includeStates` should work', async () => {
    feed = await pool.run(
      r
        .db(dbName)
        .table(tableName)
        .orderBy({ index: 'id' })
        .limit(10)
        .changes({ includeStates: true, includeInitial: true }),
    );
    let i = 0;

    await new Promise<void>((resolve, reject) => {
      feed.each((err) => {
        if (err) {
          reject(err);
        }
        i += 1;
        if (i === 10) {
          feed.close().then(resolve).catch(reject);
        }
      });
    });
  });

  it('`each` should return an error if the connection dies', async () => {
    connection = await connect(servers[0], options);
    assert(connection);

    const feed1 = await connection.run(r.db(dbName).table(tableName).changes());

    const promise = feed1.each((error: RethinkDBError) => {
      assert(
        error.message.startsWith(
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
    connection = await connect(servers[0], options);
    assert(connection);

    const feed1 = await connection.run(r.db(dbName).table(tableName).changes());
    const promise = feed1
      .eachAsync(() => undefined)
      .catch((error: Error) => {
        assert(
          error.message.startsWith(
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
    connection = await connect(servers[0], options);
    assert(connection.open);

    const feed1 = await connection.run(r.db(dbName).table(tableName).changes());
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
    const feed1 = await pool.run(r.db(dbName).table(tableName2).changes());
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

    await pool.run(r.db(dbName).table(tableName2).insert({ foo: value }));
    result = await promise;
    assert(result.new_val.foo === value);
  });
});
