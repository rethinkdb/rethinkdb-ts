import assert from 'assert';
import { Readable } from 'stream';
import {
  createRethinkdbConnection,
  createRethinkdbMasterPool,
  r,
} from '../src';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('stream', () => {
  let dbName: string;
  let tableName: string;
  let tableName2: string;
  let dumpTable: string;
  const numDocs = 100; // Number of documents in the "big table" used to test the SUCCESS_PARTIAL
  const smallNumDocs = 5; // Number of documents in the "small table"
  let pool: MasterConnectionPool;

  before(async () => {
    pool = await createRethinkdbMasterPool(config);
    dbName = uuid();
    tableName = uuid(); // Big table to test partial sequence
    tableName2 = uuid(); // small table to test success sequence
    dumpTable = uuid(); // dump table

    const result1 = await pool.run(r.dbCreate(dbName));
    assert.equal(result1.dbs_created, 1);

    const result2 = await Promise.all([
      pool.run(r.db(dbName).tableCreate(tableName)('tables_created')),
      pool.run(r.db(dbName).tableCreate(tableName2)('tables_created')),
      pool.run(r.db(dbName).tableCreate(dumpTable)('tables_created')),
    ]);
    assert.deepEqual(result2, [1, 1, 1]);

    const result3 = await pool.run(
      r.db(dbName).table(tableName).insert(Array(numDocs).fill({})),
    );
    assert.equal(result3.inserted, numDocs);

    const result4 = await pool.run(
      r.db(dbName).table(tableName2).insert(Array(smallNumDocs).fill({})),
    );
    assert.equal(result4.inserted, smallNumDocs);

    const result5 = await pool.run(
      r.db(dbName).table(tableName).update({ date: r.now() }),
    );
    assert.equal(result5.replaced, numDocs);
  });

  after(async () => {
    // remove any dbs created
    await pool.run(
      r
        .dbList()
        .filter((db) => r.expr(['rethinkdb', 'test']).contains(db).not())
        .forEach((db) => r.dbDrop(db)),
    );
    await pool.drain();
  });

  it('`table` should return a stream', async () => {
    const stream = await pool.getCursor(r.db(dbName).table(tableName));
    assert(stream);
    assert(stream instanceof Readable);
    stream.close();
  });

  it('Arrays should return a stream', async () => {
    const data = [10, 11, 12, 13, 14, 15, 16];
    const stream = await pool.getCursor(r.expr(data));
    assert(stream);
    assert(stream instanceof Readable);

    await new Promise((resolve, reject) => {
      let count = 0;
      stream.on('data', () => {
        count++;
        if (count === data.length) {
          resolve();
        }
      });
    });
  });

  it('changes() should return a stream', async () => {
    const data = [
      { n: 1 },
      { n: 2 },
      { n: 3 },
      { n: 4 },
      { n: 5 },
      { n: 6 },
      { n: 7 },
    ];
    // added include initial, so it won't hang on some extreame cases
    const stream = await pool.getCursor(
      r.db(dbName).table(tableName).changes({ includeInitial: true }),
    );
    assert(stream);
    assert(stream instanceof Readable);
    const promise = new Promise((resolve, reject) => {
      let count = 0;
      stream.on('data', (d) => {
        if (!!d.new_val.n) {
          count++;
          if (count === data.length) {
            resolve();
            stream.close();
          }
        }
      });
    });

    await pool.run(r.db(dbName).table(tableName).insert(data));
    await promise;
  });

  it('get().changes() should return a stream', async () => {
    const id = uuid();
    await pool.run(r.db(dbName).table(tableName).insert({ id }));
    const stream = await pool.getCursor(
      r.db(dbName).table(tableName).get(id).changes(),
    );
    assert(stream);
    assert(stream instanceof Readable);

    const promise = new Promise((resolve, reject) => {
      let count = 0;
      stream.on('data', () => {
        count++;
        if (count === 3) {
          resolve();
          stream.close();
        }
      });
    });
    await new Promise((resolve) => setTimeout(resolve, 200));
    await pool.run(r.db(dbName).table(tableName).get(id).update({ update: 1 }));
    await pool.run(r.db(dbName).table(tableName).get(id).update({ update: 2 }));
    await pool.run(r.db(dbName).table(tableName).get(id).update({ update: 3 }));
    await promise;
  });

  it('`table` should return a stream - testing empty SUCCESS_COMPLETE', async () => {
    const connection = await createRethinkdbConnection({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection);

    const stream = await connection.getCursor(r.db(dbName).table(tableName), {
      maxBatchRows: 1,
    });
    assert(stream);
    assert(stream instanceof Readable);
    await stream.close();
    await connection.close();
  });

  it('Test flowing - event data', async () => {
    const connection = await createRethinkdbConnection({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection);

    const stream = await connection.getCursor(r.db(dbName).table(tableName), {
      maxBatchRows: 1,
    });
    await new Promise((resolve, reject) => {
      let count = 0;
      stream.on('data', () => {
        count++;
        if (count === numDocs) {
          resolve();
        }
      });
    });
    await stream.close();
    await connection.close();
  });

  it('Test read', async () => {
    const connection = await createRethinkdbConnection({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection);

    const stream = await connection.getCursor(r.db(dbName).table(tableName), {
      maxBatchRows: 1,
    });
    await new Promise((resolve, reject) => {
      stream.once('readable', () => {
        const doc = stream.read();
        if (doc === null) {
          reject(
            new Error(
              'stream.read() should not return null when readable was emitted',
            ),
          );
        }
        let count = 1;
        stream.on('data', (data) => {
          count++;
          if (count === numDocs) {
            resolve();
          }
        });
      });
    });
    await stream.close();
    await connection.close();
  });

  it('Test flowing - event data', async () => {
    const connection = await createRethinkdbConnection({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection);

    const stream = await connection.getCursor(r.db(dbName).table(tableName), {
      maxBatchRows: 1,
    });
    await new Promise((resolve, reject) => {
      let count = 0;
      stream.on('data', () => {
        count++;
        if (count === numDocs) {
          resolve();
        }
      });
      stream.pause();
      if (count > 0) {
        reject(new Error('The stream should have been paused'));
      }
      stream.resume();
    });
    await stream.close();
    await connection.close();
  });

  it('Test read with null value', async () => {
    const connection = await createRethinkdbConnection({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection);

    const stream = await connection.getCursor(
      r
        .db(dbName)
        .table(tableName)
        .limit(10)
        .union([null])
        .union(r.db(dbName).table(tableName).limit(10)),
      { maxBatchRows: 1 },
    );
    await new Promise((resolve, reject) => {
      stream.once('readable', () => {
        let count = 0;
        stream.on('data', (data) => {
          count++;
          if (count === 20) {
            resolve();
          } else if (count > 20) {
            reject(new Error('Should not get null'));
          }
        });
      });
    });
    await stream.close();
    await connection.close();
  });

  it('Test read', async () => {
    const connection = await createRethinkdbConnection({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
    });
    assert(connection);

    const stream = await connection.getCursor(r.db(dbName).table(tableName), {
      maxBatchRows: 1,
    });
    await new Promise((resolve, reject) => {
      stream.once('readable', () => {
        stream.read() === null
          ? reject(
              new Error(
                'stream.read() should not return null when readable was emitted',
              ),
            )
          : resolve();
      });
    });
    await stream.close();
    await connection.close();
  });

  // it('Import with stream as default', async () => {
  //   const r1 = rethinkdbdash({
  //     host: config.host,
  //     port: config.port,
  //     authKey: config.authKey,
  //     buffer: config.buffer,
  //     max: config.max,
  //     discovery: false,
  //     silent: true
  //   });
  //   const stream = await pool.run(r1
  //     .db(dbName)
  //     .table(tableName)
  //     );
  //   assert(stream instanceof Readable);
  //   await stream.close();
  //   await pool.run(r1.getPool().drain();
  // });

  it('toStream', async () => {
    const stream = await pool.getCursor(r.db(dbName).table(tableName));

    await new Promise((resolve, reject) => {
      stream.once('readable', () => {
        const doc = stream.read();
        if (doc === null) {
          reject(
            new Error(
              'stream.read() should not return null when readable was emitted',
            ),
          );
        }
        let count = 1;
        stream.on('data', (data) => {
          count++;
          if (count === numDocs) {
            resolve();
          }
        });
      });
    });
    await stream.close();
  });

  it('toStream - with grouped data', async () => {
    const stream = await pool.getCursor(
      r.db(dbName).table(tableName).group({ index: 'id' }),
    );

    await new Promise((resolve, reject) => {
      stream.once('readable', () => {
        const doc = stream.read();
        if (doc === null) {
          reject(
            new Error(
              'stream.read() should not return null when readable was emitted',
            ),
          );
        }
        let count = 1;
        stream.on('data', (data) => {
          count++;
          if (count === numDocs) {
            resolve();
          }
        });
      });
    });
    await stream.close();
  });

  // it('pipe should work with a writable stream - 200-200', function (done) {
  //   await pool.run(r.connectPool({ buffer: 1, max: 2, discovery: false, silent: true })

  //   r1.db(dbName).table(tableName).toStream({ highWaterMark: 200 })
  //     .pipe(r1.db(dbName).table(dumpTable).toStream({ writable: true, highWaterMark: 200 }))
  //     .on('finish', function () {
  //       r.expr([
  //         r1.db(dbName).table(tableName).count(),
  //         r1.db(dbName).table(dumpTable).count()
  //       ])
  //         ).then(function (result) {
  //           if (result[0] !== result[1]) {
  //             done(new Error('All the data should have been streamed'))
  //           }
  //           return r1.db(dbName).table(dumpTable).delete()
  //         }).then((_) => r1.getPool().drain()).then(done).error(done)
  //     })
  // })

  // it('pipe should work with a writable stream - 200-20', function (done) {
  //   const r1 = rethinkdbdash({ buffer: 1, max: 2, discovery: false, silent: true })

  //   r1.db(dbName).table(tableName).toStream({ highWaterMark: 200 })
  //     .pipe(r1.db(dbName).table(dumpTable).toStream({ writable: true, highWaterMark: 20 }))
  //     .on('finish', function () {
  //       r.expr([
  //         r1.db(dbName).table(tableName).count(),
  //         r1.db(dbName).table(dumpTable).count()
  //       ])).then(function (result) {
  //         if (result[0] !== result[1]) {
  //           done(new Error('All the data should have been streamed'))
  //         }
  //         return r1.db(dbName).table(dumpTable).delete()
  //       }).then((_) => r1.getPool().drain()).then(done).error(done)
  //     })
  // })

  // it('pipe should work with a writable stream - 20-200', function (done) {
  //   const r1 = rethinkdbdash({ buffer: 1, max: 2, discovery: false, silent: true })

  //   r1.db(dbName).table(tableName).toStream({ highWaterMark: 20 })
  //     .pipe(r1.db(dbName).table(dumpTable).toStream({ writable: true, highWaterMark: 200 }))
  //     .on('finish', function () {
  //       r.expr([
  //         r1.db(dbName).table(tableName).count(),
  //         r1.db(dbName).table(dumpTable).count()
  //       ])).then(function (result) {
  //         if (result[0] !== result[1]) {
  //           done(new Error('All the data should have been streamed'))
  //         }
  //         return r1.db(dbName).table(dumpTable).delete()
  //       }).then((_) => r1.getPool().drain()).then(done).error(done)
  //     })
  // })

  // it('pipe should work with a writable stream - 50-50', function (done) {
  //   const r1 = rethinkdbdash({ buffer: 1, max: 2, discovery: false, silent: true })

  //   r1.db(dbName).table(tableName).toStream({ highWaterMark: 50 })
  //     .pipe(r1.db(dbName).table(dumpTable).toStream({ writable: true, highWaterMark: 50 }))
  //     .on('finish', function () {
  //       r.expr([
  //         r1.db(dbName).table(tableName).count(),
  //         r1.db(dbName).table(dumpTable).count()
  //       ])).then(function (result) {
  //         if (result[0] !== result[1]) {
  //           done(new Error('All the data should have been streamed'))
  //         }
  //         return r1.db(dbName).table(dumpTable).delete()
  //       }).then((_) => r1.getPool(0).drain()).then(done).error(done)
  //     })
  // })

  // it('toStream((writable: true}) should handle options', function (done) {
  //   const r1 = rethinkdbdash({ buffer: 1, max: 2, discovery: false, silent: true })

  //   const stream =
  // r1.db(dbName).table(dumpTable).toStream({ writable: true, highWaterMark: 50, conflict: 'replace' })
  //   stream.write({ id: 1, foo: 1 })
  //   stream.write({ id: 1, foo: 2 })
  //   stream.end({ id: 1, foo: 3 })

  //   stream.on('finish', function () {
  //     r1.db(dbName).table(dumpTable).count().then(function (result) {
  //       assert.equal(result, 1)
  //       return r1.db(dbName).table(dumpTable).get(1)
  //     }).then(function (result) {
  //       assert.deepEqual(result, { id: 1, foo: 3 })
  //       return r1.db(dbName).table(dumpTable).delete()
  //     }).then((_) => r1.getPool(0).drain()).then(done).error(done)
  //   })
  // })

  // it('test pipe all streams', function (done) {
  //   // Create a transform stream that will convert data to a string
  //   const stream = require('stream')
  //   const addfoobar = new stream.Transform()
  //   addfoobar._writableState.objectMode = true
  //   addfoobar._readableState.objectMode = true
  //   addfoobar._transform = function (data, encoding, done) {
  //     data.transform = true
  //     this.push(data)
  //     done()
  //   }
  //   const addbuzzlol = new stream.Transform()
  //   addbuzzlol._writableState.objectMode = true
  //   addbuzzlol._readableState.objectMode = true
  //   addbuzzlol._transform = function (data, encoding, done) {
  //     delete data.id
  //     data.written = true
  //     this.push(data)
  //     done()
  //   }
  //   r.db(dbName).table(tableName).without('id').toStream()
  //     .on('error', done)
  //     .pipe(addfoobar)
  //     .on('error', done)
  //     .pipe(r.db(dbName).table(dumpTable).toStream({ transform: true }))
  //     .on('error', done)
  //     .pipe(addbuzzlol)
  //     .on('error', done)
  //     .pipe(r.db(dbName).table(dumpTable).toStream({ writable: true }))
  //     .on('error', done)
  //     .on('finish', function () {
  //       r.db(dbName).table(dumpTable).filter({ written: true }).count()).then(function (result) {
  //         assert(result, numDocs)
  //         return r.db(dbName).table(dumpTable).filter({ transform: true }).count())
  //       }).then(function (result) {
  //         assert(result, numDocs * 2)
  //         return r.db(dbName).table(dumpTable).delete()
  //       }).then((_) => r.getPoolMaster().drain()).then(done).error(done)
  //     })
  // })

  // it('toStream({writable: true}) should throw on something else than a table', async function () {
  //   const r1 = rethinkdbdash({ buffer: 1, max: 2, discovery: false, silent: true })
  //   try {
  //     r.expr(dumpTable).toStream({ writable: true })
  //     assert.fail('should throw')
  //   } catch (err) {
  //     assert(err.message.match(/^Cannot create a writable stream on something else than a table/))
  //   } finally {
  //     r1.getPool().drain()
  //   }
  // })

  // it('toStream({transform: true}) should throw on something else than a table', async function () {
  //   const r1 = rethinkdbdash({ buffer: 1, max: 2, discovery: false, silent: true })
  //   try {
  //     r.expr(dumpTable).toStream({ transform: true })
  //     assert.fail('should throw')
  //   } catch (err) {
  //     assert(err.message.match(/^Cannot create a writable stream on something else than a table/))
  //   } finally {
  //     r1.getPool().drain()
  //   }
  // })
});
