import assert from 'assert';
import { createRethinkdbMasterPool, r } from '../src';
import { globals } from '../src/query-builder/globals';
import config from './config';
import { uuid } from './util/common';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('backtraces', () => {
  let dbName: string;
  let tableName: string;
  let result: any;
  let pool: MasterConnectionPool;

  before(async () => {
    globals.backtraceType = 'function';
    globals.pretty = true;
    pool = await createRethinkdbMasterPool([config.server], config.options);
    dbName = uuid();
    tableName = uuid();

    result = await pool.run(r.dbCreate(dbName));
    assert.equal(result.dbs_created, 1);

    result = await pool.run(r.db(dbName).tableCreate(tableName));
    assert.equal(result.tables_created, 1);

    result = await pool.run(
      r.db(dbName).table(tableName).insert(Array(100).fill({})),
    );
    assert.equal(result.inserted, 100);
    assert.equal(result.generated_keys.length, 100);
  });

  after(async () => {
    await pool.drain();
  });

  /*
   *** NOTE ***
   *
   * Most of the backtraces are broken on the server.
   * By broken, I mean they are most of the time not precise, like when a table doesn't exists,
   * it underlines the database and the table. Or when you add a string to a number, it underlines
   * everything and not just the string.
   *
   * We still keep tests for all the terms to be sure that at least, we properly print them.
   *
   ************
   */

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.dbDrop(1)
       ^
  */
  it('Test backtrace for r.dbDrop(1)', async () => {
    try {
      globals.nextVarId = 1;
      // @ts-ignore
      await pool.run(r.dbDrop(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.dbDrop(1)\n         ^ \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.dbCreate(1)
         ^
  */
  it('Test backtrace for r.dbCreate(1)', async () => {
    try {
      globals.nextVarId = 1;
      // @ts-ignore
      await pool.run(r.dbCreate(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.dbCreate(1)\n           ^ \n',
      );
    }
  });

  /*
  //TODO Broken on the server
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Expected type ARRAY but found STRING in:
  r.dbList().do(function(var_1) {
    return var_1.add("a")
         ^^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.dbList().do(function(x) { return x.add("a") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.dbList().do((x) => x.add('a')));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found STRING in:\nr.dbList().do(function(var_1) {\n    return var_1.add("a")\n           ^^^^^^^^^^^^^^\n})\n',
      );
    }
  });

  /*
  //TODO Broken on the server
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr(2).do(function(var_1) {
    return var_1.add("a")
         ^^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.expr(2).do(function(x) { return x.add("a") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.expr(2).do(function (x) {
          return x.add('a');
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr(2).do(function(var_1) {\n    return var_1.add("a")\n           ^^^^^^^^^^^^^^\n})\n',
      );
    }
  });

  /*
  //TODO Broken on the server
  Frames:
  []

  Error:
  Table `551f695a834f94e0fe215e19441b01c9` already exists in:
  r.db("7debc6e4a249569a1a6280fd6e871270").tableCreate("551f695a834f94e0fe215e19441b01c9")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).tableCreate(tableName)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.db(dbName).tableCreate(tableName));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Table \`${dbName}.${tableName}\` already exists in:\nr.db("${dbName}").tableCreate("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n`,
      );
    }
  });

  /*
  //TODO Broken on the server
  Frames:
  []

  Error:
  Table `nonExistingTable` does not exist in:
  r.db("4ab068e0ed6b05f71dcd4b07034698c4").tableDrop("nonExistingTable")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).tableDrop("nonExistingTable")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.db(dbName).tableDrop('nonExistingTable'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Table \`${dbName}.nonExistingTable\` does not exist in:\nr.db("${dbName}").tableDrop("nonExistingTable")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n`,
      );
    }
  });

  /*
  //TODO Broken on the server
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Expected type ARRAY but found STRING in:
  r.db("9cdeba73602f74f7ad67f77c76a87528").tableList().do(function(var_1) {
    return var_1.add("a")
         ^^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.db(dbName).tableList().do(function(x) { return x.add("a") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .tableList()
          .do((x) => x.add('a')),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type ARRAY but found STRING in:\nr.db("${dbName}").tableList().do(function(var_1) {\n    return var_1.add("a")\n           ^^^^^^^^^^^^^^\n})\n`,
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 }, { type: 'POS', pos: 1 } ]

  Error:
  Index `zoo` already exists in:
  r.expr(["zoo", "zoo"]).forEach(function(var_1) {
    return r.db("428a2a382eb5982146afe283b811f367").table("32ae19310d055f18500b41db757337f2")
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      .indexCreate(var_1)
      ^^^^^^^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.expr(["zoo", "zoo"]).forEach(function(index) { return r.db(dbName).table(tableName).indexCreate(index) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .expr(['zoo', 'zoo'])
          .forEach((index) => r.db(dbName).table(tableName).indexCreate(index)),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Index \`zoo\` already exists on table \`${dbName}.${tableName}\` in:\nr.expr(["zoo", "zoo"]).forEach(function(var_1) {\n    return r.db("${dbName}").table("${tableName}")\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n        .indexCreate(var_1)\n        ^^^^^^^^^^^^^^^^^^^\n})\n`,
      );
    }
  });

  /*
  //TODO Broken on the server
  Frames:
  []

  Error:
  Index `nonExistingIndex` does not exist in:
  r.db("91105f3567295643808ed9bab508ec25").table("35adbd4339c2fd4d285f27543e1663ec")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .indexDrop("nonExistingIndex")
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).table(tableName).indexDrop("nonExistingIndex")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.db(dbName).table(tableName).indexDrop('nonExistingIndex'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Index \`nonExistingIndex\` does not exist on table \`${dbName}.${tableName}\` in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .indexDrop("nonExistingIndex")\n    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n`,
      );
    }
  });

  /*
  //TODO Broken on the server
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Expected type ARRAY but found STRING in:
  r.db("7973e432e0aed7e4b1e6951f6049157d").table("37c62a0922bc471c6d751f8f75560cb8")
    .indexList().do(function(var_1) {
      return var_1.add("a")
           ^^^^^^^^^^^^^^
    })
  */
  it('Test backtrace for r.db(dbName).table(tableName).indexList().do(function(x) { return x.add("a") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .indexList()
          .do(function (x) {
            return x.add('a');
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type ARRAY but found STRING in:\nr.db("${dbName}").table("${tableName}")\n    .indexList().do(function(var_1) {\n        return var_1.add("a")\n               ^^^^^^^^^^^^^^\n    })\n`,
      );
    }
  });

  /*
  Frames:
  //TODO Broken on the server
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Expected type ARRAY but found STRING in:
  r.db("a0d88feb61e3d0743bde45b625e7f237").table("8e1f71fefc1f86b66348c96466951df3")
    .indexWait().do(function(var_1) {
      return var_1.add("a")
           ^^^^^^^^^^^^^^
    })
  */
  it('Test backtrace for r.db(dbName).table(tableName).indexWait().do(function(x) { return x.add("a") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .indexWait()
          .do(function (x) {
            return x.add('a');
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type ARRAY but found STRING in:\nr.db("${dbName}").table("${tableName}")\n    .indexWait().do(function(var_1) {\n        return var_1.add("a")\n               ^^^^^^^^^^^^^^\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Index `bar` was not found in:
  r.db("d095569a80834591e8053539e111299a").table("be4967584fdf58b6a5dab0cd633ba046")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .indexWait("foo", "bar")
  */
  // https://github.com/rethinkdb/rethinkdb/issues/4501
  /*
  it('Test backtrace for r.db(dbName).table(tableName).indexWait("foo", "bar")', async () => {
      r.nextVarId=1;
      await pool.run(r.db(dbName).table(tableName).indexWait("foo", "bar"))
      assert.fail('should throw')
    }
    catch(e) {
      assert(e.message === "Index `bar` was not found on table `"+dbName+"."+tableName+"` in:\nr.db(\""+dbName+"\").table(\""+tableName+"\")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .indexWait(\"foo\", \"bar\")\n")
    }
  })
  */

  /*
  //TODO Broken on the server
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.db("340daf900a4168235e5e21e53f8ccdd1").table("9276ce6940b79f4b4f64ab7812532c6e")
    .indexStatus().and(r.expr(1).add("a"))
               ^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).table(tableName).indexStatus().and( r.expr(1).add("a"))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.db(dbName).table(tableName).indexStatus().and(r.expr(1).add('a')),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type NUMBER but found STRING in:\nr.db("${dbName}").table("${tableName}")\n    .indexStatus().and(r.expr(1).add("a"))\n                       ^^^^^^^^^^^^^^^^^^ \n`,
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Index `bar` was not found on table `64f4fc7f01449d2b7aa567576b291659.449aba951895d77bc975046902f51310` in:
  r.db("64f4fc7f01449d2b7aa567576b291659").table("449aba951895d77bc975046902f51310")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      .indexStatus("foo", "bar").do(function(var_1) {
      ^^^^^^^^^^^^^^^^^^^^^^^^^^
          return var_1.add("a")
      })
  */
  it('Test backtrace for r.db(dbName).table(tableName).indexStatus("foo", "bar").do(function(x) { return x.add("a") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .indexStatus('foo', 'bar')
          .do(function (x) {
            return x.add('a');
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Index \`bar\` was not found on table \`${dbName}.${tableName}\` in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .indexStatus("foo", "bar").do(function(var_1) {\n    ^^^^^^^^^^^^^^^^^^^^^^^^^^                     \n        return var_1.add("a")\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Table `882c5069473a016b03069a24679271c5.nonExistingTable` does not exist in:
  r.db("882c5069473a016b03069a24679271c5").table("nonExistingTable").update({
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    foo: "bar"
  })
  */
  it('Test backtrace for r.db(dbName).table("nonExistingTable").update({foo: "bar"})', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.db(dbName).table('nonExistingTable').update({ foo: 'bar' }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Table \`${dbName}.nonExistingTable\` does not exist in:\nr.db("${dbName}").table("nonExistingTable").update({\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^         \n    foo: "bar"\n})\n`,
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Table `8d192301ed6e6937c7d2e6d836f79b20.nonExistingTable` does not exist in:
  r.db("8d192301ed6e6937c7d2e6d836f79b20").table("nonExistingTable").update(function(var_1) {
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    return var_1("foo")
  })
  */
  it('Test backtrace for r.db(dbName).table("nonExistingTable").update(function(doc) { return doc("foo") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table('nonExistingTable')
          .update(function (doc) {
            return doc('foo');
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Table \`${dbName}.nonExistingTable\` does not exist in:\nr.db("${dbName}").table("nonExistingTable").update(function(var_1) {\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^                         \n    return var_1("foo")\n})\n`,
      );
    }
  });

  /*
  //TODO Broken on the server
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Table `nonExistingTable` does not exist in:
  r.db("e7e04bbadd0f0b43f3561b32f2e1b5d6").table("nonExistingTable").replace({
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    foo: "bar"
  })
  */
  it('Test backtrace for r.db(dbName).table("nonExistingTable").replace({foo: "bar"})', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.db(dbName).table('nonExistingTable').replace({ foo: 'bar' }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Table \`${dbName}.nonExistingTable\` does not exist in:\nr.db("${dbName}").table("nonExistingTable").replace({\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^          \n    foo: "bar"\n})\n`,
      );
    }
  });

  /*
  //TODO Broken on the server
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Table `nonExistingTable` does not exist in:
  r.db("9ca06265cbe173eeb27decb1baedb031").table("nonExistingTable").replace(function(var_1) {
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    return var_1("foo")
  })
  */
  it('Test backtrace for r.db(dbName).table("nonExistingTable").replace(function(doc) { return doc("foo") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table('nonExistingTable')
          .replace(function (doc) {
            return doc('foo');
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Table \`${dbName}.nonExistingTable\` does not exist in:\nr.db("${dbName}").table("nonExistingTable").replace(function(var_1) {\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^                          \n    return var_1("foo")\n})\n`,
      );
    }
  });

  /*
  //TODO Broken on the server
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Table `nonExistingTable` does not exist in:
  r.db("0ec51cb31ddf56339cd7acab73b08a2c").table("nonExistingTable").delete()
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).table("nonExistingTable").delete()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.db(dbName).table('nonExistingTable').delete());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Table \`${dbName}.nonExistingTable\` does not exist in:\nr.db("${dbName}").table("nonExistingTable").delete()\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^         \n`,
      );
    }
  });

  /*
  //TODO Broken on the server
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Table `nonExistingTable` does not exist in:
  r.db("a01528b1d8902639d48b9c0adcc397a5").table("nonExistingTable").sync()
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).table("nonExistingTable").sync()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.db(dbName).table('nonExistingTable').sync());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Table \`${dbName}.nonExistingTable\` does not exist in:\nr.db("${dbName}").table("nonExistingTable").sync()\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^       \n`,
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Database `nonExistingDb` does not exist in:
  r.db("nonExistingDb").table("nonExistingTable")
  ^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.db("nonExistingDb").table("nonExistingTable")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.db('nonExistingDb').table('nonExistingTable'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Database `nonExistingDb` does not exist in:\nr.db("nonExistingDb").table("nonExistingTable")\n^^^^^^^^^^^^^^^^^^^^^                          \n',
      );
    }
  });

  /*
  //TODO Broken on the server
  Frames:
  []

  Error:
  Table `nonExistingTable` does not exist in:
  r.db("d1869ecfd2f2e939f5f9ff18b7293370").table("nonExistingTable")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).table("nonExistingTable")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.db(dbName).table('nonExistingTable'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Table \`${dbName}.nonExistingTable\` does not exist in:\nr.db("${dbName}").table("nonExistingTable")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n`,
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Expected type NUMBER but found NULL in:
  r.db("9a3275b288394920100ca6cd5d9ebc77").table("aaf8fd26eb4093b4bcd1c051acd44b80")
    .get(1).do(function(var_1) {
      return var_1.add(3)
           ^^^^^^^^^^^^
    })
  */
  it('Test backtrace for r.db(dbName).table(tableName).get(1).do(function(x) { return x.add(3) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .get(1)
          .do(function (x) {
            return x.add(3);
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type NUMBER but found NULL in:\nr.db("${dbName}").table("${tableName}")\n    .get(1).do(function(var_1) {\n        return var_1.add(3)\n               ^^^^^^^^^^^^\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Expected type DATUM but found SELECTION:
  SELECTION ON table(0c2967f3799eb2025b4cd92342dfe4a9) in:
  r.db("cd911f3c958c1ec7637f7f2dc2827245").table("0c2967f3799eb2025b4cd92342dfe4a9")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .getAll(1, 2, 3).do(function(var_1) {
    ^^^^^^^^^^^^^^^^
      return var_1.add(3)
    })
  */
  it('Test backtrace for r.db(dbName).table(tableName).getAll(1, 2, 3).do(function(x) { return x.add(3) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .getAll(1, 2, 3)
          .do(function (x) {
            return x.add(3);
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type DATUM but found SELECTION:\nSELECTION ON table(${tableName}) in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .getAll(1, 2, 3).do(function(var_1) {\n    ^^^^^^^^^^^^^^^^                     \n        return var_1.add(3)\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Expected type DATUM but found SELECTION:
  SELECTION ON table(2fb59ffdec1b6605369953703547f82d) in:
  r.db("52bdcbc788f0c0b00357fa1840f62a81").table("2fb59ffdec1b6605369953703547f82d")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .getAll(1, 2, 3, {
    ^^^^^^^^^^^^^^^^^^
      index: "foo"
      ^^^^^^^^^^^^
    }).do(function(var_1) {
    ^^
      return var_1.add(3)
    })
  */
  it('Test backtrace for r.db(dbName).table(tableName).getAll(1, 2, 3, { index: "foo"}).do(function(x) { return x.add(3) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .getAll(1, 2, 3, { index: 'foo' })
          .do(function (x) {
            return x.add(3);
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type DATUM but found SELECTION:\nSELECTION ON table(${tableName}) in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .getAll(1, 2, 3, {\n    ^^^^^^^^^^^^^^^^^^\n        index: "foo"\n        ^^^^^^^^^^^^\n    }).do(function(var_1) {\n    ^^                     \n        return var_1.add(3)\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Expected type DATUM but found TABLE_SLICE:
  SELECTION ON table(a163b9372202a469fa7485f6c20b9f4f) in:
  r.db("8bd65d3ca931f3587cc5f3acee0e9f6d").table("a163b9372202a469fa7485f6c20b9f4f")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .between(2, 3, {
    ^^^^^^^^^^^^^^^^
      index: "foo"
      ^^^^^^^^^^^^
    }).do(function(var_1) {
    ^^
      return var_1.add(3)
    })
  */
  it('Test backtrace for r.db(dbName).table(tableName).between(2, 3, { index: "foo"}).do(function(x) { return x.add(3) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .between(2, 3, { index: 'foo' })
          .do(function (x) {
            return x.add(3);
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type DATUM but found TABLE_SLICE:\nSELECTION ON table(${tableName}) in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .between(2, 3, {\n    ^^^^^^^^^^^^^^^^\n        index: "foo"\n        ^^^^^^^^^^^^\n    }).do(function(var_1) {\n    ^^                     \n        return var_1.add(3)\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Expected type DATUM but found SELECTION:
  SELECTION ON table(775cb364800937836f7ecaafc6405cf0) in:
  r.db("39ae0baa00e8cb2da57783c544f569d3").table("775cb364800937836f7ecaafc6405cf0")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .filter({
    ^^^^^^^^^
      foo: "bar"
      ^^^^^^^^^^
    }).do(function(var_1) {
    ^^
      return var_1.add(3)
    })
  */
  it('Test backtrace for r.db(dbName).table(tableName).filter({foo: "bar"}).do(function(x) { return x.add(3) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .filter({ foo: 'bar' })
          .do(function (x) {
            return x.add(3);
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type DATUM but found SELECTION:\nSELECTION ON table(${tableName}) in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .filter({\n    ^^^^^^^^^\n        foo: "bar"\n        ^^^^^^^^^^\n    }).do(function(var_1) {\n    ^^                     \n        return var_1.add(3)\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type SEQUENCE but found FUNCTION:
  VALUE FUNCTION in:
  r.expr([1, 2, 3]).innerJoin(function(var_1, var_2) {
                ^^^^^^^^^^^^^^^^^^^^^^^^
    return var_1.eq(var_2("bar").add(1))
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  }, r.db("3fdf480de398b8b0c5dee11b4594a38d").table("5f728046b728da8d63ace65a40aca6a6"))
  ^
  */
  it('Test backtrace for r.expr([1,2,3]).innerJoin( function(left, right) { return left.eq(right("bar").add(1)) }, r.db(dbName).table(tableName))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.expr([1, 2, 3]).innerJoin(function (left, right) {
          return left.eq(right('bar').add(1));
        }, r.db(dbName).table(tableName)),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type SEQUENCE but found FUNCTION:\nVALUE FUNCTION in:\nr.expr([1, 2, 3]).innerJoin(function(var_1, var_2) {\n                            ^^^^^^^^^^^^^^^^^^^^^^^^\n    return var_1.eq(var_2("bar").add(1))\n    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n}, r.db("${dbName}").table("${tableName}"))\n^                                                                                     \n`,
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 2 },
    { type: 'POS', pos: 1 },
    { type: 'POS', pos: 0 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3]).innerJoin([1, 2, 3], function(var_1, var_2) {
    return r.expr(1).add("str").add(var_1.eq(var_2("bar").add(1)))
         ^^^^^^^^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.expr([1,2,3]).innerJoin(r.expr([1,2,3]), function(left, right) { return r.expr(1).add("str").add(left.eq(right("bar").add(1))) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.expr([1, 2, 3]).innerJoin(r.expr([1, 2, 3]), function (left, right) {
          return r
            .expr(1)
            .add('str')
            .add(left.eq(right('bar').add(1)));
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3]).innerJoin([1, 2, 3], function(var_1, var_2) {\n    return r.expr(1).add("str").add(var_1.eq(var_2("bar").add(1)))\n           ^^^^^^^^^^^^^^^^^^^^                                   \n})\n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type SEQUENCE but found FUNCTION:
  VALUE FUNCTION in:
  r.expr([1, 2, 3]).outerJoin(function(var_1, var_2) {
                ^^^^^^^^^^^^^^^^^^^^^^^^
    return var_1.eq(var_2("bar").add(1))
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  }, r.db("5f21a25338fff022c0f698f8681c03c0").table("1653b107790bf38e48448f3db99ab776"))
  ^
  */
  it('Test backtrace for r.expr([1,2,3]).outerJoin( function(left, right) { return left.eq(right("bar").add(1)) }, r.db(dbName).table(tableName))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.expr([1, 2, 3]).outerJoin(function (left, right) {
          return left.eq(right('bar').add(1));
        }, r.db(dbName).table(tableName)),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type SEQUENCE but found FUNCTION:\nVALUE FUNCTION in:\nr.expr([1, 2, 3]).outerJoin(function(var_1, var_2) {\n                            ^^^^^^^^^^^^^^^^^^^^^^^^\n    return var_1.eq(var_2("bar").add(1))\n    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n}, r.db("${dbName}").table("${tableName}"))\n^                                                                                     \n`,
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Cannot perform get_field on a non-object non-sequence `1` in:
  r.expr([1, 2, 3]).eqJoin("id", r.db("5500af7b5c2c94b2672a5f0029512757").table("85bbcc72331aa82bfe0306204997613e"))
               ^^^^
    .add(1)
  */
  it('Test backtrace for r.expr([1,2,3]).eqJoin("id", r.db(dbName).table(tableName)).add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.expr([1, 2, 3]).eqJoin('id', r.db(dbName).table(tableName)).add(1),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `Cannot perform get_field on a non-object non-sequence \`1\` in:\nr.expr([1, 2, 3]).eqJoin("id", r.db("${dbName}").table("${tableName}"))\n                         ^^^^                                                                                     \n    .add(1)\n`,
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 },
    { type: 'POS', pos: 0 },
    { type: 'POS', pos: 1 } ]

  Error:
  Cannot perform get_field on a non-object non-sequence `1` in:
  r.expr([1, 2, 3]).eqJoin("id", r.db("2c1030e5160e4af3bb19923d43fe7d6c").table("8895da1f043cb7443f322ce849d7fced"))
               ^^^^
    .zip().add(1)
  */
  it('Test backtrace for r.expr([1,2,3]).eqJoin("id", r.db(dbName).table(tableName)).zip().add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .expr([1, 2, 3])
          .eqJoin('id', r.db(dbName).table(tableName))
          .zip()
          .add(1),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Cannot perform get_field on a non-object non-sequence \`1\` in:\nr.expr([1, 2, 3]).eqJoin("id", r.db("${dbName}").table("${tableName}"))\n                         ^^^^                                                                                     \n    .zip().add(1)\n`,
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type ARRAY but found NUMBER in:
  r.expr([1, 2, 3]).map(function(var_1) {
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    return var_1
    ^^^^^^^^^^^^
  }).add(1)
  ^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).map(function(v) { return v}).add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .expr([1, 2, 3])
          .map(function (v) {
            return v;
          })
          .add(1),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found NUMBER in:\nr.expr([1, 2, 3]).map(function(var_1) {\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    return var_1\n    ^^^^^^^^^^^^\n}).add(1)\n^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Cannot perform has_fields on a non-object non-sequence `1` in:
  r.expr([1, 2, 3]).withFields("foo", "bar").add(1)
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).withFields("foo", "bar").add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).withFields('foo', 'bar').add(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot perform has_fields on a non-object non-sequence `1` in:\nr.expr([1, 2, 3]).withFields("foo", "bar").add(1)\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^       \n',
      );
    }
  });

  /*
  Frames:
  [ 0, 1 ]

  Error:
  Cannot convert NUMBER to SEQUENCE in:
  r.expr([1, 2, 3]).concatMap(function(var_1) {
                              ^^^^^^^^^^^^^^^^^
      return var_1
      ^^^^^^^^^^^^
  }).add(1)
  ^
  */
  it('Test backtrace for r.expr([1,2,3]).concatMap(function(v) { return v}).add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .expr([1, 2, 3])
          .concatMap(function (v) {
            return v;
          })
          .add(1),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot convert NUMBER to SEQUENCE in:\nr.expr([1, 2, 3]).concatMap(function(var_1) {\n                            ^^^^^^^^^^^^^^^^^\n    return var_1\n    ^^^^^^^^^^^^\n}).add(1)\n^        \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Cannot perform get_field on a non-object non-sequence `2` in:
  r.expr([1, 2, 3]).orderBy("foo").add(1)
                ^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).orderBy("foo").add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).orderBy('foo').add(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot perform get_field on a non-object non-sequence `2` in:\nr.expr([1, 2, 3]).orderBy("foo").add(1)\n                          ^^^^^        \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3]).skip("foo").add(1)
               ^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).skip("foo").add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).skip('foo').add(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3]).skip("foo").add(1)\n                       ^^^^^        \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3]).limit("foo").add(1)
              ^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).limit("foo").add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).limit('foo').add(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3]).limit("foo").add(1)\n                        ^^^^^        \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3]).slice("foo", "bar").add(1)
              ^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).slice("foo", "bar").add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).slice('foo', 'bar').add(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3]).slice("foo", "bar").add(1)\n                        ^^^^^               \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3]).nth("bar").add(1)
              ^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).nth("bar").add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).nth('bar').add(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3]).nth("bar").add(1)\n                      ^^^^^        \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type ARRAY but found STRING in:
  r.expr([1, 2, 3]).offsetsOf("bar").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1, 2, 3]).offsetsOf("bar").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).offsetsOf('bar').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found STRING in:\nr.expr([1, 2, 3]).offsetsOf("bar").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.expr([1, 2, 3]).isEmpty().add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).isEmpty().add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).isEmpty().add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.expr([1, 2, 3]).isEmpty().add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type ARRAY but found STRING in:
  r.expr([1, 2, 3]).union([5, 6]).add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).union([5,6]).add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).union([5, 6]).add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found STRING in:\nr.expr([1, 2, 3]).union([5, 6]).add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3]).sample("Hello")
               ^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).sample("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).sample('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3]).sample("Hello")\n                         ^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3]).count(() => {
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    return true
    ^^^^^^^^^^^
  }).add("Hello")
  ^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).count(() => { return true}).add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .expr([1, 2, 3])
          .count(() => {
            return true;
          })
          .add('Hello'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3]).count(function() {\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    return true\n    ^^^^^^^^^^^\n}).add("Hello")\n^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type ARRAY but found STRING in:
  r.expr([1, 2, 3]).distinct().add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).distinct().add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).distinct().add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found STRING in:\nr.expr([1, 2, 3]).distinct().add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.expr([1, 2, 3]).contains("foo", "bar").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).contains("foo", "bar").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).contains('foo', 'bar').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.expr([1, 2, 3]).contains("foo", "bar").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ 0, 0 ]

  Error:
  Expected type SELECTION but found DATUM:
  [
    1,
    2,
    3
  ] in:
  r.expr([1, 2, 3]).update(r.row("foo")).add("Hello")
  ^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).update(r.row("foo")).add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).update(r.row('foo')).add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Expected type SELECTION but found DATUM:\n[\n\t1,\n\t2,\n\t3\n] in:\nr.expr([1, 2, 3]).update(r.row("foo")).add("Hello")\n^^^^^^^^^^^^^^^^^                                  \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Cannot perform pluck on a non-object non-sequence `1` in:
  r.expr([1, 2, 3]).pluck("foo").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).pluck("foo").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).pluck('foo').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot perform pluck on a non-object non-sequence `1` in:\nr.expr([1, 2, 3]).pluck("foo").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Cannot perform without on a non-object non-sequence `1` in:
  r.expr([1, 2, 3]).without("foo").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).without("foo").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).without('foo').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot perform without on a non-object non-sequence `1` in:\nr.expr([1, 2, 3]).without("foo").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Cannot perform merge on a non-object non-sequence `1` in:
  r.expr([1, 2, 3]).merge("foo").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).merge("foo").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).merge('foo').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot perform merge on a non-object non-sequence `1` in:\nr.expr([1, 2, 3]).merge("foo").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type ARRAY but found STRING in:
  r.expr([1, 2, 3]).append("foo").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).append("foo").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).append('foo').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found STRING in:\nr.expr([1, 2, 3]).append("foo").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type ARRAY but found STRING in:
  r.expr([1, 2, 3]).prepend("foo").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).prepend("foo").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).prepend('foo').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found STRING in:\nr.expr([1, 2, 3]).prepend("foo").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Cannot convert STRING to SEQUENCE in:
  r.expr([1, 2, 3]).difference("foo").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).difference("foo").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).difference('foo').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot convert STRING to SEQUENCE in:\nr.expr([1, 2, 3]).difference("foo").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type ARRAY but found STRING in:
  r.expr([1, 2, 3]).setInsert("foo").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).setInsert("foo").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).setInsert('foo').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found STRING in:\nr.expr([1, 2, 3]).setInsert("foo").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type ARRAY but found STRING in:
  r.expr([1, 2, 3]).setUnion("foo").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).setUnion("foo").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).setUnion('foo').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found STRING in:\nr.expr([1, 2, 3]).setUnion("foo").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type ARRAY but found STRING in:
  r.expr([1, 2, 3]).setIntersection("foo").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).setIntersection("foo").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).setIntersection('foo').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found STRING in:\nr.expr([1, 2, 3]).setIntersection("foo").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Cannot perform bracket on a non-object non-sequence `1` in:
  r.expr([1, 2, 3])("foo").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1, 2, 3])("foo").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3])('foo').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot perform bracket on a non-object non-sequence `1` in:\nr.expr([1, 2, 3])("foo").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Cannot perform has_fields on a non-object non-sequence `1` in:
  r.expr([1, 2, 3]).hasFields("foo").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).hasFields("foo").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).hasFields('foo').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot perform has_fields on a non-object non-sequence `1` in:\nr.expr([1, 2, 3]).hasFields("foo").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3]).insertAt("foo", 2).add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).insertAt("foo", 2).add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).insertAt('foo', 2).add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3]).insertAt("foo", 2).add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3]).spliceAt("foo", 2).add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).spliceAt("foo", 2).add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).spliceAt('foo', 2).add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3]).spliceAt("foo", 2).add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3]).deleteAt("foo", 2).add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).deleteAt("foo", 2).add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).deleteAt('foo', 2).add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3]).deleteAt("foo", 2).add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3]).changeAt("foo", 2).add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).changeAt("foo", 2).add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).changeAt('foo', 2).add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3]).changeAt("foo", 2).add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  [ 0, 0 ]

  Error:
  Cannot call `keys` on objects of type `ARRAY` in:
  r.expr([1, 2, 3]).keys().add("Hello")
  ^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for  r.expr([1,2,3]).keys().add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).keys().add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot call `keys` on objects of type `ARRAY` in:\nr.expr([1, 2, 3]).keys().add("Hello")\n^^^^^^^^^^^^^^^^^                    \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 0 } ]

  Error:
  Expected type STRING but found ARRAY in:
  r.expr([1, 2, 3]).match("foo").add("Hello")
  ^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).match("foo").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).match('foo').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Expected type STRING but found ARRAY in:\nr.expr([1, 2, 3]).match("foo").add("Hello")\n^^^^^^^^^^^^^^^^^                          \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type ARRAY but found STRING in:
  r.expr([1, 2, 3]).add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found STRING in:\nr.expr([1, 2, 3]).add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found ARRAY in:
  r.expr([1, 2, 3]).sub("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).sub("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).sub('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found ARRAY in:\nr.expr([1, 2, 3]).sub("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3]).mul("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).mul("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).mul('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3]).mul("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found ARRAY in:
  r.expr([1, 2, 3]).div("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).div("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).div('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found ARRAY in:\nr.expr([1, 2, 3]).div("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type NUMBER but found ARRAY in:
  r.expr([1, 2, 3]).mod("Hello")
  ^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).mod("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).mod('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found ARRAY in:\nr.expr([1, 2, 3]).mod("Hello")\n^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr([1, 2, 3]).and(r.expr("Hello").add(2))
              ^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).and(r.expr("Hello").add(2))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).and(r.expr('Hello').add(2)));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.expr([1, 2, 3]).and(r.expr("Hello").add(2))\n                      ^^^^^^^^^^^^^^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr(false).or(r.expr("Hello").add(2))
           ^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr(false).or(r.expr("Hello").add(2))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr(false).or(r.expr('Hello').add(2)));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.expr(false).or(r.expr("Hello").add(2))\n                 ^^^^^^^^^^^^^^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr([1, 2, 3]).eq(r.expr("Hello").add(2))
             ^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).eq(r.expr("Hello").add(2))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).eq(r.expr('Hello').add(2)));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.expr([1, 2, 3]).eq(r.expr("Hello").add(2))\n                     ^^^^^^^^^^^^^^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr([1, 2, 3]).ne(r.expr("Hello").add(2))
             ^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).ne(r.expr("Hello").add(2))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).ne(r.expr('Hello').add(2)));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.expr([1, 2, 3]).ne(r.expr("Hello").add(2))\n                     ^^^^^^^^^^^^^^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr([1, 2, 3]).gt(r.expr("Hello").add(2))
             ^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).gt(r.expr("Hello").add(2))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).gt(r.expr('Hello').add(2)));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.expr([1, 2, 3]).gt(r.expr("Hello").add(2))\n                     ^^^^^^^^^^^^^^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr([1, 2, 3]).lt(r.expr("Hello").add(2))
             ^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).lt(r.expr("Hello").add(2))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).lt(r.expr('Hello').add(2)));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.expr([1, 2, 3]).lt(r.expr("Hello").add(2))\n                     ^^^^^^^^^^^^^^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr([1, 2, 3]).le(r.expr("Hello").add(2))
             ^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).le(r.expr("Hello").add(2))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).le(r.expr('Hello').add(2)));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.expr([1, 2, 3]).le(r.expr("Hello").add(2))\n                     ^^^^^^^^^^^^^^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr([1, 2, 3]).ge(r.expr("Hello").add(2))
             ^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).ge(r.expr("Hello").add(2))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).ge(r.expr('Hello').add(2)));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.expr([1, 2, 3]).ge(r.expr("Hello").add(2))\n                     ^^^^^^^^^^^^^^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr([1, 2, 3]).not().add(r.expr("Hello").add(2))
                ^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).not().add(r.expr("Hello").add(2))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).not().add(r.expr('Hello').add(2)));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.expr([1, 2, 3]).not().add(r.expr("Hello").add(2))\n                            ^^^^^^^^^^^^^^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found STRING in:
  r.now().add("Hello")
  ^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.now().add("Hello")\n^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Error in time logic: Year is out of valid range: 1400..9999 in:
  r.time(1023, 11, 3, "Z").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.time(1023, 11, 3, "Z").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.time(1023, 11, 3, 'Z').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Error in time logic: Year is out of valid range: 1400..9999 in:\nr.time(1023, 11, 3, "Z").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found STRING in:
  r.epochTime(12132131).add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.epochTime(12132131).add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.epochTime(12132131).add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.epochTime(12132131).add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 0 } ]

  Error:
  Invalid date string `UnvalidISO961String` (got `U` but expected a digit) in:
  r.ISO8601("UnvalidISO961String").add("Hello")
        ^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.ISO8601("UnvalidISO961String").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.ISO8601('UnvalidISO961String').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Invalid date string `UnvalidISO961String` (got `U` but expected a digit) in:\nr.ISO8601("UnvalidISO961String").add("Hello")\n          ^^^^^^^^^^^^^^^^^^^^^              \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Timezone `noTimezone` does not start with `-` or `+` in:
  r.now().inTimezone("noTimezone").add("Hello")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().inTimezone("noTimezone").add("Hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().inTimezone('noTimezone').add('Hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Timezone `noTimezone` does not start with `-` or `+` in:\nr.now().inTimezone("noTimezone").add("Hello")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^             \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type STRING but found BOOL in:
  r.now().timezone().add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().timezone().add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().timezone().add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found BOOL in:\nr.now().timezone().add(true)\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.now().during(r.now(), r.now()).add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().during(r.now(), r.now()).add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().during(r.now(), r.now()).add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.now().during(r.now(), r.now()).add(true)\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.now().timeOfDay().add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().timeOfDay().add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().timeOfDay().add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.now().timeOfDay().add(true)\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.now().year().add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().year().add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().year().add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.now().year().add(true)\n^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.now().month().add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().month().add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().month().add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.now().month().add(true)\n^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.now().day().add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().day().add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().day().add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.now().day().add(true)\n^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.now().dayOfWeek().add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().dayOfWeek().add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().dayOfWeek().add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.now().dayOfWeek().add(true)\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.now().dayOfYear().add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().dayOfYear().add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().dayOfYear().add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.now().dayOfYear().add(true)\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.now().hours().add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().hours().add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().hours().add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.now().hours().add(true)\n^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.now().minutes().add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().minutes().add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().minutes().add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.now().minutes().add(true)\n^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.now().seconds().add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().seconds().add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().seconds().add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.now().seconds().add(true)\n^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type STRING but found BOOL in:
  r.now().toISO8601().add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().toISO8601().add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().toISO8601().add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found BOOL in:\nr.now().toISO8601().add(true)\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found BOOL in:
  r.now().toEpochTime().add(true)
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.now().toEpochTime().add(true)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.now().toEpochTime().add(true));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found BOOL in:\nr.now().toEpochTime().add(true)\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ 0, 1, 0, 0 ]

  Error:
  Cannot perform bracket on a non-object non-sequence `1` in:
  r.expr(1).do(function(var_1) {
    return var_1("bah").add(3)
         ^^^^^
  })
  */
  it('Test backtrace for r.expr(1).do(function(var_1) { return var_1("bah").add(3) }) ', async () => {
    try {
      globals.nextVarId = 1;

      await pool.run(
        r.expr(1).do(function (var_1) {
          return var_1('bah').add(3);
        }),
      ); // eslint-disable-line camelcase
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot perform bracket on a non-object non-sequence `1` in:\nr.expr(1).do(function(var_1) {\n    return var_1("bah").add(3)\n           ^^^^^              \n})\n',
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type NUMBER but found STRING in:
  r.branch(r.expr(1).add("hello"), "Hello", "World")
       ^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.branch(r.expr(1).add("hello"), "Hello", "World")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.branch(r.expr(1).add('hello'), 'Hello', 'World'));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Expected type NUMBER but found STRING in:\nr.expr(1).add("hello").branch("Hello", "World")\n^^^^^^^^^^^^^^^^^^^^^^                         \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Cannot convert NUMBER to SEQUENCE in:
  r.expr(1).forEach(function(var_1) {
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    return var_1("bar")
    ^^^^^^^^^^^^^^^^^^^
  })
  ^^
  */
  it('Test backtrace for r.expr(1).forEach(function(foo) { return foo("bar") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.expr(1).forEach(function (foo) {
          return foo('bar');
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot convert NUMBER to SEQUENCE in:\nr.expr(1).forEach(function(var_1) {\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    return var_1("bar")\n    ^^^^^^^^^^^^^^^^^^^\n})\n^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  foo in:
  r.error("foo")
  ^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.error("foo")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.error('foo'));
      assert.fail('should throw');
    } catch (e) {
      assert(e.message === 'foo in:\nr.error("foo")\n^^^^^^^^^^^^^^\n');
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type STRING but found NUMBER in:
  r.expr({
  ^^^^^^^^
    a: 1
    ^^^^
  })("b").default("bar").add(2)
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr({a:1})("b").default("bar").add(2)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr({ a: 1 })('b').default('bar').add(2));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Expected type STRING but found NUMBER in:\nr.expr({\n^^^^^^^^\n    a: 1\n    ^^^^\n})("b").default("bar").add(2)\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found OBJECT in:
  r.expr({
  ^^^^^^^^
    a: 1
    ^^^^
  }).add(2)
  ^^^^^^^^^
  */
  it('Test backtrace for r.expr({a:1}).add(2)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr({ a: 1 }).add(2));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found OBJECT in:\nr.expr({\n^^^^^^^^\n    a: 1\n    ^^^^\n}).add(2)\n^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found OBJECT in:
  r.expr({
  ^^^^^^^^
    a: 1
    ^^^^
  }).add(r.js("2"))
  ^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr({a:1}).add(r.js("2"))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr({ a: 1 }).add(r.js('2')));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found OBJECT in:\nr.expr({\n^^^^^^^^\n    a: 1\n    ^^^^\n}).add(r.js("2"))\n^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Cannot coerce NUMBER to ARRAY in:
  r.expr(2).coerceTo("ARRAY")
  ^^^^^^^^^
  */
  it('Test backtrace for r.expr(2).coerceTo("ARRAY")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr(2).coerceTo('ARRAY'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot coerce NUMBER to ARRAY in:\nr.expr(2).coerceTo("ARRAY")\n^^^^^^^^^                  \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr(2).add("foo").typeOf()
  ^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr(2).add("foo").typeOf()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr(2).add('foo').typeOf());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr(2).add("foo").typeOf()\n^^^^^^^^^^^^^^^^^^^^         \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr(2).add("foo").info()
  ^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr(2).add("foo").info()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr(2).add('foo').info());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr(2).add("foo").info()\n^^^^^^^^^^^^^^^^^^^^       \n',
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Failed to parse "foo" as JSON: Invalid value in:
  r.expr(2).add(r.json("foo"))
                ^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr(2).add(r.json("foo"))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr(2).add(r.json('foo')));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Failed to parse "foo" as JSON: Invalid value in:\nr.expr(2).add(r.json("foo"))\n              ^^^^^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  undefined

  Error:
  Unrecognized option `nonValid` in `replace` after:
  r.db("791087f7a75b40ba6a89f96cafefa643").table("da63855c1650bdd5e653662750771333")
  Available options are returnChanges <bool>, durability <string>, nonAtomic <bool>
  */
  it('Test backtrace for r.db(dbName).table(tableName).replace({a:1}, {nonValid:true})', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.db(dbName).table(tableName).replace({ a: 1 }, { nonValid: true }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.startsWith('Unrecognized optional argument `non_valid` in:'),
      );
    }
  });

  /*
  Frames:
  []

  /*
  Frames:
  [ 'durability' ]

  Error:
  Durability option `softt` unrecognized (options are "hard" and "soft") in:
  r.db("0fbd7374d30a23284bc64625f9b6838a").table("5429869da70a92a5e495ed4989e40e30")
    .replace({
      a: 1
    }, {
       ^
      durability: "softt"
      ^^^^^^^^^^^^^^^^^^^
    })
    ^
  */
  it('Test backtrace for r.db(dbName).table(tableName).replace({a:1}, {durability: "softt"})', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .replace({ a: 1 }, { durability: 'softt' }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `Durability option \`softt\` unrecognized (options are "hard" and "soft") in:\nr.db("${dbName}").table("${tableName}")\n    .replace({\n        a: 1\n    }, {\n        durability: "softt"\n                    ^^^^^^^\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 }, { type: 'POS', pos: 1 } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2]).map(r.row.add("eh"))
             ^^^^^^^^^^^^^^^
  */
  // it('Test backtrace for r.expr([1,2]).map(r.row.add("eh"))', async () => {
  //   try {
  //     globals.nextVarId = 1;
  //     await pool.run(r
  //       .expr([1, 2])
  //       .map(r.row.add('eh'))
  //       );
  //     assert.fail('should throw');
  //   } catch (e) {
  //     assert(
  //       e.message ===
  //         'Expected type NUMBER but found STRING in:\nr.expr([1, 2]).map(r.row.add("eh"))\n                   ^^^^^^^^^^^^^^^ \n'
  //     );
  //   }
  // });

  /*
  Frames:
  [ { type: 'POS', pos: 0 },
    { type: 'POS', pos: 0 },
    { type: 'POS', pos: 0 },
    { type: 'POS', pos: 0 },
    { type: 'POS', pos: 0 },
    { type: 'POS', pos: 0 } ]

  Error:
  Table `test.foo` does not exist in:
  r.table("foo").add(1).add(1).add("hello-super-long-string").add("another-long-string")
  ^^^^^^^^^^^^^^
    .add("one-last-string").map(function(var_1) {
      return r.expr([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]).map(function(var_2) {
        return var_2("b").add("hello-super-long-string").add("another-long-string").add("one-last-string")
          .add("hello-super-long-string").add("another-long-string").add("one-last-string")
          .add("hello-super-long-string").add("another-long-string").add("one-last-string")
          .add("hello-super-long-string").add("another-long-string").add("one-last-string")
          .add("hello-super-long-string").add("another-long-string").add("one-last-string")
          .mul(var_2("b")).merge({
            firstName: "xxxxxx",
            lastName: "yyyy",
            email: "xxxxx@yyyy.com",
            phone: "xxx-xxx-xxxx"
          })
      }).add(2).map(function(var_3) {
        return var_3.add("hello-super-long-string").add("another-long-string").add("one-last-string")
          .add("hello-super-long-string").add("another-long-string").add("one-last-string")
          .add("hello-super-long-string").add("another-long-string").add("one-last-string")
          .add("hello-super-long-string").add("another-long-string").add("one-last-string")
          .add("hello-super-long-string").add("another-long-string").add("one-last-string")
      })
    })
  */
  it('Test backtrace for r.table("foo").add(1).add(1).add("hello-super-long-string").add("another-long-string").add("one-last-string").map( function(doc) { return r.expr([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]).map(function(test) { return test("b").add("hello-super-long-string").add("another-long-string").add("one-last-string").add("hello-super-long-string").add("another-long-string").add("one-last-string").add("hello-super-long-string").add("another-long-string").add("one-last-string").add("hello-super-long-string").add("another-long-string").add("one-last-string").add("hello-super-long-string").add("another-long-string").add("one-last-string").mul(test("b")).merge({ firstName: "xxxxxx", lastName: "yyyy", email: "xxxxx@yyyy.com", phone: "xxx-xxx-xxxx" }); }).add(2).map(function(doc) { return doc.add("hello-super-long-string").add("another-long-string").add("one-last-string").add("hello-super-long-string").add("another-long-string").add("one-last-string").add("hello-super-long-string").add("another-long-string").add("one-last-string").add("hello-super-long-string").add("another-long-string").add("one-last-string").add("hello-super-long-string").add("another-long-string").add("one-last-string") }); })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .table('foo')
          .add(1)
          .add(1)
          .add('hello-super-long-string')
          .add('another-long-string')
          .add('one-last-string')
          .map(function (doc) {
            return r
              .expr([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
              .map(function (test) {
                return test('b')
                  .add('hello-super-long-string')
                  .add('another-long-string')
                  .add('one-last-string')
                  .add('hello-super-long-string')
                  .add('another-long-string')
                  .add('one-last-string')
                  .add('hello-super-long-string')
                  .add('another-long-string')
                  .add('one-last-string')
                  .add('hello-super-long-string')
                  .add('another-long-string')
                  .add('one-last-string')
                  .add('hello-super-long-string')
                  .add('another-long-string')
                  .add('one-last-string')
                  .mul(test('b'))
                  .merge({
                    firstName: 'xxxxxx',
                    lastName: 'yyyy',
                    email: 'xxxxx@yyyy.com',
                    phone: 'xxx-xxx-xxxx',
                  });
              })
              .add(2)
              .map(function (doc) {
                return doc
                  .add('hello-super-long-string')
                  .add('another-long-string')
                  .add('one-last-string')
                  .add('hello-super-long-string')
                  .add('another-long-string')
                  .add('one-last-string')
                  .add('hello-super-long-string')
                  .add('another-long-string')
                  .add('one-last-string')
                  .add('hello-super-long-string')
                  .add('another-long-string')
                  .add('one-last-string')
                  .add('hello-super-long-string')
                  .add('another-long-string')
                  .add('one-last-string');
              });
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Table `test.foo` does not exist in:\nr.db("test").table("foo").add(1).add(1).add("hello-super-long-string").add("another-long-string")\n^^^^^^^^^^^^^^^^^^^^^^^^^                                                                        \n    .add("one-last-string").map(function(var_1) {\n        return r.expr([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]).map(function(var_2) {\n            return var_2("b").add("hello-super-long-string").add("another-long-string").add("one-last-string")\n                .add("hello-super-long-string").add("another-long-string").add("one-last-string")\n                .add("hello-super-long-string").add("another-long-string").add("one-last-string")\n                .add("hello-super-long-string").add("another-long-string").add("one-last-string")\n                .add("hello-super-long-string").add("another-long-string").add("one-last-string")\n                .mul(var_2("b")).merge({\n                    firstName: "xxxxxx",\n                    lastName: "yyyy",\n                    email: "xxxxx@yyyy.com",\n                    phone: "xxx-xxx-xxxx"\n                })\n        }).add(2).map(function(var_2) {\n            return var_2.add("hello-super-long-string").add("another-long-string").add("one-last-string")\n                .add("hello-super-long-string").add("another-long-string").add("one-last-string")\n                .add("hello-super-long-string").add("another-long-string").add("one-last-string")\n                .add("hello-super-long-string").add("another-long-string").add("one-last-string")\n                .add("hello-super-long-string").add("another-long-string").add("one-last-string")\n        })\n    })\n',
      );
    }
  });

  /*
  Frames:
  [ 'b' ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr({
    a: 1,
    b: r.expr(1).add("eh")
       ^^^^^^^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.expr({a:1, b:r.expr(1).add("eh")})', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr({ a: 1, b: r.expr(1).add('eh') }));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Expected type NUMBER but found STRING in:\nr.expr({\n    a: 1,\n    b: r.expr(1).add("eh")\n       ^^^^^^^^^^^^^^^^^^^\n})\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found OBJECT in:
  r.db("330a8b0e7ff2501e855f0d45aebe6006").table("80179c85b797f92a3abbb0e40e7b06a3")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .replace({
    ^^^^^^^^^^
      a: 1
      ^^^^
    }, {
    ^^^^
      durability: "soft"
      ^^^^^^^^^^^^^^^^^^
    }).add(2)
    ^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).table(tableName).replace({a:1}, {durability:"soft"}).add(2)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .replace({ a: 1 }, { durability: 'soft' })
          .add(2),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type NUMBER but found OBJECT in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .replace({\n    ^^^^^^^^^^\n        a: 1\n        ^^^^\n    }, {\n    ^^^^\n        durability: "soft"\n        ^^^^^^^^^^^^^^^^^^\n    }).add(2)\n    ^^^^^^^^^\n`,
      );
    }
  });

  /*
  Frames:
  [ { type: 'OPT', opt: 'durability' } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.db("83db41722b445306270f0129b6bcbde0").table("1264cb52a222e32026ce2d67ac27bc23")
    .replace({
      a: 1
    }, {
       ^
      durability: r.expr(1).add("heloo")
      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    })
    ^
  */
  it('Test backtrace for r.db(dbName).table(tableName).replace({a:1}, {durability:r.expr(1).add("heloo")})', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .replace({ a: 1 }, { durability: r.expr(1).add('heloo') }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `Expected type NUMBER but found STRING in:\nr.db("${dbName}").table("${tableName}")\n    .replace({\n        a: 1\n    }, {\n        durability: r.expr(1).add("heloo")\n                    ^^^^^^^^^^^^^^^^^^^^^^\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ { type: 'OPT', opt: 'durability' } ]

  Error:
  Expected type NUMBER but found STRING in:
  r.db("6dddeb36901f203298878f980598ce0a").table("5510e0388d908dca1fa4a6dbf00c2852")
    .replace({
      a: 1
    }, {
       ^
      durability: r.expr(1).add("heloo")
      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    })
    ^
  */
  it('Test backtrace for r.db(dbName).table(tableName).replace({a:1}, {durability:r.expr(1).add("heloo")})', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .replace({ a: 1 }, { durability: r.expr(1).add('heloo') }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `Expected type NUMBER but found STRING in:\nr.db("${dbName}").table("${tableName}")\n    .replace({\n        a: 1\n    }, {\n        durability: r.expr(1).add("heloo")\n                    ^^^^^^^^^^^^^^^^^^^^^^\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ 'a' ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr({
    a: r.expr(1).add("eh"),
       ^^^^^^^^^^^^^^^^^^^
    b: 2
  })
  */
  it('Test backtrace for r.expr({a:r.expr(1).add("eh"), b: 2})', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr({ a: r.expr(1).add('eh'), b: 2 }));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Expected type NUMBER but found STRING in:\nr.expr({\n    a: r.expr(1).add("eh"),\n       ^^^^^^^^^^^^^^^^^^^ \n    b: 2\n})\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type ARRAY but found STRING in:
  r.expr([1, 2, 3]).add("eh")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).add("eh")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).add('eh'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found STRING in:\nr.expr([1, 2, 3]).add("eh")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found OBJECT in:
  r.expr({
  ^^^^^^^^
    a: 1
    ^^^^
  }).add("eh")
  ^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr({a:1}).add("eh")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr({ a: 1 }).add('eh'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found OBJECT in:\nr.expr({\n^^^^^^^^\n    a: 1\n    ^^^^\n}).add("eh")\n^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 1 } ]

  Error:
  Cannot perform get_field on a non-object non-sequence `1` in:
  r.expr([1, 2, 3]).group("foo")
              ^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).group("foo")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).group('foo'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot perform get_field on a non-object non-sequence `1` in:\nr.expr([1, 2, 3]).group("foo")\n                        ^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type GROUPED_DATA but found DATUM:
  [
    1,
    2,
    3
  ] in:
  r.expr([1, 2, 3]).ungroup()
  ^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).ungroup()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).ungroup());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type GROUPED_DATA but found DATUM:\n[\n\t1,\n\t2,\n\t3\n] in:\nr.expr([1, 2, 3]).ungroup()\n^^^^^^^^^^^^^^^^^          \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3, "hello"]).sum()
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3,"hello"]).sum()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3, 'hello']).sum());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3, "hello"]).sum()\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found STRING in:
  r.expr([1, 2, 3, "hello"]).avg()
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3,"hello"]).avg()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3, 'hello']).avg());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr([1, 2, 3, "hello"]).avg()\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Cannot take the min of an empty stream.  (If you passed `min` a field name, it may be that no elements of the stream had that field.) in:
  r.expr([]).min()
  ^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([]).min()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([]).min());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Cannot take the min of an empty stream.  (If you passed `min` a field name, it may be that no elements of the stream had that field.) in:\nr.expr([]).min()\n^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Cannot take the max of an empty stream.  (If you passed `max` a field name, it may be that no elements of the stream had that field.) in:
  r.expr([]).max()
  ^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([]).max()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([]).max());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Cannot take the max of an empty stream.  (If you passed `max` a field name, it may be that no elements of the stream had that field.) in:\nr.expr([]).max()\n^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Cannot take the average of an empty stream.  (If you passed `avg` a field name, it may be that no elements of the stream had that field.) in:
  r.expr([]).avg()
  ^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([]).avg()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([]).avg());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Cannot take the average of an empty stream.  (If you passed `avg` a field name, it may be that no elements of the stream had that field.) in:\nr.expr([]).avg()\n^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr(1).upcase()
  ^^^^^^^^^
  */
  it('Test backtrace for r.expr(1).upcase()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr(1).upcase());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.expr(1).upcase()\n^^^^^^^^^         \n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 } ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr(1).downcase()
  ^^^^^^^^^
  */
  it('Test backtrace for r.expr(1).downcase()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr(1).downcase());
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.expr(1).downcase()\n^^^^^^^^^           \n',
      );
    }
  });

  /*
  Frames:
  [ 0, 1, 0 ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr(1).do(function(var_1) {
    return r.object(1, 2)
            ^
  })
  */
  it('Test backtrace for r.expr(1).do(function(v) { return r.object(1, 2) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.expr(1).do(function (v) {
          return r.object(1, 2);
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.expr(1).do(function(var_1) {\n    return r.object(1, 2)\n                    ^    \n})\n',
      );
    }
  });

  /*
  Frames:
  [ 0, 1 ]

  Error:
  OBJECT expects an even number of arguments (but found 1) in:
  r.expr(1).do(function(var_1) {
    return r.object("a")
         ^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.expr(1).do(function(v) { return r.object("a") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.expr(1).do(function (v) {
          return r.object('a');
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'OBJECT expects an even number of arguments (but found 1) in:\nr.expr(1).do(function(var_1) {\n    return r.object("a")\n           ^^^^^^^^^^^^^\n})\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found STRING in:
  r.random(1, 2, {
  ^^^^^^^^^^^^^^^^
    float: true
    ^^^^^^^^^^^
  }).sub("foo")
  ^^^^^^^^^^^^^
  */
  it('Test backtrace for r.random(1,2,{float: true}).sub("foo")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.random(1, 2, { float: true }).sub('foo'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.random(1, 2, {\n^^^^^^^^^^^^^^^^\n    float: true\n    ^^^^^^^^^^^\n}).sub("foo")\n^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type NUMBER but found STRING in:
  r.random("foo", "bar")
       ^^^^^
  */
  it('Test backtrace for r.random("foo", "bar")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.random('foo', 'bar'));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Expected type NUMBER but found STRING in:\nr.random("foo", "bar")\n         ^^^^^        \n',
      );
    }
  });

  /*
  Frames:
  undefined

  Error:
  `random` takes at most 3 arguments, 4 provided.
  */
  it('Test backtrace for r.random("foo", "bar", "buzz", "lol")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.random('foo', 'bar', 'buzz', 'lol'));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.random` takes at most 3 arguments, 4 provided.',
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type DATUM but found SEQUENCE:
  VALUE SEQUENCE in:
  r.db("bd98452de5ea16f3572ed0d404a2e99c").table("c966dac895ef558f3dfdbfb8be003374")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .changes().add(2)
    ^^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).table(tableName).changes().add(2)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.db(dbName).table(tableName).changes().add(2));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type DATUM but found SEQUENCE:\nVALUE SEQUENCE in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .changes().add(2)\n    ^^^^^^^^^^       \n`,
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Error in HTTP GET of ``: URL using bad/illegal format or missing URL.
  header:
  null
  body:
  null in:
  r.http("").add(2)
  ^^^^^^^^^^
  */
  it('Test backtrace for r.http("").add(2)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.http('').add(2));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Error in HTTP GET of ``: URL using bad/illegal format or missing URL.\nheader:\nnull\nbody:\nnull in:\nr.http("").add(2)\n^^^^^^^^^^       \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type STRING but found NUMBER in:
  r.args(["foo", "bar"]).add(2)
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.args(["foo", "bar"]).add(2)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.args(['foo', 'bar']).add(2));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found NUMBER in:\nr.args(["foo", "bar"]).add(2)\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ { type: 'POS', pos: 0 }, { type: 'POS', pos: 1 } ]

  Error:
  Expected type NUMBER but found STRING. in:
  r.expr(1).do(function(var_1) {
    return var_1.add("foo")
         ^^^^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.do(1,function( b) { return b.add("foo") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.do(1, function (b) {
          return b.add('foo');
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr(1).do(function(var_1) {\n    return var_1.add("foo")\n           ^^^^^^^^^^^^^^^^\n})\n',
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type DATUM but found TABLE_SLICE:
  SELECTION ON table(e1bf8f82ad33ed56f7e04e0c2ba97fcc) in:
  r.db("0c24bc6421463a0da33452e38a842f20").table("e1bf8f82ad33ed56f7e04e0c2ba97fcc")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .between("foo", "bar", {
    ^^^^^^^^^^^^^^^^^^^^^^^^
      index: "id"
      ^^^^^^^^^^^
    }).add(1)
    ^^
  */
  it('Test backtrace for r.db(dbName).table(tableName).between("foo", "bar", {index: "id"}).add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .between('foo', 'bar', { index: 'id' })
          .add(1),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type DATUM but found TABLE_SLICE:\nSELECTION ON table(${tableName}) in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .between("foo", "bar", {\n    ^^^^^^^^^^^^^^^^^^^^^^^^\n        index: "id"\n        ^^^^^^^^^^^\n    }).add(1)\n    ^^       \n`,
      );
    }
  });

  /*
  // Note: Buggy? It should be SELECTION, not TABLE_SLICE
  Frames:
  [ 0 ]

  Error:
  Expected type DATUM but found TABLE_SLICE:
  SELECTION ON table(d6063464b40e094f48ec13bbee46d457) in:
  r.db("bdff9659bb18007d268d810929df5d90").table("d6063464b40e094f48ec13bbee46d457")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .orderBy({
    ^^^^^^^^^^
      index: "id"
      ^^^^^^^^^^^
    }).add(1)
    ^^
  */
  it('Test backtrace for r.db(dbName).table(tableName).orderBy({index: "id"}).add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.db(dbName).table(tableName).orderBy({ index: 'id' }).add(1),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type DATUM but found TABLE_SLICE:\nSELECTION ON table(${tableName}) in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .orderBy({\n    ^^^^^^^^^^\n        index: "id"\n        ^^^^^^^^^^^\n    }).add(1)\n    ^^       \n`,
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found PTYPE<BINARY> in:
  r.binary("foo").add(1)
  ^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.binary("foo").add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.binary('foo').add(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found PTYPE<BINARY> in:\nr.binary("foo").add(1)\n^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found PTYPE<BINARY> in:
  r.binary(<Buffer>).add(1)
  ^^^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.binary(new Buffer([0,1,2,3,4])).add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.binary(Buffer.from([0, 1, 2, 3, 4])).add(1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found PTYPE<BINARY> in:\nr.binary(<Buffer>).add(1)\n^^^^^^^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ 0, 1 ]

  Error:
  Expected type NUMBER but found PTYPE<GEOMETRY> in:
  r.expr(1).do(function(var_1) {
    return r.point(1, 2).add("foo")
         ^^^^^^^^^^^^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.do(1,function( b) { return r.point(1, 2).add("foo") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.do(1, function (b) {
          return r.point(1, 2).add('foo');
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found PTYPE<GEOMETRY> in:\nr.expr(1).do(function(var_1) {\n    return r.point(1, 2).add("foo")\n           ^^^^^^^^^^^^^^^^^^^^^^^^\n})\n',
      );
    }
  });

  /*
  Frames:
  [ 0, 1, 0 ]

  Error:
  Expected type ARRAY but found NUMBER in:
  r.expr(1).do(function(var_1) {
    return r.line(1, 2).add("foo")
         ^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.do(1,function( b) { return r.line(1, 2).add("foo") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.do(1, function (b) {
          return r.line(1, 2).add('foo');
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found NUMBER in:\nr.expr(1).do(function(var_1) {\n    return r.line(1, 2).add("foo")\n           ^^^^^^^^^^^^           \n})\n',
      );
    }
  });

  /*
  Frames:
  [ 0, 1, 0 ]

  Error:
  Expected type ARRAY but found NUMBER in:
  r.expr(1).do(function(var_1) {
    return r.circle(1, 2).add("foo")
         ^^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.do(1,function( b) { return r.circle(1, 2).add("foo") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.do(1, function (b) {
          return r.circle(1, 2).add('foo');
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found NUMBER in:\nr.expr(1).do(function(var_1) {\n    return r.circle(1, 2).add("foo")\n           ^^^^^^^^^^^^^^           \n})\n',
      );
    }
  });

  /*
  Frames:
  [ 0, 1, 0 ]

  Error:
  Expected type ARRAY but found NUMBER in:
  r.expr(1).do(function(var_1) {
    return r.polygon(1, 2, 3).add("foo")
         ^^^^^^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.do(1,function( b) { return r.polygon(1, 2, 3).add("foo") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.do(1, function (b) {
          return r.polygon(1, 2, 3).add('foo');
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type ARRAY but found NUMBER in:\nr.expr(1).do(function(var_1) {\n    return r.polygon(1, 2, 3).add("foo")\n           ^^^^^^^^^^^^^^^^^^           \n})\n',
      );
    }
  });

  /*
  Frames:
  [ 0, 1, 0, 1 ]

  Error:
  Not a GEOMETRY pseudotype: `3` in:
  r.expr(1).do(function(var_1) {
    return r.polygon([0, 0], [1, 1], [2, 3]).polygonSub(3).add("foo")
                              ^
  })
  */
  it('Test backtrace for r.do(1,function( b) { return r.polygon([0,0], [1,1], [2,3]).polygonSub(3).add("foo") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.do(1, function (b) {
          return r.polygon([0, 0], [1, 1], [2, 3]).polygonSub(3).add('foo');
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Not a GEOMETRY pseudotype: `3` in:\nr.expr(1).do(function(var_1) {\n    return r.polygon([0, 0], [1, 1], [2, 3]).polygonSub(3).add("foo")\n                                                        ^            \n})\n',
      );
    }
  });

  /*
  Frames:
  [ 0, 1, 0, 0 ]

  Error:
  Expected geometry of type `LineString` but found `Polygon` in:
  r.expr(1).do(function(var_1) {
    return r.polygon([0, 0], [1, 1], [2, 3]).fill().polygonSub(3).add("foo")
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.do(1,function( b) { return r.polygon([0,0], [1,1], [2,3]).fill().polygonSub(3).add("foo") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.do(1, function (b) {
          return r
            .polygon([0, 0], [1, 1], [2, 3])
            .fill()
            .polygonSub(3)
            .add('foo');
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected geometry of type `LineString` but found `Polygon` in:\nr.expr(1).do(function(var_1) {\n    return r.polygon([0, 0], [1, 1], [2, 3]).fill().polygonSub(3).add("foo")\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^                         \n})\n',
      );
    }
  });

  /*
  Frames:
  [ 0, 1, 0, 1, 0 ]

  Error:
  Not a GEOMETRY pseudotype: `"foo"` in:
  r.expr(1).do(function(var_1) {
    return r.polygon([0, 0], [1, 1], [2, 3]).distance(r.expr("foo").polygonSub(3)).add("foo")
                              ^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.do(1,function( b) { return r.polygon([0,0], [1,1], [2,3]).distance(r.expr("foo").polygonSub(3)).add("foo") })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.do(1, function (b) {
          return r
            .polygon([0, 0], [1, 1], [2, 3])
            .distance(r.expr('foo').polygonSub(3))
            .add('foo');
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Not a GEOMETRY pseudotype: `"foo"` in:\nr.expr(1).do(function(var_1) {\n    return r.polygon([0, 0], [1, 1], [2, 3]).distance(r.expr("foo").polygonSub(3)).add("foo")\n                                                      ^^^^^^^^^^^^^                          \n})\n',
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Expected type ARRAY but found NUMBER in:
  r.db("43d3ef1b574ce3176bd8a6a573be3417").table("4428ef38de9fae93c0ca5f880a296b31")
    .getIntersecting(r.circle(0, 1))
             ^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).table(tableName).getIntersecting(r.circle(0, 1), 3)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.db(dbName).table(tableName).getIntersecting(r.circle(0, 1), 3),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `Second argument of \`getIntersecting\` must be an object in:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Expected type ARRAY but found NUMBER in:
  r.db("9fd8231574663499a11f93d205835f51").table("4463dfde70cea8f03266cd78cfec151d")
    .getNearest(r.circle(0, 1))
          ^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).table(tableName).getNearest(r.circle(0, 1), 3)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.db(dbName).table(tableName).getNearest(r.circle(0, 1), 3),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        `Second argument of \`getNearest\` must be an object in:\nr.db("${dbName}").table("${tableName}")\n`,
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Not a GEOMETRY pseudotype: `[
    0,
    1,
    3
  ]` in:
  r.polygon([0, 0], [0, 1], [1, 1]).includes([0, 1, 3])
                                             ^^^^^^^^^
  */
  it('Test backtrace for r.polygon([0, 0], [0, 1], [1, 1]).includes(r.expr([0, 1, 3]))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.polygon([0, 0], [0, 1], [1, 1]).includes(r.expr([0, 1, 3])),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Not a GEOMETRY pseudotype: `[\n\t0,\n\t1,\n\t3\n]` in:\nr.polygon([0, 0], [0, 1], [1, 1]).includes([0, 1, 3])\n                                           ^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Not a GEOMETRY pseudotype: `[
    0,
    1,
    3
  ]` in:
  r.polygon([0, 0], [0, 1], [1, 1]).intersects([0, 1, 3])
                                               ^^^^^^^^^
  */
  it('Test backtrace for r.polygon([0, 0], [0, 1], [1, 1]).intersects(r.expr([0, 1, 3]))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.polygon([0, 0], [0, 1], [1, 1]).intersects(r.expr([0, 1, 3])),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Not a GEOMETRY pseudotype: `[\n\t0,\n\t1,\n\t3\n]` in:\nr.polygon([0, 0], [0, 1], [1, 1]).intersects([0, 1, 3])\n                                             ^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Not a GEOMETRY pseudotype: `[
    0,
    1,
    3
  ]` in:
  r.polygon([0, 0], [0, 1], [1, 1]).includes([0, 1, 3])
                                             ^^^^^^^^^
  */
  it('Test backtrace for r.polygon([0, 0], [0, 1], [1, 1]).includes(r.expr([0, 1, 3]))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.polygon([0, 0], [0, 1], [1, 1]).includes(r.expr([0, 1, 3])),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Not a GEOMETRY pseudotype: `[\n\t0,\n\t1,\n\t3\n]` in:\nr.polygon([0, 0], [0, 1], [1, 1]).includes([0, 1, 3])\n                                           ^^^^^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type DATUM but found SELECTION:
  SELECTION ON table(bbbe68e9a13071ae0c579471d1e30f45) in:
  r.db("92cc9fb8833587dcb7e02a62bdf53145").table("bbbe68e9a13071ae0c579471d1e30f45")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .orderBy(r.desc("foo")).add(1)
    ^^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).table(tableName).orderBy(r.desc("foo")).add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.db(dbName).table(tableName).orderBy(r.desc('foo')).add(1),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type DATUM but found SELECTION:\nSELECTION ON table(${tableName}) in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .orderBy(r.desc("foo")).add(1)\n    ^^^^^^^^^^^^^^^^^^^^^^^       \n`,
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type DATUM but found SELECTION:
  SELECTION ON table(0488a0abab5f89bd2de2cbf816649aa3) in:
  r.db("7af5e5289c00f3ea521a3859c666f03c").table("0488a0abab5f89bd2de2cbf816649aa3")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .orderBy(r.asc("foo")).add(1)
    ^^^^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.db(dbName).table(tableName).orderBy(r.asc("foo")).add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.db(dbName).table(tableName).orderBy(r.asc('foo')).add(1),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type DATUM but found SELECTION:\nSELECTION ON table(${tableName}) in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .orderBy(r.asc("foo")).add(1)\n    ^^^^^^^^^^^^^^^^^^^^^^       \n`,
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type NUMBER but found STRING in:
  r.range("foo")
      ^^^^^
  */
  it('Test backtrace for r.range("foo")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.range('foo'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.range("foo")\n        ^^^^^ \n',
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Expected type DATUM but found SEQUENCE:
  VALUE SEQUENCE in:
  r.range(1, 10).do(function(var_1) {
  ^^^^^^^^^^^^^^
    return var_1.add(4)
  })
  */
  it('Test backtrace for r.range(1,10).do(function(x) { return x.add(4) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r.range(1, 10).do(function (x) {
          return x.add(4);
        }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type DATUM but found SEQUENCE:\nVALUE SEQUENCE in:\nr.range(1, 10).do(function(var_1) {\n^^^^^^^^^^^^^^                     \n    return var_1.add(4)\n})\n',
      );
    }
  });

  /*
  Frames:
  [ 1, 0 ]

  Error:
  Expected type DATUM but found SEQUENCE:
  VALUE SEQUENCE in:
  r.range(1, 10).toJSON().do(function(var_1) {
  ^^^^^^^^^^^^^^
    return var_1.add(4)
  })
  */
  it('Test backtrace for r.range(1,10).toJSON().do(function(x) { return x.add(4) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .range(1, 10)
          .toJSON()
          .do(function (x) {
            return x.add(4);
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type DATUM but found SEQUENCE:\nVALUE SEQUENCE in:\nr.range(1, 10).toJSON().do(function(var_1) {\n^^^^^^^^^^^^^^                              \n    return var_1.add(4)\n})\n',
      );
    }
  });

  /*
  Frames:
  [ 0, 1 ]

  Error:
  Expected type NUMBER but found OBJECT in:
  r.db("0bf896ead3b69f218ceff0de67476afe").table("a382350695a78865de1505c44c481223")
    .config().do(function(var_1) {
      return var_1.add(4)
           ^^^^^^^^^^^^
    })
  */
  it('Test backtrace for r.db(dbName).table(tableName).config().do(function(x) { return x.add(4) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .config()
          .do(function (x) {
            return x.add(4);
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type NUMBER but found OBJECT in:\nr.db("${dbName}").table("${tableName}")\n    .config().do(function(var_1) {\n        return var_1.add(4)\n               ^^^^^^^^^^^^\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ 0, 1 ]

  Error:
  Expected type NUMBER but found OBJECT in:
  r.db("c38ab783cfde80ea7ecf4db42eb942a0").table("7d81632cf83797a5b65a5f2b2adb2c8a")
    .status().do(function(var_1) {
      return var_1.add(4)
           ^^^^^^^^^^^^
    })
  */
  it('Test backtrace for r.db(dbName).table(tableName).status().do(function(x) { return x.add(4) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .status()
          .do(function (x) {
            return x.add(4);
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type NUMBER but found OBJECT in:\nr.db("${dbName}").table("${tableName}")\n    .status().do(function(var_1) {\n        return var_1.add(4)\n               ^^^^^^^^^^^^\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ 0, 1 ]

  Error:
  Expected type NUMBER but found OBJECT in:
  r.db("6c42577f50299d1e88bf57acd87ba5ea").table("031bd87d16ecf4b8d295dd6960fd4800")
    .wait().do(function(var_1) {
      return var_1.add(4)
           ^^^^^^^^^^^^
    })
  */
  it('Test backtrace for r.db(dbName).table(tableName).wait().do(function(x) { return x.add(4) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .wait()
          .do(function (x) {
            return x.add(4);
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type NUMBER but found OBJECT in:\nr.db("${dbName}").table("${tableName}")\n    .wait().do(function(var_1) {\n        return var_1.add(4)\n               ^^^^^^^^^^^^\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ 1 ]

  Error:
  Missing required argument `replicas` in:
  r.db("997da59afc784220d0d5093ef2e698cf").table("dd9f7cdc9eb6c2dc21ef3d63d8fea221")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .reconfigure({
    ^^^^^^^^^^^^^^
      shards: 1
      ^^^^^^^^^
    }).do(function(var_1) {
    ^^
      return var_1.add(4)
    })
  */
  it('Test backtrace for r.db(dbName).table(tableName).reconfigure({ shards: 1 }).do(function(x) { return x.add(4) })', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .reconfigure({ shards: 1 })
          .do(function (x) {
            return x.add(4);
          }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Missing required argument \`replicas\` in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .reconfigure({\n    ^^^^^^^^^^^^^^\n        shards: 1\n        ^^^^^^^^^\n    }).do(function(var_1) {\n    ^^                     \n        return var_1.add(4)\n    })\n`,
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr(1).add("foo").add(r.db("987367b7c251d403119132131f6ba8ae").table("90ff34f973a3f837d1c892027790a95c")
  ^^^^^^^^^^^^^^^^^^^^
    .rebalance().do(function(var_1) {
      return var_1.add(4)
    }))
  */
  it('Test backtrace for r.expr(1).add("foo").add(r.db(dbName).table(tableName).rebalance().do(function(x) { return x.add(4) }))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .expr(1)
          .add('foo')
          .add(
            r
              .db(dbName)
              .table(tableName)
              .rebalance()
              .do(function (x) {
                return x.add(4);
              }),
          ),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type NUMBER but found STRING in:\nr.expr(1).add("foo").add(r.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^                                                                                       \n    .rebalance().do(function(var_1) {\n        return var_1.add(4)\n    }))\n`,
      );
    }
  });

  /*
  Frames:
  []

  Error:
  The function passed to `map` expects 1 argument, but 2 sequences were found in:
  r.map(r.expr([1, 2, 3]), [1, 2, 3], function(var_1) {
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    return var_1("bah").add(3)
    ^^^^^^^^^^^^^^^^^^^^^^^^^^
  })
  ^^
  */
  // it('Test backtrace for r.map([1,2,3], [1,2,3], function(var_1) { return var_1("bah").add(3) })', async () => {
  //   try {
  //     globals.nextVarId = 1;
  //     await pool.run(r
  //       .map([1, 2, 3], [1, 2, 3], function(var_1) {
  //         return var_1('bah').add(3);
  //       })
  //       ); // eslint-disable-line camelcase
  //     assert.fail('should throw');
  //   } catch (e) {
  //     assert.equal(
  //       e.message,
  //       'The function passed to `map` expects 1 argument, but 2 sequences were found in:\nr.map(r.expr([1, 2, 3]), [1, 2, 3], function(var_1) {\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    return var_1("bah").add(3)\n    ^^^^^^^^^^^^^^^^^^^^^^^^^^\n})\n^^\n'
  //     );
  //   }
  // });

  /*
  Frames:
  [ 2, 1, 0, 0 ]

  Error:
  Cannot perform bracket on a non-object non-sequence `1` in:
  r.map(r.expr([1, 2, 3]), [1, 2, 3], function(var_1, var_2) {
    return var_1("bah").add(3)
         ^^^^^
  })
  */
  // it('Test backtrace for r.map([1,2,3], [1,2,3], function(var_1, var_2) { return var_1("bah").add(3) })', async () => {
  //   try {
  //     globals.nextVarId = 1;
  //     await pool.run(r
  //       .map([1, 2, 3], [1, 2, 3], function(var_1, var_2) {
  //         return var_1('bah').add(3);
  //       })
  //       ); // eslint-disable-line camelcase
  //     assert.fail('should throw');
  //   } catch (e) {
  //     assert.equal(
  //       e.message,
  //       'Cannot perform bracket on a non-object non-sequence `1` in:\nr.map(r.expr([1, 2, 3]), [1, 2, 3], function(var_1, var_2) {\n    return var_1("bah").add(3)\n           ^^^^^              \n})\n'
  //     );
  //   }
  // });

  /*
  Frames:
  [ 0, 0 ]

  Error:
  Expected type STRING but found ARRAY in:
  r.expr([1, 2, 3]).split(",", 3).add(3)
  ^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr([1,2,3]).split(",", 3).add(3)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr([1, 2, 3]).split(',', 3).add(3));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type STRING but found ARRAY in:\nr.expr([1, 2, 3]).split(",", 3).add(3)\n^^^^^^^^^^^^^^^^^                     \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found OBJECT in:
  r.expr({}).merge({
  ^^^^^^^^^^^^^^^^^^
    a: r.literal({
    ^^^^^^^^^^^^^^
      foo: "bar"
      ^^^^^^^^^^
    })
    ^^
  }).add(2)
  ^^^^^^^^^
  */
  it('Test backtrace for r.expr({}).merge({a: r.literal({foo: "bar"})}).add(2)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .expr({})
          .merge({ a: r.literal({ foo: 'bar' }) })
          .add(2),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Expected type NUMBER but found OBJECT in:\nr.expr({}).merge({\n^^^^^^^^^^^^^^^^^^\n    a: r.literal({\n    ^^^^^^^^^^^^^^\n        foo: "bar"\n        ^^^^^^^^^^\n    })\n    ^^\n}).add(2)\n^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found ARRAY in:
  r.monday.add([1])
  ^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.monday.add([1])', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.monday.add([1]));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found ARRAY in:\nr.monday.add([1])\n^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found ARRAY in:
  r.november.add([1])
  ^^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.november.add([1])', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.november.add([1]));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found ARRAY in:\nr.november.add([1])\n^^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type NUMBER but found OBJECT in:
  r.expr({
  ^^^^^^^^
    a: r.wednesday
    ^^^^^^^^^^^^^^
  }).add([1])
  ^^^^^^^^^^^
  */
  it('Test backtrace for r.expr({a: r.wednesday}).add([1])', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr({ a: r.wednesday }).add([1]));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found OBJECT in:\nr.expr({\n^^^^^^^^\n    a: r.wednesday\n    ^^^^^^^^^^^^^^\n}).add([1])\n^^^^^^^^^^^\n',
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type DATUM but found TABLE_SLICE:
  SELECTION ON table(0efb72285d551009ac6f2387173b3443) in:
  r.db("d2292d5a5fb3f4780426609087b88fa4").table("0efb72285d551009ac6f2387173b3443")
  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .between(r.minval, r.maxval, {
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      index: "foo"
      ^^^^^^^^^^^^
    }).add(1)
    ^^
  */
  it('Test backtrace for r.db(dbName).table(tableName).between(r.minval, r.maxval, {index: "foo"}).add(1)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .db(dbName)
          .table(tableName)
          .between(r.minval, r.maxval, { index: 'foo' })
          .add(1),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          `Expected type DATUM but found TABLE_SLICE:\nSELECTION ON table(${tableName}) in:\nr.db("${dbName}").table("${tableName}")\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n    .between(r.minval, r.maxval, {\n    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n        index: "foo"\n        ^^^^^^^^^^^^\n    }).add(1)\n    ^^       \n`,
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr(1).add("bar").add(r.ISO8601("dadsa", {
  ^^^^^^^^^^^^^^^^^^^^
    defaultTimezone: "dsada"
  }))
  */
  it('Test backtrace for r.expr(1).add("bar").add(r.ISO8601("dadsa",{defaultTimezone: "dsada"}))', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .expr(1)
          .add('bar')
          .add(r.ISO8601('dadsa', { defaultTimezone: 'dsada' })),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr(1).add("bar").add(r.ISO8601("dadsa", {\n^^^^^^^^^^^^^^^^^^^^                         \n    defaultTimezone: "dsada"\n}))\n',
      );
    }
  });

  /*
  Frames:
  [ 1, 'bar' ]

  Error:
  Expected type STRING but found NUMBER in:
  r.expr({
      foo: "bar"
  }).merge({
      foo: r.literal(),
      bar: r.expr("lol").add(1)
           ^^^^^^^^^^^^^^^^^^^^
  })
  */
  it('Test backtrace for r.expr({foo: "bar"}).merge({foo: r.literal(), bar: r.expr("lol").add(1)})', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(
        r
          .expr({ foo: 'bar' })
          .merge({ foo: r.literal(), bar: r.expr('lol').add(1) }),
      );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Expected type STRING but found NUMBER in:\nr.expr({\n    foo: "bar"\n}).merge({\n    foo: r.literal(),\n    bar: r.expr("lol").add(1)\n         ^^^^^^^^^^^^^^^^^^^^\n})\n',
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr("hello").floor()
  ^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.floor("hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.floor('hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr("hello").floor()\n^^^^^^^^^^^^^^^        \n',
      );
    }
  });

  /*
  Frames:
  undefined

  Error:
  `r.floor` takes 1 argument, 0 provided.
  */
  it('Test backtrace for r.floor()', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.floor());
      assert.fail('should throw');
    } catch (e) {
      assert(e.message === '`r.floor` takes 1 argument, 0 provided.');
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr("hello").round()
  ^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.round("hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.round('hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr("hello").round()\n^^^^^^^^^^^^^^^        \n',
      );
    }
  });

  /*
  Frames:
  [ 0 ]

  Error:
  Expected type NUMBER but found STRING in:
  r.expr("hello").ceil()
  ^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.ceil("hello")', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.ceil('hello'));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message ===
          'Expected type NUMBER but found STRING in:\nr.expr("hello").ceil()\n^^^^^^^^^^^^^^^       \n',
      );
    }
  });

  /*
  Frames:
  []

  Error:
  Expected type ARRAY but found NUMBER in:
  r.expr({
  ^^^^^^^^
      a: 1,
      ^^^^^
      b: 2,
      ^^^^^
      c: 3
      ^^^^
  }).values().add(2)
  ^^^^^^^^^^^^^^^^^^
  */
  it('Test backtrace for r.expr({a:1, b:2, c: 3}).values().add(2)', async () => {
    try {
      globals.nextVarId = 1;
      await pool.run(r.expr({ a: 1, b: 2, c: 3 }).values().add(2));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Expected type ARRAY but found NUMBER in:\nr.expr({\n^^^^^^^^\n    a: 1,\n    ^^^^^\n    b: 2,\n    ^^^^^\n    c: 3\n    ^^^^\n}).values().add(2)\n^^^^^^^^^^^^^^^^^^\n',
      );
    }
  });
});
