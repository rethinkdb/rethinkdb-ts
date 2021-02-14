import assert from 'assert';
import {
  createRethinkdbMasterPool,
  r,
  setArrayLimit,
  setNestingLevel,
} from '../src';
import config from './config';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('datum', () => {
  let pool: MasterConnectionPool;
  before(async () => {
    pool = await createRethinkdbMasterPool([config.server], config.options);
  });

  after(async () => {
    await pool.drain();
  });

  it('All raws datum should be defined', async () => {
    const result1 = await pool.run(r.expr(1));
    assert.equal(result1, 1);

    const result2 = await pool.run(r.expr(null));
    assert.equal(result2, null);

    const result3 = await pool.run(r.expr(false));
    assert.equal(result3, false);

    const result4 = await pool.run(r.expr(true));
    assert.equal(result4, true);

    const result5 = await pool.run(r.expr('Hello'));
    assert.equal(result5, 'Hello');

    const result6 = await pool.run(r.expr([0, 1, 2]));
    assert.deepEqual(result6, [0, 1, 2]);

    const result7 = await pool.run(r.expr({ a: 0, b: 1 }));
    assert.deepEqual(result7, { a: 0, b: 1 });
  });

  it('`expr` is not defined after a term', async () => {
    try {
      await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .expr('foo'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.expr is not a function'));
    }
  });

  it('`r.expr` should take a nestingLevel value and throw if the nesting level is reached', async () => {
    try {
      r.expr({ a: { b: { c: { d: 1 } } } }, 2);
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        'Nesting depth limit exceeded.\nYou probably have a circular reference somewhere.',
      );
    }
  });

  describe('nesting level', () => {
    afterEach(() => {
      setNestingLevel(20);
    });

    it('`r.expr` should throw when setNestingLevel is too small', async () => {
      setNestingLevel(2);
      try {
        await pool.run(r.expr({ a: { b: { c: { d: 1 } } } }));
        assert.fail('should throw');
      } catch (e) {
        assert.equal(
          e.message,
          'Nesting depth limit exceeded.\nYou probably have a circular reference somewhere.',
        );
      }
    });

    it('`r.expr` should work when setNestingLevel set back the value to 100', async () => {
      setNestingLevel(100);
      const result = await pool.run(r.expr({ a: { b: { c: { d: 1 } } } }));
      assert.deepEqual(result, { a: { b: { c: { d: 1 } } } });
    });
  });

  describe('array limit', () => {
    afterEach(() => {
      setArrayLimit();
    });

    it('`r.expr` should throw when ArrayLimit is too small', async () => {
      try {
        await pool.run(r.expr([0, 1, 2, 3, 4, 5, 6, 8, 9]), { arrayLimit: 2 });
        assert.fail('should throw');
      } catch (e) {
        assert(e.message.match(/^Array over size limit `2`/));
      }
    });

    it('`r.expr` should throw when ArrayLimit is too small - options in run take precedence', async () => {
      setArrayLimit(100);
      try {
        await pool.run(r.expr([0, 1, 2, 3, 4, 5, 6, 8, 9]), { arrayLimit: 2 });
        assert.fail('should throw');
      } catch (e) {
        assert(e.message.match(/^Array over size limit `2`/));
      }
    });

    it('`r.expr` should throw when setArrayLimit is too small', async () => {
      setArrayLimit(2);
      try {
        await pool.run(r.expr([0, 1, 2, 3, 4, 5, 6, 8, 9]));
        assert.fail('shold throw');
      } catch (e) {
        assert(e.message.match(/^Array over size limit `2`/));
      }
    });

    it('`r.expr` should work when setArrayLimit set back the value to 100000', async () => {
      setArrayLimit(100000);
      const result = await pool.run(r.expr([0, 1, 2, 3, 4, 5, 6, 8, 9]));
      assert.deepEqual(result, [0, 1, 2, 3, 4, 5, 6, 8, 9]);
    });
  });

  it('`r.expr` should fail with NaN', async () => {
    try {
      await pool.run(r.expr(NaN));
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^Cannot convert `NaN` to JSON/));
    }
  });

  // it('`r.expr` should not NaN if not run', async () => {
  //   r.expr(NaN);
  // });

  it('`r.expr` should fail with Infinity', async () => {
    try {
      await pool.run(r.expr(Infinity));
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^Cannot convert `Infinity` to JSON/));
    }
  });

  // it('`r.expr` should not Infinity if not run', async () => {
  //   r.expr(Infinity);
  // });

  it('`r.expr` should work with high unicode char', async () => {
    const result = await pool.run(r.expr('“'));
    assert.equal(result, '“');
  });

  it('`r.binary` should work - with a buffer', async () => {
    const result = await pool.run(r.binary(Buffer.from([1, 2, 3, 4, 5, 6])));
    assert(result instanceof Buffer);
    assert.deepEqual(result.toJSON().data, [1, 2, 3, 4, 5, 6]);
  });

  it('`r.binary` should work - with a ReQL term', async () => {
    const result1 = await pool.run(r.binary(r.expr('foo')));
    assert(result1 instanceof Buffer);
    const result2 = await pool.run(r.expr(result1).coerceTo('STRING'));
    assert.equal(result2, 'foo');
  });

  it('`r.expr` should work with binaries', async () => {
    const result = await pool.run(r.expr(Buffer.from([1, 2, 3, 4, 5, 6])));
    assert(result instanceof Buffer);
    assert.deepEqual(result.toJSON().data, [1, 2, 3, 4, 5, 6]);
  });
});
