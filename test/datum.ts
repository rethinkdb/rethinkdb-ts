import assert from 'assert';
import { r } from '../src';
import config from './config';

describe('datum', () => {
  before(async () => {
    await r.connectPool(config);
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  it('All raws datum should be defined', async () => {
    const result1 = await r.expr(1).run();
    assert.equal(result1, 1);

    const result2 = await r.expr(null).run();
    assert.equal(result2, null);

    const result3 = await r.expr(false).run();
    assert.equal(result3, false);

    const result4 = await r.expr(true).run();
    assert.equal(result4, true);

    const result5 = await r.expr('Hello').run();
    assert.equal(result5, 'Hello');

    const result6 = await r.expr([0, 1, 2]).run();
    assert.deepEqual(result6, [0, 1, 2]);

    const result7 = await r.expr({ a: 0, b: 1 }).run();
    assert.deepEqual(result7, { a: 0, b: 1 });
  });

  it('`expr` is not defined after a term', async () => {
    try {
      await r
        .expr(1)
        // @ts-ignore
        .expr('foo')
        .run();
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
      r.setNestingLevel(20);
    });

    it('`r.expr` should throw when setNestingLevel is too small', async () => {
      r.setNestingLevel(2);
      try {
        await r.expr({ a: { b: { c: { d: 1 } } } }).run();
        assert.fail('should throw');
      } catch (e) {
        assert.equal(
          e.message,
          'Nesting depth limit exceeded.\nYou probably have a circular reference somewhere.',
        );
      }
    });

    it('`r.expr` should work when setNestingLevel set back the value to 100', async () => {
      r.setNestingLevel(100);
      const result = await r.expr({ a: { b: { c: { d: 1 } } } }).run();
      assert.deepEqual(result, { a: { b: { c: { d: 1 } } } });
    });
  });

  describe('array limit', () => {
    afterEach(() => {
      r.setArrayLimit();
    });

    it('`r.expr` should throw when ArrayLimit is too small', async () => {
      try {
        await r.expr([0, 1, 2, 3, 4, 5, 6, 8, 9]).run({ arrayLimit: 2 });
        assert.fail('should throw');
      } catch (e) {
        assert(e.message.match(/^Array over size limit `2`/));
      }
    });

    it('`r.expr` should throw when ArrayLimit is too small - options in run take precedence', async () => {
      r.setArrayLimit(100);
      try {
        await r.expr([0, 1, 2, 3, 4, 5, 6, 8, 9]).run({ arrayLimit: 2 });
        assert.fail('should throw');
      } catch (e) {
        assert(e.message.match(/^Array over size limit `2`/));
      }
    });

    it('`r.expr` should throw when setArrayLimit is too small', async () => {
      r.setArrayLimit(2);
      try {
        await r.expr([0, 1, 2, 3, 4, 5, 6, 8, 9]).run();
        assert.fail('shold throw');
      } catch (e) {
        assert(e.message.match(/^Array over size limit `2`/));
      }
    });

    it('`r.expr` should work when setArrayLimit set back the value to 100000', async () => {
      r.setArrayLimit(100000);
      const result = await r.expr([0, 1, 2, 3, 4, 5, 6, 8, 9]).run();
      assert.deepEqual(result, [0, 1, 2, 3, 4, 5, 6, 8, 9]);
    });
  });

  it('`r.expr` should fail with NaN', async () => {
    try {
      await r.expr(NaN).run();
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
      await r.expr(Infinity).run();
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.match(/^Cannot convert `Infinity` to JSON/));
    }
  });

  // it('`r.expr` should not Infinity if not run', async () => {
  //   r.expr(Infinity);
  // });

  it('`r.expr` should work with high unicode char', async () => {
    const result = await r.expr('“').run();
    assert.equal(result, '“');
  });

  it('`r.binary` should work - with a buffer', async () => {
    const result = await r.binary(Buffer.from([1, 2, 3, 4, 5, 6])).run();
    assert(result instanceof Buffer);
    assert.deepEqual(result.toJSON().data, [1, 2, 3, 4, 5, 6]);
  });

  it('`r.binary` should work - with a ReQL term', async () => {
    const result1 = await r.binary(r.expr('foo')).run();
    assert(result1 instanceof Buffer);
    const result2 = await r.expr(result1).coerceTo('STRING').run();
    assert.equal(result2, 'foo');
  });

  it('`r.expr` should work with binaries', async () => {
    const result = await r.expr(Buffer.from([1, 2, 3, 4, 5, 6])).run();
    assert(result instanceof Buffer);
    assert.deepEqual(result.toJSON().data, [1, 2, 3, 4, 5, 6]);
  });
});
