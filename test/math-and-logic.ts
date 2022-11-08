import assert from 'assert';
import { r } from '../src';
import config from './config';

describe('math and logic', () => {
  before(async () => {
    await r.connectPool(config);
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  it('`add` should work', async () => {
    let result = await r.expr(1).add(1).run();
    assert.equal(result, 2);

    result = await r.expr(1).add(1).add(1).run();
    assert.equal(result, 3);

    result = await r.expr(1).add(1, 1).run();
    assert.equal(result, 3);

    result = await r.add(1, 1, 1).run();
    assert.equal(result, 3);
  });

  it('`add` should throw if no argument has been passed', async () => {
    try {
      await r.expr(1).add().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`add` takes at least 1 argument, 0 provided after:\nr.expr(1)\n',
      );
    }
  });

  it('`add` should throw if no argument has been passed -- r.add', async () => {
    try {
      // @ts-ignore
      await r.add().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.add` takes at least 2 arguments, 0 provided.',
      );
    }
  });

  it('`add` should throw if just one argument has been passed -- r.add', async () => {
    try {
      await r.add(1).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.add` takes at least 2 arguments, 1 provided.',
      );
    }
  });

  it('`sub` should work', async () => {
    let result = await r.expr(1).sub(1).run();
    assert.equal(result, 0);

    result = await r.sub(5, 3, 1).run();
    assert.equal(result, 1);
  });

  it('`sub` should throw if no argument has been passed', async () => {
    try {
      await r.expr(1).sub().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`sub` takes at least 1 argument, 0 provided after:\nr.expr(1)\n',
      );
    }
  });

  it('`sub` should throw if no argument has been passed -- r.sub', async () => {
    try {
      // @ts-ignore;
      await r.sub().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.sub` takes at least 2 arguments, 0 provided.',
      );
    }
  });

  it('`sub` should throw if just one argument has been passed -- r.sub', async () => {
    try {
      await r.sub(1).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.sub` takes at least 2 arguments, 1 provided.',
      );
    }
  });

  it('`mul` should work', async () => {
    let result = await r.expr(2).mul(3).run();
    assert.equal(result, 6);

    result = await r.mul(2, 3, 4).run();
    assert.equal(result, 24);
  });

  it('`mul` should throw if no argument has been passed', async () => {
    try {
      await r.expr(1).mul().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`mul` takes at least 1 argument, 0 provided after:\nr.expr(1)\n',
      );
    }
  });

  it('`mul` should throw if no argument has been passed -- r.mul', async () => {
    try {
      // @ts-ignore
      await r.mul().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.mul` takes at least 2 arguments, 0 provided.',
      );
    }
  });

  it('`mul` should throw if just one argument has been passed -- r.mul', async () => {
    try {
      await r.mul(1).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.mul` takes at least 2 arguments, 1 provided.',
      );
    }
  });

  it('`div` should work', async () => {
    let result = await r.expr(24).div(2).run();
    assert.equal(result, 12);

    result = await r.div(20, 2, 5, 1).run();
    assert.equal(result, 2);
  });

  it('`div` should throw if no argument has been passed', async () => {
    try {
      await r.expr(1).div().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`div` takes at least 1 argument, 0 provided after:\nr.expr(1)\n',
      );
    }
  });

  it('`div` should throw if no argument has been passed -- r.div', async () => {
    try {
      // @ts-ignore
      await r.div().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.div` takes at least 2 arguments, 0 provided.',
      );
    }
  });

  it('`div` should throw if just one argument has been passed -- r.div', async () => {
    try {
      await r.div(1).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.div` takes at least 2 arguments, 1 provided.',
      );
    }
  });

  it('`mod` should work', async () => {
    let result = await r.expr(24).mod(7).run();
    assert.equal(result, 3);

    result = await r.mod(24, 7).run();
    assert.equal(result, 3);
  });

  it('`mod` should throw if no argument has been passed', async () => {
    try {
      await r.expr(1).mod().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`mod` takes 1 argument, 0 provided after:\nr.expr(1)\n',
      );
    }
  });

  it('`mod` should throw if more than two arguments -- r.mod', async () => {
    try {
      await r.mod(24, 7, 2).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.mod` takes 2 arguments, 3 provided.');
    }
  });

  it('`and` should work', async () => {
    let result = await r.expr(true).and(false).run();
    assert.equal(result, false);

    result = await r.expr(true).and(true).run();
    assert.equal(result, true);

    result = await r.and(true, true, true).run();
    assert.equal(result, true);

    result = await r.and(true, true, true, false).run();
    assert.equal(result, false);

    result = await r.and(r.args([true, true, true])).run();
    assert.equal(result, true);
  });

  // it('`and` should work if no argument has been passed -- r.and', async () => {
  //   const result = await r.and().run();
  //   assert.equal(result, true);
  // });

  it('`or` should work', async () => {
    let result = await r.expr(true).or(false).run();
    assert.equal(result, true);

    result = await r.expr(false).or(false).run();
    assert.equal(result, false);

    result = await r.or(true, true, true).run();
    assert.equal(result, true);

    result = await r.or(r.args([false, false, true])).run();
    assert.equal(result, true);

    result = await r.or(false, false, false, false).run();
    assert.equal(result, false);
  });

  // it('`or` should work if no argument has been passed -- r.or', async () => {
  //   const result = await r.or().run();
  //   assert.equal(result, false);
  // });

  it('`eq` should work', async () => {
    let result = await r.expr(1).eq(1).run();
    assert.equal(result, true);

    result = await r.expr(1).eq(2).run();
    assert.equal(result, false);

    result = await r.eq(1, 1, 1, 1).run();
    assert.equal(result, true);

    result = await r.eq(1, 1, 2, 1).run();
    assert.equal(result, false);
  });

  it('`eq` should throw if no argument has been passed', async () => {
    try {
      await r.expr(1).eq().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`eq` takes at least 1 argument, 0 provided after:\nr.expr(1)\n',
      );
    }
  });

  it('`eq` should throw if no argument has been passed -- r.eq', async () => {
    try {
      // @ts-ignore
      await r.eq().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.eq` takes at least 2 arguments, 0 provided.');
    }
  });

  it('`eq` should throw if just one argument has been passed -- r.eq', async () => {
    try {
      await r.eq(1).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.eq` takes at least 2 arguments, 1 provided.');
    }
  });

  it('`ne` should work', async () => {
    let result = await r.expr(1).ne(1).run();
    assert.equal(result, false);

    result = await r.expr(1).ne(2).run();
    assert.equal(result, true);

    result = await r.ne(1, 1, 1, 1).run();
    assert.equal(result, false);

    result = await r.ne(1, 1, 2, 1).run();
    assert.equal(result, true);
  });

  it('`ne` should throw if no argument has been passed', async () => {
    try {
      await r.expr(1).ne().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`ne` takes at least 1 argument, 0 provided after:\nr.expr(1)\n',
      );
    }
  });

  it('`ne` should throw if no argument has been passed -- r.ne', async () => {
    try {
      // @ts-ignore
      await r.ne().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.ne` takes at least 2 arguments, 0 provided.');
    }
  });

  it('`ne` should throw if just one argument has been passed -- r.ne', async () => {
    try {
      await r.ne(1).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.ne` takes at least 2 arguments, 1 provided.');
    }
  });

  it('`gt` should work', async () => {
    let result = await r.expr(1).gt(2).run();
    assert.equal(result, false);
    result = await r.expr(2).gt(2).run();
    assert.equal(result, false);
    result = await r.expr(3).gt(2).run();
    assert.equal(result, true);

    result = await r.gt(10, 9, 7, 2).run();
    assert.equal(result, true);

    result = await r.gt(10, 9, 9, 1).run();
    assert.equal(result, false);
  });

  it('`gt` should throw if no argument has been passed', async () => {
    try {
      await r.expr(1).gt().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`gt` takes at least 1 argument, 0 provided after:\nr.expr(1)\n',
      );
    }
  });

  it('`gt` should throw if no argument has been passed -- r.gt', async () => {
    try {
      // @ts-ignore
      await r.gt().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.gt` takes at least 2 arguments, 0 provided.');
    }
  });
  it('`gt` should throw if just one argument has been passed -- r.gt', async () => {
    try {
      await r.gt(1).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.gt` takes at least 2 arguments, 1 provided.');
    }
  });

  it('`ge` should work', async () => {
    let result = await r.expr(1).ge(2).run();
    assert.equal(result, false);
    result = await r.expr(2).ge(2).run();
    assert.equal(result, true);
    result = await r.expr(3).ge(2).run();
    assert.equal(result, true);

    result = await r.ge(10, 9, 7, 2).run();
    assert.equal(result, true);

    result = await r.ge(10, 9, 9, 1).run();
    assert.equal(result, true);

    result = await r.ge(10, 9, 10, 1).run();
    assert.equal(result, false);
  });

  it('`ge` should throw if no argument has been passed', async () => {
    try {
      await r.expr(1).ge().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`ge` takes at least 1 argument, 0 provided after:\nr.expr(1)\n',
      );
    }
  });

  it('`ge` should throw if no argument has been passed -- r.ge', async () => {
    try {
      // @ts-ignore
      await r.ge().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.ge` takes at least 2 arguments, 0 provided.');
    }
  });

  it('`ge` should throw if just one argument has been passed -- r.ge', async () => {
    try {
      await r.ge(1).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.ge` takes at least 2 arguments, 1 provided.');
    }
  });

  it('`lt` should work', async () => {
    let result = await r.expr(1).lt(2).run();
    assert.equal(result, true);
    result = await r.expr(2).lt(2).run();
    assert.equal(result, false);
    result = await r.expr(3).lt(2).run();
    assert.equal(result, false);

    result = await r.lt(0, 2, 4, 20).run();
    assert.equal(result, true);

    result = await r.lt(0, 2, 2, 4).run();
    assert.equal(result, false);

    result = await r.lt(0, 2, 1, 20).run();
    assert.equal(result, false);
  });

  it('`lt` should throw if no argument has been passed', async () => {
    try {
      await r.expr(1).lt().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`lt` takes at least 1 argument, 0 provided after:\nr.expr(1)\n',
      );
    }
  });

  it('`lt` should throw if no argument has been passed -- r.lt', async () => {
    try {
      // @ts-ignore
      await r.lt().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.lt` takes at least 2 arguments, 0 provided.');
    }
  });

  it('`lt` should throw if just one argument has been passed -- r.lt', async () => {
    try {
      await r.lt(1).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.lt` takes at least 2 arguments, 1 provided.');
    }
  });

  it('`le` should work', async () => {
    let result = await r.expr(1).le(2).run();
    assert.equal(result, true);
    result = await r.expr(2).le(2).run();
    assert.equal(result, true);
    result = await r.expr(3).le(2).run();
    assert.equal(result, false);

    result = await r.le(0, 2, 4, 20).run();
    assert.equal(result, true);

    result = await r.le(0, 2, 2, 4).run();
    assert.equal(result, true);

    result = await r.le(0, 2, 1, 20).run();
    assert.equal(result, false);
  });

  it('`le` should throw if no argument has been passed', async () => {
    try {
      await r.expr(1).le().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`le` takes at least 1 argument, 0 provided after:\nr.expr(1)\n',
      );
    }
  });

  it('`le` should throw if no argument has been passed -- r.le', async () => {
    try {
      // @ts-ignore
      await r.le().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.le` takes at least 2 arguments, 0 provided.');
    }
  });

  it('`le` should throw if just one argument has been passed -- r.le', async () => {
    try {
      await r.le(1).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.le` takes at least 2 arguments, 1 provided.');
    }
  });

  it('`not` should work', async () => {
    let result = await r.expr(true).not().run();
    assert.equal(result, false);
    result = await r.expr(false).not().run();
    assert.equal(result, true);
  });

  it('`random` should work', async () => {
    let result = await r.random().run();
    assert(result > 0 && result < 1);

    result = await r.random(10).run();
    assert(result >= 0 && result < 10);
    assert.equal(Math.floor(result), result);

    result = await r.random(5, 10).run();
    assert(result >= 5 && result < 10);
    assert.equal(Math.floor(result), result);

    result = await r.random(5, 10, { float: true }).run();
    assert(result >= 5 && result < 10);
    assert.notEqual(Math.floor(result), result); // that's "almost" safe

    result = await r.random(5, { float: true }).run();
    assert(result < 5 && result > 0);
    assert.notEqual(Math.floor(result), result); // that's "almost" safe
  });

  it('`r.floor` should work', async () => {
    let result = await r.floor(1.2).run();
    assert.equal(result, 1);
    result = await r.expr(1.2).floor().run();
    assert.equal(result, 1);
    result = await r.floor(1.8).run();
    assert.equal(result, 1);
    result = await r.expr(1.8).floor().run();
    assert.equal(result, 1);
  });

  it('`r.ceil` should work', async () => {
    let result = await r.ceil(1.2).run();
    assert.equal(result, 2);
    result = await r.expr(1.2).ceil().run();
    assert.equal(result, 2);
    result = await r.ceil(1.8).run();
    assert.equal(result, 2);
    result = await r.expr(1.8).ceil().run();
    assert.equal(result, 2);
  });

  it('`r.round` should work', async () => {
    let result = await r.round(1.8).run();
    assert.equal(result, 2);
    result = await r.expr(1.8).round().run();
    assert.equal(result, 2);
    result = await r.round(1.2).run();
    assert.equal(result, 1);
    result = await r.expr(1.2).round().run();
    assert.equal(result, 1);
  });
});
