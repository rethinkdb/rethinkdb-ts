import assert from 'assert';
import { createRethinkdbMasterPool, r } from "../src";
import config from './config';
import { MasterConnectionPool } from "../src/connection/master-pool";

describe('math and logic', () => {
  let pool: MasterConnectionPool;

  before(async () => {
    pool = await createRethinkdbMasterPool(config);
  });

  after(async () => {
    await pool.drain();
  });

  it('`add` should work', async () => {
    let result = await pool.run(r
      .expr(1)
      .add(1)
      );
    assert.equal(result, 2);

    result = await pool.run(r
      .expr(1)
      .add(1)
      .add(1)
      );
    assert.equal(result, 3);

    result = await pool.run(r
      .expr(1)
      .add(1, 1)
      );
    assert.equal(result, 3);

    result = await pool.run(r.add(1, 1, 1));
    assert.equal(result, 3);
  });

  it('`add` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r
        .expr(1)
        .add()
        );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`add` takes at least 1 argument, 0 provided after:\nr.expr(1)\n'
      );
    }
  });

  it('`add` should throw if no argument has been passed -- r.add', async () => {
    try {
      // @ts-ignore
      await pool.run(r.add());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.add` takes at least 2 arguments, 0 provided.'
      );
    }
  });

  it('`add` should throw if just one argument has been passed -- r.add', async () => {
    try {
      await pool.run(r.add(1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.add` takes at least 2 arguments, 1 provided.'
      );
    }
  });

  it('`sub` should work', async () => {
    let result = await pool.run(r
      .expr(1)
      .sub(1)
      );
    assert.equal(result, 0);

    result = await pool.run(r.sub(5, 3, 1));
    assert.equal(result, 1);
  });

  it('`sub` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r
        .expr(1)
        .sub()
        );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`sub` takes at least 1 argument, 0 provided after:\nr.expr(1)\n'
      );
    }
  });

  it('`sub` should throw if no argument has been passed -- r.sub', async () => {
    try {
      // @ts-ignore;
      await pool.run(r.sub());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.sub` takes at least 2 arguments, 0 provided.'
      );
    }
  });

  it('`sub` should throw if just one argument has been passed -- r.sub', async () => {
    try {
      await pool.run(r.sub(1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.sub` takes at least 2 arguments, 1 provided.'
      );
    }
  });

  it('`mul` should work', async () => {
    let result = await pool.run(r
      .expr(2)
      .mul(3)
      );
    assert.equal(result, 6);

    result = await pool.run(r.mul(2, 3, 4));
    assert.equal(result, 24);
  });

  it('`mul` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r
        .expr(1)
        .mul()
        );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`mul` takes at least 1 argument, 0 provided after:\nr.expr(1)\n'
      );
    }
  });

  it('`mul` should throw if no argument has been passed -- r.mul', async () => {
    try {
      // @ts-ignore
      await pool.run(r.mul());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.mul` takes at least 2 arguments, 0 provided.'
      );
    }
  });

  it('`mul` should throw if just one argument has been passed -- r.mul', async () => {
    try {
      await pool.run(r.mul(1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.mul` takes at least 2 arguments, 1 provided.'
      );
    }
  });

  it('`div` should work', async () => {
    let result = await pool.run(r
      .expr(24)
      .div(2)
      );
    assert.equal(result, 12);

    result = await pool.run(r.div(20, 2, 5, 1));
    assert.equal(result, 2);
  });

  it('`div` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r
        .expr(1)
        .div()
        );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`div` takes at least 1 argument, 0 provided after:\nr.expr(1)\n'
      );
    }
  });

  it('`div` should throw if no argument has been passed -- r.div', async () => {
    try {
      // @ts-ignore
      await pool.run(r.div());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.div` takes at least 2 arguments, 0 provided.'
      );
    }
  });

  it('`div` should throw if just one argument has been passed -- r.div', async () => {
    try {
      await pool.run(r.div(1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.div` takes at least 2 arguments, 1 provided.'
      );
    }
  });

  it('`mod` should work', async () => {
    let result = await pool.run(r
      .expr(24)
      .mod(7)
      );
    assert.equal(result, 3);

    result = await pool.run(r.mod(24, 7));
    assert.equal(result, 3);
  });

  it('`mod` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r
        .expr(1)
        .mod()
        );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`mod` takes 1 argument, 0 provided after:\nr.expr(1)\n'
      );
    }
  });

  it('`mod` should throw if more than two arguments -- r.mod', async () => {
    try {
      await pool.run(r.mod(24, 7, 2));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.mod` takes 2 arguments, 3 provided.');
    }
  });

  it('`and` should work', async () => {
    let result = await pool.run(r
      .expr(true)
      .and(false)
      );
    assert.equal(result, false);

    result = await pool.run(r
      .expr(true)
      .and(true)
      );
    assert.equal(result, true);

    result = await pool.run(r.and(true, true, true));
    assert.equal(result, true);

    result = await pool.run(r.and(true, true, true, false));
    assert.equal(result, false);

    result = await pool.run(r.and(r.args([true, true, true])));
    assert.equal(result, true);
  });

  // it('`and` should work if no argument has been passed -- r.and', async () => {
  //   const result = await pool.run(r.and());
  //   assert.equal(result, true);
  // });

  it('`or` should work', async () => {
    let result = await pool.run(r
      .expr(true)
      .or(false)
      );
    assert.equal(result, true);

    result = await pool.run(r
      .expr(false)
      .or(false)
      );
    assert.equal(result, false);

    result = await pool.run(r.or(true, true, true));
    assert.equal(result, true);

    result = await pool.run(r.or(r.args([false, false, true])));
    assert.equal(result, true);

    result = await pool.run(r.or(false, false, false, false));
    assert.equal(result, false);
  });

  // it('`or` should work if no argument has been passed -- r.or', async () => {
  //   const result = await pool.run(r.or());
  //   assert.equal(result, false);
  // });

  it('`eq` should work', async () => {
    let result = await pool.run(r
      .expr(1)
      .eq(1)
      );
    assert.equal(result, true);

    result = await pool.run(r
      .expr(1)
      .eq(2)
      );
    assert.equal(result, false);

    result = await pool.run(r.eq(1, 1, 1, 1));
    assert.equal(result, true);

    result = await pool.run(r.eq(1, 1, 2, 1));
    assert.equal(result, false);
  });

  it('`eq` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r
        .expr(1)
        .eq()
        );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`eq` takes at least 1 argument, 0 provided after:\nr.expr(1)\n'
      );
    }
  });

  it('`eq` should throw if no argument has been passed -- r.eq', async () => {
    try {
      // @ts-ignore
      await pool.run(r.eq());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.eq` takes at least 2 arguments, 0 provided.');
    }
  });

  it('`eq` should throw if just one argument has been passed -- r.eq', async () => {
    try {
      await pool.run(r.eq(1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.eq` takes at least 2 arguments, 1 provided.');
    }
  });

  it('`ne` should work', async () => {
    let result = await pool.run(r
      .expr(1)
      .ne(1)
      );
    assert.equal(result, false);

    result = await pool.run(r
      .expr(1)
      .ne(2)
      );
    assert.equal(result, true);

    result = await pool.run(r.ne(1, 1, 1, 1));
    assert.equal(result, false);

    result = await pool.run(r.ne(1, 1, 2, 1));
    assert.equal(result, true);
  });

  it('`ne` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r
        .expr(1)
        .ne()
        );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`ne` takes at least 1 argument, 0 provided after:\nr.expr(1)\n'
      );
    }
  });

  it('`ne` should throw if no argument has been passed -- r.ne', async () => {
    try {
      // @ts-ignore
      await pool.run(r.ne());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.ne` takes at least 2 arguments, 0 provided.');
    }
  });

  it('`ne` should throw if just one argument has been passed -- r.ne', async () => {
    try {
      await pool.run(r.ne(1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.ne` takes at least 2 arguments, 1 provided.');
    }
  });

  it('`gt` should work', async () => {
    let result = await pool.run(r
      .expr(1)
      .gt(2)
      );
    assert.equal(result, false);
    result = await pool.run(r
      .expr(2)
      .gt(2)
      );
    assert.equal(result, false);
    result = await pool.run(r
      .expr(3)
      .gt(2)
      );
    assert.equal(result, true);

    result = await pool.run(r.gt(10, 9, 7, 2));
    assert.equal(result, true);

    result = await pool.run(r.gt(10, 9, 9, 1));
    assert.equal(result, false);
  });

  it('`gt` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r
        .expr(1)
        .gt()
        );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`gt` takes at least 1 argument, 0 provided after:\nr.expr(1)\n'
      );
    }
  });

  it('`gt` should throw if no argument has been passed -- r.gt', async () => {
    try {
      // @ts-ignore
      await pool.run(r.gt());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.gt` takes at least 2 arguments, 0 provided.');
    }
  });
  it('`gt` should throw if just one argument has been passed -- r.gt', async () => {
    try {
      await pool.run(r.gt(1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.gt` takes at least 2 arguments, 1 provided.');
    }
  });

  it('`ge` should work', async () => {
    let result = await pool.run(r
      .expr(1)
      .ge(2)
      );
    assert.equal(result, false);
    result = await pool.run(r
      .expr(2)
      .ge(2)
      );
    assert.equal(result, true);
    result = await pool.run(r
      .expr(3)
      .ge(2)
      );
    assert.equal(result, true);

    result = await pool.run(r.ge(10, 9, 7, 2));
    assert.equal(result, true);

    result = await pool.run(r.ge(10, 9, 9, 1));
    assert.equal(result, true);

    result = await pool.run(r.ge(10, 9, 10, 1));
    assert.equal(result, false);
  });

  it('`ge` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r
        .expr(1)
        .ge()
        );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`ge` takes at least 1 argument, 0 provided after:\nr.expr(1)\n'
      );
    }
  });

  it('`ge` should throw if no argument has been passed -- r.ge', async () => {
    try {
      // @ts-ignore
      await pool.run(r.ge());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.ge` takes at least 2 arguments, 0 provided.');
    }
  });

  it('`ge` should throw if just one argument has been passed -- r.ge', async () => {
    try {
      await pool.run(r.ge(1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.ge` takes at least 2 arguments, 1 provided.');
    }
  });

  it('`lt` should work', async () => {
    let result = await pool.run(r
      .expr(1)
      .lt(2)
      );
    assert.equal(result, true);
    result = await pool.run(r
      .expr(2)
      .lt(2)
      );
    assert.equal(result, false);
    result = await pool.run(r
      .expr(3)
      .lt(2)
      );
    assert.equal(result, false);

    result = await pool.run(r.lt(0, 2, 4, 20));
    assert.equal(result, true);

    result = await pool.run(r.lt(0, 2, 2, 4));
    assert.equal(result, false);

    result = await pool.run(r.lt(0, 2, 1, 20));
    assert.equal(result, false);
  });

  it('`lt` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r
        .expr(1)
        .lt()
        );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`lt` takes at least 1 argument, 0 provided after:\nr.expr(1)\n'
      );
    }
  });

  it('`lt` should throw if no argument has been passed -- r.lt', async () => {
    try {
      // @ts-ignore
      await pool.run(r.lt());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.lt` takes at least 2 arguments, 0 provided.');
    }
  });

  it('`lt` should throw if just one argument has been passed -- r.lt', async () => {
    try {
      await pool.run(r.lt(1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.lt` takes at least 2 arguments, 1 provided.');
    }
  });

  it('`le` should work', async () => {
    let result = await pool.run(r
      .expr(1)
      .le(2)
      );
    assert.equal(result, true);
    result = await pool.run(r
      .expr(2)
      .le(2)
      );
    assert.equal(result, true);
    result = await pool.run(r
      .expr(3)
      .le(2)
      );
    assert.equal(result, false);

    result = await pool.run(r.le(0, 2, 4, 20));
    assert.equal(result, true);

    result = await pool.run(r.le(0, 2, 2, 4));
    assert.equal(result, true);

    result = await pool.run(r.le(0, 2, 1, 20));
    assert.equal(result, false);
  });

  it('`le` should throw if no argument has been passed', async () => {
    try {
      await pool.run(r
        .expr(1)
        .le()
        );
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`le` takes at least 1 argument, 0 provided after:\nr.expr(1)\n'
      );
    }
  });

  it('`le` should throw if no argument has been passed -- r.le', async () => {
    try {
      // @ts-ignore
      await pool.run(r.le());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.le` takes at least 2 arguments, 0 provided.');
    }
  });

  it('`le` should throw if just one argument has been passed -- r.le', async () => {
    try {
      await pool.run(r.le(1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.le` takes at least 2 arguments, 1 provided.');
    }
  });

  it('`not` should work', async () => {
    let result = await pool.run(r
      .expr(true)
      .not()
      );
    assert.equal(result, false);
    result = await pool.run(r
      .expr(false)
      .not()
      );
    assert.equal(result, true);
  });

  it('`random` should work', async () => {
    let result = await pool.run(r.random());
    assert(result > 0 && result < 1);

    result = await pool.run(r.random(10));
    assert(result >= 0 && result < 10);
    assert.equal(Math.floor(result), result);

    result = await pool.run(r.random(5, 10));
    assert(result >= 5 && result < 10);
    assert.equal(Math.floor(result), result);

    result = await pool.run(r.random(5, 10, { float: true }));
    assert(result >= 5 && result < 10);
    assert.notEqual(Math.floor(result), result); // that's "almost" safe

    result = await pool.run(r.random(5, { float: true }));
    assert(result < 5 && result > 0);
    assert.notEqual(Math.floor(result), result); // that's "almost" safe
  });

  it('`r.floor` should work', async () => {
    let result = await pool.run(r.floor(1.2));
    assert.equal(result, 1);
    result = await pool.run(r
      .expr(1.2)
      .floor()
      );
    assert.equal(result, 1);
    result = await pool.run(r.floor(1.8));
    assert.equal(result, 1);
    result = await pool.run(r
      .expr(1.8)
      .floor()
      );
    assert.equal(result, 1);
  });

  it('`r.ceil` should work', async () => {
    let result = await pool.run(r.ceil(1.2));
    assert.equal(result, 2);
    result = await pool.run(r
      .expr(1.2)
      .ceil()
      );
    assert.equal(result, 2);
    result = await pool.run(r.ceil(1.8));
    assert.equal(result, 2);
    result = await pool.run(r
      .expr(1.8)
      .ceil()
      );
    assert.equal(result, 2);
  });

  it('`r.round` should work', async () => {
    let result = await pool.run(r.round(1.8));
    assert.equal(result, 2);
    result = await pool.run(r
      .expr(1.8)
      .round()
      );
    assert.equal(result, 2);
    result = await pool.run(r.round(1.2));
    assert.equal(result, 1);
    result = await pool.run(r
      .expr(1.2)
      .round()
      );
    assert.equal(result, 1);
  });
});
