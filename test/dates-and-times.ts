import assert from 'assert';
import { createRethinkdbMasterPool, r } from '../src';
import config from './config';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('dates and times', () => {
  let pool: MasterConnectionPool;
  before(async () => {
    pool = await createRethinkdbMasterPool(config);
  });

  after(async () => {
    await pool.drain();
  });

  it('`r.now` should return a date', async () => {
    const result1 = await pool.run(r.now());
    assert(result1 instanceof Date);

    const result2 = await pool.run(r.expr({ a: r.now() }));
    assert(result2.a instanceof Date);

    const result3 = await pool.run(r.expr([r.now()]));
    assert(result3[0] instanceof Date);

    const result4 = await pool.run(r.expr([{}, { a: r.now() }]));
    assert(result4[1].a instanceof Date);

    const result5 = await pool.run(r.expr({ b: [{}, { a: r.now() }] }));
    assert(result5.b[1].a instanceof Date);
  });

  it('`now` is not defined after a term', async () => {
    try {
      await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .now('foo'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.now is not a function'));
    }
  });

  it('`r.time` should return a date -- with date and time', async () => {
    const result1 = await pool.run(r.time(1986, 11, 3, 12, 0, 0, 'Z'));
    assert.equal(result1 instanceof Date, true);

    const result2 = await pool.run(
      r.time(1986, 11, 3, 12, 20, 0, 'Z').minutes(),
    );
    assert.equal(result2, 20);
  });

  it('`r.time` should work with r.args', async () => {
    // @ts-ignore
    const result = await pool.run(r.time(r.args([1986, 11, 3, 12, 0, 0, 'Z'])));
    assert.equal(result instanceof Date, true);
  });

  it('`r.time` should return a date -- just with a date', async () => {
    let result = await pool.run(r.time(1986, 11, 3, 'Z'));
    assert.equal(result instanceof Date, true);
    result = await pool.run(r.time(1986, 11, 3, 0, 0, 0, 'Z'));
    assert.equal(result instanceof Date, true);
  });

  it('`r.time` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.time());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.time` takes at least 4 arguments, 0 provided.',
      );
    }
  });

  it('`r.time` should throw if no 5 arguments', async () => {
    try {
      // @ts-ignore
      await pool.run(r.time(1, 1, 1, 1, 1));
      assert.fail('should throw');
    } catch (e) {
      assert(
        e.message.startsWith('Got 5 arguments to TIME (expected 4 or 7) in:'),
      );
    }
  });

  it('`time` is not defined after a term', async () => {
    try {
      await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .time(1, 2, 3, 'Z'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.time is not a function'));
    }
  });

  it('`epochTime` should work', async () => {
    const now = new Date();
    const result = await pool.run(r.epochTime(now.getTime() / 1000));
    assert.deepEqual(now, result);
  });

  it('`r.epochTime` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.epochTime());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.message, '`r.epochTime` takes 1 argument, 0 provided.');
    }
  });

  it('`epochTime` is not defined after a term', async () => {
    try {
      await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .epochTime(Date.now()),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.epochTime is not a function'));
    }
  });

  it('`ISO8601` should work', async () => {
    const result = await pool.run(r.ISO8601('1986-11-03T08:30:00-08:00'));
    assert.equal(result.getTime(), Date.UTC(1986, 10, 3, 8 + 8, 30, 0));
  });

  it('`ISO8601` should work with a timezone', async () => {
    const result = await pool.run(
      r.ISO8601('1986-11-03T08:30:00', { defaultTimezone: '-08:00' }),
    );
    assert.equal(result.getTime(), Date.UTC(1986, 10, 3, 8 + 8, 30, 0));
  });

  it('`r.ISO8601` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.ISO8601());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.ISO8601` takes at least 1 argument, 0 provided.',
      );
    }
  });

  it('`r.ISO8601` should throw if too many arguments', async () => {
    try {
      // @ts-ignore
      await pool.run(r.ISO8601(1, 1, 1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`r.ISO8601` takes at most 2 arguments, 3 provided.',
      );
    }
  });

  it('`ISO8601` is not defined after a term', async () => {
    try {
      await pool.run(
        r
          .expr(1)
          // @ts-ignore
          .ISO8601('validISOstring'),
      );
      assert.fail('should throw');
    } catch (e) {
      assert(e.message.endsWith('.ISO8601 is not a function'));
    }
  });

  it('`inTimezone` should work', async () => {
    const result = await pool.run(
      r
        .now()
        .inTimezone('-08:00')
        .hours()
        .do((h) => {
          return r.branch(
            h.eq(0),
            r.expr(23).eq(r.now().inTimezone('-09:00').hours()),
            h.eq(r.now().inTimezone('-09:00').hours().add(1)),
          );
        }),
    );
    assert.equal(result, true);
  });

  it('`inTimezone` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.now().inTimezone());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`inTimezone` takes 1 argument, 0 provided after:\nr.now()\n',
      );
    }
  });

  it('`timezone` should work', async () => {
    const result = await pool.run(
      r.ISO8601('1986-11-03T08:30:00-08:00').timezone(),
    );
    assert.equal(result, '-08:00');
  });

  it('`during` should work', async () => {
    let result = await pool.run(
      r.now().during(r.time(2013, 12, 1, 'Z'), r.now().add(1000)),
    );
    assert.equal(result, true);

    result = await pool.run(
      r.now().during(r.time(2013, 12, 1, 'Z'), r.now(), {
        leftBound: 'closed',
        rightBound: 'closed',
      }),
    );
    assert.equal(result, true);

    result = await pool.run(
      r.now().during(r.time(2013, 12, 1, 'Z'), r.now(), {
        leftBound: 'closed',
        rightBound: 'open',
      }),
    );
    assert.equal(result, false);
  });

  it('`during` should throw if no argument has been given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.now().during());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`during` takes at least 2 arguments, 0 provided after:\nr.now()\n',
      );
    }
  });

  it('`during` should throw if just one argument has been given', async () => {
    try {
      // @ts-ignore
      await pool.run(r.now().during(1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`during` takes at least 2 arguments, 1 provided after:\nr.now()\n',
      );
    }
  });

  it('`during` should throw if too many arguments', async () => {
    try {
      // @ts-ignore
      await pool.run(r.now().during(1, 1, 1, 1, 1));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`during` takes at most 3 arguments, 5 provided after:\nr.now()\n',
      );
    }
  });

  it('`date` should work', async () => {
    let result = await pool.run(r.now().date().hours());
    assert.equal(result, 0);

    result = await pool.run(r.now().date().minutes());
    assert.equal(result, 0);

    result = await pool.run(r.now().date().seconds());
    assert.equal(result, 0);
  });

  it('`timeOfDay` should work', async () => {
    const result = await pool.run(r.now().timeOfDay());
    assert(result >= 0);
  });

  it('`year` should work', async () => {
    const result = await pool.run(
      r.now().inTimezone(new Date().toString().match(' GMT([^ ]*)')[1]).year(),
    );
    assert.equal(result, new Date().getFullYear());
  });

  it('`month` should work', async () => {
    const result = await pool.run(
      r.now().inTimezone(new Date().toString().match(' GMT([^ ]*)')[1]).month(),
    );
    assert.equal(result, new Date().getMonth() + 1);
  });

  it('`day` should work', async () => {
    const result = await pool.run(
      r.now().inTimezone(new Date().toString().match(' GMT([^ ]*)')[1]).day(),
    );
    assert.equal(result, new Date().getDate());
  });

  it('`dayOfYear` should work', async () => {
    const result = await pool.run(
      r
        .now()
        .inTimezone(new Date().toString().match(' GMT([^ ]*)')[1])
        .dayOfYear(),
    );
    assert(result > new Date().getMonth() * 28 + new Date().getDate() - 1);
  });

  it('`dayOfWeek` should work', async () => {
    let result = await pool.run(
      r
        .now()
        .inTimezone(new Date().toString().match(' GMT([^ ]*)')[1])
        .dayOfWeek(),
    );
    if (result === 7) {
      result = 0;
    }
    assert.equal(result, new Date().getDay());
  });

  it('`toISO8601` should work', async () => {
    const result = await pool.run(r.now().toISO8601());
    assert.equal(typeof result, 'string');
  });

  it('`toEpochTime` should work', async () => {
    const result = await pool.run(r.now().toEpochTime());
    assert.equal(typeof result, 'number');
  });

  it('Constant terms should work', async () => {
    let result = await pool.run(r.monday);
    assert.equal(result, 1);

    result = await pool.run(
      r.expr([
        r.monday,
        r.tuesday,
        r.wednesday,
        r.thursday,
        r.friday,
        r.saturday,
        r.sunday,
        r.january,
        r.february,
        r.march,
        r.april,
        r.may,
        r.june,
        r.july,
        r.august,
        r.september,
        r.october,
        r.november,
        r.december,
      ]),
    );
    assert.deepEqual(result, [
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
    ]);
  });

  it('`epochTime` should work', async () => {
    const now = new Date();
    const result = await pool.run(r.epochTime(now.getTime() / 1000), {
      timeFormat: 'raw',
    });
    // @ts-ignore
    assert.equal(result.$reql_type$, 'TIME');
  });

  it('`ISO8601` run parameter should work', async () => {
    const result = await pool.run(r.time(2018, 5, 2, 13, 0, 0, '-03:00'), {
      timeFormat: 'ISO8601',
    });
    assert.equal(typeof result, 'string');
    assert.equal(result, '2018-05-02T13:00:00.000-03:00');
  });

  it('Date should be parsed correctly', async () => {
    const date = new Date();
    const result = await pool.run(r.expr({ date }));
    assert.equal(result.date.getTime(), date.getTime());
  });
});
