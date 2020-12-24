import assert from 'assert';
import { createRethinkdbMasterPool, r } from '../src';
import config from './config';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('string manipulation', () => {
  let pool: MasterConnectionPool;
  before(async () => {
    pool = await createRethinkdbMasterPool(config);
  });

  after(async () => {
    await pool.drain();
  });

  it('`match` should work', async () => {
    const result = await pool.run(r.expr('hello').match('hello'));
    assert.deepEqual(result, { end: 5, groups: [], start: 0, str: 'hello' });
  });

  it('`match` should throw if no arguement has been passed', async () => {
    try {
      // @ts-ignore
      await pool.run(r.expr('foo').match());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`match` takes 1 argument, 0 provided after:\nr.expr("foo")\n',
      );
    }
  });

  it('`upcase` should work', async () => {
    const result = await pool.run(r.expr('helLo').upcase());
    assert.equal(result, 'HELLO');
  });

  it('`downcase` should work', async () => {
    const result = await pool.run(r.expr('HElLo').downcase());
    assert.equal(result, 'hello');
  });

  it('`split` should work', async () => {
    const result = await pool.run(r.expr('foo  bar bax').split());
    assert.deepEqual(result, ['foo', 'bar', 'bax']);
  });

  it('`split(separator)` should work', async () => {
    const result = await pool.run(r.expr('12,37,,22,').split(','));
    assert.deepEqual(result, ['12', '37', '', '22', '']);
  });

  it('`split(separtor, max)` should work', async () => {
    const result = await pool.run(r.expr('foo  bar bax').split(null, 1));
    assert.deepEqual(result, ['foo', 'bar bax']);
  });
});
