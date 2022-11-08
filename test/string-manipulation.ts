import assert from 'assert';
import { r } from '../src';
import config from './config';

describe('string manipulation', () => {
  before(async () => {
    await r.connectPool(config);
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  it('`match` should work', async () => {
    const result = await r.expr('hello').match('hello').run();
    assert.deepEqual(result, { end: 5, groups: [], start: 0, str: 'hello' });
  });

  it('`match` should throw if no arguement has been passed', async () => {
    try {
      // @ts-ignore
      await r.expr('foo').match().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(
        e.message,
        '`match` takes 1 argument, 0 provided after:\nr.expr("foo")\n',
      );
    }
  });

  it('`upcase` should work', async () => {
    const result = await r.expr('helLo').upcase().run();
    assert.equal(result, 'HELLO');
  });

  it('`downcase` should work', async () => {
    const result = await r.expr('HElLo').downcase().run();
    assert.equal(result, 'hello');
  });

  it('`split` should work', async () => {
    const result = await r.expr('foo  bar bax').split().run();
    assert.deepEqual(result, ['foo', 'bar', 'bax']);
  });

  it('`split(separator)` should work', async () => {
    const result = await r.expr('12,37,,22,').split(',').run();
    assert.deepEqual(result, ['12', '37', '', '22', '']);
  });

  it('`split(separtor, max)` should work', async () => {
    const result = await r.expr('foo  bar bax').split(null, 1).run();
    assert.deepEqual(result, ['foo', 'bar bax']);
  });
});
