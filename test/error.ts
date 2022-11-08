import assert from 'assert';
import { r } from '../src';
import config from './config';

describe('errors', () => {
  before(async () => {
    await r.connectPool(config);
  });

  after(async () => {
    await r.getPoolMaster().drain();
  });

  it('ReqlResourceError', async () => {
    try {
      await r.expr([1, 2, 3, 4]).run({ arrayLimit: 2 });
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.name, 'ReqlResourceError');
    }
  });

  it('ReqlLogicError', async () => {
    try {
      await r.expr(1).add('foo').run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.name, 'ReqlLogicError');
    }
  });

  it('ReqlOpFailedError', async () => {
    try {
      await r.db('DatabaseThatDoesNotExist').tableList().run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.name, 'ReqlOpFailedError');
    }
  });

  it('ReqlUserError', async () => {
    try {
      await r.branch(r.error('a'), 1, 2).run();
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.name, 'ReqlUserError');
    }
  });

  describe('Missing tests', () => {
    it('ReqlInternalError no easy way to trigger', () => undefined);
    it('ReqlOpIndeterminateError no easy way to trigger', () => undefined);
  });
});
