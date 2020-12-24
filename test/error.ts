import assert from 'assert';
import { createRethinkdbMasterPool, r } from '../src';
import config from './config';
import { MasterConnectionPool } from '../src/connection/master-pool';

describe('errors', () => {
  let pool: MasterConnectionPool;
  before(async () => {
    pool = await createRethinkdbMasterPool(config);
  });

  after(async () => {
    await pool.drain();
  });

  it('ReqlResourceError', async () => {
    try {
      await pool.run(r.expr([1, 2, 3, 4]), { arrayLimit: 2 });
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.name, 'ReqlResourceError');
    }
  });

  it('ReqlLogicError', async () => {
    try {
      await pool.run(r.expr(1).add('foo'));
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.name, 'ReqlLogicError');
    }
  });

  it('ReqlOpFailedError', async () => {
    try {
      await pool.run(r.db('DatabaseThatDoesNotExist').tableList());
      assert.fail('should throw');
    } catch (e) {
      assert.equal(e.name, 'ReqlOpFailedError');
    }
  });

  it('ReqlUserError', async () => {
    try {
      await pool.run(r.branch(r.error('a'), 1, 2));
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
