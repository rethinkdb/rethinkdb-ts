import assert from 'assert';
import { TermType } from '../src/proto/enums';
import {
  bracket,
  funcall,
  rConfig,
  rConsts,
  termConfig,
} from '../src/query-builder/query-config';

describe('coverage', () => {
  const keys = Object.keys(TermType)
    .filter((key) => isNaN(key as any))
    .map((key) => TermType[key]);
  // Test that the term appears somewhere in the file, which find terms that were not implemented
  it('all terms should be present in query-config.js', async () => {
    const ignoredKeys = [
      TermType.FUNC,
      TermType.VAR,
      TermType.IMPLICIT_VAR,
      // not implemented since we use the JSON protocol
      TermType.MAKE_ARRAY,
      TermType.DATUM,
      TermType.MAKE_OBJ,
      TermType.BETWEEN_DEPRECATED,
    ];
    const missing = [];
    const supportedTerms = [
      bracket[0],
      funcall[0],
      ...termConfig.map((t) => t[0]),
      ...rConfig.map((t) => t[0]),
      ...rConsts.map((t) => t[0]),
    ];
    for (const key of keys) {
      if (ignoredKeys.includes(key)) {
        continue;
      }
      if (!supportedTerms.includes(key)) {
        missing.push(TermType[key]);
      }
    }

    if (missing.length > 0) {
      assert.fail('Some terms were not found:', JSON.stringify(missing));
    }
  });
});
