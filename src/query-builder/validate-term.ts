import { RethinkDBError } from '../error';
import { TermType } from '../proto/enums';
import type { TermJson } from '../types';

function validateTerm(term: any): TermJson {
  if (term === undefined) {
    throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
  }
  if (typeof term === 'function') {
    throw new RethinkDBError(`Invalid term:\n${term.toString()}\n`);
  }
  if (typeof term === 'object') {
    if (Array.isArray(term)) {
      if (term.length > 3) {
        throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
      }
      const [func, args, options] = term;
      if (typeof func !== 'number' || TermType[func] === undefined) {
        throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
      }
      if (args !== undefined) {
        if (!Array.isArray(args)) {
          throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
        }
        if (!args.every((arg) => validateTerm(arg))) {
          throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
        }
      }
      if (
        options !== undefined &&
        !Object.values(term).every((value) => validateTerm(value))
      ) {
        throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
      }
    } else if (!Object.values(term).every((value) => validateTerm(value))) {
      throw new RethinkDBError(`Invalid term:\n${JSON.stringify(term)}\n`);
    }
  }
  return term;
}

export { validateTerm };
