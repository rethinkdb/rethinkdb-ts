import type { TermJson } from '../types';
import { TermType } from '../proto/enums';

export function hasImplicitVar(term: TermJson | undefined): boolean {
  if (!term) {
    return false;
  }
  if (!Array.isArray(term)) {
    if (term.constructor === Object) {
      return Object.values(term).some(hasImplicitVar);
    }
    return false;
  }
  if (term[0] === TermType.IMPLICIT_VAR) {
    return true;
  }
  const termParam = term[1];
  if (termParam) {
    return termParam.some(hasImplicitVar);
  }
  return false;
}
