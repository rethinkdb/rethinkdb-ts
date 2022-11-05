import { TermJson } from '../internal-types';
import { TermType } from '../proto/enums';

export function hasImplicitVar(term: TermJson | undefined): boolean {
  if (!Array.isArray(term)) {
    if (term !== null && typeof term === 'object') {
      return Object.values(term).some((value) => hasImplicitVar(value));
    }
    return false;
  }
  if (term[0] === TermType.IMPLICIT_VAR) {
    return true;
  }
  const termParam = term[1];
  if (termParam) {
    return termParam.some((value: TermJson) => hasImplicitVar(value));
  }
  return false;
}
