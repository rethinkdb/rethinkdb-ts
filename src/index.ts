import { RQuery, toQuery } from './query-builder/query';
import { validateTerm } from './query-builder/validate-term';
import { globals } from './query-builder/globals';

export { r } from './query-builder/r';
export { isQuery, RQuery } from './query-builder/query';
export * from './types';
export { isRethinkDBError } from './error';
export * from './response/cursor';
export * from './connection';
export * from './connection/connection';

export function serialize(query: RQuery): string {
  return JSON.stringify(query.term);
}

export function deserialize(termStr: string): RQuery {
  return toQuery(validateTerm(JSON.parse(termStr)));
}

export const setNestingLevel = (level: number): void => {
  globals.nestingLevel = level;
};
export const setArrayLimit = (limit?: number): void => {
  globals.arrayLimit = limit;
};
