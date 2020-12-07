import { camelToSnake, isObject } from '../util';
import { parseParam } from './param-parser';
import { TermJson } from '../internal-types';

export function parseOptarg(
  obj?: Record<string, unknown>,
): Record<string, TermJson> | void {
  if (!isObject(obj) || Array.isArray(obj)) {
    return undefined;
  }
  return Object.entries(obj).reduce<Record<string, TermJson>>(
    (acc, [key, value]) => {
      acc[camelToSnake(key)] = parseParam(value);
      return acc;
    },
    {},
  );
}
