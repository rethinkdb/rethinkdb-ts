import { RethinkDBError, RethinkDBErrorType } from '../error/error';
import { TermJson } from '../internal-types';
import { TermType } from '../proto/enums';
import { globals } from './globals';
import { isQuery } from './query';
import { toQuery } from './query-runner';
import { isDate, isFunction } from '../util';
import { hasImplicitVar } from './has-implicit-var';

export function parseParam(
  param: any,
  nestingLevel = globals.nestingLevel,
): TermJson {
  if (nestingLevel === 0) {
    throw new RethinkDBError(
      'Nesting depth limit exceeded.\nYou probably have a circular reference somewhere.',
      { type: RethinkDBErrorType.PARSE },
    );
  }
  if (param === null) {
    return null;
  }
  if (isQuery(param)) {
    if (param.term === undefined) {
      throw new RethinkDBError("'r' cannot be an argument", {
        type: RethinkDBErrorType.PARSE,
      });
    }
    if (
      globals.nextVarId === 1 &&
      nestingLevel === globals.nestingLevel &&
      hasImplicitVar(param.term)
    ) {
      return [TermType.FUNC, [[TermType.MAKE_ARRAY, [1]], param.term]];
    }
    return param.term;
  }
  if (Array.isArray(param)) {
    const arrTerm = [
      TermType.MAKE_ARRAY,
      param.map((p) => parseParam(p, nestingLevel - 1)),
    ];
    if (hasImplicitVar(arrTerm)) {
      return [TermType.FUNC, [[TermType.MAKE_ARRAY, [1]], arrTerm]];
    }
    return arrTerm;
  }
  if (isDate(param)) {
    return {
      $reql_type$: 'TIME',
      epoch_time: param.getTime() / 1000,
      timezone: '+00:00',
    };
  }
  if (Buffer.isBuffer(param)) {
    return { $reql_type$: 'BINARY', data: param.toString('base64') };
  }
  if (isFunction(param)) {
    const { nextVarId } = globals;
    globals.nextVarId = nextVarId + param.length;
    try {
      const funcResult = param(
        ...Array.from({ length: param.length }, (_, i) =>
          toQuery([TermType.VAR, [i + nextVarId]]),
        ),
      );
      if (funcResult === undefined) {
        throw new RethinkDBError(
          `Anonymous function returned \`undefined\`. Did you forget a \`return\`? in:\n${param.toString()}`,
          { type: RethinkDBErrorType.PARSE },
        );
      }
      return [
        TermType.FUNC,
        [
          [
            TermType.MAKE_ARRAY,
            Array.from({ length: param.length }, (_, i) => i + nextVarId),
          ],
          parseParam(funcResult),
        ],
      ];
    } finally {
      globals.nextVarId = nextVarId;
    }
  }
  if (typeof param === 'object') {
    const objTerm = Object.entries(param).reduce<Record<string, TermJson>>(
      (acc, [key, value]) => {
        acc[key] = parseParam(value, nestingLevel - 1);
        return acc;
      },
      {},
    );
    return hasImplicitVar(objTerm)
      ? [TermType.FUNC, [[TermType.MAKE_ARRAY, [1]], objTerm]]
      : objTerm;
  }
  if (
    typeof param === 'number' &&
    (Number.isNaN(param) || !Number.isFinite(param))
  ) {
    throw new RethinkDBError(`Cannot convert \`${param}\` to JSON`, {
      type: RethinkDBErrorType.PARSE,
    });
  }
  return param;
}
