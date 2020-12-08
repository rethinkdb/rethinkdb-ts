import { TermJson, ComplexTermJson } from '../internal-types';
import { bracket, funcall, TermConfig, termConfig } from './query-config';
import { RQuery, RethinkDBErrorType } from '../types';
import { RethinkDBError } from '../error/error';
import { TermType } from '../proto/enums';
import { backtraceTerm } from '../error/term-backtrace';
import { globals } from './globals';
import { camelToSnake, isDate, isFunction, isObject } from '../util';
import { hasImplicitVar } from './has-implicit-var';

export const querySymbol = Symbol('RethinkDBQuery');

export const isQuery = (query: unknown): query is RQuery =>
  query === Object(query) && Object.hasOwnProperty.call(query, querySymbol);

export function parseParam(
  param: unknown,
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

const numToStringArr = ['', 'First', 'Second', 'Third', 'Fourth', 'Fifth'];
function numToString(num: number) {
  return numToStringArr.map((_, i) => i).includes(num)
    ? numToStringArr[num]
    : num.toString();
}

export function termBuilder(
  [termType, termName, minArgs, maxArgs, optargType]: TermConfig,
  currentTerm?: TermJson,
): (...args: any[]) => RQuery {
  return (...args: any[]): RQuery => {
    let optarg: Record<string, unknown> | undefined;
    const params: TermJson[] = currentTerm !== undefined ? [currentTerm] : [];
    // @ts-ignore
    if (isQuery(args[0]) && args[0].term[0] === TermType.ARGS) {
      params.push(parseParam(args[0]));
      optarg = optargType !== false ? args[1] : undefined;
    } else {
      const argsLength = args.length;
      if (minArgs === maxArgs && argsLength !== minArgs) {
        throw new RethinkDBError(
          `\`${
            !currentTerm ? `r.${termName}` : termName
          }\` takes ${minArgs} argument${
            minArgs === 1 ? '' : 's'
          }, ${argsLength} provided${!currentTerm ? '.' : ' after:'}`,
          { term: currentTerm, type: RethinkDBErrorType.ARITY },
        );
      }
      if (argsLength < minArgs) {
        throw new RethinkDBError(
          `\`${
            !currentTerm ? `r.${termName}` : termName
          }\` takes at least ${minArgs} argument${
            minArgs === 1 ? '' : 's'
          }, ${argsLength} provided${!currentTerm ? '.' : ' after:'}`,
          { term: currentTerm, type: RethinkDBErrorType.ARITY },
        );
      }
      if (maxArgs !== -1 && argsLength > maxArgs) {
        throw new RethinkDBError(
          `\`${
            !currentTerm ? `r.${termName}` : termName
          }\` takes at most ${maxArgs} argument${
            maxArgs === 1 ? '' : 's'
          }, ${argsLength} provided${!currentTerm ? '.' : ' after:'}`,
          { term: currentTerm, type: RethinkDBErrorType.ARITY },
        );
      }
      switch (optargType) {
        case 'last':
          // @ts-ignore
          optarg = parseOptarg(args[maxArgs - 1]);
          break;
        case 'required':
        case 'optional':
        case 'last-optional':
          // @ts-ignore
          optarg = parseOptarg(args[argsLength - 1]);
      }
      if (
        !optarg &&
        (optargType === 'required' ||
          (argsLength === maxArgs &&
            ['last', 'last-optional'].includes(optargType as any)))
      ) {
        throw new RethinkDBError(
          `${numToString(
            argsLength,
          )} argument of \`${termName}\` must be an object.`,
          { term: currentTerm, type: RethinkDBErrorType.ARITY },
        );
      }
      params.push(
        ...args
          .filter((_, i) => (optarg ? i < argsLength - 1 : true))
          .map((x) => parseParam(x)),
      );
    }
    const term: ComplexTermJson = [termType];
    if (params.length > 0) {
      term[1] = params;
    }
    if (optarg) {
      term[2] = optarg;
    }
    const query: any = termBuilder(bracket, term);
    query.term = term;

    query[querySymbol] = true;

    query.toString = () => backtraceTerm(term)[0];
    query.do = (...doArgs: any[]) => {
      const last = doArgs.pop();
      const tb = termBuilder(funcall);
      return last ? tb(last, query, ...doArgs) : tb(query);
    };

    for (let i = 0; i < termConfig.length; i += 1) {
      const config = termConfig[i];
      query[config[1]] = termBuilder(config, term);
    }
    return query;
  };
}

export function toQuery(term: TermJson): RQuery {
  const query = termBuilder(bracket, term) as RQuery;
  query.term = term;

  query[querySymbol] = true;

  query.toString = () => backtraceTerm(term)[0];
  query.do = (...args: any[]) => {
    const last = args.pop();
    const tb = termBuilder(funcall);
    return last ? tb(last, query, ...args) : tb(query);
  };

  for (let i = 0; i < termConfig.length; i += 1) {
    const config = termConfig[i];
    query[config[1]] = termBuilder(config, term);
  }
  return query;
}

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
