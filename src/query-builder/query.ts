import type { ComplexTermJson, RDatum, TermJson } from '../types';
import { RethinkDBError, RethinkDBErrorType } from '../error';
import { TermType } from '../proto/enums';
import { backtraceTerm } from '../error/term-backtrace';
import { camelToSnake, isDate, isFunction, isObject } from '../util';
import { globals } from './globals';
import { hasImplicitVar } from './has-implicit-var';
import { bracket, funcall, TermConfig, termConfig } from './query-config';

const querySymbol = Symbol('RethinkDBQuery');

export interface RQuery<T = unknown> {
  (...args: unknown[]): RQuery<T>;
  term: TermJson;
  [querySymbol]: true;
  do: () => RQuery;
  typeOf(): RDatum<string>;
  info(): RDatum<{
    value?: string;
    db?: { id: string; name: string; type: string };
    // eslint-disable-next-line camelcase
    doc_count_estimates?: number[];
    id?: string;
    indexes?: string[];
    name?: string;
    // eslint-disable-next-line camelcase
    primary_key?: string;
    type: string;
  }>;
}

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
      throw new RethinkDBError('"r" cannot be an argument', {
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
    // @ts-ignore
    if (hasImplicitVar(arrTerm)) {
      // @ts-ignore
      return [TermType.FUNC, [[TermType.MAKE_ARRAY, [1]], arrTerm]];
    }
    return arrTerm as TermJson;
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
          // eslint-disable-next-line no-use-before-define
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
    // @ts-ignore
    const objTerm = Object.entries(param).reduce<Record<string, TermJson>>(
      (acc, [key, value]) => {
        // @ts-ignore
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
  // @ts-ignore
  return param;
}

const numToStringArr = ['', 'First', 'Second', 'Third', 'Fourth', 'Fifth'];
function numToString(num: number) {
  return numToStringArr.map((_, i) => i).includes(num)
    ? numToStringArr[num]
    : num.toString();
}

export function parseOptarg(
  obj: Record<string, unknown>,
): Record<string, TermJson> | undefined {
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

export function termBuilder(
  [termType, termName, minArgs, maxArgs, optargType]: TermConfig,
  currentTerm?: TermJson,
): (...args: any[]) => RQuery {
  return (...args: any[]): RQuery => {
    let optarg: Record<string, TermJson>;
    const params: TermJson[] = currentTerm !== undefined ? [currentTerm] : [];
    // @ts-ignore
    if (
      isQuery(args[0]) &&
      args[0].term &&
      Array.isArray(args[0].term) &&
      args[0].term[0] === TermType.ARGS
    ) {
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
        case 'last': {
          const parsedOptArg = parseOptarg(args[maxArgs - 1]);
          if (parsedOptArg) {
            optarg = parsedOptArg;
          }
          break;
        }
        case 'required':
        case 'optional':
        case 'last-optional': {
          const parsedOptArg = parseOptarg(args[argsLength - 1]);
          if (parsedOptArg) {
            optarg = parsedOptArg;
          }
          break;
        }
        default:
          break;
      }
      if (
        // @ts-ignore TODO why?
        !optarg &&
        (optargType === 'required' ||
          (argsLength === maxArgs &&
            typeof optargType === 'string' &&
            ['last', 'last-optional'].includes(optargType)))
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
    // @ts-ignore TODO why?
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
    // @ts-ignore TODO rewrite both termBuilder and toQuery to
    //  different RTable, RDatabase, etc implementations with strict methods
    query[config[1]] = termBuilder(config, term);
  }
  return query;
}
