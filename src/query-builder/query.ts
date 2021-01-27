import { TermJson, ComplexTermJson } from '../internal-types';
import { TermConfig } from './query-config';
import { RQuery, RethinkDBErrorType } from '../types';
import { RethinkDBError } from '../error/error';
import { TermType } from '../proto/enums';
import { parseParam } from './param-parser';
import { parseOptarg } from './parse-opt-arg';

export const querySymbol = Symbol('RethinkDBQuery');

export type RunnableRQuery = RQuery & {
  term: TermJson;
  [querySymbol]: true;
  run: () => any;
  getCursor: () => any;
  do: () => any;
};

export const isQuery = (query: unknown): query is RunnableRQuery =>
  query === Object(query) && Object.hasOwnProperty.call(query, querySymbol);

const numToStringArr = ['', 'First', 'Second', 'Third', 'Fourth', 'Fifth'];
function numToString(num: number) {
  return numToStringArr.map((_, i) => i).includes(num)
    ? numToStringArr[num]
    : num.toString();
}

export function termBuilder(
  [termType, termName, minArgs, maxArgs, optargType]: TermConfig,
  toQuery: (term: TermJson) => RunnableRQuery,
  currentTerm?: TermJson,
) {
  return (...args: any[]) => {
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
    return toQuery(term);
  };
}
