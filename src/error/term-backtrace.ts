import { TermJson } from '../internal-types';
import { TermType } from '../proto/enums';
import { globals } from '../query-builder/globals';
import { hasImplicitVar } from '../query-builder/has-implicit-var';
import { rConfig, rConsts, termConfig } from '../query-builder/query-config';

function snakeToCamel(name: string) {
  return name.replace(/(_[a-z])/g, (x) => x.charAt(1).toUpperCase());
}

function nextBacktrace(i: number, backtrace?: Array<number | string>) {
  if (backtrace && backtrace[0] === i) {
    return backtrace.slice(1);
  }
}

function joinMultiArray(acc: string[], next: string[]): [string, string] {
  return acc[0]
    ? [`${acc[0]}, ${next[0]}`, `${acc[1]}  ${next[1]}`]
    : [next[0], next[1]];
}

function getMarked(
  str: string | [string, string],
  backtrace?: Array<number | string>,
): [string, string] {
  const s = Array.isArray(str) ? str[0] : str;
  const emptyMarks = Array.isArray(str) ? str[1] : ' '.repeat(str.length);
  return backtrace && backtrace.length === 0
    ? [s, '^'.repeat(s.length)]
    : [s, emptyMarks];
}

function combineMarks(
  literals: TemplateStringsArray,
  ...placeholders: Array<string | string[]>
): [string, string] {
  let result = '';
  let mark = '';

  for (let i = 0; i < placeholders.length; i++) {
    result += literals[i];
    mark += ' '.repeat(literals[i].length);
    if (!Array.isArray(placeholders[i])) {
      result += placeholders[i];
      mark += ' '.repeat(placeholders[i].length);
    } else {
      result += placeholders[i][0];
      mark += placeholders[i][1];
    }
  }

  // add the last literal
  result += literals[literals.length - 1];
  mark += ' '.repeat(literals[literals.length - 1].length);
  return [result, mark];
}

function backtraceObject(obj: any, backtrace?: Array<number | string>) {
  const [param, ...nextB]: any = backtrace || [];
  if (obj.$reql_type$ === 'BINARY') {
    return getMarked('<Buffer>', backtrace);
  }
  if (Object.keys(obj).length === 0) {
    return getMarked('{}', backtrace);
  }
  return combineMarks`{ ${Object.entries(obj)
    .map(([key, val]) => {
      const next = param === key ? nextB : undefined;
      return getMarked(
        combineMarks`${snakeToCamel(key)}: ${backtraceTerm(
          val as TermJson,
          false,
          next,
        )}`,
      );
    })
    .reduce(joinMultiArray, ['', ''])} }`;
}

export function backtraceTerm(
  term?: TermJson,
  head = true,
  backtrace?: Array<number | string>,
): [string, string] {
  const parseArg = (
    arg: TermJson,
    index: number,
    all?: any[],
    forceHead = false,
  ) =>
    backtraceTerm(
      arg,
      forceHead || (!all && index === 0),
      nextBacktrace(index, backtrace),
    );
  if (term === undefined) {
    return getMarked('');
  }
  if (!Array.isArray(term)) {
    let termStr: [string, string] = ['', ''];
    if (term === null) {
      termStr = getMarked('null');
    } else if (typeof term === 'object') {
      termStr = backtraceObject(term, backtrace);
    } else if (typeof term === 'string') {
      termStr = getMarked(`"${term}"`);
    } else {
      termStr = getMarked(term.toString());
    }
    return getMarked(
      head ? combineMarks`r.expr(${termStr})` : termStr,
      backtrace,
    );
  }
  const [type, args, optarg] = term;
  const hasArgs = !!args && !!args.length;
  switch (type) {
    case TermType.MAKE_ARRAY: {
      if (!args) {
        return getMarked('');
      }
      return getMarked(
        head
          ? combineMarks`r.expr([${args
              .map(parseArg)
              .reduce(joinMultiArray, ['', ''])}])`
          : combineMarks`[${args
              .map(parseArg)
              .reduce(joinMultiArray, ['', ''])}]`,
        backtrace,
      );
    }
    case TermType.IMPLICIT_VAR:
      return getMarked('r.row', backtrace);
    case TermType.FUNC: {
      if (!!args && Array.isArray(args) && hasImplicitVar(term)) {
        return backtraceTerm(args[1], false, nextBacktrace(1, backtrace));
      }
      const paramsBacktrace = nextBacktrace(0, backtrace);
      const params = (args as any)[0][1].map((i: number) =>
        getMarked(`var_${i}`, nextBacktrace(i, paramsBacktrace)),
      );
      if (globals.backtraceType === 'lambda') {
        return getMarked(
          combineMarks`(${params.reduce(joinMultiArray, [
            '',
            '',
          ])}) => ${backtraceTerm(
            (args as any)[1],
            false,
            nextBacktrace(1, backtrace),
          )}`,
          backtrace,
        );
      }
      return getMarked(
        combineMarks`function(${params.reduce(joinMultiArray, [
          '',
          '',
        ])}) { return ${backtraceTerm(
          (args as any)[1],
          false,
          nextBacktrace(1, backtrace),
        )} }`,
        backtrace,
      );
    }
    case TermType.VAR: {
      return getMarked(`var_${(args as any)[0]}`, backtrace);
    }
    case TermType.FUNCALL: {
      if (!args) {
        return getMarked('');
      }
      const [func, caller, ...params] = args;
      const parsedParams = params
        .map((a, i) => parseArg(a, i + 2))
        .reduce(joinMultiArray, ['', '']);
      const parsedFunc = parseArg(func, 0);
      const parsedCaller = parseArg(caller, 1, undefined, true);
      return getMarked(
        parsedParams[0]
          ? combineMarks`${parsedCaller}.do(${parsedParams}, ${parsedFunc})`
          : combineMarks`${parsedCaller}.do(${parsedFunc})`,
        backtrace,
      );
    }
    case TermType.BRACKET: {
      if (!args) {
        return getMarked('');
      }
      const [caller, ...params] = args;
      const parsedParams = [...params]
        .map((a, i) => parseArg(a, i + 1))
        .reduce(joinMultiArray, ['', '']);
      return getMarked(
        combineMarks`${parseArg(caller, 0)}(${parsedParams})`,
        backtrace,
      );
    }
    default: {
      const c = rConsts.find((co) => co[0] === type);
      if (c) {
        return getMarked(`r.${c[1]}`, backtrace);
      }
      const func = termConfig.find((conf) => conf[0] === type);
      if (!func) {
        const rfunc = rConfig.find((conf) => conf[0] === type);
        if (rfunc) {
          const rparsedParams = [...(args || [])]
            .map(parseArg)
            .reduce(joinMultiArray, ['', '']);
          return getMarked(
            optarg
              ? hasArgs
                ? combineMarks`r.${
                    rfunc[1]
                  }(${rparsedParams}, ${backtraceObject(optarg, backtrace)})`
                : combineMarks`r.${rfunc[1]}(${backtraceObject(
                    optarg,
                    backtrace,
                  )})`
              : combineMarks`r.${rfunc[1]}(${rparsedParams})`,
            backtrace,
          );
        }
        return getMarked('');
      }
      if (!args) {
        return getMarked(
          combineMarks`r.${func[1]}(${backtraceObject(optarg, backtrace)})`,
          backtrace,
        );
      }
      const [caller, ...params] = args;
      // TODO: Maybe this is better but should fix tests
      // if (
      //   typeof caller === 'object' &&
      //   (!Array.isArray(caller) || caller[0] === TermType.MAKE_ARRAY)
      // ) {
      //   const rparsedParams = [...(args || [])]
      //     .map(parseArg)
      //     .reduce(joinMultiArray, ['', '']);
      //   return getMarked(
      //     optarg
      //       ? hasArgs
      //         ? combineMarks`r.${func[1]}(${rparsedParams}, ${backtraceObject(
      //             optarg,
      //             backtrace
      //           )})`
      //         : combineMarks`r.${func[1]}(${backtraceObject(
      //             optarg,
      //             backtrace
      //           )})`
      //       : combineMarks`r.${func[1]}(${rparsedParams})`,
      //     backtrace
      //   );
      // }
      const hasParams = params.length > 0;
      const parsedParams = [...params]
        .map((a, i) => parseArg(a, i + 1))
        .reduce(joinMultiArray, ['', '']);
      const parsedCaller = parseArg(caller, 0);
      const parsedOptarg = optarg
        ? backtraceObject(optarg, backtrace)
        : undefined;
      return getMarked(
        parsedOptarg
          ? hasParams
            ? combineMarks`${parsedCaller}.${func[1]}(${parsedParams}, ${parsedOptarg})`
            : combineMarks`${parsedCaller}.${func[1]}(${parsedOptarg})`
          : combineMarks`${parsedCaller}.${func[1]}(${parsedParams})`,
        backtrace,
      );
    }
  }
}
