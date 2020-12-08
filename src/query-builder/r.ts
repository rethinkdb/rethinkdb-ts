import { R, RQuery } from '../types';
import { globals } from './globals';
import { funcall, rConfig, rConsts, termConfig } from './query-config';
import { isQuery, termBuilder, parseParam, toQuery } from './query';
import { validateTerm } from './validate-term';

const expr = (arg: unknown, nestingLevel: number = globals.nestingLevel) => {
  if (isQuery(arg)) {
    return arg;
  }
  return toQuery(parseParam(arg, nestingLevel));
};

export const r: R = expr as any;
r.setNestingLevel = (level: number) => {
  globals.nestingLevel = level;
};
r.setArrayLimit = (limit?: number) => {
  globals.arrayLimit = limit;
};
// @ts-ignore
r.serialize = (termStr: RQuery) => JSON.stringify(termStr.term);
// @ts-ignore
r.deserialize = (termStr: string) => toQuery(validateTerm(JSON.parse(termStr)));
// @ts-ignore
r.expr = expr;
// @ts-ignore
r.do = (...args: any[]) => {
  const last = args.pop();
  return termBuilder(funcall)(last, ...args);
};
rConfig.forEach((config) => ((r as any)[config[1]] = termBuilder(config)));
rConsts.forEach(([type, name]) => ((r as any)[name] = toQuery([type])));
termConfig
  .filter(([_, name]) => !(name in r))
  .forEach(
    ([type, name, minArgs, maxArgs, optArgs]) =>
      ((r as any)[name] = termBuilder([
        type,
        name,
        minArgs + 1,
        maxArgs === -1 ? maxArgs : maxArgs + 1,
        optArgs,
      ])),
  );
