import { RethinkDBError } from '../error/error';
import { backtraceTerm } from '../error/term-backtrace';
import { TermJson } from '../internal-types';
import { bracket, termConfig } from './query-config';
import {
  doTermFunc,
  getCursorQueryFunc,
  runQueryFunc,
  termBuilder,
} from './term-builder';

export const querySymbol = Symbol('RethinkDBQuery');

export const isQuery = (query: any) =>
  query === Object(query) && Object.hasOwnProperty.call(query, querySymbol);

export function toQuery(term: TermJson) {
  const query: any = termBuilder(bracket, term);
  query.term = term;

  query[querySymbol] = true;

  query.toString = () => backtraceTerm(term)[0];
  query.run = runQueryFunc(term);
  query.getCursor = getCursorQueryFunc(term);
  query.do = doTermFunc(query);

  for (let i = 0; i < termConfig.length; i += 1) {
    const config = termConfig[i];
    query[config[1]] = termBuilder(config, term);
  }
  return query;
}
