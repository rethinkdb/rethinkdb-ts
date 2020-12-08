import { querySymbol } from './query';
import { DeepPartial } from '../internal-types';
import {
  DBChangeResult,
  FieldSelector,
  FilterOperatorOptions,
  Func,
  GroupResults,
  IndexChangeResult,
  JoinResult,
  MatchResults,
  MultiFieldSelector,
  RStream,
  RTable,
  RValue,
  WriteResult,
} from '../types';

export type RTerm = JSON[];
export type RBase<DoArg = any, DoRes = any> = {
  [querySymbol]: true;
  term: RTerm;
  toJSON: () => JSON;
  toString: () => string;
  do: (arg: RBase<DoArg>) => RBase<DoRes>;
};

export interface RDatum<DoArg = any, DoRes = any> extends RBase<DoArg, DoRes> {
  do<U>(
    ...args: Array<RDatum | ((arg: RDatum<DoArg>, ...args: RDatum[]) => U)>
  ): U extends RStream ? RStream : RDatum;
  <U extends string | number>(attribute: RValue<U>): U extends keyof DoArg
    ? RDatum<DoArg[U]>
    : RDatum;
  getField<U extends string | number>(
    attribute: RValue<U>,
  ): U extends keyof DoArg ? RDatum<DoArg[U]> : RDatum;
  nth(
    attribute: RValue<number>,
  ): DoArg extends Array<infer T1> ? RDatum<T1> : never;
  default<U>(value: RValue<U>): RDatum<DoArg | U>;
  hasFields(
    ...fields: MultiFieldSelector[]
  ): DoArg extends Array<infer T1> ? RDatum<DoArg> : RDatum<boolean>;
  // Works only if DoArg is an array
  append<U>(value: RValue<U>): DoArg extends U[] ? RDatum<DoArg> : never;
  prepend<U>(value: RValue<U>): DoArg extends U[] ? RDatum<DoArg> : never;
  difference<U>(value: RValue<U[]>): DoArg extends U[] ? RDatum<DoArg> : never;
  setInsert<U>(value: RValue<U>): DoArg extends U[] ? RDatum<DoArg> : never;
  setUnion<U>(value: RValue<U[]>): DoArg extends U[] ? RDatum<DoArg> : never;
  setIntersection<U>(
    value: RValue<U[]>,
  ): DoArg extends U[] ? RDatum<DoArg> : never;
  setDifference<U>(
    value: RValue<U[]>,
  ): DoArg extends U[] ? RDatum<DoArg> : never;
  insertAt<U>(
    index: RValue<number>,
    value: RValue<U>,
  ): DoArg extends U[] ? RDatum<DoArg> : never;
  changeAt<U>(
    index: RValue<number>,
    value: RValue<U>,
  ): DoArg extends U[] ? RDatum<DoArg> : never;
  spliceAt<U>(
    index: RValue<number>,
    value: RValue<U[]>,
  ): DoArg extends U[] ? RDatum<DoArg> : never;
  deleteAt<U>(
    offset: RValue<number>,
    endOffset?: RValue<number>,
  ): DoArg extends U[] ? RDatum<DoArg> : never;
  union<U = DoArg extends Array<infer T1> ? T1 : never>(
    ...other: Array<RStream<U> | RValue<U[]> | { interleave: boolean | string }>
  ): DoArg extends any[] ? RDatum<U[]> : never;
  map<Res = any, U = DoArg extends Array<infer T1> ? T1 : never>(
    ...args: Array<RStream | ((arg: RDatum<U>, ...args: RDatum[]) => any)>
  ): DoArg extends any[] ? RDatum<Res[]> : never;
  concatMap<Res = any, U = DoArg extends Array<infer T1> ? T1 : never>(
    ...args: Array<RStream | ((arg: RDatum<U>, ...args: RDatum[]) => any)>
  ): DoArg extends any[] ? RDatum<Res[]> : never;
  forEach<
    U = any,
    ONE = DoArg extends Array<infer T1> ? T1 : never,
    RES extends
      | RDatum<WriteResult<U>>
      | RDatum<DBChangeResult>
      | RDatum<IndexChangeResult> = RDatum<WriteResult<U>>
  >(
    func: (res: RDatum<ONE>) => RES,
  ): DoArg extends any[] ? RES : never;

  withFields(
    ...fields: MultiFieldSelector[]
  ): DoArg extends Array<infer T1> ? RDatum<Array<Partial<T1>>> : never;
  filter<U = DoArg extends Array<infer T1> ? T1 : never>(
    predicate: DeepPartial<U> | ((doc: RDatum<U>) => RValue),
    options?: FilterOperatorOptions,
  ): this;
  includes(
    geometry: RDatum,
  ): DoArg extends Array<infer T1> ? RDatum<DoArg> : never;
  intersects(
    geometry: RDatum,
  ): DoArg extends Array<infer T1> ? RDatum<DoArg> : never;

  // LOGIC
  contains<U = DoArg extends Array<infer T1> ? T1 : never>(
    val1: any[] | null | string | number | Record<string, unknown> | Func<U>,
    ...value: Array<
      any[] | null | string | number | Record<string, unknown> | Func<U>
    >
  ): DoArg extends Array<infer T1> ? RDatum<boolean> : never; // also predicate

  // ORDER BY
  orderBy<U = DoArg extends Array<infer T1> ? T1 : never>(
    ...fields: Array<FieldSelector<DoArg>>
  ): DoArg extends Array<infer T1> ? RDatum<DoArg> : never;

  // GROUP
  group<
    F extends DoArg extends Array<infer T1> ? keyof T1 : never,
    D extends DoArg extends Array<infer T2> ? T2 : never
  >(
    ...fieldOrFunc: Array<FieldSelector<DoArg>>
  ): DoArg extends Array<infer T1> ? RDatum : never; // <GroupResults<DoArg[U], DoArg[]>>;

  ungroup(): RDatum<Array<GroupResults>>;

  // SELECT FUNCTIONS
  count<U = DoArg extends Array<infer T1> ? T1 : never>(
    value?: RValue<U> | Func<U, boolean>,
  ): DoArg extends Array<infer T1> ? RDatum<number> : never;
  sum<U = DoArg extends Array<infer T1> ? T1 : never>(
    value?: FieldSelector<U, number | null>,
  ): DoArg extends Array<infer T1> ? RDatum<number> : never;
  avg<U = DoArg extends Array<infer T1> ? T1 : never>(
    value?: FieldSelector<U, number | null>,
  ): DoArg extends Array<infer T1> ? RDatum<number> : never;
  min<U = DoArg extends Array<infer T1> ? T1 : never>(
    value?: FieldSelector<U, number | null>,
  ): DoArg extends Array<infer T1> ? RDatum<number> : never;
  max<U = DoArg extends Array<infer T1> ? T1 : never>(
    value?: FieldSelector<U, number | null>,
  ): DoArg extends Array<infer T1> ? RDatum<number> : never;
  reduce<U = any, ONE = DoArg extends Array<infer T1> ? T1 : never>(
    reduceFunction: (left: RDatum<ONE>, right: RDatum<ONE>) => any,
  ): DoArg extends Array<infer T1> ? RDatum<U> : never;
  fold<ACC = any, RES = any, ONE = DoArg extends Array<infer T1> ? T1 : never>(
    base: any,
    foldFunction: (acc: RDatum<ACC>, next: RDatum<ONE>) => any, // this any is ACC
    options?: {
      emit?: (
        acc: RDatum<ACC>,
        next: RDatum<ONE>,
        // tslint:disable-next-line:variable-name
        new_acc: RDatum<ACC>,
      ) => any[]; // this any is RES
      finalEmit?: (acc: RStream) => any[]; // this any is also RES
    },
  ): DoArg extends Array<infer T1> ? RDatum<RES[]> : never;
  // SELECT
  distinct(): RDatum<DoArg>;

  pluck(
    ...fields: MultiFieldSelector[]
  ): RDatum<Partial<DoArg>[] | Partial<DoArg>>;

  without(
    ...fields: MultiFieldSelector[]
  ): DoArg extends Array<infer T1>
    ? RDatum<Array<Partial<T1>>>
    : RDatum<Partial<DoArg>>;

  merge<U = any>(
    ...objects: Array<
      Record<string, unknown> | RDatum | ((arg: RDatum<DoArg>) => any)
    >
  ): RDatum<U>;

  innerJoin<U, T2 = DoArg extends Array<infer T1> ? T1 : never>(
    other: RStream<U> | RValue<U[]>,
    predicate: (doc1: RDatum<T2>, doc2: RDatum<U>) => RValue<boolean>,
  ): RDatum<Array<JoinResult<T2, U>>>;
  outerJoin<U, T2 = DoArg extends Array<infer T1> ? T1 : never>(
    other: RStream<U> | RValue<U[]>,
    predicate: (doc1: RDatum<T2>, doc2: RDatum<U>) => RValue<boolean>,
  ): RDatum<Array<JoinResult<T2, U>>>; // actually left join
  eqJoin<U, T2 = DoArg extends Array<infer T1> ? T1 : never>(
    fieldOrPredicate: RValue<keyof T2> | Func<T2, boolean>,
    rightTable: RTable<U>,
    options?: { index: string },
  ): RStream<JoinResult<T2, U>>;
  skip(
    n: RValue<number>,
  ): DoArg extends Array<infer T1> ? RDatum<DoArg> : never;
  limit(
    n: RValue<number>,
  ): DoArg extends Array<infer T1> ? RDatum<DoArg> : never;
  slice(
    start: RValue<number>,
    end?: RValue<number>,
    options?: { leftBound?: 'open' | 'closed'; rightBound?: 'open' | 'closed' },
  ): DoArg extends Array<infer T1> ? RDatum<DoArg> : never;
  slice(
    start: RValue<number>,
    options?: { leftBound?: 'open' | 'closed'; rightBound?: 'open' | 'closed' },
  ): DoArg extends Array<infer T1> ? RDatum<DoArg> : never;
  sample(
    n: RValue<number>,
  ): DoArg extends Array<infer T1> ? RDatum<DoArg> : never;
  offsetsOf<U = DoArg extends Array<infer T1> ? T1 : never>(
    single: RValue<U> | Func<U, boolean>,
  ): DoArg extends Array<infer T1> ? RDatum<number[]> : never;

  isEmpty(): DoArg extends Array<infer T1> ? RDatum<boolean> : never;

  coerceTo<U = any>(
    type: 'object' | 'OBJECT',
  ): DoArg extends Array<infer T1> ? RDatum<U> : never;
  coerceTo(type: 'string' | 'STRING'): RDatum<string>;
  coerceTo(type: 'array' | 'ARRAY'): RDatum<any[]>;
  // Works only if DoArg is a string
  coerceTo(
    type: 'number' | 'NUMBER',
  ): DoArg extends string ? RDatum<number> : never;
  coerceTo(
    type: 'binary' | 'BINARY',
  ): DoArg extends string ? RDatum<Buffer> : never;
  match(
    regexp: RValue<string>,
  ): DoArg extends string ? RDatum<MatchResults | null> : never;
  split(
    seperator?: RValue<string>,
    maxSplits?: RValue<number>,
  ): DoArg extends string ? RDatum<string[]> : never;
  upcase(): DoArg extends string ? RDatum<string> : never;
  downcase(): DoArg extends string ? RDatum<string> : never;
  add(
    ...str: Array<RValue<string> | RValue<number>>
  ): DoArg extends string | number | Date ? RDatum<DoArg> : never;
  gt(
    ...value: Array<RValue<string> | RValue<number> | RValue<Date>>
  ): DoArg extends string | number | Date ? RDatum<boolean> : never;
  ge(
    ...value: Array<RValue<string> | RValue<number> | RValue<Date>>
  ): DoArg extends string | number | Date ? RDatum<boolean> : never;
  lt(
    ...value: Array<RValue<string> | RValue<number> | RValue<Date>>
  ): DoArg extends string | number | Date ? RDatum<boolean> : never;
  le(
    ...value: Array<RValue<string> | RValue<number> | RValue<Date>>
  ): DoArg extends string | number | Date ? RDatum<boolean> : never;
  // Works only for numbers
  sub(
    ...num: Array<RValue<number>>
  ): DoArg extends number
    ? RDatum<number>
    : DoArg extends Date
    ? RDatum<Date>
    : never;
  sub(date: RValue<Date>): DoArg extends Date ? RDatum<number> : never;
  mul(
    ...num: Array<RValue<number>>
  ): DoArg extends number ? RDatum<number> : never;
  div(
    ...num: Array<RValue<number>>
  ): DoArg extends number ? RDatum<number> : never;
  mod(
    ...num: Array<RValue<number>>
  ): DoArg extends number ? RDatum<number> : never;

  bitAnd(
    ...num: Array<RValue<number>>
  ): DoArg extends number ? RDatum<number> : never;
  bitOr(
    ...num: Array<RValue<number>>
  ): DoArg extends number ? RDatum<number> : never;
  bitXor(
    ...num: Array<RValue<number>>
  ): DoArg extends number ? RDatum<number> : never;
  bitNot(
    ...num: Array<RValue<number>>
  ): DoArg extends number ? RDatum<number> : never;
  bitSal(
    ...num: Array<RValue<number>>
  ): DoArg extends number ? RDatum<number> : never;
  bitShl(
    ...num: Array<RValue<number>>
  ): DoArg extends number ? RDatum<number> : never;
  bitSar(
    ...num: Array<RValue<number>>
  ): DoArg extends number ? RDatum<number> : never;
  bitSht(
    ...num: Array<RValue<number>>
  ): DoArg extends number ? RDatum<number> : never;

  round(): DoArg extends number ? RDatum<number> : never;
  ceil(): DoArg extends number ? RDatum<number> : never;
  floor(): DoArg extends number ? RDatum<number> : never;
  // Works only for bool
  branch(
    trueBranch: any,
    falseBranchOrTest: any,
    ...branches: any[]
  ): DoArg extends boolean ? RDatum : never;
  and(
    ...bool: Array<RValue<boolean>>
  ): DoArg extends boolean ? RDatum<boolean> : never;
  or(
    ...bool: Array<RValue<boolean>>
  ): DoArg extends boolean ? RDatum<boolean> : never;
  not(): DoArg extends boolean ? RDatum<boolean> : never;
  // Works only for Date
  inTimezone(timezone: string): DoArg extends Date ? RDatum<Date> : never;
  timezone(): DoArg extends Date ? RDatum<string> : never;
  during(
    start: RValue<Date>,
    end: RValue<Date>,
    options?: { leftBound: 'open' | 'closed'; rightBound: 'open' | 'closed' },
  ): DoArg extends Date ? RDatum<boolean> : never;
  date(): DoArg extends Date ? RDatum<Date> : never;
  timeOfDay(): DoArg extends Date ? RDatum<number> : never;
  year(): DoArg extends Date ? RDatum<number> : never;
  month(): DoArg extends Date ? RDatum<number> : never;
  day(): DoArg extends Date ? RDatum<number> : never;
  dayOfWeek(): DoArg extends Date ? RDatum<number> : never;
  dayOfYear(): DoArg extends Date ? RDatum<number> : never;
  hours(): DoArg extends Date ? RDatum<number> : never;
  minutes(): DoArg extends Date ? RDatum<number> : never;
  seconds(): DoArg extends Date ? RDatum<number> : never;
  toISO8601(): DoArg extends Date ? RDatum<string> : never;
  toEpochTime(): DoArg extends Date ? RDatum<number> : never;
  // Works only for geo
  distance(
    geo: RValue,
    options?: { geoSystem?: string; unit?: string },
  ): RDatum<number>;
  toGeojson(): RDatum;
  // Works only for line
  fill(): RDatum;
  polygonSub(polygon2: RValue): RDatum;

  toJsonString(): RDatum<string>;
  toJSON(): RDatum<string>;

  eq(...value: RValue[]): RDatum<boolean>;
  ne(...value: RValue[]): RDatum<boolean>;

  keys(): RDatum<string[]>;
  values(): RDatum<Array<DoArg[keyof DoArg]>>;
}
